#!/usr/bin/env node
/**
 * Spitestack CLI Postinstall Script
 *
 * Downloads the correct binary for the current platform from GitHub Releases.
 */

const https = require("https");
const fs = require("fs");
const path = require("path");

const VERSION = require("./package.json").version;
const REPO = "spitestack/spitestack";

const PLATFORM_MAP = {
  "darwin-arm64": "spite-darwin-arm64",
  "darwin-x64": "spite-darwin-x64",
  "linux-x64": "spite-linux-x64",
  "linux-arm64": "spite-linux-arm64",
  "win32-x64": "spite-windows-x64.exe",
};

async function main() {
  const key = `${process.platform}-${process.arch}`;
  const binary = PLATFORM_MAP[key];

  if (!binary) {
    console.error(`\nspitestack: Unsupported platform: ${key}`);
    console.error("Supported platforms: darwin-arm64, darwin-x64, linux-x64, linux-arm64, win32-x64");
    console.error("\nYou can build from source: https://github.com/spitestack/spitestack\n");
    process.exit(1);
  }

  const url = `https://github.com/${REPO}/releases/download/v${VERSION}/${binary}`;
  const isWindows = process.platform === "win32";
  const destName = isWindows ? "spite.exe" : "spite-bin";
  const dest = path.join(__dirname, "bin", destName);

  // Ensure bin directory exists
  const binDir = path.dirname(dest);
  if (!fs.existsSync(binDir)) {
    fs.mkdirSync(binDir, { recursive: true });
  }

  console.log(`\nDownloading spite v${VERSION} for ${key}...`);

  try {
    await download(url, dest);

    // Make executable on Unix
    if (!isWindows) {
      fs.chmodSync(dest, 0o755);
    }

    console.log("Done!\n");
  } catch (error) {
    console.error(`\nFailed to download spite binary: ${error.message}`);
    console.error(`URL: ${url}`);
    console.error("\nYou can manually download from: https://github.com/spitestack/spitestack/releases\n");
    process.exit(1);
  }
}

function download(url, dest) {
  return new Promise((resolve, reject) => {
    const request = (currentUrl) => {
      https
        .get(currentUrl, { headers: { "User-Agent": "spitestack-installer" } }, (res) => {
          // Handle redirects (GitHub releases redirect to CDN)
          if (res.statusCode === 301 || res.statusCode === 302) {
            const redirectUrl = res.headers.location;
            if (!redirectUrl) {
              reject(new Error("Redirect without location header"));
              return;
            }
            request(redirectUrl);
            return;
          }

          if (res.statusCode !== 200) {
            reject(new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`));
            return;
          }

          const file = fs.createWriteStream(dest);
          res.pipe(file);

          file.on("finish", () => {
            file.close();
            resolve();
          });

          file.on("error", (err) => {
            fs.unlink(dest, () => {}); // Clean up partial file
            reject(err);
          });
        })
        .on("error", reject);
    };

    request(url);
  });
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
