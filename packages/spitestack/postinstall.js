#!/usr/bin/env node
/**
 * Spitestack CLI Postinstall Script
 *
 * Downloads the correct binary for the current platform from GitHub Releases.
 */

// Debug: confirm script is running
console.error("[spitestack] postinstall starting...");

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

function formatBytes(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / (1024 * 1024)).toFixed(1) + " MB";
}

let lastPrintedPercent = -1;

function printProgress(downloaded, total) {
  const percent = total ? Math.round((downloaded / total) * 100) : 0;

  // Only print at 0%, 25%, 50%, 75%, 100% to avoid spam
  const milestones = [0, 25, 50, 75, 100];
  const nearestMilestone = milestones.find(m => percent <= m) || 100;

  if (percent >= nearestMilestone && lastPrintedPercent < nearestMilestone) {
    lastPrintedPercent = nearestMilestone;
    console.error(`  ${percent}% (${formatBytes(downloaded)})`);
  }
}

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
  const destName = isWindows ? "spitestack-bin.exe" : "spitestack-bin";
  const dest = path.join(__dirname, "bin", destName);

  // Ensure bin directory exists
  const binDir = path.dirname(dest);
  if (!fs.existsSync(binDir)) {
    fs.mkdirSync(binDir, { recursive: true });
  }

  // Use stderr - npm suppresses stdout during postinstall
  console.error(`\n  Downloading spitestack v${VERSION} for ${key}...`);

  try {
    await download(url, dest);

    // Make executable on Unix
    if (!isWindows) {
      fs.chmodSync(dest, 0o755);
    }

    console.error("\n  Done!\n");
  } catch (error) {
    console.error(`\n\n  Failed to download: ${error.message}`);
    console.error(`  URL: ${url}`);
    console.error("\n  Manual download: https://github.com/spitestack/spitestack/releases\n");
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

          const totalSize = parseInt(res.headers["content-length"], 10) || 0;
          let downloaded = 0;

          const file = fs.createWriteStream(dest);

          res.on("data", (chunk) => {
            downloaded += chunk.length;
            printProgress(downloaded, totalSize);
          });

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
