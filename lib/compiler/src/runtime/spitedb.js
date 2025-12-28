// @bun
var __commonJS = (cb, mod) => () => (mod || cb((mod = { exports: {} }).exports, mod), mod.exports);
var __require = import.meta.require;

// ../../node_modules/.bun/detect-libc@2.1.2/node_modules/detect-libc/lib/process.js
var require_process = __commonJS((exports, module) => {
  var isLinux = () => process.platform === "linux";
  var report = null;
  var getReport = () => {
    if (!report) {
      if (isLinux() && process.report) {
        const orig = process.report.excludeNetwork;
        process.report.excludeNetwork = true;
        report = process.report.getReport();
        process.report.excludeNetwork = orig;
      } else {
        report = {};
      }
    }
    return report;
  };
  module.exports = { isLinux, getReport };
});

// ../../node_modules/.bun/detect-libc@2.1.2/node_modules/detect-libc/lib/filesystem.js
var require_filesystem = __commonJS((exports, module) => {
  var fs = __require("fs");
  var LDD_PATH = "/usr/bin/ldd";
  var SELF_PATH = "/proc/self/exe";
  var MAX_LENGTH = 2048;
  var readFileSync = (path) => {
    const fd = fs.openSync(path, "r");
    const buffer = Buffer.alloc(MAX_LENGTH);
    const bytesRead = fs.readSync(fd, buffer, 0, MAX_LENGTH, 0);
    fs.close(fd, () => {});
    return buffer.subarray(0, bytesRead);
  };
  var readFile = (path) => new Promise((resolve, reject) => {
    fs.open(path, "r", (err, fd) => {
      if (err) {
        reject(err);
      } else {
        const buffer = Buffer.alloc(MAX_LENGTH);
        fs.read(fd, buffer, 0, MAX_LENGTH, 0, (_, bytesRead) => {
          resolve(buffer.subarray(0, bytesRead));
          fs.close(fd, () => {});
        });
      }
    });
  });
  module.exports = {
    LDD_PATH,
    SELF_PATH,
    readFileSync,
    readFile
  };
});

// ../../node_modules/.bun/detect-libc@2.1.2/node_modules/detect-libc/lib/elf.js
var require_elf = __commonJS((exports, module) => {
  var interpreterPath = (elf) => {
    if (elf.length < 64) {
      return null;
    }
    if (elf.readUInt32BE(0) !== 2135247942) {
      return null;
    }
    if (elf.readUInt8(4) !== 2) {
      return null;
    }
    if (elf.readUInt8(5) !== 1) {
      return null;
    }
    const offset = elf.readUInt32LE(32);
    const size = elf.readUInt16LE(54);
    const count = elf.readUInt16LE(56);
    for (let i = 0;i < count; i++) {
      const headerOffset = offset + i * size;
      const type = elf.readUInt32LE(headerOffset);
      if (type === 3) {
        const fileOffset = elf.readUInt32LE(headerOffset + 8);
        const fileSize = elf.readUInt32LE(headerOffset + 32);
        return elf.subarray(fileOffset, fileOffset + fileSize).toString().replace(/\0.*$/g, "");
      }
    }
    return null;
  };
  module.exports = {
    interpreterPath
  };
});

// ../../node_modules/.bun/detect-libc@2.1.2/node_modules/detect-libc/lib/detect-libc.js
var require_detect_libc = __commonJS((exports, module) => {
  var childProcess = __require("child_process");
  var { isLinux, getReport } = require_process();
  var { LDD_PATH, SELF_PATH, readFile, readFileSync } = require_filesystem();
  var { interpreterPath } = require_elf();
  var cachedFamilyInterpreter;
  var cachedFamilyFilesystem;
  var cachedVersionFilesystem;
  var command = "getconf GNU_LIBC_VERSION 2>&1 || true; ldd --version 2>&1 || true";
  var commandOut = "";
  var safeCommand = () => {
    if (!commandOut) {
      return new Promise((resolve) => {
        childProcess.exec(command, (err, out) => {
          commandOut = err ? " " : out;
          resolve(commandOut);
        });
      });
    }
    return commandOut;
  };
  var safeCommandSync = () => {
    if (!commandOut) {
      try {
        commandOut = childProcess.execSync(command, { encoding: "utf8" });
      } catch (_err) {
        commandOut = " ";
      }
    }
    return commandOut;
  };
  var GLIBC = "glibc";
  var RE_GLIBC_VERSION = /LIBC[a-z0-9 \-).]*?(\d+\.\d+)/i;
  var MUSL = "musl";
  var isFileMusl = (f) => f.includes("libc.musl-") || f.includes("ld-musl-");
  var familyFromReport = () => {
    const report = getReport();
    if (report.header && report.header.glibcVersionRuntime) {
      return GLIBC;
    }
    if (Array.isArray(report.sharedObjects)) {
      if (report.sharedObjects.some(isFileMusl)) {
        return MUSL;
      }
    }
    return null;
  };
  var familyFromCommand = (out) => {
    const [getconf, ldd1] = out.split(/[\r\n]+/);
    if (getconf && getconf.includes(GLIBC)) {
      return GLIBC;
    }
    if (ldd1 && ldd1.includes(MUSL)) {
      return MUSL;
    }
    return null;
  };
  var familyFromInterpreterPath = (path) => {
    if (path) {
      if (path.includes("/ld-musl-")) {
        return MUSL;
      } else if (path.includes("/ld-linux-")) {
        return GLIBC;
      }
    }
    return null;
  };
  var getFamilyFromLddContent = (content) => {
    content = content.toString();
    if (content.includes("musl")) {
      return MUSL;
    }
    if (content.includes("GNU C Library")) {
      return GLIBC;
    }
    return null;
  };
  var familyFromFilesystem = async () => {
    if (cachedFamilyFilesystem !== undefined) {
      return cachedFamilyFilesystem;
    }
    cachedFamilyFilesystem = null;
    try {
      const lddContent = await readFile(LDD_PATH);
      cachedFamilyFilesystem = getFamilyFromLddContent(lddContent);
    } catch (e) {}
    return cachedFamilyFilesystem;
  };
  var familyFromFilesystemSync = () => {
    if (cachedFamilyFilesystem !== undefined) {
      return cachedFamilyFilesystem;
    }
    cachedFamilyFilesystem = null;
    try {
      const lddContent = readFileSync(LDD_PATH);
      cachedFamilyFilesystem = getFamilyFromLddContent(lddContent);
    } catch (e) {}
    return cachedFamilyFilesystem;
  };
  var familyFromInterpreter = async () => {
    if (cachedFamilyInterpreter !== undefined) {
      return cachedFamilyInterpreter;
    }
    cachedFamilyInterpreter = null;
    try {
      const selfContent = await readFile(SELF_PATH);
      const path = interpreterPath(selfContent);
      cachedFamilyInterpreter = familyFromInterpreterPath(path);
    } catch (e) {}
    return cachedFamilyInterpreter;
  };
  var familyFromInterpreterSync = () => {
    if (cachedFamilyInterpreter !== undefined) {
      return cachedFamilyInterpreter;
    }
    cachedFamilyInterpreter = null;
    try {
      const selfContent = readFileSync(SELF_PATH);
      const path = interpreterPath(selfContent);
      cachedFamilyInterpreter = familyFromInterpreterPath(path);
    } catch (e) {}
    return cachedFamilyInterpreter;
  };
  var family = async () => {
    let family2 = null;
    if (isLinux()) {
      family2 = await familyFromInterpreter();
      if (!family2) {
        family2 = await familyFromFilesystem();
        if (!family2) {
          family2 = familyFromReport();
        }
        if (!family2) {
          const out = await safeCommand();
          family2 = familyFromCommand(out);
        }
      }
    }
    return family2;
  };
  var familySync = () => {
    let family2 = null;
    if (isLinux()) {
      family2 = familyFromInterpreterSync();
      if (!family2) {
        family2 = familyFromFilesystemSync();
        if (!family2) {
          family2 = familyFromReport();
        }
        if (!family2) {
          const out = safeCommandSync();
          family2 = familyFromCommand(out);
        }
      }
    }
    return family2;
  };
  var isNonGlibcLinux = async () => isLinux() && await family() !== GLIBC;
  var isNonGlibcLinuxSync = () => isLinux() && familySync() !== GLIBC;
  var versionFromFilesystem = async () => {
    if (cachedVersionFilesystem !== undefined) {
      return cachedVersionFilesystem;
    }
    cachedVersionFilesystem = null;
    try {
      const lddContent = await readFile(LDD_PATH);
      const versionMatch = lddContent.match(RE_GLIBC_VERSION);
      if (versionMatch) {
        cachedVersionFilesystem = versionMatch[1];
      }
    } catch (e) {}
    return cachedVersionFilesystem;
  };
  var versionFromFilesystemSync = () => {
    if (cachedVersionFilesystem !== undefined) {
      return cachedVersionFilesystem;
    }
    cachedVersionFilesystem = null;
    try {
      const lddContent = readFileSync(LDD_PATH);
      const versionMatch = lddContent.match(RE_GLIBC_VERSION);
      if (versionMatch) {
        cachedVersionFilesystem = versionMatch[1];
      }
    } catch (e) {}
    return cachedVersionFilesystem;
  };
  var versionFromReport = () => {
    const report = getReport();
    if (report.header && report.header.glibcVersionRuntime) {
      return report.header.glibcVersionRuntime;
    }
    return null;
  };
  var versionSuffix = (s) => s.trim().split(/\s+/)[1];
  var versionFromCommand = (out) => {
    const [getconf, ldd1, ldd2] = out.split(/[\r\n]+/);
    if (getconf && getconf.includes(GLIBC)) {
      return versionSuffix(getconf);
    }
    if (ldd1 && ldd2 && ldd1.includes(MUSL)) {
      return versionSuffix(ldd2);
    }
    return null;
  };
  var version = async () => {
    let version2 = null;
    if (isLinux()) {
      version2 = await versionFromFilesystem();
      if (!version2) {
        version2 = versionFromReport();
      }
      if (!version2) {
        const out = await safeCommand();
        version2 = versionFromCommand(out);
      }
    }
    return version2;
  };
  var versionSync = () => {
    let version2 = null;
    if (isLinux()) {
      version2 = versionFromFilesystemSync();
      if (!version2) {
        version2 = versionFromReport();
      }
      if (!version2) {
        const out = safeCommandSync();
        version2 = versionFromCommand(out);
      }
    }
    return version2;
  };
  module.exports = {
    GLIBC,
    MUSL,
    family,
    familySync,
    isNonGlibcLinux,
    isNonGlibcLinuxSync,
    version,
    versionSync
  };
});

// ../../node_modules/.bun/node-gyp-build-optional-packages@5.2.2/node_modules/node-gyp-build-optional-packages/node-gyp-build.js
var require_node_gyp_build = __commonJS((exports, module) => {
  var fs = __require("fs");
  var path = __require("path");
  var url = __require("url");
  var os = __require("os");
  var runtimeRequire = typeof __webpack_require__ === "function" ? __non_webpack_require__ : __require;
  var vars = process.config && process.config.variables || {};
  var prebuildsOnly = !!process.env.PREBUILDS_ONLY;
  var versions = process.versions;
  var abi = versions.modules;
  if (versions.deno || process.isBun) {
    abi = "unsupported";
  }
  var runtime = isElectron() ? "electron" : isNwjs() ? "node-webkit" : "node";
  var arch = process.env.npm_config_arch || os.arch();
  var platform = process.env.npm_config_platform || os.platform();
  var libc2 = process.env.LIBC || (isMusl(platform) ? "musl" : "glibc");
  var armv = process.env.ARM_VERSION || (arch === "arm64" ? "8" : vars.arm_version) || "";
  var uv = (versions.uv || "").split(".")[0];
  module.exports = load;
  function load(dir) {
    return runtimeRequire(load.resolve(dir));
  }
  load.resolve = load.path = function(dir) {
    dir = path.resolve(dir || ".");
    var packageName = "";
    var packageNameError;
    try {
      packageName = runtimeRequire(path.join(dir, "package.json")).name;
      var varName = packageName.toUpperCase().replace(/-/g, "_");
      if (process.env[varName + "_PREBUILD"])
        dir = process.env[varName + "_PREBUILD"];
    } catch (err) {
      packageNameError = err;
    }
    if (!prebuildsOnly) {
      var release = getFirst(path.join(dir, "build/Release"), matchBuild);
      if (release)
        return release;
      var debug = getFirst(path.join(dir, "build/Debug"), matchBuild);
      if (debug)
        return debug;
    }
    var prebuild = resolve(dir);
    if (prebuild)
      return prebuild;
    var nearby = resolve(path.dirname(process.execPath));
    if (nearby)
      return nearby;
    var platformPackage = (packageName[0] == "@" ? "" : "@" + packageName + "/") + packageName + "-" + platform + "-" + arch;
    var packageResolutionError;
    try {
      var prebuildPackage = path.dirname(__require("module").createRequire(url.pathToFileURL(path.join(dir, "package.json"))).resolve(platformPackage));
      return resolveFile(prebuildPackage);
    } catch (error) {
      packageResolutionError = error;
    }
    var target2 = [
      "platform=" + platform,
      "arch=" + arch,
      "runtime=" + runtime,
      "abi=" + abi,
      "uv=" + uv,
      armv ? "armv=" + armv : "",
      "libc=" + libc2,
      "node=" + process.versions.node,
      process.versions.electron ? "electron=" + process.versions.electron : "",
      typeof __webpack_require__ === "function" ? "webpack=true" : ""
    ].filter(Boolean).join(" ");
    let errMessage = "No native build was found for " + target2 + `
    attempted loading from: ` + dir + " and package:" + " " + platformPackage + `
`;
    if (packageNameError) {
      errMessage += "Error finding package.json: " + packageNameError.message + `
`;
    }
    if (packageResolutionError) {
      errMessage += "Error resolving package: " + packageResolutionError.message + `
`;
    }
    throw new Error(errMessage);
    function resolve(dir2) {
      var tuples = readdirSync2(path.join(dir2, "prebuilds")).map(parseTuple);
      var tuple = tuples.filter(matchTuple(platform, arch)).sort(compareTuples)[0];
      if (!tuple)
        return;
      return resolveFile(path.join(dir2, "prebuilds", tuple.name));
    }
    function resolveFile(prebuilds) {
      var parsed = readdirSync2(prebuilds).map(parseTags);
      var candidates = parsed.filter(matchTags(runtime, abi));
      var winner = candidates.sort(compareTags(runtime))[0];
      if (winner)
        return path.join(prebuilds, winner.file);
    }
  };
  function readdirSync2(dir) {
    try {
      return fs.readdirSync(dir);
    } catch (err) {
      return [];
    }
  }
  function getFirst(dir, filter) {
    var files = readdirSync2(dir).filter(filter);
    return files[0] && path.join(dir, files[0]);
  }
  function matchBuild(name) {
    return /\.node$/.test(name);
  }
  function parseTuple(name) {
    var arr = name.split("-");
    if (arr.length !== 2)
      return;
    var platform2 = arr[0];
    var architectures = arr[1].split("+");
    if (!platform2)
      return;
    if (!architectures.length)
      return;
    if (!architectures.every(Boolean))
      return;
    return { name, platform: platform2, architectures };
  }
  function matchTuple(platform2, arch2) {
    return function(tuple) {
      if (tuple == null)
        return false;
      if (tuple.platform !== platform2)
        return false;
      return tuple.architectures.includes(arch2);
    };
  }
  function compareTuples(a, b) {
    return a.architectures.length - b.architectures.length;
  }
  function parseTags(file) {
    var arr = file.split(".");
    var extension = arr.pop();
    var tags = { file, specificity: 0 };
    if (extension !== "node")
      return;
    for (var i = 0;i < arr.length; i++) {
      var tag = arr[i];
      if (tag === "node" || tag === "electron" || tag === "node-webkit") {
        tags.runtime = tag;
      } else if (tag === "napi") {
        tags.napi = true;
      } else if (tag.slice(0, 3) === "abi") {
        tags.abi = tag.slice(3);
      } else if (tag.slice(0, 2) === "uv") {
        tags.uv = tag.slice(2);
      } else if (tag.slice(0, 4) === "armv") {
        tags.armv = tag.slice(4);
      } else if (tag === "glibc" || tag === "musl") {
        tags.libc = tag;
      } else {
        continue;
      }
      tags.specificity++;
    }
    return tags;
  }
  function matchTags(runtime2, abi2) {
    return function(tags) {
      if (tags == null)
        return false;
      if (tags.runtime !== runtime2 && !runtimeAgnostic(tags))
        return false;
      if (tags.abi !== abi2 && !tags.napi)
        return false;
      if (tags.uv && tags.uv !== uv)
        return false;
      if (tags.armv && tags.armv !== armv)
        return false;
      if (tags.libc && tags.libc !== libc2)
        return false;
      return true;
    };
  }
  function runtimeAgnostic(tags) {
    return tags.runtime === "node" && tags.napi;
  }
  function compareTags(runtime2) {
    return function(a, b) {
      if (a.runtime !== b.runtime) {
        return a.runtime === runtime2 ? -1 : 1;
      } else if (a.abi !== b.abi) {
        return a.abi ? -1 : 1;
      } else if (a.specificity !== b.specificity) {
        return a.specificity > b.specificity ? -1 : 1;
      } else {
        return 0;
      }
    };
  }
  function isNwjs() {
    return !!(process.versions && process.versions.nw);
  }
  function isElectron() {
    if (process.versions && process.versions.electron)
      return true;
    if (process.env.ELECTRON_RUN_AS_NODE)
      return true;
    return typeof window !== "undefined" && window.process && window.process.type === "renderer";
  }
  function isMusl(platform2) {
    if (platform2 !== "linux")
      return false;
    const { familySync, MUSL } = require_detect_libc();
    return familySync() === MUSL;
  }
  load.parseTags = parseTags;
  load.matchTags = matchTags;
  load.compareTags = compareTags;
  load.parseTuple = parseTuple;
  load.matchTuple = matchTuple;
  load.compareTuples = compareTuples;
});

// ../../node_modules/.bun/node-gyp-build-optional-packages@5.2.2/node_modules/node-gyp-build-optional-packages/index.js
var require_node_gyp_build_optional_packages = __commonJS((exports, module) => {
  var runtimeRequire = typeof __webpack_require__ === "function" ? __non_webpack_require__ : __require;
  if (typeof runtimeRequire.addon === "function") {
    module.exports = runtimeRequire.addon.bind(runtimeRequire);
  } else {
    module.exports = require_node_gyp_build();
  }
});

// ../../node_modules/.bun/msgpackr-extract@3.0.3/node_modules/msgpackr-extract/index.js
var require_msgpackr_extract = __commonJS((exports, module) => {
  var __dirname = "/Users/ryanwible/projects/spitestack/node_modules/.bun/msgpackr-extract@3.0.3/node_modules/msgpackr-extract";
  module.exports = require_node_gyp_build_optional_packages()(__dirname);
});

// src/infrastructure/storage/segments/segment-header.ts
var SEGMENT_MAGIC = 1397770580;
var SEGMENT_VERSION = 1;
var SEGMENT_HEADER_SIZE = 32;

class InvalidSegmentHeaderError extends Error {
  constructor(message) {
    super(message);
    this.name = "InvalidSegmentHeaderError";
  }
}
function encodeSegmentHeader(header) {
  if (!Number.isSafeInteger(header.basePosition) || header.basePosition < 0) {
    throw new InvalidSegmentHeaderError("Base position must be a non-negative safe integer");
  }
  const buffer = new ArrayBuffer(SEGMENT_HEADER_SIZE);
  const view = new DataView(buffer);
  view.setUint32(0, SEGMENT_MAGIC, false);
  view.setUint8(4, SEGMENT_VERSION);
  view.setUint8(5, header.flags);
  view.setUint16(6, 0, false);
  view.setBigUint64(8, header.segmentId, false);
  view.setBigUint64(16, BigInt(header.basePosition), false);
  return new Uint8Array(buffer);
}
function decodeSegmentHeader(data) {
  if (data.length < SEGMENT_HEADER_SIZE) {
    throw new InvalidSegmentHeaderError(`Header too short: expected ${SEGMENT_HEADER_SIZE} bytes, got ${data.length}`);
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const magic = view.getUint32(0, false);
  if (magic !== SEGMENT_MAGIC) {
    throw new InvalidSegmentHeaderError(`Invalid magic number: expected 0x${SEGMENT_MAGIC.toString(16).toUpperCase()}, got 0x${magic.toString(16).toUpperCase()}`);
  }
  const version = view.getUint8(4);
  if (version !== SEGMENT_VERSION) {
    throw new InvalidSegmentHeaderError(`Unsupported version: expected ${SEGMENT_VERSION}, got ${version}`);
  }
  const basePosition = Number(view.getBigUint64(16, false));
  if (!Number.isSafeInteger(basePosition)) {
    throw new InvalidSegmentHeaderError("Base position exceeds safe integer range");
  }
  return {
    magic,
    version,
    flags: view.getUint8(5),
    segmentId: view.getBigUint64(8, false),
    basePosition
  };
}
function isValidSegmentHeader(data) {
  if (data.length < SEGMENT_HEADER_SIZE) {
    return false;
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const magic = view.getUint32(0, false);
  if (magic !== SEGMENT_MAGIC) {
    return false;
  }
  const version = view.getUint8(4);
  if (version !== SEGMENT_VERSION) {
    return false;
  }
  return true;
}

// src/infrastructure/storage/support/crc32.ts
var CRC32_TABLE = new Uint32Array(256);
var POLYNOMIAL = 3988292384;
for (let i = 0;i < 256; i++) {
  let crc = i;
  for (let j = 0;j < 8; j++) {
    crc = crc & 1 ? crc >>> 1 ^ POLYNOMIAL : crc >>> 1;
  }
  CRC32_TABLE[i] = crc;
}
function crc32(data, initial = 4294967295) {
  let crc = initial;
  for (let i = 0;i < data.length; i++) {
    const tableIndex = (crc ^ data[i]) & 255;
    crc = CRC32_TABLE[tableIndex] ^ crc >>> 8;
  }
  return (crc ^ 4294967295) >>> 0;
}

class CRC32Calculator {
  crc = 4294967295;
  update(data) {
    for (let i = 0;i < data.length; i++) {
      const tableIndex = (this.crc ^ data[i]) & 255;
      this.crc = CRC32_TABLE[tableIndex] ^ this.crc >>> 8;
    }
  }
  finalize() {
    const result = (this.crc ^ 4294967295) >>> 0;
    this.crc = 4294967295;
    return result;
  }
  reset() {
    this.crc = 4294967295;
  }
}

// src/infrastructure/storage/batch/batch-record.ts
var BATCH_MAGIC = 1111577667;
var BATCH_HEADER_SIZE = 28;

class InvalidBatchError extends Error {
  constructor(message) {
    super(message);
    this.name = "InvalidBatchError";
  }
}

class BatchChecksumError extends InvalidBatchError {
  constructor(expected, actual) {
    super(`Checksum mismatch: expected 0x${expected.toString(16).padStart(8, "0")}, ` + `got 0x${actual.toString(16).padStart(8, "0")}`);
    this.name = "BatchChecksumError";
  }
}
function encodeBatchRecord(batchId, eventCount, compressedPayload, uncompressedLength) {
  const totalSize = BATCH_HEADER_SIZE + compressedPayload.length;
  const buffer = new ArrayBuffer(totalSize);
  const view = new DataView(buffer);
  const bytes = new Uint8Array(buffer);
  view.setUint32(0, BATCH_MAGIC, false);
  view.setUint32(8, compressedPayload.length, false);
  view.setUint32(12, uncompressedLength, false);
  view.setBigUint64(16, batchId, false);
  view.setUint32(24, eventCount, false);
  bytes.set(compressedPayload, BATCH_HEADER_SIZE);
  const checksumData = bytes.subarray(8);
  const checksum = crc32(checksumData);
  view.setUint32(4, checksum, false);
  return bytes;
}
function decodeBatchHeader(data, validateChecksum = true) {
  if (data.length < BATCH_HEADER_SIZE) {
    throw new InvalidBatchError(`Header too short: expected at least ${BATCH_HEADER_SIZE} bytes, got ${data.length}`);
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const magic = view.getUint32(0, false);
  if (magic !== BATCH_MAGIC) {
    throw new InvalidBatchError(`Invalid magic number: expected 0x${BATCH_MAGIC.toString(16).toUpperCase()}, ` + `got 0x${magic.toString(16).toUpperCase()}`);
  }
  const storedChecksum = view.getUint32(4, false);
  const compressedLength = view.getUint32(8, false);
  const uncompressedLength = view.getUint32(12, false);
  const batchId = view.getBigUint64(16, false);
  const eventCount = view.getUint32(24, false);
  if (validateChecksum) {
    const expectedSize = BATCH_HEADER_SIZE + compressedLength;
    if (data.length < expectedSize) {
      throw new InvalidBatchError(`Incomplete batch: expected ${expectedSize} bytes, got ${data.length}`);
    }
    const checksumData = data.subarray(8, expectedSize);
    const computedChecksum = crc32(checksumData);
    if (computedChecksum !== storedChecksum) {
      throw new BatchChecksumError(storedChecksum, computedChecksum);
    }
  }
  return {
    magic,
    checksum: storedChecksum,
    compressedLength,
    uncompressedLength,
    batchId,
    eventCount
  };
}
function extractBatchPayload(data, header) {
  const h = header ?? decodeBatchHeader(data, false);
  return data.subarray(BATCH_HEADER_SIZE, BATCH_HEADER_SIZE + h.compressedLength);
}
function isValidBatchMagic(data) {
  if (data.length < 4) {
    return false;
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  return view.getUint32(0, false) === BATCH_MAGIC;
}

// src/infrastructure/storage/segments/segment-index-file.ts
function compareStreamIds(a, b) {
  const minLen = Math.min(a.length, b.length);
  for (let i = 0;i < minLen; i++) {
    const diff = a.charCodeAt(i) - b.charCodeAt(i);
    if (diff !== 0)
      return diff;
  }
  return a.length - b.length;
}
var INDEX_MAGIC = 1229215811;
var INDEX_VERSION = 1;
var INDEX_HEADER_SIZE = 32;
var INDEX_ENTRY_SIZE = 24;

class IndexCorruptedError extends Error {
  constructor(message) {
    super(message);
    this.name = "IndexCorruptedError";
  }
}

class SegmentIndexFile {
  data = null;
  header = null;
  entries = [];
  entriesByGlobalPosition = null;
  stringTable = new Map;
  loaded = false;
  async load(fs, path) {
    try {
      this.data = await fs.mmap(path);
    } catch {
      this.data = await fs.readFile(path);
    }
    this.parse();
  }
  async loadIntoMemory(fs, path) {
    this.data = await fs.readFile(path);
    this.parse();
  }
  parse() {
    if (!this.data) {
      throw new Error("No data loaded");
    }
    if (this.data.length < INDEX_HEADER_SIZE) {
      throw new IndexCorruptedError("File too small for header");
    }
    const view = new DataView(this.data.buffer, this.data.byteOffset, this.data.byteLength);
    this.header = {
      magic: view.getUint32(0, true),
      version: view.getUint32(4, true),
      segmentId: view.getBigUint64(8, true),
      entryCount: view.getUint32(16, true),
      stringTableOffset: view.getUint32(20, true),
      checksum: view.getUint32(24, true)
    };
    if (this.header.magic !== INDEX_MAGIC) {
      throw new IndexCorruptedError(`Invalid magic: expected 0x${INDEX_MAGIC.toString(16)}, got 0x${this.header.magic.toString(16)}`);
    }
    if (this.header.version !== INDEX_VERSION) {
      throw new IndexCorruptedError(`Unsupported version: expected ${INDEX_VERSION}, got ${this.header.version}`);
    }
    if (!this.validateChecksum()) {
      throw new IndexCorruptedError("Checksum mismatch");
    }
    this.parseStringTable();
    this.parseEntries();
    this.entriesByGlobalPosition = null;
    this.loaded = true;
  }
  parseStringTable() {
    if (!this.data || !this.header)
      return;
    const decoder = new TextDecoder;
    let offset = this.header.stringTableOffset;
    while (offset < this.data.length) {
      const start = offset;
      while (offset < this.data.length && this.data[offset] !== 0) {
        offset++;
      }
      const str = decoder.decode(this.data.subarray(start, offset));
      this.stringTable.set(start - this.header.stringTableOffset, str);
      offset++;
    }
  }
  parseEntries() {
    if (!this.data || !this.header)
      return;
    const view = new DataView(this.data.buffer, this.data.byteOffset, this.data.byteLength);
    const entriesStart = INDEX_HEADER_SIZE;
    this.entries = [];
    for (let i = 0;i < this.header.entryCount; i++) {
      const entryOffset = entriesStart + i * INDEX_ENTRY_SIZE;
      const stringOffset = view.getUint32(entryOffset, true);
      const revision = view.getUint32(entryOffset + 4, true);
      const globalPosition = Number(view.getBigUint64(entryOffset + 8, true));
      if (!Number.isSafeInteger(globalPosition)) {
        throw new IndexCorruptedError("Global position exceeds safe integer range");
      }
      const batchOffset = view.getUint32(entryOffset + 16, true);
      const streamId = this.stringTable.get(stringOffset);
      if (streamId === undefined) {
        throw new IndexCorruptedError(`Invalid string table offset ${stringOffset} at entry ${i}`);
      }
      this.entries.push({
        streamId,
        revision,
        globalPosition,
        batchOffset
      });
    }
  }
  validateChecksum() {
    if (!this.data || !this.header)
      return false;
    const before = this.data.subarray(0, 24);
    const after = this.data.subarray(28);
    const combined = new Uint8Array(before.length + after.length);
    combined.set(before);
    combined.set(after, before.length);
    const calculated = crc32(combined);
    return calculated === this.header.checksum;
  }
  isLoaded() {
    return this.loaded;
  }
  getSegmentId() {
    return this.header?.segmentId ?? 0n;
  }
  getEntryCount() {
    return this.entries.length;
  }
  getAllEntries() {
    return [...this.entries];
  }
  findBatchOffsetForGlobalPosition(position) {
    if (this.entries.length === 0) {
      return null;
    }
    const sorted = this.getEntriesSortedByGlobalPosition();
    let left = 0;
    let right = sorted.length - 1;
    let matchIndex = sorted.length;
    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      const entry = sorted[mid];
      if (entry.globalPosition >= position) {
        matchIndex = mid;
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }
    if (matchIndex >= sorted.length) {
      return null;
    }
    return sorted[matchIndex].batchOffset;
  }
  getAllStreamIds() {
    const ids = new Set;
    for (const entry of this.entries) {
      ids.add(entry.streamId);
    }
    return ids;
  }
  findByStream(streamId, fromRevision, toRevision) {
    let left = 0;
    let right = this.entries.length - 1;
    let firstIndex = -1;
    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      const entry = this.entries[mid];
      const cmp = compareStreamIds(entry.streamId, streamId);
      if (cmp < 0) {
        left = mid + 1;
      } else if (cmp > 0) {
        right = mid - 1;
      } else {
        firstIndex = mid;
        right = mid - 1;
      }
    }
    if (firstIndex === -1) {
      return [];
    }
    const results = [];
    for (let i = firstIndex;i < this.entries.length; i++) {
      const entry = this.entries[i];
      if (entry.streamId !== streamId)
        break;
      if (fromRevision !== undefined && entry.revision < fromRevision)
        continue;
      if (toRevision !== undefined && entry.revision > toRevision)
        continue;
      results.push(entry);
    }
    return results;
  }
  findByGlobalPosition(position) {
    return this.entries.find((e) => e.globalPosition === position);
  }
  getStreamMaxRevision(streamId) {
    const entries = this.findByStream(streamId);
    if (entries.length === 0)
      return -1;
    return Math.max(...entries.map((e) => e.revision));
  }
  getEntriesSortedByGlobalPosition() {
    if (this.entriesByGlobalPosition) {
      return this.entriesByGlobalPosition;
    }
    this.entriesByGlobalPosition = [...this.entries].sort((a, b) => a.globalPosition < b.globalPosition ? -1 : a.globalPosition > b.globalPosition ? 1 : 0);
    return this.entriesByGlobalPosition;
  }
  static async write(fs, path, segmentId, entries) {
    const encoder = new TextEncoder;
    const sorted = [...entries].sort((a, b) => {
      const cmp = compareStreamIds(a.streamId, b.streamId);
      if (cmp !== 0)
        return cmp;
      return a.revision - b.revision;
    });
    const stringTable = new Map;
    const encodedStrings = [];
    let stringOffset = 0;
    for (const entry of sorted) {
      if (!stringTable.has(entry.streamId)) {
        stringTable.set(entry.streamId, stringOffset);
        const encoded = encoder.encode(entry.streamId);
        encodedStrings.push(encoded);
        stringOffset += encoded.length + 1;
      }
    }
    const entriesSize = sorted.length * INDEX_ENTRY_SIZE;
    const stringTableStart = INDEX_HEADER_SIZE + entriesSize;
    const totalSize = stringTableStart + stringOffset;
    const buffer = new Uint8Array(totalSize);
    const view = new DataView(buffer.buffer);
    view.setUint32(0, INDEX_MAGIC, true);
    view.setUint32(4, INDEX_VERSION, true);
    view.setBigUint64(8, segmentId, true);
    view.setUint32(16, sorted.length, true);
    view.setUint32(20, stringTableStart, true);
    view.setUint32(24, 0, true);
    view.setUint32(28, 0, true);
    for (let i = 0;i < sorted.length; i++) {
      const entry = sorted[i];
      if (!Number.isSafeInteger(entry.globalPosition) || entry.globalPosition < 0) {
        throw new Error("Global position must be a non-negative safe integer");
      }
      const entryOffset = INDEX_HEADER_SIZE + i * INDEX_ENTRY_SIZE;
      view.setUint32(entryOffset, stringTable.get(entry.streamId), true);
      view.setUint32(entryOffset + 4, entry.revision, true);
      view.setBigUint64(entryOffset + 8, BigInt(entry.globalPosition), true);
      view.setUint32(entryOffset + 16, entry.batchOffset, true);
      view.setUint32(entryOffset + 20, 0, true);
    }
    let strWriteOffset = stringTableStart;
    for (const encoded of encodedStrings) {
      buffer.set(encoded, strWriteOffset);
      strWriteOffset += encoded.length;
      buffer[strWriteOffset] = 0;
      strWriteOffset++;
    }
    const beforeChecksum = buffer.subarray(0, 24);
    const afterChecksum = buffer.subarray(28);
    const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
    checksumData.set(beforeChecksum);
    checksumData.set(afterChecksum, beforeChecksum.length);
    const checksum = crc32(checksumData);
    view.setUint32(24, checksum, true);
    const tempPath = path + ".tmp";
    const handle = await fs.open(tempPath, "write");
    await fs.write(handle, buffer);
    await fs.sync(handle);
    await fs.close(handle);
    await fs.rename(tempPath, path);
  }
}

// src/infrastructure/storage/support/write-all-bytes.ts
class ShortWriteError extends Error {
  bytesRequested;
  bytesWritten;
  constructor(bytesRequested, bytesWritten) {
    super(`Short write: only ${bytesWritten}/${bytesRequested} bytes written`);
    this.bytesRequested = bytesRequested;
    this.bytesWritten = bytesWritten;
    this.name = "ShortWriteError";
  }
}
async function writeAllBytes(fs, handle, data, offset) {
  let written = 0;
  while (written < data.length) {
    const remaining = data.subarray(written);
    const writeOffset = offset !== undefined ? offset + written : undefined;
    const bytesWritten = await fs.write(handle, remaining, writeOffset);
    if (bytesWritten === 0) {
      throw new ShortWriteError(data.length, written);
    }
    written += bytesWritten;
  }
}

// src/infrastructure/storage/segments/segment-writer.ts
class SegmentWriter {
  fs;
  serializer;
  compressor;
  handle = null;
  currentOffset = 0;
  batchIdCounter = 0n;
  path = "";
  segmentId = 0n;
  indexEntries = [];
  constructor(fs, serializer, compressor) {
    this.fs = fs;
    this.serializer = serializer;
    this.compressor = compressor;
  }
  async open(path, segmentId, basePosition) {
    if (this.handle) {
      throw new Error("SegmentWriter already has an open file");
    }
    this.path = path;
    this.segmentId = segmentId;
    this.indexEntries = [];
    this.handle = await this.fs.open(path, "write");
    const header = encodeSegmentHeader({
      flags: 0,
      segmentId,
      basePosition
    });
    await writeAllBytes(this.fs, this.handle, header, 0);
    this.currentOffset = SEGMENT_HEADER_SIZE;
    await this.fs.sync(this.handle);
  }
  async resume(path, offset, lastBatchId) {
    if (this.handle) {
      throw new Error("SegmentWriter already has an open file");
    }
    this.path = path;
    this.handle = await this.fs.open(path, "readwrite");
    this.currentOffset = offset;
    this.batchIdCounter = lastBatchId + 1n;
  }
  async appendBatch(events) {
    if (!this.handle) {
      throw new Error("SegmentWriter is not open");
    }
    if (events.length === 0) {
      throw new Error("Cannot write empty batch");
    }
    const batchId = this.batchIdCounter++;
    const batchOffset = this.currentOffset;
    const serialized = this.serializer.encode(events);
    const compressed = this.compressor.compress(serialized);
    const record = encodeBatchRecord(batchId, events.length, compressed, serialized.length);
    await writeAllBytes(this.fs, this.handle, record, batchOffset);
    this.currentOffset += record.length;
    for (const event of events) {
      this.indexEntries.push({
        streamId: event.streamId,
        revision: event.revision,
        globalPosition: event.globalPosition,
        batchOffset
      });
    }
    return {
      batchId,
      offset: batchOffset,
      length: record.length,
      eventCount: events.length
    };
  }
  async sync() {
    if (!this.handle) {
      throw new Error("SegmentWriter is not open");
    }
    await this.fs.sync(this.handle);
  }
  async close() {
    if (!this.handle) {
      return;
    }
    if (this.indexEntries.length > 0) {
      const idxPath = this.path.replace(".log", ".idx");
      await SegmentIndexFile.write(this.fs, idxPath, this.segmentId, this.indexEntries);
    }
    await this.fs.close(this.handle);
    this.handle = null;
    this.path = "";
    this.indexEntries = [];
  }
  get offset() {
    return this.currentOffset;
  }
  get isOpen() {
    return this.handle !== null;
  }
  get filePath() {
    return this.path;
  }
  get nextBatchId() {
    return this.batchIdCounter;
  }
}

// src/infrastructure/storage/segments/segment-reader.ts
class SegmentReader {
  fs;
  serializer;
  compressor;
  profiler;
  constructor(fs, serializer, compressor, profiler) {
    this.fs = fs;
    this.serializer = serializer;
    this.compressor = compressor;
    this.profiler = profiler;
  }
  async readHeader(path) {
    const data = await this.fs.readFileSlice(path, 0, SEGMENT_HEADER_SIZE);
    return decodeSegmentHeader(data);
  }
  async readBatch(path, offset) {
    const totalStart = performance.now();
    const headerStart = performance.now();
    const headerData = await this.fs.readFileSlice(path, offset, offset + BATCH_HEADER_SIZE);
    const headerReadMs = performance.now() - headerStart;
    const header = decodeBatchHeader(headerData, false);
    const fullBatchSize = BATCH_HEADER_SIZE + header.compressedLength;
    const payloadStart = performance.now();
    const batchData = await this.fs.readFileSlice(path, offset, offset + fullBatchSize);
    const payloadReadMs = performance.now() - payloadStart;
    decodeBatchHeader(batchData, true);
    const payload = extractBatchPayload(batchData, header);
    const decompressStart = performance.now();
    const decompressed = this.compressor.decompress(payload);
    const decompressMs = performance.now() - decompressStart;
    const decodeStart = performance.now();
    const events = this.serializer.decode(decompressed);
    const decodeMs = performance.now() - decodeStart;
    const totalMs = performance.now() - totalStart;
    this.profiler?.record({
      headerReadMs,
      payloadReadMs,
      decompressMs,
      decodeMs,
      totalMs,
      eventCount: events.length,
      compressedBytes: header.compressedLength,
      uncompressedBytes: header.uncompressedLength
    });
    return events;
  }
  async readBatchWithMetadata(path, offset) {
    const totalStart = performance.now();
    const headerStart = performance.now();
    const headerData = await this.fs.readFileSlice(path, offset, offset + BATCH_HEADER_SIZE);
    const headerReadMs = performance.now() - headerStart;
    const header = decodeBatchHeader(headerData, false);
    const fullBatchSize = BATCH_HEADER_SIZE + header.compressedLength;
    const payloadStart = performance.now();
    const batchData = await this.fs.readFileSlice(path, offset, offset + fullBatchSize);
    const payloadReadMs = performance.now() - payloadStart;
    decodeBatchHeader(batchData, true);
    const payload = extractBatchPayload(batchData, header);
    const decompressStart = performance.now();
    const decompressed = this.compressor.decompress(payload);
    const decompressMs = performance.now() - decompressStart;
    const decodeStart = performance.now();
    const events = this.serializer.decode(decompressed);
    const decodeMs = performance.now() - decodeStart;
    const totalMs = performance.now() - totalStart;
    this.profiler?.record({
      headerReadMs,
      payloadReadMs,
      decompressMs,
      decodeMs,
      totalMs,
      eventCount: events.length,
      compressedBytes: header.compressedLength,
      uncompressedBytes: header.uncompressedLength
    });
    return {
      events,
      batchId: header.batchId,
      nextOffset: offset + fullBatchSize
    };
  }
  async* readAllBatches(path, startOffset = SEGMENT_HEADER_SIZE) {
    const fileStat = await this.fs.stat(path);
    let offset = Math.max(startOffset, SEGMENT_HEADER_SIZE);
    while (offset < fileStat.size) {
      const remaining = fileStat.size - offset;
      if (remaining < BATCH_HEADER_SIZE) {
        break;
      }
      let headerData;
      try {
        headerData = await this.fs.readFileSlice(path, offset, offset + BATCH_HEADER_SIZE);
      } catch (error) {
        throw error;
      }
      if (!isValidBatchMagic(headerData)) {
        break;
      }
      let header;
      try {
        header = decodeBatchHeader(headerData, false);
      } catch (error) {
        if (error instanceof InvalidBatchError || error instanceof BatchChecksumError) {
          break;
        }
        throw error;
      }
      const fullBatchSize = BATCH_HEADER_SIZE + header.compressedLength;
      if (offset + fullBatchSize > fileStat.size) {
        break;
      }
      try {
        const events = await this.readBatch(path, offset);
        yield events;
      } catch (error) {
        if (error instanceof InvalidBatchError || error instanceof BatchChecksumError) {
          break;
        }
        throw error;
      }
      offset += fullBatchSize;
    }
  }
  async validateSegment(path) {
    const errors = [];
    let batchCount = 0;
    let eventCount = 0;
    let lastValidOffset = 0;
    if (!await this.fs.exists(path)) {
      return {
        valid: false,
        lastValidOffset: 0,
        batchCount: 0,
        eventCount: 0,
        errors: ["File does not exist"]
      };
    }
    const fileStat = await this.fs.stat(path);
    if (fileStat.size < SEGMENT_HEADER_SIZE) {
      return {
        valid: false,
        lastValidOffset: 0,
        batchCount: 0,
        eventCount: 0,
        errors: [`File too small: ${fileStat.size} bytes (need at least ${SEGMENT_HEADER_SIZE})`]
      };
    }
    const headerData = await this.fs.readFileSlice(path, 0, SEGMENT_HEADER_SIZE);
    if (!isValidSegmentHeader(headerData)) {
      return {
        valid: false,
        lastValidOffset: 0,
        batchCount: 0,
        eventCount: 0,
        errors: ["Invalid segment header"]
      };
    }
    lastValidOffset = SEGMENT_HEADER_SIZE;
    let offset = SEGMENT_HEADER_SIZE;
    while (offset < fileStat.size) {
      const remaining = fileStat.size - offset;
      if (remaining < BATCH_HEADER_SIZE) {
        errors.push(`Incomplete batch header at offset ${offset}: only ${remaining} bytes remaining`);
        break;
      }
      try {
        const headerData2 = await this.fs.readFileSlice(path, offset, offset + BATCH_HEADER_SIZE);
        if (!isValidBatchMagic(headerData2)) {
          errors.push(`Invalid batch magic at offset ${offset}`);
          break;
        }
        const header = decodeBatchHeader(headerData2, false);
        const fullBatchSize = BATCH_HEADER_SIZE + header.compressedLength;
        if (offset + fullBatchSize > fileStat.size) {
          errors.push(`Incomplete batch at offset ${offset}: need ${fullBatchSize} bytes, have ${remaining}`);
          break;
        }
        const batchData = await this.fs.readFileSlice(path, offset, offset + fullBatchSize);
        decodeBatchHeader(batchData, true);
        const payload = extractBatchPayload(batchData, header);
        const decompressed = this.compressor.decompress(payload);
        const events = this.serializer.decode(decompressed);
        batchCount++;
        eventCount += events.length;
        offset += fullBatchSize;
        lastValidOffset = offset;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        errors.push(`Error at offset ${offset}: ${message}`);
        break;
      }
    }
    return {
      valid: errors.length === 0,
      lastValidOffset,
      batchCount,
      eventCount,
      errors
    };
  }
  async getFileSize(path) {
    const stat = await this.fs.stat(path);
    return stat.size;
  }
}

// src/infrastructure/storage/segments/segment-index.ts
class SegmentIndex {
  streamIndex = new Map;
  positionIndex = new Map;
  indexedSegments = new Set;
  addBatch(segmentId, batchOffset, events) {
    this.indexedSegments.add(segmentId);
    for (const event of events) {
      let streamEntries = this.streamIndex.get(event.streamId);
      if (!streamEntries) {
        streamEntries = [];
        this.streamIndex.set(event.streamId, streamEntries);
      }
      streamEntries.push({
        segmentId,
        batchOffset,
        revision: event.revision,
        globalPosition: event.globalPosition
      });
      this.positionIndex.set(event.globalPosition, {
        segmentId,
        batchOffset,
        streamId: event.streamId
      });
    }
  }
  findByStream(streamId, options = {}) {
    const entries = this.streamIndex.get(streamId);
    if (!entries || entries.length === 0) {
      return [];
    }
    const sorted = [...entries].sort((a, b) => a.revision - b.revision);
    let filtered = sorted;
    if (options.fromRevision !== undefined) {
      filtered = filtered.filter((e) => e.revision >= options.fromRevision);
    }
    if (options.toRevision !== undefined) {
      filtered = filtered.filter((e) => e.revision <= options.toRevision);
    }
    if (options.direction === "backward") {
      filtered.reverse();
    }
    return filtered;
  }
  findByPosition(position) {
    return this.positionIndex.get(position);
  }
  getStreamRevision(streamId) {
    const entries = this.streamIndex.get(streamId);
    if (!entries || entries.length === 0) {
      return -1;
    }
    return Math.max(...entries.map((e) => e.revision));
  }
  hasStream(streamId) {
    const entries = this.streamIndex.get(streamId);
    return entries !== undefined && entries.length > 0;
  }
  getStreamIds() {
    return Array.from(this.streamIndex.keys());
  }
  getStreamEventCount(streamId) {
    return this.streamIndex.get(streamId)?.length ?? 0;
  }
  evictSegment(segmentId) {
    if (!this.indexedSegments.has(segmentId)) {
      return;
    }
    for (const [streamId, entries] of this.streamIndex) {
      const filtered = entries.filter((e) => e.segmentId !== segmentId);
      if (filtered.length === 0) {
        this.streamIndex.delete(streamId);
      } else {
        this.streamIndex.set(streamId, filtered);
      }
    }
    for (const [position, entry] of this.positionIndex) {
      if (entry.segmentId === segmentId) {
        this.positionIndex.delete(position);
      }
    }
    this.indexedSegments.delete(segmentId);
  }
  isSegmentIndexed(segmentId) {
    return this.indexedSegments.has(segmentId);
  }
  async rebuildFromSegment(reader, path, segmentId) {
    this.evictSegment(segmentId);
    let batchCount = 0;
    let eventCount = 0;
    let offset = 32;
    const fileSize = await reader.getFileSize(path);
    while (offset < fileSize) {
      try {
        const { events, nextOffset } = await reader.readBatchWithMetadata(path, offset);
        this.addBatch(segmentId, offset, events);
        batchCount++;
        eventCount += events.length;
        offset = nextOffset;
      } catch {
        break;
      }
    }
    return { batchCount, eventCount };
  }
  clear() {
    this.streamIndex.clear();
    this.positionIndex.clear();
    this.indexedSegments.clear();
  }
  getStats() {
    let totalEntries = 0;
    for (const entries of this.streamIndex.values()) {
      totalEntries += entries.length;
    }
    return {
      streamCount: this.streamIndex.size,
      totalEntries,
      segmentCount: this.indexedSegments.size
    };
  }
}

// src/infrastructure/storage/support/stream-map.ts
class StreamMap {
  streams = new Map;
  getRevision(streamId) {
    return this.streams.get(streamId)?.latestRevision ?? -1;
  }
  getSegments(streamId) {
    const meta = this.streams.get(streamId);
    return meta ? Array.from(meta.segments) : [];
  }
  hasStream(streamId) {
    return this.streams.has(streamId);
  }
  updateStream(streamId, revision, segmentId) {
    const existing = this.streams.get(streamId);
    if (!existing) {
      this.streams.set(streamId, {
        latestRevision: revision,
        segments: new Set([segmentId])
      });
      return;
    }
    if (revision > existing.latestRevision) {
      existing.latestRevision = revision;
    }
    existing.segments.add(segmentId);
  }
  clear() {
    this.streams.clear();
  }
  getStreamCount() {
    return this.streams.size;
  }
  getAllStreamIds() {
    return Array.from(this.streams.keys()).sort();
  }
  getStats() {
    let totalSegmentRefs = 0;
    for (const meta of this.streams.values()) {
      totalSegmentRefs += meta.segments.size;
    }
    return {
      streamCount: this.streams.size,
      totalSegmentRefs
    };
  }
  evictSegment(segmentId) {
    const streamsToRemove = [];
    for (const [streamId, meta] of this.streams) {
      if (meta.segments.has(segmentId)) {
        meta.segments.delete(segmentId);
        if (meta.segments.size === 0) {
          streamsToRemove.push(streamId);
        }
      }
    }
    for (const streamId of streamsToRemove) {
      this.streams.delete(streamId);
    }
  }
  toJSON() {
    const streams = [];
    for (const [streamId, meta] of this.streams) {
      streams.push({
        streamId,
        latestRevision: meta.latestRevision,
        segments: Array.from(meta.segments).map((s) => s.toString())
      });
    }
    return { streams };
  }
  static fromJSON(data) {
    const map = new StreamMap;
    for (const entry of data.streams) {
      map.streams.set(entry.streamId, {
        latestRevision: entry.latestRevision,
        segments: new Set(entry.segments.map((s) => BigInt(s)))
      });
    }
    return map;
  }
}

// src/infrastructure/storage/support/lru-cache.ts
class LRUCache {
  cache = new Map;
  maxSize;
  constructor(maxSize) {
    if (maxSize < 1) {
      throw new Error("LRUCache maxSize must be at least 1");
    }
    this.maxSize = maxSize;
  }
  get(key) {
    if (!this.cache.has(key)) {
      return;
    }
    const value = this.cache.get(key);
    this.cache.delete(key);
    this.cache.set(key, value);
    return value;
  }
  set(key, value) {
    if (this.cache.has(key)) {
      this.cache.delete(key);
    }
    if (this.cache.size >= this.maxSize) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey !== undefined) {
        this.cache.delete(oldestKey);
      }
    }
    this.cache.set(key, value);
  }
  has(key) {
    return this.cache.has(key);
  }
  delete(key) {
    return this.cache.delete(key);
  }
  clear() {
    this.cache.clear();
  }
  get size() {
    return this.cache.size;
  }
  keys() {
    return this.cache.keys();
  }
  values() {
    return this.cache.values();
  }
  entries() {
    return this.cache.entries();
  }
}

// src/infrastructure/storage/support/manifest.ts
var MANIFEST_VERSION = 1;
var MANIFEST_FILENAME = ".manifest";
var MANIFEST_TEMP_FILENAME = ".manifest.tmp";

class Manifest {
  fs;
  segments = new Map;
  globalPosition = 0;
  nextSegmentId = 0n;
  dataDir = "";
  dirty = false;
  saveCounter = 0;
  constructor(fs) {
    this.fs = fs;
  }
  async load(dataDir) {
    this.dataDir = dataDir;
    const manifestPath = `${dataDir}/${MANIFEST_FILENAME}`;
    try {
      if (!await this.fs.exists(manifestPath)) {
        return false;
      }
      const content = await this.fs.readFile(manifestPath);
      const json = new TextDecoder().decode(content);
      const data = JSON.parse(json);
      if (data.version !== MANIFEST_VERSION) {
        console.warn(`[spitedb] Manifest version mismatch: expected ${MANIFEST_VERSION}, got ${data.version}`);
        return false;
      }
      this.globalPosition = Number(data.globalPosition);
      this.nextSegmentId = BigInt(data.nextSegmentId);
      this.segments.clear();
      for (const seg of data.segments) {
        const segment = {
          id: BigInt(seg.id),
          path: seg.path,
          basePosition: Number(seg.basePosition)
        };
        this.segments.set(segment.id, segment);
      }
      this.dirty = false;
      return true;
    } catch (error) {
      console.warn(`[spitedb] Failed to load manifest: ${error}`);
      return false;
    }
  }
  async save() {
    if (!this.dataDir) {
      throw new Error("Manifest not initialized. Call load() first.");
    }
    const data = {
      version: MANIFEST_VERSION,
      globalPosition: this.globalPosition.toString(),
      nextSegmentId: this.nextSegmentId.toString(),
      segments: Array.from(this.segments.values()).map((seg) => ({
        id: seg.id.toString(),
        path: seg.path,
        basePosition: seg.basePosition.toString()
      }))
    };
    const json = JSON.stringify(data, null, 2);
    const content = new TextEncoder().encode(json);
    const tempPath = `${this.dataDir}/${MANIFEST_TEMP_FILENAME}.${this.saveCounter++}`;
    const finalPath = `${this.dataDir}/${MANIFEST_FILENAME}`;
    const handle = await this.fs.open(tempPath, "write");
    await this.fs.write(handle, content);
    await this.fs.sync(handle);
    await this.fs.close(handle);
    await this.fs.rename(tempPath, finalPath);
    this.dirty = false;
  }
  addSegment(segment) {
    this.segments.set(segment.id, segment);
    if (segment.id >= this.nextSegmentId) {
      this.nextSegmentId = segment.id + 1n;
    }
    this.dirty = true;
  }
  removeSegment(segmentId) {
    const removed = this.segments.delete(segmentId);
    if (removed) {
      this.dirty = true;
    }
    return removed;
  }
  getSegment(segmentId) {
    return this.segments.get(segmentId);
  }
  getSegments() {
    return Array.from(this.segments.values()).sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
  }
  getSegmentCount() {
    return this.segments.size;
  }
  getGlobalPosition() {
    return this.globalPosition;
  }
  setGlobalPosition(position) {
    if (position !== this.globalPosition) {
      this.globalPosition = position;
      this.dirty = true;
    }
  }
  getNextSegmentId() {
    return this.nextSegmentId;
  }
  setNextSegmentId(id) {
    if (id !== this.nextSegmentId) {
      this.nextSegmentId = id;
      this.dirty = true;
    }
  }
  allocateSegmentId() {
    const id = this.nextSegmentId;
    this.nextSegmentId++;
    this.dirty = true;
    return id;
  }
  isDirty() {
    return this.dirty;
  }
  clear() {
    this.segments.clear();
    this.globalPosition = 0;
    this.nextSegmentId = 0n;
    this.dirty = true;
  }
  initializeFromScan(segments, globalPosition, nextSegmentId) {
    this.segments.clear();
    for (const seg of segments) {
      this.segments.set(seg.id, seg);
    }
    this.globalPosition = globalPosition;
    this.nextSegmentId = nextSegmentId;
    this.dirty = true;
  }
}

// src/infrastructure/storage/segments/segment-manager.ts
var DEFAULT_MAX_SEGMENT_SIZE = 128 * 1024 * 1024;
var DEFAULT_INDEX_CACHE_SIZE = 10;

class SegmentManager {
  fs;
  serializer;
  compressor;
  config;
  reader;
  indexCache;
  indexFileCache;
  streamMap;
  manifest;
  segments = new Map;
  activeWriter = null;
  activeSegmentId = 0n;
  nextSegmentId = 0n;
  globalPosition = 0;
  initialized = false;
  pendingCloses = [];
  pendingBatch = null;
  constructor(fs, serializer, compressor, config) {
    this.fs = fs;
    this.serializer = serializer;
    this.compressor = compressor;
    this.config = {
      dataDir: config.dataDir,
      maxSegmentSize: config.maxSegmentSize ?? DEFAULT_MAX_SEGMENT_SIZE,
      indexCacheSize: config.indexCacheSize ?? DEFAULT_INDEX_CACHE_SIZE
    };
    this.reader = new SegmentReader(fs, serializer, compressor, config.readProfiler);
    this.indexCache = new LRUCache(this.config.indexCacheSize);
    this.indexFileCache = new LRUCache(this.config.indexCacheSize);
    this.streamMap = new StreamMap;
    this.manifest = new Manifest(fs);
  }
  async initialize() {
    if (this.initialized) {
      throw new Error("SegmentManager already initialized");
    }
    if (!await this.fs.exists(this.config.dataDir)) {
      await this.fs.mkdir(this.config.dataDir, { recursive: true });
    }
    const manifestLoaded = await this.manifest.load(this.config.dataDir);
    if (manifestLoaded) {
      for (const seg of this.manifest.getSegments()) {
        const path = `${this.config.dataDir}/${seg.path}`;
        if (await this.fs.exists(path)) {
          try {
            const stat = await this.fs.stat(path);
            this.segments.set(seg.id, {
              id: seg.id,
              path,
              size: stat.size,
              basePosition: seg.basePosition,
              isActive: false
            });
          } catch {}
        }
      }
      this.nextSegmentId = this.manifest.getNextSegmentId();
      this.globalPosition = this.manifest.getGlobalPosition();
    } else {
      await this.scanDirectory();
      this.manifest.initializeFromScan(Array.from(this.segments.values()).map((seg) => ({
        id: seg.id,
        path: seg.path.split("/").pop(),
        basePosition: seg.basePosition
      })), this.globalPosition, this.nextSegmentId);
      await this.manifest.save();
    }
    await this.rebuildStreamMap();
    this.initialized = true;
  }
  async scanDirectory() {
    const files = await this.fs.readdir(this.config.dataDir);
    const segmentFiles = files.filter((f) => f.startsWith("segment-") && f.endsWith(".log"));
    for (const file of segmentFiles) {
      const path = `${this.config.dataDir}/${file}`;
      try {
        const stat = await this.fs.stat(path);
        const header = await this.reader.readHeader(path);
        this.segments.set(header.segmentId, {
          id: header.segmentId,
          path,
          size: stat.size,
          basePosition: header.basePosition,
          isActive: false
        });
        if (header.segmentId >= this.nextSegmentId) {
          this.nextSegmentId = header.segmentId + 1n;
        }
      } catch (error) {
        console.warn(`Skipping invalid segment file: ${file}`, error);
      }
    }
  }
  async rebuildStreamMap() {
    this.streamMap.clear();
    for (const [segmentId, info] of this.segments) {
      try {
        const indexFile = await this.loadOrRebuildIndexFile(segmentId, info.path);
        for (const streamId of indexFile.getAllStreamIds()) {
          const maxRevision = indexFile.getStreamMaxRevision(streamId);
          this.streamMap.updateStream(streamId, maxRevision, segmentId);
          const entries = indexFile.findByStream(streamId);
          for (const entry of entries) {
            if (entry.globalPosition >= this.globalPosition) {
              this.globalPosition = entry.globalPosition + 1;
            }
          }
        }
      } catch (error) {
        console.warn(`Failed to load index for segment ${segmentId}:`, error);
        try {
          await this.rebuildStreamMapFromLog(segmentId, info.path);
        } catch (scanError) {
          console.warn(`Failed to scan segment ${segmentId} for recovery:`, scanError);
        }
      }
    }
  }
  async rebuildStreamMapFromLog(segmentId, logPath) {
    for await (const batch of this.reader.readAllBatches(logPath)) {
      for (const event of batch) {
        this.streamMap.updateStream(event.streamId, event.revision, segmentId);
        if (event.globalPosition >= this.globalPosition) {
          this.globalPosition = event.globalPosition + 1;
        }
      }
    }
  }
  async loadOrRebuildIndexFile(segmentId, logPath) {
    const idxPath = logPath.replace(".log", ".idx");
    if (await this.fs.exists(idxPath)) {
      try {
        const indexFile = new SegmentIndexFile;
        await indexFile.load(this.fs, idxPath);
        return indexFile;
      } catch (error) {
        if (error instanceof IndexCorruptedError) {
          console.warn(`Index file corrupted for segment ${segmentId}, rebuilding...`);
        } else {
          throw error;
        }
      }
    }
    return await this.rebuildIndexFile(segmentId, logPath);
  }
  async rebuildIndexFile(segmentId, logPath) {
    const idxPath = logPath.replace(".log", ".idx");
    const entries = [];
    const fileStat = await this.fs.stat(logPath);
    let offset = SEGMENT_HEADER_SIZE;
    while (offset < fileStat.size) {
      try {
        const result = await this.reader.readBatchWithMetadata(logPath, offset);
        const batchOffset = offset;
        for (const event of result.events) {
          entries.push({
            streamId: event.streamId,
            revision: event.revision,
            globalPosition: event.globalPosition,
            batchOffset
          });
        }
        offset = result.nextOffset;
      } catch {
        break;
      }
    }
    if (entries.length > 0) {
      await SegmentIndexFile.write(this.fs, idxPath, segmentId, entries);
    }
    const indexFile = new SegmentIndexFile;
    if (entries.length > 0) {
      await indexFile.load(this.fs, idxPath);
    }
    return indexFile;
  }
  async getWriter(basePosition) {
    this.ensureInitialized();
    if (!this.activeWriter) {
      await this.rotateSegment(basePosition);
    }
    const activeInfo = this.segments.get(this.activeSegmentId);
    if (activeInfo && activeInfo.size >= this.config.maxSegmentSize) {
      await this.rotateSegment(basePosition);
    }
    return this.activeWriter;
  }
  async rotateSegment(basePosition = this.globalPosition) {
    this.ensureInitialized();
    const oldWriter = this.activeWriter;
    const oldSegmentId = this.activeSegmentId;
    if (oldWriter) {
      await oldWriter.sync();
      const finalSize = oldWriter.offset;
      const activeInfo = this.segments.get(oldSegmentId);
      if (activeInfo) {
        activeInfo.isActive = false;
        activeInfo.size = finalSize;
      }
    }
    const segmentId = this.nextSegmentId++;
    const path = this.getSegmentPath(segmentId);
    this.activeWriter = new SegmentWriter(this.fs, this.serializer, this.compressor);
    await this.activeWriter.open(path, segmentId, basePosition);
    this.activeSegmentId = segmentId;
    this.segments.set(segmentId, {
      id: segmentId,
      path,
      size: SEGMENT_HEADER_SIZE,
      basePosition,
      isActive: true
    });
    const segmentFileName = path.split("/").pop();
    this.manifest.addSegment({
      id: segmentId,
      path: segmentFileName,
      basePosition
    });
    this.manifest.setNextSegmentId(this.nextSegmentId);
    this.manifest.save().catch((err) => {
      console.warn(`Failed to save manifest after rotation:`, err);
    });
    if (oldWriter) {
      const closePromise = oldWriter.close().catch((err) => {
        console.warn(`Background index write failed for segment ${oldSegmentId}:`, err);
      });
      this.pendingCloses.push(closePromise);
    }
  }
  async writeBatch(events) {
    const firstPosition = events[0].globalPosition;
    const lastPosition = events[events.length - 1].globalPosition;
    if (this.pendingBatch && this.pendingBatch.firstPosition === firstPosition && this.pendingBatch.lastPosition === lastPosition && this.pendingBatch.eventCount === events.length) {
      return {
        batchId: 0n,
        offset: 0,
        length: 0,
        eventCount: events.length,
        alreadyWritten: true
      };
    }
    const writer = await this.getWriter(events[0].globalPosition);
    const result = await writer.appendBatch(events);
    this.pendingBatch = {
      firstPosition,
      lastPosition,
      eventCount: events.length
    };
    const activeInfo = this.segments.get(this.activeSegmentId);
    if (activeInfo) {
      activeInfo.size = writer.offset;
    }
    for (const event of events) {
      if (event.globalPosition >= this.globalPosition) {
        this.globalPosition = event.globalPosition + 1;
      }
      this.streamMap.updateStream(event.streamId, event.revision, this.activeSegmentId);
    }
    return result;
  }
  async sync() {
    if (this.activeWriter) {
      await this.activeWriter.sync();
      this.pendingBatch = null;
    }
  }
  async getIndex(segmentId) {
    this.ensureInitialized();
    let index = this.indexCache.get(segmentId);
    if (index) {
      return index;
    }
    const segmentInfo = this.segments.get(segmentId);
    if (!segmentInfo) {
      throw new Error(`Segment ${segmentId} not found`);
    }
    index = new SegmentIndex;
    await index.rebuildFromSegment(this.reader, segmentInfo.path, segmentId);
    this.indexCache.set(segmentId, index);
    return index;
  }
  async recover() {
    this.ensureInitialized();
    const errors = [];
    let recoveredSegments = 0;
    let totalEvents = 0;
    for (const [segmentId, info] of this.segments) {
      try {
        const result = await this.reader.validateSegment(info.path);
        if (!result.valid) {
          const handle = await this.fs.open(info.path, "readwrite");
          await this.fs.truncate(handle, result.lastValidOffset);
          await this.fs.close(handle);
          recoveredSegments++;
          info.size = result.lastValidOffset;
          this.indexCache.delete(segmentId);
          errors.push(`Segment ${segmentId}: truncated from ${info.size} to ${result.lastValidOffset} bytes. ` + `Errors: ${result.errors.join(", ")}`);
        }
        totalEvents += result.eventCount;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        errors.push(`Segment ${segmentId}: recovery failed - ${message}`);
      }
    }
    return {
      segmentCount: this.segments.size,
      recoveredSegments,
      totalEvents,
      errors
    };
  }
  async close() {
    if (this.pendingCloses.length > 0) {
      await Promise.all(this.pendingCloses);
      this.pendingCloses = [];
    }
    if (this.activeWriter) {
      await this.activeWriter.close();
      this.activeWriter = null;
    }
    this.manifest.setGlobalPosition(this.globalPosition);
    try {
      await this.manifest.save();
    } catch (err) {
      console.warn(`Failed to save manifest on close:`, err);
    }
    this.indexCache.clear();
    this.indexFileCache.clear();
    this.streamMap.clear();
    this.segments.clear();
    this.initialized = false;
  }
  getReader() {
    return this.reader;
  }
  getSegments() {
    return Array.from(this.segments.values()).sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
  }
  getGlobalPosition() {
    return this.globalPosition;
  }
  getNextGlobalPosition() {
    return this.globalPosition;
  }
  allocateGlobalPosition() {
    return this.globalPosition++;
  }
  getActiveSegmentId() {
    return this.activeSegmentId;
  }
  isInitialized() {
    return this.initialized;
  }
  getStreamRevision(streamId) {
    return this.streamMap.getRevision(streamId);
  }
  getStreamSegments(streamId) {
    return this.streamMap.getSegments(streamId);
  }
  hasStream(streamId) {
    return this.streamMap.hasStream(streamId);
  }
  getAllStreamIds() {
    return this.streamMap.getAllStreamIds();
  }
  getStreamMap() {
    return this.streamMap;
  }
  async getSegmentIndexFile(segmentId) {
    this.ensureInitialized();
    let indexFile = this.indexFileCache.get(segmentId);
    if (indexFile) {
      return indexFile;
    }
    const segmentInfo = this.segments.get(segmentId);
    if (!segmentInfo) {
      throw new Error(`Segment ${segmentId} not found`);
    }
    indexFile = await this.loadOrRebuildIndexFile(segmentId, segmentInfo.path);
    this.indexFileCache.set(segmentId, indexFile);
    return indexFile;
  }
  getSegment(segmentId) {
    return this.segments.get(segmentId);
  }
  getSegmentPath(segmentId) {
    const paddedId = segmentId.toString().padStart(8, "0");
    return `${this.config.dataDir}/segment-${paddedId}.log`;
  }
  ensureInitialized() {
    if (!this.initialized) {
      throw new Error("SegmentManager not initialized. Call initialize() first.");
    }
  }
}

// src/domain/errors/invalid-stream-id.error.ts
class InvalidStreamIdError extends Error {
  constructor(message) {
    super(message);
    this.name = "InvalidStreamIdError";
    Object.setPrototypeOf(this, InvalidStreamIdError.prototype);
  }
}
// src/domain/errors/invalid-position.error.ts
class InvalidPositionError extends Error {
  constructor(message) {
    super(message);
    this.name = "InvalidPositionError";
    Object.setPrototypeOf(this, InvalidPositionError.prototype);
  }
}
// src/domain/errors/concurrency.error.ts
class ConcurrencyError extends Error {
  streamId;
  expectedRevision;
  actualRevision;
  constructor(streamId, expectedRevision, actualRevision) {
    super(`Concurrency conflict on stream '${streamId}': ` + `expected revision ${expectedRevision}, ` + `but actual revision is ${actualRevision}`);
    this.streamId = streamId;
    this.expectedRevision = expectedRevision;
    this.actualRevision = actualRevision;
    this.name = "ConcurrencyError";
    Object.setPrototypeOf(this, ConcurrencyError.prototype);
  }
}
// src/domain/errors/store-fatal.error.ts
class StoreFatalError extends Error {
  cause;
  constructor(message, cause) {
    super(message);
    this.cause = cause;
    this.name = "StoreFatalError";
    Object.setPrototypeOf(this, StoreFatalError.prototype);
  }
}
// src/infrastructure/resource-limits.ts
import { dlopen, FFIType, ptr, suffix } from "bun:ffi";
var RLIMIT_NOFILE = process.platform === "darwin" ? 8 : 7;
var LOCK_SH = 1;
var LOCK_EX = 2;
var LOCK_UN = 8;
var libc = null;
function getLibcCandidates() {
  if (process.platform === "darwin") {
    return ["libc.dylib"];
  }
  if (process.platform === "linux") {
    return ["libc.so.6", "libc.so"];
  }
  return [`libc.${suffix}`];
}
function getLibc() {
  if (!libc) {
    try {
      for (const candidate of getLibcCandidates()) {
        try {
          libc = dlopen(candidate, {
            getrlimit: {
              args: [FFIType.i32, FFIType.ptr],
              returns: FFIType.i32
            },
            setrlimit: {
              args: [FFIType.i32, FFIType.ptr],
              returns: FFIType.i32
            },
            flock: {
              args: [FFIType.i32, FFIType.i32],
              returns: FFIType.i32
            }
          });
          break;
        } catch {}
      }
    } catch (e) {
      return null;
    }
  }
  return libc;
}
function getResourceLimits() {
  const lib = getLibc();
  if (!lib)
    return null;
  const getrlimitFn = lib.symbols["getrlimit"];
  if (!getrlimitFn)
    return null;
  const buffer = new BigUint64Array(2);
  const result = getrlimitFn(RLIMIT_NOFILE, ptr(buffer));
  if (result !== 0) {
    return null;
  }
  return {
    soft: buffer[0],
    hard: buffer[1]
  };
}
function setResourceLimits(soft, hard) {
  const lib = getLibc();
  if (!lib)
    return false;
  const setrlimitFn = lib.symbols["setrlimit"];
  if (!setrlimitFn)
    return false;
  const buffer = new BigUint64Array([soft, hard]);
  const result = setrlimitFn(RLIMIT_NOFILE, ptr(buffer));
  return result === 0;
}
function flock(fd, operation) {
  const lib = getLibc();
  if (!lib) {
    throw new Error("FFI not available for flock");
  }
  const flockFn = lib.symbols["flock"];
  if (!flockFn) {
    throw new Error("flock symbol not available");
  }
  const result = flockFn(fd, operation);
  return result === 0;
}
var TARGET_FD_LIMIT = 65536n;
var MIN_RECOMMENDED_FD_LIMIT = 1024n;
function increaseFileDescriptorLimit() {
  const limits = getResourceLimits();
  if (!limits) {
    return {
      success: false,
      previousLimit: 0,
      newLimit: 0,
      hardLimit: 0,
      warning: "Could not read file descriptor limits (FFI unavailable)"
    };
  }
  const previousLimit = Number(limits.soft);
  const hardLimit = Number(limits.hard);
  const target = limits.hard < TARGET_FD_LIMIT ? limits.hard : TARGET_FD_LIMIT;
  if (limits.soft >= target) {
    return {
      success: true,
      previousLimit,
      newLimit: previousLimit,
      hardLimit
    };
  }
  const success = setResourceLimits(target, limits.hard);
  if (success) {
    const newLimit = Number(target);
    const result = {
      success: true,
      previousLimit,
      newLimit,
      hardLimit
    };
    if (limits.hard < MIN_RECOMMENDED_FD_LIMIT) {
      result.warning = `File descriptor hard limit is ${hardLimit}. For large deployments, ` + `increase it with: sudo launchctl limit maxfiles 65536 65536 (macOS) ` + `or edit /etc/security/limits.conf (Linux)`;
    }
    return result;
  }
  return {
    success: false,
    previousLimit,
    newLimit: previousLimit,
    hardLimit,
    warning: `Could not increase file descriptor limit from ${previousLimit} to ${Number(target)}. ` + `You may need to increase the hard limit manually.`
  };
}

// src/application/event-store/event-store.ts
var DEFAULT_AUTO_FLUSH_COUNT = 1000;
var DEFAULT_MAX_CACHED_EVENTS = 1e5;

class EventStore {
  config;
  segmentManager = null;
  pendingEvents = [];
  streamRevisions = new Map;
  dataDir = "";
  lockHandle = null;
  lastFlushedGlobalPosition = -1;
  writeQueue = Promise.resolve();
  failed = false;
  failedError = null;
  recentlyFlushedEvents = new Map;
  maxCachedEvents = DEFAULT_MAX_CACHED_EVENTS;
  constructor(config) {
    this.config = {
      ...config,
      autoFlushCount: config.autoFlushCount ?? DEFAULT_AUTO_FLUSH_COUNT
    };
  }
  async open(dataDir) {
    if (this.segmentManager) {
      throw new Error("EventStore already open");
    }
    const fdResult = increaseFileDescriptorLimit();
    if (fdResult.warning) {
      console.warn(`[spitedb] ${fdResult.warning}`);
    }
    this.dataDir = dataDir;
    if (!await this.config.fs.exists(dataDir)) {
      await this.config.fs.mkdir(dataDir, { recursive: true });
    }
    const lockPath = `${dataDir}/.lock`;
    const disableFlock = process.env["SPITEDB_DISABLE_FLOCK"] === "1" || process.env["SPITEDB_DISABLE_FLOCK"] === "true";
    if (disableFlock) {
      console.warn("[spitedb] File locking disabled (SPITEDB_DISABLE_FLOCK=1).");
    } else {
      try {
        this.lockHandle = await this.config.fs.open(lockPath, "write");
        await this.config.fs.flock(this.lockHandle, "exclusive");
      } catch (error) {
        if (this.lockHandle) {
          try {
            await this.config.fs.close(this.lockHandle);
          } catch {}
          this.lockHandle = null;
        }
        throw new Error(`Failed to open EventStore: another process may have it open. ` + `Lock file: ${lockPath}. Original error: ${error instanceof Error ? error.message : error}`);
      }
    }
    try {
      const segmentConfig = {
        dataDir,
        maxSegmentSize: this.config.maxSegmentSize,
        indexCacheSize: this.config.indexCacheSize
      };
      if (this.config.readProfiler) {
        segmentConfig.readProfiler = this.config.readProfiler;
      }
      this.segmentManager = new SegmentManager(this.config.fs, this.config.serializer, this.config.compressor, segmentConfig);
      await this.segmentManager.initialize();
      await this.rebuildStreamRevisions();
      const currentGlobal = this.segmentManager.getGlobalPosition();
      this.lastFlushedGlobalPosition = currentGlobal > 0 ? currentGlobal - 1 : -1;
    } catch (error) {
      if (this.lockHandle) {
        try {
          await this.config.fs.close(this.lockHandle);
        } catch {}
        this.lockHandle = null;
      }
      this.segmentManager = null;
      throw error;
    }
  }
  async close() {
    if (!this.segmentManager) {
      return;
    }
    if (!this.failed) {
      await this.flush();
    }
    this.failed = false;
    this.failedError = null;
    await this.segmentManager.close();
    this.segmentManager = null;
    this.streamRevisions.clear();
    this.pendingEvents = [];
    this.recentlyFlushedEvents.clear();
    this.lastFlushedGlobalPosition = -1;
    if (this.lockHandle) {
      await this.config.fs.close(this.lockHandle);
      this.lockHandle = null;
    }
  }
  async append(streamId, events, options = {}) {
    return this.enqueueWrite(async () => {
      this.ensureOpen();
      if (events.length === 0) {
        throw new Error("Cannot append empty event list");
      }
      const tenantId = options.tenantId ?? "default";
      const currentRevision = this.getStreamRevision(streamId);
      if (options.expectedRevision !== undefined) {
        if (options.expectedRevision === -1) {
          if (currentRevision >= 0) {
            throw new ConcurrencyError(streamId, options.expectedRevision, currentRevision);
          }
        } else if (options.expectedRevision !== currentRevision) {
          throw new ConcurrencyError(streamId, options.expectedRevision, currentRevision);
        }
      }
      const timestamp = this.config.clock.now();
      const firstGlobalPosition = this.segmentManager.getNextGlobalPosition();
      let revision = currentRevision + 1;
      const storedEvents = [];
      for (const event of events) {
        storedEvents.push({
          streamId,
          type: event.type,
          data: event.data,
          metadata: event.metadata,
          revision,
          globalPosition: this.segmentManager.allocateGlobalPosition(),
          timestamp,
          tenantId
        });
        revision++;
      }
      this.pendingEvents.push(...storedEvents);
      this.streamRevisions.set(streamId, revision - 1);
      if (this.config.autoFlushCount > 0 && this.pendingEvents.length >= this.config.autoFlushCount) {
        await this.flushInternal();
      }
      return {
        streamRevision: revision - 1,
        globalPosition: firstGlobalPosition,
        eventCount: events.length
      };
    });
  }
  async appendBatch(operations) {
    return this.enqueueWrite(async () => {
      this.ensureOpen();
      if (operations.length === 0) {
        return {
          streams: new Map,
          globalPosition: this.segmentManager.getNextGlobalPosition(),
          totalEvents: 0
        };
      }
      for (const op of operations) {
        if (op.events.length === 0) {
          throw new Error(`Cannot append empty event list for stream ${op.streamId}`);
        }
        if (op.expectedRevision !== undefined) {
          const current = this.getStreamRevision(op.streamId);
          if (op.expectedRevision === -1) {
            if (current >= 0) {
              throw new ConcurrencyError(op.streamId, -1, current);
            }
          } else if (op.expectedRevision !== current) {
            throw new ConcurrencyError(op.streamId, op.expectedRevision, current);
          }
        }
      }
      const timestamp = this.config.clock.now();
      const firstGlobalPosition = this.segmentManager.getNextGlobalPosition();
      const storedEvents = [];
      const results = new Map;
      for (const op of operations) {
        const tenantId = op.tenantId ?? "default";
        let revision = this.getStreamRevision(op.streamId) + 1;
        for (const event of op.events) {
          storedEvents.push({
            streamId: op.streamId,
            type: event.type,
            data: event.data,
            metadata: event.metadata,
            revision,
            globalPosition: this.segmentManager.allocateGlobalPosition(),
            timestamp,
            tenantId
          });
          revision++;
        }
        this.streamRevisions.set(op.streamId, revision - 1);
        results.set(op.streamId, {
          streamRevision: revision - 1,
          eventCount: op.events.length
        });
      }
      this.pendingEvents.push(...storedEvents);
      if (this.config.autoFlushCount > 0 && this.pendingEvents.length >= this.config.autoFlushCount) {
        await this.flushInternal();
      }
      return {
        streams: results,
        globalPosition: firstGlobalPosition,
        totalEvents: storedEvents.length
      };
    });
  }
  async flush() {
    await this.enqueueWrite(async () => {
      this.ensureOpen();
      await this.flushInternal();
    });
  }
  enqueueWrite(fn) {
    const run = this.writeQueue.then(fn);
    this.writeQueue = run.then(() => {
      return;
    }, () => {
      return;
    });
    return run;
  }
  async flushInternal() {
    if (this.pendingEvents.length === 0) {
      return;
    }
    const result = await this.segmentManager.writeBatch(this.pendingEvents);
    try {
      await this.segmentManager.sync();
    } catch (error) {
      this.failed = true;
      this.failedError = error instanceof Error ? error : new Error(String(error));
      throw new StoreFatalError("Sync failed after write. Store is now in failed state and must be closed and reopened. " + "Events may need to be re-appended after recovery.", this.failedError);
    }
    if (!result.alreadyWritten) {
      for (const event of this.pendingEvents) {
        this.recentlyFlushedEvents.set(event.globalPosition, event);
      }
      this.trimRecentCache();
    }
    this.lastFlushedGlobalPosition = this.pendingEvents[this.pendingEvents.length - 1].globalPosition;
    this.pendingEvents = [];
  }
  trimRecentCache() {
    if (this.recentlyFlushedEvents.size <= this.maxCachedEvents) {
      return;
    }
    const toRemove = this.recentlyFlushedEvents.size - this.maxCachedEvents;
    let removed = 0;
    for (const key of this.recentlyFlushedEvents.keys()) {
      if (removed >= toRemove)
        break;
      this.recentlyFlushedEvents.delete(key);
      removed++;
    }
  }
  readGlobalCached(fromPosition, maxCount) {
    if (fromPosition > this.lastFlushedGlobalPosition) {
      return null;
    }
    const events = [];
    for (let i = 0;i < maxCount; i++) {
      const pos = fromPosition + i;
      if (pos > this.lastFlushedGlobalPosition) {
        break;
      }
      const cached = this.recentlyFlushedEvents.get(pos);
      if (!cached) {
        return null;
      }
      events.push(cached);
    }
    return events;
  }
  async readStream(streamId, options = {}) {
    this.ensureOpen();
    const events = [];
    const maxCount = options.maxCount ?? Infinity;
    for await (const event of this.streamEvents(streamId, options)) {
      events.push(event);
      if (events.length >= maxCount) {
        break;
      }
    }
    return events;
  }
  async* streamEvents(streamId, options = {}) {
    this.ensureOpen();
    const fromRevision = options.fromRevision ?? 0;
    const toRevision = options.toRevision;
    const direction = options.direction ?? "forward";
    const maxCount = options.maxCount ?? Infinity;
    const durableEvents = await this.readDurableStreamEvents(streamId, fromRevision, toRevision);
    const pendingEvents = this.pendingEvents.filter((event) => {
      if (event.streamId !== streamId)
        return false;
      if (event.revision < fromRevision)
        return false;
      if (toRevision !== undefined && event.revision > toRevision)
        return false;
      return true;
    });
    const combined = [...durableEvents, ...pendingEvents];
    combined.sort((a, b) => a.revision - b.revision);
    if (direction === "backward") {
      combined.reverse();
    }
    let yielded = 0;
    for (const event of combined) {
      if (yielded >= maxCount) {
        break;
      }
      yield event;
      yielded++;
    }
  }
  async readGlobal(fromPosition = 0, options = {}) {
    this.ensureOpen();
    const events = [];
    const maxCount = options.maxCount ?? Infinity;
    for await (const event of this.streamGlobal(fromPosition)) {
      events.push(event);
      if (events.length >= maxCount) {
        break;
      }
    }
    return events;
  }
  async readGlobalDurable(fromPosition = 0, options = {}) {
    this.ensureOpen();
    const events = [];
    const maxCount = options.maxCount ?? Infinity;
    for await (const event of this.streamGlobalDurable(fromPosition)) {
      events.push(event);
      if (events.length >= maxCount) {
        break;
      }
    }
    return events;
  }
  async* streamGlobalDurable(fromPosition = 0) {
    this.ensureOpen();
    const maxDurablePosition = this.lastFlushedGlobalPosition;
    if (maxDurablePosition < 0 || maxDurablePosition < fromPosition) {
      return;
    }
    let lastPosition = fromPosition > 0 ? fromPosition - 1 : -1;
    const reader = this.segmentManager.getReader();
    const segments = this.segmentManager.getSegments();
    let startIndex = 0;
    if (segments.length > 0 && fromPosition > segments[0].basePosition) {
      for (let i = 0;i < segments.length; i += 1) {
        const segment = segments[i];
        const next = segments[i + 1];
        if (fromPosition >= segment.basePosition && (!next || fromPosition < next.basePosition)) {
          startIndex = i;
          break;
        }
      }
    }
    for (let i = startIndex;i < segments.length; i += 1) {
      const segment = segments[i];
      if (segment.basePosition > maxDurablePosition) {
        break;
      }
      let startOffset = SEGMENT_HEADER_SIZE;
      if (i === startIndex && fromPosition > segment.basePosition) {
        try {
          const indexFile = await this.segmentManager.getSegmentIndexFile(segment.id);
          const batchOffset = indexFile.findBatchOffsetForGlobalPosition(fromPosition);
          if (batchOffset !== null) {
            startOffset = batchOffset;
          }
        } catch {}
      }
      for await (const batch of reader.readAllBatches(segment.path, startOffset)) {
        for (const event of batch) {
          if (event.globalPosition < fromPosition) {
            continue;
          }
          if (event.globalPosition > maxDurablePosition) {
            return;
          }
          if (event.globalPosition <= lastPosition) {
            return;
          }
          yield event;
          lastPosition = event.globalPosition;
        }
      }
    }
  }
  async* streamGlobal(fromPosition = 0) {
    this.ensureOpen();
    const maxDurablePosition = this.lastFlushedGlobalPosition;
    let lastPosition = fromPosition > 0 ? fromPosition - 1 : -1;
    const pendingSnapshot = this.pendingEvents.slice();
    if (maxDurablePosition >= 0 && maxDurablePosition >= fromPosition) {
      const reader = this.segmentManager.getReader();
      const segments = this.segmentManager.getSegments();
      let startIndex = 0;
      if (segments.length > 0 && fromPosition > segments[0].basePosition) {
        for (let i = 0;i < segments.length; i += 1) {
          const segment = segments[i];
          const next = segments[i + 1];
          if (fromPosition >= segment.basePosition && (!next || fromPosition < next.basePosition)) {
            startIndex = i;
            break;
          }
        }
      }
      readDurable:
        for (let i = startIndex;i < segments.length; i += 1) {
          const segment = segments[i];
          if (segment.basePosition > maxDurablePosition) {
            break;
          }
          let startOffset = SEGMENT_HEADER_SIZE;
          if (i === startIndex && fromPosition > segment.basePosition) {
            try {
              const indexFile = await this.segmentManager.getSegmentIndexFile(segment.id);
              const batchOffset = indexFile.findBatchOffsetForGlobalPosition(fromPosition);
              if (batchOffset !== null) {
                startOffset = batchOffset;
              }
            } catch {}
          }
          for await (const batch of reader.readAllBatches(segment.path, startOffset)) {
            for (const event of batch) {
              if (event.globalPosition < fromPosition) {
                continue;
              }
              if (event.globalPosition > maxDurablePosition) {
                break readDurable;
              }
              if (event.globalPosition <= lastPosition) {
                return;
              }
              yield event;
              lastPosition = event.globalPosition;
            }
          }
        }
    }
    if (pendingSnapshot.length > 0) {
      for (const event of pendingSnapshot) {
        if (event.globalPosition <= maxDurablePosition) {
          continue;
        }
        if (event.globalPosition >= fromPosition) {
          if (event.globalPosition <= lastPosition) {
            return;
          }
          yield event;
          lastPosition = event.globalPosition;
        }
      }
    }
  }
  getStreamRevision(streamId) {
    const cached = this.streamRevisions.get(streamId);
    if (cached !== undefined) {
      return cached;
    }
    for (let i = this.pendingEvents.length - 1;i >= 0; i--) {
      if (this.pendingEvents[i].streamId === streamId) {
        return this.pendingEvents[i].revision;
      }
    }
    return -1;
  }
  hasStream(streamId) {
    return this.getStreamRevision(streamId) >= 0;
  }
  async getStreamIds() {
    this.ensureOpen();
    const streamIds = new Set(this.segmentManager.getAllStreamIds());
    for (const event of this.pendingEvents) {
      streamIds.add(event.streamId);
    }
    return Array.from(streamIds).sort();
  }
  getGlobalPosition() {
    this.ensureOpen();
    return this.segmentManager.getGlobalPosition();
  }
  getDurableGlobalPosition() {
    this.ensureOpen();
    return this.lastFlushedGlobalPosition;
  }
  isOpen() {
    return this.segmentManager !== null;
  }
  async rebuildStreamRevisions() {
    for (const segment of this.segmentManager.getSegments()) {
      const index = await this.segmentManager.getIndex(segment.id);
      for (const streamId of index.getStreamIds()) {
        const currentMax = this.streamRevisions.get(streamId) ?? -1;
        const segmentMax = index.getStreamRevision(streamId);
        if (segmentMax > currentMax) {
          this.streamRevisions.set(streamId, segmentMax);
        }
      }
    }
  }
  async readDurableStreamEvents(streamId, fromRevision = 0, toRevision) {
    const maxDurablePosition = this.lastFlushedGlobalPosition;
    if (maxDurablePosition < 0) {
      return [];
    }
    const segmentIds = this.segmentManager.getStreamSegments(streamId);
    if (segmentIds.length === 0) {
      return [];
    }
    const events = [];
    const batchCache = new Map;
    const reader = this.segmentManager.getReader();
    for (const segmentId of segmentIds) {
      const segment = this.segmentManager.getSegment(segmentId);
      if (!segment) {
        continue;
      }
      if (segment.basePosition > maxDurablePosition) {
        continue;
      }
      if (segment.isActive) {
        for await (const batch of reader.readAllBatches(segment.path)) {
          for (const event of batch) {
            if (event.streamId !== streamId) {
              continue;
            }
            if (event.globalPosition > maxDurablePosition) {
              continue;
            }
            if (event.revision < fromRevision) {
              continue;
            }
            if (toRevision !== undefined && event.revision > toRevision) {
              continue;
            }
            events.push(event);
          }
        }
        continue;
      }
      const indexFile = await this.segmentManager.getSegmentIndexFile(segmentId);
      const entries = indexFile.findByStream(streamId, fromRevision, toRevision);
      for (const entry of entries) {
        if (entry.globalPosition > maxDurablePosition) {
          continue;
        }
        const cacheKey = `${segmentId}:${entry.batchOffset}`;
        let batch = batchCache.get(cacheKey);
        if (!batch) {
          try {
            batch = await reader.readBatch(segment.path, entry.batchOffset);
          } catch {
            continue;
          }
          batchCache.set(cacheKey, batch);
        }
        const event = batch.find((e) => e.streamId === streamId && e.revision === entry.revision);
        if (event) {
          events.push(event);
        }
      }
    }
    return events;
  }
  ensureOpen() {
    if (!this.segmentManager) {
      throw new Error("EventStore not open. Call open() first.");
    }
    if (this.failed) {
      throw new StoreFatalError("EventStore is in failed state due to previous sync error. Close and reopen to recover.", this.failedError ?? undefined);
    }
  }
  isFailed() {
    return this.failed;
  }
  getFailedError() {
    return this.failedError;
  }
}
// src/errors/spitedb-error.ts
class SpiteDBError extends Error {
  constructor(message) {
    super(`[SpiteDB] ${message}`);
    this.name = "SpiteDBError";
    Object.setPrototypeOf(this, SpiteDBError.prototype);
  }
}

class SpiteDBNotOpenError extends SpiteDBError {
  constructor() {
    super("Database is not open. Call SpiteDB.open() first.");
    this.name = "SpiteDBNotOpenError";
    Object.setPrototypeOf(this, SpiteDBNotOpenError.prototype);
  }
}

class ProjectionsNotStartedError extends SpiteDBError {
  constructor() {
    super("Projections not started. Call startProjections() first.");
    this.name = "ProjectionsNotStartedError";
    Object.setPrototypeOf(this, ProjectionsNotStartedError.prototype);
  }
}

class ProjectionBackpressureError extends SpiteDBError {
  projectionName;
  lag;
  maxLag;
  constructor(projectionName, lag, maxLag) {
    super(`Append blocked: projection '${projectionName}' is lagging by ${lag} (max ${maxLag}).`);
    this.projectionName = projectionName;
    this.lag = lag;
    this.maxLag = maxLag;
    this.name = "ProjectionBackpressureError";
    Object.setPrototypeOf(this, ProjectionBackpressureError.prototype);
  }
}

class ProjectionBackpressureTimeoutError extends ProjectionBackpressureError {
  waitedMs;
  maxWaitMs;
  constructor(projectionName, lag, maxLag, waitedMs, maxWaitMs) {
    super(projectionName, lag, maxLag);
    this.waitedMs = waitedMs;
    this.maxWaitMs = maxWaitMs;
    this.name = "ProjectionBackpressureTimeoutError";
    Object.setPrototypeOf(this, ProjectionBackpressureTimeoutError.prototype);
  }
}
// src/errors/projection-errors.ts
class ProjectionError extends Error {
  projectionName;
  constructor(message, projectionName) {
    super(`[Projection: ${projectionName}] ${message}`);
    this.projectionName = projectionName;
    this.name = "ProjectionError";
    Object.setPrototypeOf(this, ProjectionError.prototype);
  }
}

class ProjectionBuildError extends ProjectionError {
  eventType;
  globalPosition;
  cause;
  constructor(projectionName, eventType, globalPosition, cause) {
    super(`Failed to process event '${eventType}' at position ${globalPosition}: ${cause.message}`, projectionName);
    this.eventType = eventType;
    this.globalPosition = globalPosition;
    this.cause = cause;
    this.name = "ProjectionBuildError";
    Object.setPrototypeOf(this, ProjectionBuildError.prototype);
  }
}

class ProjectionNotFoundError extends ProjectionError {
  constructor(projectionName) {
    super(`Projection not found`, projectionName);
    this.name = "ProjectionNotFoundError";
    Object.setPrototypeOf(this, ProjectionNotFoundError.prototype);
  }
}

class ProjectionAlreadyRegisteredError extends ProjectionError {
  constructor(projectionName) {
    super(`Projection is already registered`, projectionName);
    this.name = "ProjectionAlreadyRegisteredError";
    Object.setPrototypeOf(this, ProjectionAlreadyRegisteredError.prototype);
  }
}

class ProjectionDisabledError extends ProjectionError {
  constructor(projectionName) {
    super(`Projection is disabled`, projectionName);
    this.name = "ProjectionDisabledError";
    Object.setPrototypeOf(this, ProjectionDisabledError.prototype);
  }
}

class ProjectionCoordinatorError extends Error {
  constructor(message) {
    super(message);
    this.name = "ProjectionCoordinatorError";
    Object.setPrototypeOf(this, ProjectionCoordinatorError.prototype);
  }
}

class ProjectionCatchUpTimeoutError extends ProjectionCoordinatorError {
  projectionName;
  currentPosition;
  targetPosition;
  timeoutMs;
  constructor(projectionName, currentPosition, targetPosition, timeoutMs) {
    super(`Projection '${projectionName}' failed to catch up: ` + `at position ${currentPosition}, target ${targetPosition}, timeout ${timeoutMs}ms`);
    this.projectionName = projectionName;
    this.currentPosition = currentPosition;
    this.targetPosition = targetPosition;
    this.timeoutMs = timeoutMs;
    this.name = "ProjectionCatchUpTimeoutError";
    Object.setPrototypeOf(this, ProjectionCatchUpTimeoutError.prototype);
  }
}
// src/errors/checkpoint-errors.ts
class CheckpointWriteError extends Error {
  projectionName;
  path;
  cause;
  constructor(projectionName, path, cause) {
    super(`Failed to write checkpoint for '${projectionName}' to ${path}: ${cause.message}`);
    this.projectionName = projectionName;
    this.path = path;
    this.cause = cause;
    this.name = "CheckpointWriteError";
    Object.setPrototypeOf(this, CheckpointWriteError.prototype);
  }
}

class CheckpointLoadError extends Error {
  projectionName;
  path;
  cause;
  constructor(projectionName, path, cause) {
    super(`Failed to load checkpoint for '${projectionName}' from ${path}: ${cause.message}`);
    this.projectionName = projectionName;
    this.path = path;
    this.cause = cause;
    this.name = "CheckpointLoadError";
    Object.setPrototypeOf(this, CheckpointLoadError.prototype);
  }
}

class CheckpointCorruptionError extends Error {
  projectionName;
  path;
  reason;
  constructor(projectionName, path, reason) {
    super(`Corrupted checkpoint for '${projectionName}' at ${path}: ${reason}`);
    this.projectionName = projectionName;
    this.path = path;
    this.reason = reason;
    this.name = "CheckpointCorruptionError";
    Object.setPrototypeOf(this, CheckpointCorruptionError.prototype);
  }
}

class CheckpointVersionError extends Error {
  projectionName;
  path;
  version;
  supportedVersions;
  constructor(projectionName, path, version, supportedVersions) {
    super(`Unsupported checkpoint version ${version} for '${projectionName}' at ${path}. ` + `Supported versions: ${supportedVersions.join(", ")}`);
    this.projectionName = projectionName;
    this.path = path;
    this.version = version;
    this.supportedVersions = supportedVersions;
    this.name = "CheckpointVersionError";
    Object.setPrototypeOf(this, CheckpointVersionError.prototype);
  }
}
// src/application/projections/checkpoint-manager.ts
var CHECKPOINT_MAGIC = 1128811344;
var CHECKPOINT_VERSION = 1;
var SUPPORTED_VERSIONS = [1];
var CHECKPOINT_HEADER_SIZE = 32;

class CheckpointManager {
  fs;
  serializer;
  clock;
  dataDir;
  jitterMs;
  scheduledTimes = new Map;
  constructor(config) {
    this.fs = config.fs;
    this.serializer = config.serializer;
    this.clock = config.clock;
    this.dataDir = config.dataDir;
    this.jitterMs = config.jitterMs ?? 1000;
  }
  async initialize() {
    if (!await this.fs.exists(this.dataDir)) {
      await this.fs.mkdir(this.dataDir, { recursive: true });
    }
  }
  scheduleCheckpoint(projectionName, baseIntervalMs) {
    const now = this.clock.now();
    const lastScheduled = this.scheduledTimes.get(projectionName) ?? now;
    const jitter = (Math.random() * 2 - 1) * this.jitterMs;
    const nextTime = Math.max(now, lastScheduled) + baseIntervalMs + jitter;
    this.scheduledTimes.set(projectionName, nextTime);
    return nextTime;
  }
  shouldCheckpoint(projectionName) {
    const scheduled = this.scheduledTimes.get(projectionName);
    if (scheduled === undefined)
      return false;
    return this.clock.now() >= scheduled;
  }
  async writeCheckpoint(checkpoint) {
    const path = this.getCheckpointPath(checkpoint.projectionName);
    const tempPath = path + ".tmp";
    try {
      if (!Number.isSafeInteger(checkpoint.position) || checkpoint.position < 0) {
        throw new Error("Checkpoint position must be a non-negative safe integer");
      }
      if (!Number.isSafeInteger(checkpoint.timestamp) || checkpoint.timestamp < 0) {
        throw new Error("Checkpoint timestamp must be a non-negative safe integer");
      }
      const stateData = this.serializer.encode(checkpoint.state);
      const totalSize = CHECKPOINT_HEADER_SIZE + stateData.length;
      const buffer = new Uint8Array(totalSize);
      const view = new DataView(buffer.buffer);
      view.setUint32(0, CHECKPOINT_MAGIC, true);
      view.setUint32(4, CHECKPOINT_VERSION, true);
      view.setBigUint64(8, BigInt(checkpoint.position), true);
      view.setBigUint64(16, BigInt(checkpoint.timestamp), true);
      view.setUint32(24, stateData.length, true);
      view.setUint32(28, 0, true);
      buffer.set(stateData, CHECKPOINT_HEADER_SIZE);
      const beforeChecksum = buffer.subarray(0, 28);
      const afterChecksum = buffer.subarray(32);
      const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
      checksumData.set(beforeChecksum);
      checksumData.set(afterChecksum, beforeChecksum.length);
      const checksum = crc32(checksumData);
      view.setUint32(28, checksum, true);
      const handle = await this.fs.open(tempPath, "write");
      try {
        await this.fs.write(handle, buffer);
        await this.fs.sync(handle);
      } finally {
        await this.fs.close(handle);
      }
      await this.fs.rename(tempPath, path);
    } catch (error) {
      try {
        if (await this.fs.exists(tempPath)) {
          await this.fs.unlink(tempPath);
        }
      } catch {}
      throw new CheckpointWriteError(checkpoint.projectionName, path, error instanceof Error ? error : new Error(String(error)));
    }
  }
  async loadCheckpoint(projectionName) {
    const path = this.getCheckpointPath(projectionName);
    if (!await this.fs.exists(path)) {
      return null;
    }
    try {
      const data = await this.fs.readFile(path);
      if (data.length < CHECKPOINT_HEADER_SIZE) {
        throw new CheckpointCorruptionError(projectionName, path, `File too small: ${data.length} bytes, expected at least ${CHECKPOINT_HEADER_SIZE}`);
      }
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      const header = {
        magic: view.getUint32(0, true),
        version: view.getUint32(4, true),
        position: Number(view.getBigUint64(8, true)),
        timestamp: Number(view.getBigUint64(16, true)),
        stateLength: view.getUint32(24, true),
        checksum: view.getUint32(28, true)
      };
      if (header.magic !== CHECKPOINT_MAGIC) {
        throw new CheckpointCorruptionError(projectionName, path, `Invalid magic: expected 0x${CHECKPOINT_MAGIC.toString(16)}, got 0x${header.magic.toString(16)}`);
      }
      if (!SUPPORTED_VERSIONS.includes(header.version)) {
        throw new CheckpointVersionError(projectionName, path, header.version, SUPPORTED_VERSIONS);
      }
      const expectedSize = CHECKPOINT_HEADER_SIZE + header.stateLength;
      if (data.length < expectedSize) {
        throw new CheckpointCorruptionError(projectionName, path, `File too small for state: ${data.length} bytes, expected ${expectedSize}`);
      }
      const beforeChecksum = data.subarray(0, 28);
      const afterChecksum = data.subarray(32);
      const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
      checksumData.set(beforeChecksum);
      checksumData.set(afterChecksum, beforeChecksum.length);
      const calculatedChecksum = crc32(checksumData);
      if (calculatedChecksum !== header.checksum) {
        throw new CheckpointCorruptionError(projectionName, path, `Checksum mismatch: expected 0x${header.checksum.toString(16)}, calculated 0x${calculatedChecksum.toString(16)}`);
      }
      const stateData = data.subarray(CHECKPOINT_HEADER_SIZE, expectedSize);
      const state = this.serializer.decode(stateData);
      return {
        projectionName,
        position: header.position,
        state,
        timestamp: Number(header.timestamp)
      };
    } catch (error) {
      if (error instanceof CheckpointCorruptionError || error instanceof CheckpointVersionError) {
        throw error;
      }
      throw new CheckpointLoadError(projectionName, path, error instanceof Error ? error : new Error(String(error)));
    }
  }
  async deleteCheckpoint(projectionName) {
    const path = this.getCheckpointPath(projectionName);
    if (!await this.fs.exists(path)) {
      return false;
    }
    await this.fs.unlink(path);
    this.scheduledTimes.delete(projectionName);
    return true;
  }
  async listCheckpoints() {
    if (!await this.fs.exists(this.dataDir)) {
      return [];
    }
    const files = await this.fs.readdir(this.dataDir);
    return files.filter((f) => f.endsWith(".ckpt")).map((f) => f.slice(0, -5));
  }
  getCheckpointPath(projectionName) {
    const safeName = projectionName.replace(/[^a-zA-Z0-9_-]/g, "_");
    return `${this.dataDir}/${safeName}.ckpt`;
  }
}
// src/application/projections/projection-runner.ts
class ProjectionRunner {
  eventStore;
  projection;
  store;
  metadata;
  clock;
  pollingIntervalMs;
  checkpointIntervalMs;
  jitterMs;
  eventFilter;
  batchSize;
  useIncrementalStore;
  useSharedReader;
  subscribedEventTypes;
  currentPosition = -1;
  pollingTimer = null;
  running = false;
  eventsProcessed = 0;
  errorsCount = 0;
  lastCheckpointPosition = -1;
  lastCheckpointTime = 0;
  nextCheckpointTime = 0;
  stateDirty = false;
  constructor(config) {
    this.eventStore = config.eventStore;
    this.projection = config.projection;
    this.store = config.store;
    this.metadata = config.metadata;
    this.clock = config.clock;
    this.pollingIntervalMs = config.pollingIntervalMs ?? 100;
    this.checkpointIntervalMs = config.checkpointIntervalMs ?? 5000;
    this.jitterMs = config.jitterMs ?? 1000;
    this.eventFilter = config.eventFilter;
    this.batchSize = config.batchSize ?? 100;
    this.useSharedReader = config.useSharedReader ?? false;
    this.useIncrementalStore = typeof this.projection.applyToStore === "function" && typeof this.store.setByKey === "function";
    this.subscribedEventTypes = new Set(config.metadata.subscribedEvents);
  }
  async start() {
    if (this.running) {
      return;
    }
    const checkpointPosition = await this.store.load();
    if (checkpointPosition !== null) {
      this.currentPosition = checkpointPosition;
      this.lastCheckpointPosition = checkpointPosition;
      this.lastCheckpointTime = this.clock.now();
      this.projection.setState(this.store.get());
      this.stateDirty = false;
    }
    this.scheduleNextCheckpoint();
    this.running = true;
    if (!this.useSharedReader) {
      this.poll();
    }
  }
  async stop() {
    if (!this.running) {
      return;
    }
    this.running = false;
    if (this.pollingTimer) {
      this.pollingTimer.cancel();
      this.pollingTimer = null;
    }
    await this.checkpoint();
  }
  getStatus() {
    return {
      name: this.metadata.name,
      kind: this.metadata.kind,
      running: this.running,
      currentPosition: this.currentPosition,
      eventsProcessed: this.eventsProcessed,
      errorsCount: this.errorsCount,
      lastCheckpointPosition: this.lastCheckpointPosition,
      lastCheckpointTime: this.lastCheckpointTime,
      nextCheckpointTime: this.nextCheckpointTime
    };
  }
  getCurrentPosition() {
    return this.currentPosition;
  }
  processBatch(events) {
    if (!this.running || events.length === 0) {
      return;
    }
    this.applyEvents(events);
  }
  async checkpointIfNeeded() {
    await this.maybeCheckpoint();
  }
  async forceCheckpoint() {
    await this.checkpoint();
  }
  async waitForCatchUp(targetPosition, timeoutMs = 30000) {
    const startTime = this.clock.now();
    const realStartTime = Date.now();
    const isSimulatedClock = typeof this.clock.tick === "function";
    while (this.currentPosition < targetPosition) {
      const elapsed = this.clock.now() - startTime;
      const realElapsed = Date.now() - realStartTime;
      if (elapsed > timeoutMs || realElapsed > timeoutMs) {
        return false;
      }
      if (this.running) {
        const catchUpBudget = Math.min(1000, timeoutMs);
        await this.processEvents(catchUpBudget);
      }
      const remaining = timeoutMs - (this.clock.now() - startTime);
      if (remaining <= 0) {
        return false;
      }
      if (isSimulatedClock) {
        await Promise.resolve();
      } else {
        await this.clock.sleep(Math.min(10, remaining));
      }
    }
    return true;
  }
  poll() {
    if (!this.running) {
      return;
    }
    this.pollingTimer = this.clock.setTimeout(async () => {
      try {
        await this.processEvents();
        await this.maybeCheckpoint();
      } catch (error) {
        console.error(`[Projection: ${this.metadata.name}] Poll error:`, error);
        this.errorsCount++;
      }
      this.poll();
    }, this.pollingIntervalMs);
  }
  async processEvents(maxDurationMs) {
    const durationBudget = maxDurationMs ?? this.pollingIntervalMs;
    const startTime = this.clock.now();
    const realStartTime = Date.now();
    while (this.currentPosition < this.eventStore.getDurableGlobalPosition()) {
      const events = await this.eventStore.readGlobalDurable(this.currentPosition, {
        maxCount: this.batchSize
      });
      if (events.length === 0) {
        break;
      }
      this.applyEvents(events);
      if (!this.running) {
        break;
      }
      const elapsed = this.clock.now() - startTime;
      const realElapsed = Date.now() - realStartTime;
      if (elapsed >= durationBudget || realElapsed >= durationBudget) {
        break;
      }
      if (events.length < this.batchSize) {
        break;
      }
    }
    this.commitStateIfNeeded();
  }
  applyEvents(events) {
    let updated = false;
    for (const event of events) {
      if (event.globalPosition <= this.currentPosition) {
        continue;
      }
      const subscribesAll = this.subscribedEventTypes.has("*");
      if (!subscribesAll && !this.subscribedEventTypes.has(event.type)) {
        this.currentPosition = event.globalPosition;
        continue;
      }
      if (this.eventFilter && !this.eventFilter(event)) {
        this.currentPosition = event.globalPosition;
        continue;
      }
      try {
        this.projection.build(event);
        if (this.useIncrementalStore) {
          this.projection.applyToStore?.(event, this.store);
        }
        this.eventsProcessed++;
        this.currentPosition = event.globalPosition;
        updated = true;
      } catch (error) {
        this.errorsCount++;
        throw new ProjectionBuildError(this.metadata.name, event.type, event.globalPosition, error instanceof Error ? error : new Error(String(error)));
      }
    }
    if (updated && !this.useIncrementalStore) {
      this.stateDirty = true;
    }
  }
  commitStateIfNeeded() {
    if (!this.useIncrementalStore && this.stateDirty) {
      this.store.set(this.projection.getState());
      this.stateDirty = false;
    }
  }
  async maybeCheckpoint() {
    const now = this.clock.now();
    if (now >= this.nextCheckpointTime) {
      await this.checkpoint();
    }
  }
  async checkpoint() {
    if (this.currentPosition === this.lastCheckpointPosition) {
      this.scheduleNextCheckpoint();
      return;
    }
    try {
      this.commitStateIfNeeded();
      await this.store.persist(this.currentPosition);
      this.lastCheckpointPosition = this.currentPosition;
      this.lastCheckpointTime = this.clock.now();
    } catch (error) {
      console.error(`[Projection: ${this.metadata.name}] Checkpoint error:`, error);
      this.errorsCount++;
    }
    this.scheduleNextCheckpoint();
  }
  scheduleNextCheckpoint() {
    const now = this.clock.now();
    const jitter = (Math.random() * 2 - 1) * this.jitterMs;
    this.nextCheckpointTime = now + this.checkpointIntervalMs + jitter;
  }
}
// src/infrastructure/projections/default-registry.ts
class DefaultProjectionRegistry {
  registrations = new Map;
  defaultOptions = {
    checkpointIntervalMs: 5000,
    memoryThresholdBytes: 50 * 1024 * 1024,
    enabled: true
  };
  register(registration, options = {}) {
    const { name } = registration.metadata;
    if (this.registrations.has(name)) {
      throw new Error(`Projection '${name}' is already registered`);
    }
    const resolved = {
      registration,
      options: {
        checkpointIntervalMs: options.checkpointIntervalMs ?? registration.metadata.checkpointIntervalMs ?? this.defaultOptions.checkpointIntervalMs,
        memoryThresholdBytes: options.memoryThresholdBytes ?? this.defaultOptions.memoryThresholdBytes,
        enabled: options.enabled ?? this.defaultOptions.enabled,
        eventFilter: options.eventFilter
      }
    };
    this.registrations.set(name, resolved);
  }
  getAll() {
    return Array.from(this.registrations.values());
  }
  get(name) {
    return this.registrations.get(name);
  }
  has(name) {
    return this.registrations.has(name);
  }
  count() {
    return this.registrations.size;
  }
  clear() {
    this.registrations.clear();
  }
}

// src/infrastructure/projections/stores/aggregator-store.ts
var AGGREGATOR_MAGIC = 1095189328;
var AGGREGATOR_VERSION = 1;
var SUPPORTED_VERSIONS2 = [1];
var HEADER_SIZE = 32;

class AggregatorStore {
  fs;
  serializer;
  clock;
  dataDir;
  projectionName;
  state;
  initialState;
  constructor(config, initialState) {
    this.fs = config.fs;
    this.serializer = config.serializer;
    this.clock = config.clock;
    this.dataDir = config.dataDir;
    this.projectionName = config.projectionName;
    this.initialState = typeof initialState === "object" && initialState !== null ? JSON.parse(JSON.stringify(initialState)) : initialState;
    this.state = this.cloneState(this.initialState);
  }
  async initialize() {
    if (!await this.fs.exists(this.dataDir)) {
      await this.fs.mkdir(this.dataDir, { recursive: true });
    }
  }
  get() {
    return this.state;
  }
  set(state) {
    this.state = state;
  }
  async persist(position) {
    const path = this.getCheckpointPath();
    const tempPath = path + ".tmp";
    try {
      const stateData = this.serializer.encode(this.state);
      const totalSize = HEADER_SIZE + stateData.length;
      const buffer = new Uint8Array(totalSize);
      const view = new DataView(buffer.buffer);
      const timestamp = BigInt(this.clock.now());
      view.setUint32(0, AGGREGATOR_MAGIC, true);
      view.setUint32(4, AGGREGATOR_VERSION, true);
      view.setBigUint64(8, BigInt(position), true);
      view.setBigUint64(16, timestamp, true);
      view.setUint32(24, stateData.length, true);
      view.setUint32(28, 0, true);
      buffer.set(stateData, HEADER_SIZE);
      const beforeChecksum = buffer.subarray(0, 28);
      const afterChecksum = buffer.subarray(32);
      const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
      checksumData.set(beforeChecksum);
      checksumData.set(afterChecksum, beforeChecksum.length);
      const checksum = crc32(checksumData);
      view.setUint32(28, checksum, true);
      const handle = await this.fs.open(tempPath, "write");
      try {
        await this.fs.write(handle, buffer);
        await this.fs.sync(handle);
      } finally {
        await this.fs.close(handle);
      }
      await this.fs.rename(tempPath, path);
    } catch (error) {
      try {
        if (await this.fs.exists(tempPath)) {
          await this.fs.unlink(tempPath);
        }
      } catch {}
      throw new CheckpointWriteError(this.projectionName, path, error instanceof Error ? error : new Error(String(error)));
    }
  }
  async load() {
    const path = this.getCheckpointPath();
    if (!await this.fs.exists(path)) {
      return null;
    }
    try {
      const data = await this.fs.readFile(path);
      if (data.length < HEADER_SIZE) {
        throw new CheckpointCorruptionError(this.projectionName, path, `File too small: ${data.length} bytes`);
      }
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      const magic = view.getUint32(0, true);
      const version = view.getUint32(4, true);
      const position = Number(view.getBigUint64(8, true));
      const stateLength = view.getUint32(24, true);
      const storedChecksum = view.getUint32(28, true);
      if (magic !== AGGREGATOR_MAGIC) {
        throw new CheckpointCorruptionError(this.projectionName, path, `Invalid magic: 0x${magic.toString(16)}`);
      }
      if (!SUPPORTED_VERSIONS2.includes(version)) {
        throw new CheckpointVersionError(this.projectionName, path, version, SUPPORTED_VERSIONS2);
      }
      const expectedSize = HEADER_SIZE + stateLength;
      if (data.length < expectedSize) {
        throw new CheckpointCorruptionError(this.projectionName, path, `File too small for state: ${data.length} bytes, expected ${expectedSize}`);
      }
      const beforeChecksum = data.subarray(0, 28);
      const afterChecksum = data.subarray(32);
      const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
      checksumData.set(beforeChecksum);
      checksumData.set(afterChecksum, beforeChecksum.length);
      const calculatedChecksum = crc32(checksumData);
      if (calculatedChecksum !== storedChecksum) {
        throw new CheckpointCorruptionError(this.projectionName, path, `Checksum mismatch: expected 0x${storedChecksum.toString(16)}, got 0x${calculatedChecksum.toString(16)}`);
      }
      const stateData = data.subarray(HEADER_SIZE, expectedSize);
      this.state = this.serializer.decode(stateData);
      return position;
    } catch (error) {
      if (error instanceof CheckpointCorruptionError || error instanceof CheckpointVersionError) {
        throw error;
      }
      throw new CheckpointLoadError(this.projectionName, path, error instanceof Error ? error : new Error(String(error)));
    }
  }
  async close() {}
  getMemoryUsage() {
    try {
      const encoded = this.serializer.encode(this.state);
      return encoded.length;
    } catch {
      return 0;
    }
  }
  reset() {
    this.state = this.cloneState(this.initialState);
  }
  getCheckpointPath() {
    const safeName = this.projectionName.replace(/[^a-zA-Z0-9_-]/g, "_");
    return `${this.dataDir}/${safeName}.ckpt`;
  }
  cloneState(state) {
    if (typeof state === "object" && state !== null) {
      return JSON.parse(JSON.stringify(state));
    }
    return state;
  }
}

// src/infrastructure/projections/stores/index-structures.ts
class EqualityIndex {
  fieldName;
  index = new Map;
  reverse = new Map;
  constructor(fieldName) {
    this.fieldName = fieldName;
  }
  add(primaryKey, fieldValue) {
    let keys = this.index.get(fieldValue);
    if (!keys) {
      keys = new Set;
      this.index.set(fieldValue, keys);
    }
    keys.add(primaryKey);
    this.reverse.set(primaryKey, fieldValue);
  }
  remove(primaryKey, fieldValue) {
    const keys = this.index.get(fieldValue);
    if (keys) {
      keys.delete(primaryKey);
      if (keys.size === 0) {
        this.index.delete(fieldValue);
      }
    }
    this.reverse.delete(primaryKey);
  }
  update(primaryKey, oldValue, newValue) {
    if (oldValue === newValue) {
      return;
    }
    this.remove(primaryKey, oldValue);
    this.add(primaryKey, newValue);
  }
  updateFromKey(primaryKey, newValue) {
    const oldValue = this.reverse.get(primaryKey);
    if (oldValue !== undefined) {
      this.remove(primaryKey, oldValue);
    }
    this.add(primaryKey, newValue);
  }
  find(value) {
    return this.index.get(value) ?? new Set;
  }
  has(value) {
    const keys = this.index.get(value);
    return keys !== undefined && keys.size > 0;
  }
  getValues() {
    return Array.from(this.index.keys());
  }
  countFor(value) {
    return this.index.get(value)?.size ?? 0;
  }
  get size() {
    return this.reverse.size;
  }
  clear() {
    this.index.clear();
    this.reverse.clear();
  }
  getMemoryUsage() {
    let bytes = 0;
    bytes += this.index.size * 50;
    bytes += this.reverse.size * 50;
    for (const key of this.reverse.keys()) {
      bytes += key.length * 2;
    }
    for (const keys of this.index.values()) {
      bytes += keys.size * 30;
    }
    return bytes;
  }
  toJSON() {
    const entries = [];
    for (const [value, keys] of this.index) {
      entries.push([value, Array.from(keys)]);
    }
    return { entries };
  }
  static fromJSON(fieldName, data) {
    const index = new EqualityIndex(fieldName);
    for (const [value, keys] of data.entries) {
      for (const key of keys) {
        index.add(key, value);
      }
    }
    return index;
  }
}

class SortedIndex {
  entries = [];
  keyToIndex = new Map;
  dirty = false;
  set(key, value) {
    const existingIndex = this.keyToIndex.get(key);
    if (existingIndex !== undefined) {
      this.entries[existingIndex][1] = value;
    } else {
      this.entries.push([key, value]);
      this.keyToIndex.set(key, this.entries.length - 1);
      this.dirty = true;
    }
  }
  get(key) {
    this.ensureSorted();
    const index = this.binarySearchExact(key);
    return index >= 0 ? this.entries[index][1] : undefined;
  }
  has(key) {
    return this.keyToIndex.has(key);
  }
  delete(key) {
    const index = this.keyToIndex.get(key);
    if (index === undefined) {
      return false;
    }
    this.entries.splice(index, 1);
    this.rebuildKeyToIndex();
    return true;
  }
  queryRange(start, end) {
    this.ensureSorted();
    const result = new Map;
    let startIndex = 0;
    if (start !== undefined) {
      startIndex = this.binarySearchLeft(start);
    }
    for (let i = startIndex;i < this.entries.length; i++) {
      const entry = this.entries[i];
      const key = entry[0];
      if (end !== undefined && key > end) {
        break;
      }
      result.set(key, entry[1]);
    }
    return result;
  }
  getAll() {
    this.ensureSorted();
    return new Map(this.entries);
  }
  get size() {
    return this.entries.length;
  }
  clear() {
    this.entries = [];
    this.keyToIndex.clear();
    this.dirty = false;
  }
  getMemoryUsage() {
    let bytes = 0;
    bytes += this.entries.length * 30;
    for (const [key] of this.entries) {
      bytes += key.length * 2;
    }
    bytes += this.keyToIndex.size * 50;
    return bytes;
  }
  toJSON() {
    this.ensureSorted();
    return { entries: [...this.entries] };
  }
  static fromJSON(data) {
    const index = new SortedIndex;
    for (const [key, value] of data.entries) {
      index.set(key, value);
    }
    return index;
  }
  ensureSorted() {
    if (!this.dirty) {
      return;
    }
    this.entries.sort((a, b) => a[0].localeCompare(b[0]));
    this.rebuildKeyToIndex();
    this.dirty = false;
  }
  rebuildKeyToIndex() {
    this.keyToIndex.clear();
    for (let i = 0;i < this.entries.length; i++) {
      this.keyToIndex.set(this.entries[i][0], i);
    }
  }
  binarySearchExact(key) {
    let left = 0;
    let right = this.entries.length - 1;
    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      const cmp = this.entries[mid][0].localeCompare(key);
      if (cmp === 0) {
        return mid;
      } else if (cmp < 0) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }
    return -1;
  }
  binarySearchLeft(key) {
    let left = 0;
    let right = this.entries.length;
    while (left < right) {
      const mid = Math.floor((left + right) / 2);
      if (this.entries[mid][0].localeCompare(key) < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    return left;
  }
}

class IndexCollection {
  equalityIndexes = new Map;
  sortedIndexes = new Map;
  addEqualityIndex(fieldName) {
    const index = new EqualityIndex(fieldName);
    this.equalityIndexes.set(fieldName, index);
    return index;
  }
  addSortedIndex(fieldName) {
    const index = new SortedIndex;
    this.sortedIndexes.set(fieldName, index);
    return index;
  }
  getEqualityIndex(fieldName) {
    return this.equalityIndexes.get(fieldName);
  }
  getSortedIndex(fieldName) {
    return this.sortedIndexes.get(fieldName);
  }
  indexRow(primaryKey, row) {
    for (const [fieldName, index] of this.equalityIndexes) {
      const value = row[fieldName];
      if (value !== undefined) {
        index.updateFromKey(primaryKey, value);
      }
    }
    for (const [fieldName, index] of this.sortedIndexes) {
      const value = row[fieldName];
      if (value !== undefined) {
        index.set(primaryKey, value);
      }
    }
  }
  removeRow(primaryKey, row) {
    for (const [fieldName, index] of this.equalityIndexes) {
      const value = row[fieldName];
      if (value !== undefined) {
        index.remove(primaryKey, value);
      }
    }
    for (const [, index] of this.sortedIndexes) {
      index.delete(primaryKey);
    }
  }
  clear() {
    for (const index of this.equalityIndexes.values()) {
      index.clear();
    }
    for (const index of this.sortedIndexes.values()) {
      index.clear();
    }
  }
  getMemoryUsage() {
    let bytes = 0;
    for (const index of this.equalityIndexes.values()) {
      bytes += index.getMemoryUsage();
    }
    for (const index of this.sortedIndexes.values()) {
      bytes += index.getMemoryUsage();
    }
    return bytes;
  }
}

// src/infrastructure/projections/stores/denormalized-view-store.ts
var DENORMALIZED_MAGIC = 1145979728;
var DENORMALIZED_VERSION = 1;
var SUPPORTED_VERSIONS3 = [1];
var DEFAULT_MEMORY_THRESHOLD = 50 * 1024 * 1024;

class DenormalizedViewStore {
  fs;
  serializer;
  clock;
  dataDir;
  projectionName;
  memoryThreshold;
  rows = new Map;
  indexes;
  indexFields;
  rangeFields;
  constructor(config) {
    this.fs = config.fs;
    this.serializer = config.serializer;
    this.clock = config.clock;
    this.dataDir = config.dataDir;
    this.projectionName = config.projectionName;
    this.memoryThreshold = config.memoryThreshold ?? DEFAULT_MEMORY_THRESHOLD;
    this.indexFields = config.indexFields ?? [];
    this.rangeFields = config.rangeFields ?? [];
    this.indexes = new IndexCollection;
    for (const field of this.indexFields) {
      this.indexes.addEqualityIndex(field);
    }
    for (const field of this.rangeFields) {
      this.indexes.addSortedIndex(field);
    }
  }
  async initialize() {
    if (!await this.fs.exists(this.dataDir)) {
      await this.fs.mkdir(this.dataDir, { recursive: true });
    }
  }
  get() {
    return new Map(this.rows);
  }
  set(state) {
    this.rows.clear();
    this.indexes.clear();
    for (const [key, row] of state) {
      this.rows.set(key, row);
      this.indexes.indexRow(key, row);
    }
  }
  getByKey(key) {
    return this.rows.get(key);
  }
  setByKey(key, value) {
    const existingRow = this.rows.get(key);
    if (existingRow) {
      this.indexes.removeRow(key, existingRow);
    }
    this.rows.set(key, value);
    this.indexes.indexRow(key, value);
  }
  deleteByKey(key) {
    const row = this.rows.get(key);
    if (!row) {
      return false;
    }
    this.indexes.removeRow(key, row);
    this.rows.delete(key);
    return true;
  }
  query(filter) {
    if (Object.keys(filter).length === 0) {
      return Array.from(this.rows.values());
    }
    let candidateKeys = null;
    for (const [field, value] of Object.entries(filter)) {
      const index = this.indexes.getEqualityIndex(field);
      if (index) {
        const matchingKeys = index.find(value);
        if (candidateKeys === null) {
          candidateKeys = new Set(matchingKeys);
        } else {
          const intersection = new Set;
          for (const k of candidateKeys) {
            if (matchingKeys.has(k)) {
              intersection.add(k);
            }
          }
          candidateKeys = intersection;
        }
      }
    }
    if (candidateKeys === null) {
      candidateKeys = new Set(this.rows.keys());
    }
    const results = [];
    for (const key of candidateKeys) {
      const row = this.rows.get(key);
      if (row && this.matchesFilter(row, filter)) {
        results.push(row);
      }
    }
    return results;
  }
  queryRange(range) {
    const result = new Map;
    if (this.rangeFields.length > 0) {
      const rangeField = this.rangeFields[0];
      const sortedIndex = this.indexes.getSortedIndex(rangeField);
      if (sortedIndex) {
        const keyMatches = sortedIndex.queryRange(range.start, range.end);
        for (const [key] of keyMatches) {
          const row = this.rows.get(key);
          if (row) {
            result.set(key, row);
          }
        }
        return result;
      }
    }
    for (const [key, row] of this.rows) {
      if (this.keyInRange(key, range)) {
        result.set(key, row);
      }
    }
    return result;
  }
  get size() {
    return this.rows.size;
  }
  async persist(position) {
    const path = this.getCheckpointPath();
    const tempPath = path + ".tmp";
    try {
      const stateObject = {
        rows: Array.from(this.rows.entries()),
        indexFields: this.indexFields,
        rangeFields: this.rangeFields
      };
      const stateData = this.serializer.encode(stateObject);
      const headerSize = 32;
      const totalSize = headerSize + stateData.length;
      const buffer = new Uint8Array(totalSize);
      const view = new DataView(buffer.buffer);
      const timestamp = BigInt(this.clock.now());
      view.setUint32(0, DENORMALIZED_MAGIC, true);
      view.setUint32(4, DENORMALIZED_VERSION, true);
      view.setBigUint64(8, BigInt(position), true);
      view.setBigUint64(16, timestamp, true);
      view.setUint32(24, stateData.length, true);
      view.setUint32(28, 0, true);
      buffer.set(stateData, headerSize);
      const beforeChecksum = buffer.subarray(0, 28);
      const afterChecksum = buffer.subarray(32);
      const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
      checksumData.set(beforeChecksum);
      checksumData.set(afterChecksum, beforeChecksum.length);
      const checksum = crc32(checksumData);
      view.setUint32(28, checksum, true);
      const handle = await this.fs.open(tempPath, "write");
      try {
        await this.fs.write(handle, buffer);
        await this.fs.sync(handle);
      } finally {
        await this.fs.close(handle);
      }
      await this.fs.rename(tempPath, path);
    } catch (error) {
      try {
        if (await this.fs.exists(tempPath)) {
          await this.fs.unlink(tempPath);
        }
      } catch {}
      throw new CheckpointWriteError(this.projectionName, path, error instanceof Error ? error : new Error(String(error)));
    }
  }
  async load() {
    const path = this.getCheckpointPath();
    if (!await this.fs.exists(path)) {
      return null;
    }
    try {
      const data = await this.fs.readFile(path);
      const headerSize = 32;
      if (data.length < headerSize) {
        throw new CheckpointCorruptionError(this.projectionName, path, "File too small");
      }
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      const magic = view.getUint32(0, true);
      const version = view.getUint32(4, true);
      const position = Number(view.getBigUint64(8, true));
      const stateLength = view.getUint32(24, true);
      const storedChecksum = view.getUint32(28, true);
      if (magic !== DENORMALIZED_MAGIC) {
        throw new CheckpointCorruptionError(this.projectionName, path, `Invalid magic: 0x${magic.toString(16)}`);
      }
      if (!SUPPORTED_VERSIONS3.includes(version)) {
        throw new CheckpointVersionError(this.projectionName, path, version, SUPPORTED_VERSIONS3);
      }
      const expectedSize = headerSize + stateLength;
      if (data.length < expectedSize) {
        throw new CheckpointCorruptionError(this.projectionName, path, "File too small for state");
      }
      const beforeChecksum = data.subarray(0, 28);
      const afterChecksum = data.subarray(32);
      const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
      checksumData.set(beforeChecksum);
      checksumData.set(afterChecksum, beforeChecksum.length);
      const calculatedChecksum = crc32(checksumData);
      if (calculatedChecksum !== storedChecksum) {
        throw new CheckpointCorruptionError(this.projectionName, path, "Checksum mismatch");
      }
      const stateData = data.subarray(headerSize, expectedSize);
      const stateObject = this.serializer.decode(stateData);
      this.rows.clear();
      this.indexes.clear();
      for (const [key, row] of stateObject.rows) {
        this.rows.set(key, row);
        this.indexes.indexRow(key, row);
      }
      return position;
    } catch (error) {
      if (error instanceof CheckpointCorruptionError || error instanceof CheckpointVersionError) {
        throw error;
      }
      throw new CheckpointLoadError(this.projectionName, path, error instanceof Error ? error : new Error(String(error)));
    }
  }
  async close() {}
  getMemoryUsage() {
    let bytes = 0;
    try {
      const rowSample = this.rows.size > 0 ? this.rows.values().next().value : undefined;
      if (rowSample) {
        const sampleSize = this.serializer.encode(rowSample).length;
        bytes += this.rows.size * (sampleSize + 30);
      }
    } catch {
      bytes += this.rows.size * 200;
    }
    bytes += this.indexes.getMemoryUsage();
    return bytes;
  }
  clear() {
    this.rows.clear();
    this.indexes.clear();
  }
  isMemoryThresholdExceeded() {
    return this.getMemoryUsage() > this.memoryThreshold;
  }
  getCheckpointPath() {
    const safeName = this.projectionName.replace(/[^a-zA-Z0-9_-]/g, "_");
    return `${this.dataDir}/${safeName}.ckpt`;
  }
  matchesFilter(row, filter) {
    for (const [field, value] of Object.entries(filter)) {
      if (row[field] !== value) {
        return false;
      }
    }
    return true;
  }
  keyInRange(key, range) {
    if (range.start !== undefined && key < range.start) {
      return false;
    }
    if (range.end !== undefined && key > range.end) {
      return false;
    }
    return true;
  }
}

// src/application/projections/projection-coordinator.ts
class ProjectionCoordinator {
  config;
  registry;
  runners = new Map;
  stores = new Map;
  projectionInstances = new Map;
  running = false;
  lockHandle = null;
  sharedPollingTimer = null;
  sharedCursor = null;
  sharedPollingPaused = false;
  constructor(config) {
    this.config = {
      ...config,
      pollingIntervalMs: config.pollingIntervalMs ?? 100,
      defaultCheckpointIntervalMs: config.defaultCheckpointIntervalMs ?? 5000,
      checkpointJitterMs: config.checkpointJitterMs ?? 1000,
      defaultBatchSize: config.defaultBatchSize ?? 100,
      sharedReader: config.sharedReader ?? true
    };
    this.registry = new DefaultProjectionRegistry;
  }
  getRegistry() {
    return this.registry;
  }
  async start() {
    if (this.running) {
      throw new ProjectionCoordinatorError("Coordinator already running");
    }
    if (!this.config.eventStore.isOpen()) {
      throw new ProjectionCoordinatorError("EventStore is not open");
    }
    if (!await this.config.fs.exists(this.config.dataDir)) {
      await this.config.fs.mkdir(this.config.dataDir, { recursive: true });
    }
    const lockPath = `${this.config.dataDir}/.lock`;
    try {
      this.lockHandle = await this.config.fs.open(lockPath, "write");
      await this.config.fs.flock(this.lockHandle, "exclusive");
    } catch (error) {
      if (this.lockHandle) {
        try {
          await this.config.fs.close(this.lockHandle);
        } catch {}
        this.lockHandle = null;
      }
      throw new ProjectionCoordinatorError(`Failed to acquire lock on ${lockPath}: ${error instanceof Error ? error.message : String(error)}`);
    }
    try {
      const checkpointDir = `${this.config.dataDir}/checkpoints`;
      if (!await this.config.fs.exists(checkpointDir)) {
        await this.config.fs.mkdir(checkpointDir, { recursive: true });
      }
      const startPromises = [];
      for (const resolved of this.registry.getAll()) {
        if (!resolved.options.enabled) {
          continue;
        }
        const { registration, options } = resolved;
        const { metadata } = registration;
        const projection = registration.factory();
        const store = this.createStore(metadata.name, metadata.kind, projection, resolved);
        await store.initialize();
        const runner = new ProjectionRunner({
          eventStore: this.config.eventStore,
          projection,
          store,
          metadata,
          clock: this.config.clock,
          pollingIntervalMs: this.config.pollingIntervalMs,
          checkpointIntervalMs: options.checkpointIntervalMs,
          jitterMs: this.config.checkpointJitterMs,
          eventFilter: options.eventFilter,
          batchSize: this.config.defaultBatchSize,
          useSharedReader: this.config.sharedReader
        });
        this.runners.set(metadata.name, runner);
        this.stores.set(metadata.name, store);
        if (registration.getInstance) {
          this.projectionInstances.set(metadata.name, registration.getInstance());
        } else {
          this.projectionInstances.set(metadata.name, projection);
        }
        startPromises.push(runner.start());
      }
      await Promise.all(startPromises);
      this.running = true;
      if (this.config.sharedReader) {
        this.startSharedReader();
      }
    } catch (error) {
      await this.cleanupAfterStartFailure();
      throw error;
    }
  }
  async stop() {
    if (!this.running) {
      return;
    }
    if (this.sharedPollingTimer) {
      this.sharedPollingTimer.cancel();
      this.sharedPollingTimer = null;
    }
    const stopPromises = Array.from(this.runners.values()).map((runner) => runner.stop());
    await Promise.all(stopPromises);
    const closePromises = Array.from(this.stores.values()).map((store) => store.close());
    await Promise.all(closePromises);
    this.runners.clear();
    this.stores.clear();
    this.projectionInstances.clear();
    if (this.lockHandle) {
      try {
        await this.config.fs.close(this.lockHandle);
      } catch {}
      this.lockHandle = null;
    }
    this.running = false;
  }
  async cleanupAfterStartFailure() {
    if (this.sharedPollingTimer) {
      this.sharedPollingTimer.cancel();
      this.sharedPollingTimer = null;
    }
    const stopPromises = Array.from(this.runners.values()).map((runner) => runner.stop().catch(() => {
      return;
    }));
    await Promise.all(stopPromises);
    const closePromises = Array.from(this.stores.values()).map((store) => store.close().catch(() => {
      return;
    }));
    await Promise.all(closePromises);
    this.runners.clear();
    this.stores.clear();
    this.projectionInstances.clear();
    if (this.lockHandle) {
      try {
        await this.config.fs.close(this.lockHandle);
      } catch {}
      this.lockHandle = null;
    }
    this.running = false;
  }
  startSharedReader() {
    this.sharedCursor = null;
    this.sharedPollingPaused = false;
    this.scheduleSharedPoll();
  }
  scheduleSharedPoll(immediate = false) {
    if (!this.running) {
      return;
    }
    const delay = immediate ? 0 : this.config.pollingIntervalMs;
    this.sharedPollingTimer = this.config.clock.setTimeout(async () => {
      let hadEvents = false;
      try {
        if (!this.sharedPollingPaused) {
          hadEvents = await this.pollShared();
        }
      } catch (error) {
        console.error("[ProjectionCoordinator] Shared poll error:", error);
      } finally {
        this.scheduleSharedPoll(hadEvents);
      }
    }, delay);
  }
  async pollShared() {
    if (!this.running) {
      return false;
    }
    const startTime = this.config.clock.now();
    const realStart = Date.now();
    const durationBudget = this.config.pollingIntervalMs;
    let processedAny = false;
    while (this.running) {
      const events = await this.readSharedBatch();
      if (events.length === 0) {
        break;
      }
      processedAny = true;
      this.dispatchBatch(events);
      const elapsed = this.config.clock.now() - startTime;
      const realElapsed = Date.now() - realStart;
      if (elapsed >= durationBudget || realElapsed >= durationBudget) {
        await this.tickCheckpoints();
        return true;
      }
    }
    await this.tickCheckpoints();
    return false;
  }
  async tickCheckpoints() {
    const checkpointPromises = Array.from(this.runners.values()).map((runner) => runner.checkpointIfNeeded());
    await Promise.all(checkpointPromises);
  }
  dispatchBatch(events) {
    for (const runner of this.runners.values()) {
      try {
        runner.processBatch(events);
      } catch (error) {
        console.error(`[ProjectionCoordinator] Runner '${runner.getStatus().name}' error:`, error);
      }
    }
  }
  async readSharedBatch() {
    if (this.sharedCursor === null) {
      this.sharedCursor = this.getSharedStartCursor();
    }
    const cached = this.config.eventStore.readGlobalCached(this.sharedCursor, this.config.defaultBatchSize);
    if (cached !== null && cached.length > 0) {
      const last = cached[cached.length - 1];
      this.sharedCursor = last.globalPosition + 1;
      return cached;
    }
    const events = await this.config.eventStore.readGlobal(this.sharedCursor, {
      maxCount: this.config.defaultBatchSize
    });
    if (events.length > 0) {
      const last = events[events.length - 1];
      this.sharedCursor = last.globalPosition + 1;
    }
    return events;
  }
  getSharedStartCursor() {
    let minPosition = null;
    for (const runner of this.runners.values()) {
      const position = runner.getCurrentPosition();
      if (minPosition === null || position < minPosition) {
        minPosition = position;
      }
    }
    if (minPosition === null || minPosition < 0) {
      return 0;
    }
    return minPosition + 1;
  }
  getProjection(name) {
    return this.projectionInstances.get(name);
  }
  requireProjection(name) {
    const resolved = this.registry.get(name);
    if (!resolved) {
      throw new ProjectionNotFoundError(name);
    }
    if (!resolved.options.enabled) {
      throw new ProjectionDisabledError(name);
    }
    const projection = this.projectionInstances.get(name);
    if (!projection) {
      throw new ProjectionNotFoundError(name);
    }
    return projection;
  }
  async waitForCatchUp(timeoutMs = 30000) {
    const globalPosition = this.config.eventStore.getGlobalPosition();
    const targetPosition = globalPosition > 0 ? globalPosition - 1 : -1;
    if (this.config.sharedReader) {
      const success = await this.drainSharedUntil(targetPosition, timeoutMs);
      if (!success) {
        const firstRunner = this.runners.values().next().value;
        const current = firstRunner ? firstRunner.getCurrentPosition() : -1;
        const name = firstRunner ? firstRunner.getStatus().name : "unknown";
        throw new ProjectionCatchUpTimeoutError(name, current, targetPosition, timeoutMs);
      }
      return;
    }
    const catchUpPromises = Array.from(this.runners.entries()).map(async ([name, runner]) => {
      const success = await runner.waitForCatchUp(targetPosition, timeoutMs);
      if (!success) {
        throw new ProjectionCatchUpTimeoutError(name, runner.getCurrentPosition(), targetPosition, timeoutMs);
      }
    });
    await Promise.all(catchUpPromises);
  }
  async drainSharedUntil(targetPosition, timeoutMs) {
    const startTime = this.config.clock.now();
    const realStart = Date.now();
    this.sharedPollingPaused = true;
    try {
      while (this.getMinRunnerPosition() < targetPosition) {
        const events = await this.readSharedBatch();
        if (events.length > 0) {
          this.dispatchBatch(events);
          await this.tickCheckpoints();
        } else {
          await this.config.clock.sleep(10);
        }
        const elapsed = this.config.clock.now() - startTime;
        const realElapsed = Date.now() - realStart;
        if (elapsed > timeoutMs || realElapsed > timeoutMs) {
          return false;
        }
      }
    } finally {
      this.sharedPollingPaused = false;
    }
    return true;
  }
  getMinRunnerPosition() {
    let minPosition = null;
    for (const runner of this.runners.values()) {
      const position = runner.getCurrentPosition();
      if (minPosition === null || position < minPosition) {
        minPosition = position;
      }
    }
    return minPosition ?? -1;
  }
  async forceCheckpoint() {
    const checkpointPromises = Array.from(this.runners.values()).map((runner) => runner.forceCheckpoint());
    await Promise.all(checkpointPromises);
  }
  getStatus() {
    const projections = Array.from(this.runners.values()).map((runner) => runner.getStatus());
    const enabledCount = this.registry.getAll().filter((r) => r.options.enabled).length;
    return {
      running: this.running,
      registeredCount: this.registry.count(),
      enabledCount,
      projections,
      globalPosition: this.config.eventStore.isOpen() ? this.config.eventStore.getGlobalPosition() : 0
    };
  }
  getProjectionStatus(name) {
    return this.runners.get(name)?.getStatus();
  }
  isRunning() {
    return this.running;
  }
  createStore(name, kind, projection, resolved) {
    const storeConfig = {
      fs: this.config.fs,
      serializer: this.config.serializer,
      clock: this.config.clock,
      dataDir: `${this.config.dataDir}/checkpoints`,
      projectionName: name
    };
    switch (kind) {
      case "aggregator":
        projection.reset();
        const initialState = projection.getState();
        return new AggregatorStore(storeConfig, initialState);
      case "denormalized_view":
        const { accessPatterns } = resolved.registration.metadata;
        const indexFields = [];
        const rangeFields = [];
        for (const pattern of accessPatterns) {
          if (pattern.isRange) {
            rangeFields.push(...pattern.indexFields);
          } else {
            indexFields.push(...pattern.indexFields);
          }
        }
        return new DenormalizedViewStore({
          ...storeConfig,
          indexFields: [...new Set(indexFields)],
          rangeFields: [...new Set(rangeFields)],
          memoryThreshold: resolved.options.memoryThresholdBytes
        });
      default:
        throw new ProjectionCoordinatorError(`Unknown projection kind: ${kind}`);
    }
  }
}
// src/infrastructure/filesystem/bun-filesystem.ts
import {
  statSync,
  existsSync,
  renameSync,
  unlinkSync,
  mkdirSync,
  readdirSync,
  rmdirSync
} from "fs";
import { open as openAsync } from "fs/promises";
var handles = new Map;

class BunFileSystem {
  async open(path, mode) {
    const flags = this.modeToFlags(mode);
    const nodeHandle = await openAsync(path, flags);
    const fd = nodeHandle.fd;
    handles.set(fd, nodeHandle);
    return { fd };
  }
  async close(handle) {
    const nodeHandle = handles.get(handle.fd);
    if (nodeHandle) {
      await nodeHandle.close();
      handles.delete(handle.fd);
    }
  }
  async read(handle, offset, length) {
    const nodeHandle = handles.get(handle.fd);
    if (!nodeHandle) {
      throw new Error(`Invalid file handle: fd=${handle.fd}`);
    }
    const buffer = new Uint8Array(length);
    const result = await nodeHandle.read(buffer, 0, length, offset);
    return buffer.subarray(0, result.bytesRead);
  }
  async write(handle, data, offset) {
    const nodeHandle = handles.get(handle.fd);
    if (!nodeHandle) {
      throw new Error(`Invalid file handle: fd=${handle.fd}`);
    }
    const result = await nodeHandle.write(data, 0, data.length, offset ?? null);
    return result.bytesWritten;
  }
  async sync(handle) {
    const nodeHandle = handles.get(handle.fd);
    if (!nodeHandle) {
      throw new Error(`Invalid file handle: fd=${handle.fd}`);
    }
    await nodeHandle.sync();
  }
  async readFile(path) {
    return Bun.file(path).bytes();
  }
  async readFileSlice(path, start, end) {
    const file = Bun.file(path);
    const blob = file.slice(start, end);
    const arrayBuffer = await blob.arrayBuffer();
    return new Uint8Array(arrayBuffer);
  }
  async stat(path) {
    const stats = statSync(path);
    return {
      size: stats.size,
      isFile: stats.isFile(),
      isDirectory: stats.isDirectory(),
      mtime: stats.mtime
    };
  }
  async exists(path) {
    return existsSync(path);
  }
  async truncate(handle, length) {
    const nodeHandle = handles.get(handle.fd);
    if (!nodeHandle) {
      throw new Error(`Invalid file handle: fd=${handle.fd}`);
    }
    await nodeHandle.truncate(length);
  }
  async rename(from, to) {
    renameSync(from, to);
  }
  async unlink(path) {
    unlinkSync(path);
  }
  async mkdir(path, options) {
    mkdirSync(path, { recursive: options?.recursive ?? false });
  }
  async readdir(path) {
    return readdirSync(path);
  }
  async rmdir(path, options) {
    rmdirSync(path, { recursive: options?.recursive ?? false });
  }
  async mmap(path) {
    try {
      return Bun.mmap(path);
    } catch {
      return await Bun.file(path).bytes();
    }
  }
  async flock(handle, mode) {
    const operation = mode === "exclusive" ? LOCK_EX : LOCK_SH;
    const success = flock(handle.fd, operation);
    if (!success) {
      throw new Error(`Failed to acquire ${mode} lock on file descriptor ${handle.fd}. ` + `Another process may be holding an incompatible lock.`);
    }
  }
  async funlock(handle) {
    const success = flock(handle.fd, LOCK_UN);
    if (!success) {
      throw new Error(`Failed to release lock on file descriptor ${handle.fd}`);
    }
  }
  modeToFlags(mode) {
    switch (mode) {
      case "read":
        return "r";
      case "write":
        return "w";
      case "append":
        return "a";
      case "readwrite":
        return "r+";
    }
  }
}

// src/infrastructure/time/bun-clock.ts
class BunTimer {
  handle;
  constructor(handle) {
    this.handle = handle;
  }
  cancel() {
    clearTimeout(this.handle);
  }
}

class BunClock {
  now() {
    return Date.now();
  }
  async sleep(ms) {
    return Bun.sleep(ms);
  }
  setTimeout(callback, ms) {
    return new BunTimer(setTimeout(callback, ms));
  }
  setInterval(callback, ms) {
    return new BunTimer(setInterval(callback, ms));
  }
}

// ../../node_modules/.bun/msgpackr@1.11.8/node_modules/msgpackr/unpack.js
var decoder;
try {
  decoder = new TextDecoder;
} catch (error) {}
var src;
var srcEnd;
var position = 0;
var EMPTY_ARRAY = [];
var strings = EMPTY_ARRAY;
var stringPosition = 0;
var currentUnpackr = {};
var currentStructures;
var srcString;
var srcStringStart = 0;
var srcStringEnd = 0;
var bundledStrings;
var referenceMap;
var currentExtensions = [];
var dataView;
var defaultOptions = {
  useRecords: false,
  mapsAsObjects: true
};

class C1Type {
}
var C1 = new C1Type;
C1.name = "MessagePack 0xC1";
var sequentialMode = false;
var inlineObjectReadThreshold = 2;
var readStruct;
var onLoadedStructures;
var onSaveState;
try {
  new Function("");
} catch (error) {
  inlineObjectReadThreshold = Infinity;
}

class Unpackr {
  constructor(options) {
    if (options) {
      if (options.useRecords === false && options.mapsAsObjects === undefined)
        options.mapsAsObjects = true;
      if (options.sequential && options.trusted !== false) {
        options.trusted = true;
        if (!options.structures && options.useRecords != false) {
          options.structures = [];
          if (!options.maxSharedStructures)
            options.maxSharedStructures = 0;
        }
      }
      if (options.structures)
        options.structures.sharedLength = options.structures.length;
      else if (options.getStructures) {
        (options.structures = []).uninitialized = true;
        options.structures.sharedLength = 0;
      }
      if (options.int64AsNumber) {
        options.int64AsType = "number";
      }
    }
    Object.assign(this, options);
  }
  unpack(source, options) {
    if (src) {
      return saveState(() => {
        clearSource();
        return this ? this.unpack(source, options) : Unpackr.prototype.unpack.call(defaultOptions, source, options);
      });
    }
    if (!source.buffer && source.constructor === ArrayBuffer)
      source = typeof Buffer !== "undefined" ? Buffer.from(source) : new Uint8Array(source);
    if (typeof options === "object") {
      srcEnd = options.end || source.length;
      position = options.start || 0;
    } else {
      position = 0;
      srcEnd = options > -1 ? options : source.length;
    }
    stringPosition = 0;
    srcStringEnd = 0;
    srcString = null;
    strings = EMPTY_ARRAY;
    bundledStrings = null;
    src = source;
    try {
      dataView = source.dataView || (source.dataView = new DataView(source.buffer, source.byteOffset, source.byteLength));
    } catch (error) {
      src = null;
      if (source instanceof Uint8Array)
        throw error;
      throw new Error("Source must be a Uint8Array or Buffer but was a " + (source && typeof source == "object" ? source.constructor.name : typeof source));
    }
    if (this instanceof Unpackr) {
      currentUnpackr = this;
      if (this.structures) {
        currentStructures = this.structures;
        return checkedRead(options);
      } else if (!currentStructures || currentStructures.length > 0) {
        currentStructures = [];
      }
    } else {
      currentUnpackr = defaultOptions;
      if (!currentStructures || currentStructures.length > 0)
        currentStructures = [];
    }
    return checkedRead(options);
  }
  unpackMultiple(source, forEach) {
    let values, lastPosition = 0;
    try {
      sequentialMode = true;
      let size = source.length;
      let value = this ? this.unpack(source, size) : defaultUnpackr.unpack(source, size);
      if (forEach) {
        if (forEach(value, lastPosition, position) === false)
          return;
        while (position < size) {
          lastPosition = position;
          if (forEach(checkedRead(), lastPosition, position) === false) {
            return;
          }
        }
      } else {
        values = [value];
        while (position < size) {
          lastPosition = position;
          values.push(checkedRead());
        }
        return values;
      }
    } catch (error) {
      error.lastPosition = lastPosition;
      error.values = values;
      throw error;
    } finally {
      sequentialMode = false;
      clearSource();
    }
  }
  _mergeStructures(loadedStructures, existingStructures) {
    if (onLoadedStructures)
      loadedStructures = onLoadedStructures.call(this, loadedStructures);
    loadedStructures = loadedStructures || [];
    if (Object.isFrozen(loadedStructures))
      loadedStructures = loadedStructures.map((structure) => structure.slice(0));
    for (let i = 0, l = loadedStructures.length;i < l; i++) {
      let structure = loadedStructures[i];
      if (structure) {
        structure.isShared = true;
        if (i >= 32)
          structure.highByte = i - 32 >> 5;
      }
    }
    loadedStructures.sharedLength = loadedStructures.length;
    for (let id in existingStructures || []) {
      if (id >= 0) {
        let structure = loadedStructures[id];
        let existing = existingStructures[id];
        if (existing) {
          if (structure)
            (loadedStructures.restoreStructures || (loadedStructures.restoreStructures = []))[id] = structure;
          loadedStructures[id] = existing;
        }
      }
    }
    return this.structures = loadedStructures;
  }
  decode(source, options) {
    return this.unpack(source, options);
  }
}
function checkedRead(options) {
  try {
    if (!currentUnpackr.trusted && !sequentialMode) {
      let sharedLength = currentStructures.sharedLength || 0;
      if (sharedLength < currentStructures.length)
        currentStructures.length = sharedLength;
    }
    let result;
    if (currentUnpackr.randomAccessStructure && src[position] < 64 && src[position] >= 32 && readStruct) {
      result = readStruct(src, position, srcEnd, currentUnpackr);
      src = null;
      if (!(options && options.lazy) && result)
        result = result.toJSON();
      position = srcEnd;
    } else
      result = read();
    if (bundledStrings) {
      position = bundledStrings.postBundlePosition;
      bundledStrings = null;
    }
    if (sequentialMode)
      currentStructures.restoreStructures = null;
    if (position == srcEnd) {
      if (currentStructures && currentStructures.restoreStructures)
        restoreStructures();
      currentStructures = null;
      src = null;
      if (referenceMap)
        referenceMap = null;
    } else if (position > srcEnd) {
      throw new Error("Unexpected end of MessagePack data");
    } else if (!sequentialMode) {
      let jsonView;
      try {
        jsonView = JSON.stringify(result, (_, value) => typeof value === "bigint" ? `${value}n` : value).slice(0, 100);
      } catch (error) {
        jsonView = "(JSON view not available " + error + ")";
      }
      throw new Error("Data read, but end of buffer not reached " + jsonView);
    }
    return result;
  } catch (error) {
    if (currentStructures && currentStructures.restoreStructures)
      restoreStructures();
    clearSource();
    if (error instanceof RangeError || error.message.startsWith("Unexpected end of buffer") || position > srcEnd) {
      error.incomplete = true;
    }
    throw error;
  }
}
function restoreStructures() {
  for (let id in currentStructures.restoreStructures) {
    currentStructures[id] = currentStructures.restoreStructures[id];
  }
  currentStructures.restoreStructures = null;
}
function read() {
  let token = src[position++];
  if (token < 160) {
    if (token < 128) {
      if (token < 64)
        return token;
      else {
        let structure = currentStructures[token & 63] || currentUnpackr.getStructures && loadStructures()[token & 63];
        if (structure) {
          if (!structure.read) {
            structure.read = createStructureReader(structure, token & 63);
          }
          return structure.read();
        } else
          return token;
      }
    } else if (token < 144) {
      token -= 128;
      if (currentUnpackr.mapsAsObjects) {
        let object = {};
        for (let i = 0;i < token; i++) {
          let key = readKey();
          if (key === "__proto__")
            key = "__proto_";
          object[key] = read();
        }
        return object;
      } else {
        let map = new Map;
        for (let i = 0;i < token; i++) {
          map.set(read(), read());
        }
        return map;
      }
    } else {
      token -= 144;
      let array = new Array(token);
      for (let i = 0;i < token; i++) {
        array[i] = read();
      }
      if (currentUnpackr.freezeData)
        return Object.freeze(array);
      return array;
    }
  } else if (token < 192) {
    let length = token - 160;
    if (srcStringEnd >= position) {
      return srcString.slice(position - srcStringStart, (position += length) - srcStringStart);
    }
    if (srcStringEnd == 0 && srcEnd < 140) {
      let string = length < 16 ? shortStringInJS(length) : longStringInJS(length);
      if (string != null)
        return string;
    }
    return readFixedString(length);
  } else {
    let value;
    switch (token) {
      case 192:
        return null;
      case 193:
        if (bundledStrings) {
          value = read();
          if (value > 0)
            return bundledStrings[1].slice(bundledStrings.position1, bundledStrings.position1 += value);
          else
            return bundledStrings[0].slice(bundledStrings.position0, bundledStrings.position0 -= value);
        }
        return C1;
      case 194:
        return false;
      case 195:
        return true;
      case 196:
        value = src[position++];
        if (value === undefined)
          throw new Error("Unexpected end of buffer");
        return readBin(value);
      case 197:
        value = dataView.getUint16(position);
        position += 2;
        return readBin(value);
      case 198:
        value = dataView.getUint32(position);
        position += 4;
        return readBin(value);
      case 199:
        return readExt(src[position++]);
      case 200:
        value = dataView.getUint16(position);
        position += 2;
        return readExt(value);
      case 201:
        value = dataView.getUint32(position);
        position += 4;
        return readExt(value);
      case 202:
        value = dataView.getFloat32(position);
        if (currentUnpackr.useFloat32 > 2) {
          let multiplier = mult10[(src[position] & 127) << 1 | src[position + 1] >> 7];
          position += 4;
          return (multiplier * value + (value > 0 ? 0.5 : -0.5) >> 0) / multiplier;
        }
        position += 4;
        return value;
      case 203:
        value = dataView.getFloat64(position);
        position += 8;
        return value;
      case 204:
        return src[position++];
      case 205:
        value = dataView.getUint16(position);
        position += 2;
        return value;
      case 206:
        value = dataView.getUint32(position);
        position += 4;
        return value;
      case 207:
        if (currentUnpackr.int64AsType === "number") {
          value = dataView.getUint32(position) * 4294967296;
          value += dataView.getUint32(position + 4);
        } else if (currentUnpackr.int64AsType === "string") {
          value = dataView.getBigUint64(position).toString();
        } else if (currentUnpackr.int64AsType === "auto") {
          value = dataView.getBigUint64(position);
          if (value <= BigInt(2) << BigInt(52))
            value = Number(value);
        } else
          value = dataView.getBigUint64(position);
        position += 8;
        return value;
      case 208:
        return dataView.getInt8(position++);
      case 209:
        value = dataView.getInt16(position);
        position += 2;
        return value;
      case 210:
        value = dataView.getInt32(position);
        position += 4;
        return value;
      case 211:
        if (currentUnpackr.int64AsType === "number") {
          value = dataView.getInt32(position) * 4294967296;
          value += dataView.getUint32(position + 4);
        } else if (currentUnpackr.int64AsType === "string") {
          value = dataView.getBigInt64(position).toString();
        } else if (currentUnpackr.int64AsType === "auto") {
          value = dataView.getBigInt64(position);
          if (value >= BigInt(-2) << BigInt(52) && value <= BigInt(2) << BigInt(52))
            value = Number(value);
        } else
          value = dataView.getBigInt64(position);
        position += 8;
        return value;
      case 212:
        value = src[position++];
        if (value == 114) {
          return recordDefinition(src[position++] & 63);
        } else {
          let extension = currentExtensions[value];
          if (extension) {
            if (extension.read) {
              position++;
              return extension.read(read());
            } else if (extension.noBuffer) {
              position++;
              return extension();
            } else
              return extension(src.subarray(position, ++position));
          } else
            throw new Error("Unknown extension " + value);
        }
      case 213:
        value = src[position];
        if (value == 114) {
          position++;
          return recordDefinition(src[position++] & 63, src[position++]);
        } else
          return readExt(2);
      case 214:
        return readExt(4);
      case 215:
        return readExt(8);
      case 216:
        return readExt(16);
      case 217:
        value = src[position++];
        if (srcStringEnd >= position) {
          return srcString.slice(position - srcStringStart, (position += value) - srcStringStart);
        }
        return readString8(value);
      case 218:
        value = dataView.getUint16(position);
        position += 2;
        if (srcStringEnd >= position) {
          return srcString.slice(position - srcStringStart, (position += value) - srcStringStart);
        }
        return readString16(value);
      case 219:
        value = dataView.getUint32(position);
        position += 4;
        if (srcStringEnd >= position) {
          return srcString.slice(position - srcStringStart, (position += value) - srcStringStart);
        }
        return readString32(value);
      case 220:
        value = dataView.getUint16(position);
        position += 2;
        return readArray(value);
      case 221:
        value = dataView.getUint32(position);
        position += 4;
        return readArray(value);
      case 222:
        value = dataView.getUint16(position);
        position += 2;
        return readMap(value);
      case 223:
        value = dataView.getUint32(position);
        position += 4;
        return readMap(value);
      default:
        if (token >= 224)
          return token - 256;
        if (token === undefined) {
          let error = new Error("Unexpected end of MessagePack data");
          error.incomplete = true;
          throw error;
        }
        throw new Error("Unknown MessagePack token " + token);
    }
  }
}
var validName = /^[a-zA-Z_$][a-zA-Z\d_$]*$/;
function createStructureReader(structure, firstId) {
  function readObject() {
    if (readObject.count++ > inlineObjectReadThreshold) {
      let readObject2 = structure.read = new Function("r", "return function(){return " + (currentUnpackr.freezeData ? "Object.freeze" : "") + "({" + structure.map((key) => key === "__proto__" ? "__proto_:r()" : validName.test(key) ? key + ":r()" : "[" + JSON.stringify(key) + "]:r()").join(",") + "})}")(read);
      if (structure.highByte === 0)
        structure.read = createSecondByteReader(firstId, structure.read);
      return readObject2();
    }
    let object = {};
    for (let i = 0, l = structure.length;i < l; i++) {
      let key = structure[i];
      if (key === "__proto__")
        key = "__proto_";
      object[key] = read();
    }
    if (currentUnpackr.freezeData)
      return Object.freeze(object);
    return object;
  }
  readObject.count = 0;
  if (structure.highByte === 0) {
    return createSecondByteReader(firstId, readObject);
  }
  return readObject;
}
var createSecondByteReader = (firstId, read0) => {
  return function() {
    let highByte = src[position++];
    if (highByte === 0)
      return read0();
    let id = firstId < 32 ? -(firstId + (highByte << 5)) : firstId + (highByte << 5);
    let structure = currentStructures[id] || loadStructures()[id];
    if (!structure) {
      throw new Error("Record id is not defined for " + id);
    }
    if (!structure.read)
      structure.read = createStructureReader(structure, firstId);
    return structure.read();
  };
};
function loadStructures() {
  let loadedStructures = saveState(() => {
    src = null;
    return currentUnpackr.getStructures();
  });
  return currentStructures = currentUnpackr._mergeStructures(loadedStructures, currentStructures);
}
var readFixedString = readStringJS;
var readString8 = readStringJS;
var readString16 = readStringJS;
var readString32 = readStringJS;
var isNativeAccelerationEnabled = false;
function setExtractor(extractStrings) {
  isNativeAccelerationEnabled = true;
  readFixedString = readString(1);
  readString8 = readString(2);
  readString16 = readString(3);
  readString32 = readString(5);
  function readString(headerLength) {
    return function readString(length) {
      let string = strings[stringPosition++];
      if (string == null) {
        if (bundledStrings)
          return readStringJS(length);
        let byteOffset = src.byteOffset;
        let extraction = extractStrings(position - headerLength + byteOffset, srcEnd + byteOffset, src.buffer);
        if (typeof extraction == "string") {
          string = extraction;
          strings = EMPTY_ARRAY;
        } else {
          strings = extraction;
          stringPosition = 1;
          srcStringEnd = 1;
          string = strings[0];
          if (string === undefined)
            throw new Error("Unexpected end of buffer");
        }
      }
      let srcStringLength = string.length;
      if (srcStringLength <= length) {
        position += length;
        return string;
      }
      srcString = string;
      srcStringStart = position;
      srcStringEnd = position + srcStringLength;
      position += length;
      return string.slice(0, length);
    };
  }
}
function readStringJS(length) {
  let result;
  if (length < 16) {
    if (result = shortStringInJS(length))
      return result;
  }
  if (length > 64 && decoder)
    return decoder.decode(src.subarray(position, position += length));
  const end = position + length;
  const units = [];
  result = "";
  while (position < end) {
    const byte1 = src[position++];
    if ((byte1 & 128) === 0) {
      units.push(byte1);
    } else if ((byte1 & 224) === 192) {
      const byte2 = src[position++] & 63;
      units.push((byte1 & 31) << 6 | byte2);
    } else if ((byte1 & 240) === 224) {
      const byte2 = src[position++] & 63;
      const byte3 = src[position++] & 63;
      units.push((byte1 & 31) << 12 | byte2 << 6 | byte3);
    } else if ((byte1 & 248) === 240) {
      const byte2 = src[position++] & 63;
      const byte3 = src[position++] & 63;
      const byte4 = src[position++] & 63;
      let unit = (byte1 & 7) << 18 | byte2 << 12 | byte3 << 6 | byte4;
      if (unit > 65535) {
        unit -= 65536;
        units.push(unit >>> 10 & 1023 | 55296);
        unit = 56320 | unit & 1023;
      }
      units.push(unit);
    } else {
      units.push(byte1);
    }
    if (units.length >= 4096) {
      result += fromCharCode.apply(String, units);
      units.length = 0;
    }
  }
  if (units.length > 0) {
    result += fromCharCode.apply(String, units);
  }
  return result;
}
function readString(source, start, length) {
  let existingSrc = src;
  src = source;
  position = start;
  try {
    return readStringJS(length);
  } finally {
    src = existingSrc;
  }
}
function readArray(length) {
  let array = new Array(length);
  for (let i = 0;i < length; i++) {
    array[i] = read();
  }
  if (currentUnpackr.freezeData)
    return Object.freeze(array);
  return array;
}
function readMap(length) {
  if (currentUnpackr.mapsAsObjects) {
    let object = {};
    for (let i = 0;i < length; i++) {
      let key = readKey();
      if (key === "__proto__")
        key = "__proto_";
      object[key] = read();
    }
    return object;
  } else {
    let map = new Map;
    for (let i = 0;i < length; i++) {
      map.set(read(), read());
    }
    return map;
  }
}
var fromCharCode = String.fromCharCode;
function longStringInJS(length) {
  let start = position;
  let bytes = new Array(length);
  for (let i = 0;i < length; i++) {
    const byte = src[position++];
    if ((byte & 128) > 0) {
      position = start;
      return;
    }
    bytes[i] = byte;
  }
  return fromCharCode.apply(String, bytes);
}
function shortStringInJS(length) {
  if (length < 4) {
    if (length < 2) {
      if (length === 0)
        return "";
      else {
        let a = src[position++];
        if ((a & 128) > 1) {
          position -= 1;
          return;
        }
        return fromCharCode(a);
      }
    } else {
      let a = src[position++];
      let b = src[position++];
      if ((a & 128) > 0 || (b & 128) > 0) {
        position -= 2;
        return;
      }
      if (length < 3)
        return fromCharCode(a, b);
      let c = src[position++];
      if ((c & 128) > 0) {
        position -= 3;
        return;
      }
      return fromCharCode(a, b, c);
    }
  } else {
    let a = src[position++];
    let b = src[position++];
    let c = src[position++];
    let d = src[position++];
    if ((a & 128) > 0 || (b & 128) > 0 || (c & 128) > 0 || (d & 128) > 0) {
      position -= 4;
      return;
    }
    if (length < 6) {
      if (length === 4)
        return fromCharCode(a, b, c, d);
      else {
        let e = src[position++];
        if ((e & 128) > 0) {
          position -= 5;
          return;
        }
        return fromCharCode(a, b, c, d, e);
      }
    } else if (length < 8) {
      let e = src[position++];
      let f = src[position++];
      if ((e & 128) > 0 || (f & 128) > 0) {
        position -= 6;
        return;
      }
      if (length < 7)
        return fromCharCode(a, b, c, d, e, f);
      let g = src[position++];
      if ((g & 128) > 0) {
        position -= 7;
        return;
      }
      return fromCharCode(a, b, c, d, e, f, g);
    } else {
      let e = src[position++];
      let f = src[position++];
      let g = src[position++];
      let h = src[position++];
      if ((e & 128) > 0 || (f & 128) > 0 || (g & 128) > 0 || (h & 128) > 0) {
        position -= 8;
        return;
      }
      if (length < 10) {
        if (length === 8)
          return fromCharCode(a, b, c, d, e, f, g, h);
        else {
          let i = src[position++];
          if ((i & 128) > 0) {
            position -= 9;
            return;
          }
          return fromCharCode(a, b, c, d, e, f, g, h, i);
        }
      } else if (length < 12) {
        let i = src[position++];
        let j = src[position++];
        if ((i & 128) > 0 || (j & 128) > 0) {
          position -= 10;
          return;
        }
        if (length < 11)
          return fromCharCode(a, b, c, d, e, f, g, h, i, j);
        let k = src[position++];
        if ((k & 128) > 0) {
          position -= 11;
          return;
        }
        return fromCharCode(a, b, c, d, e, f, g, h, i, j, k);
      } else {
        let i = src[position++];
        let j = src[position++];
        let k = src[position++];
        let l = src[position++];
        if ((i & 128) > 0 || (j & 128) > 0 || (k & 128) > 0 || (l & 128) > 0) {
          position -= 12;
          return;
        }
        if (length < 14) {
          if (length === 12)
            return fromCharCode(a, b, c, d, e, f, g, h, i, j, k, l);
          else {
            let m = src[position++];
            if ((m & 128) > 0) {
              position -= 13;
              return;
            }
            return fromCharCode(a, b, c, d, e, f, g, h, i, j, k, l, m);
          }
        } else {
          let m = src[position++];
          let n = src[position++];
          if ((m & 128) > 0 || (n & 128) > 0) {
            position -= 14;
            return;
          }
          if (length < 15)
            return fromCharCode(a, b, c, d, e, f, g, h, i, j, k, l, m, n);
          let o = src[position++];
          if ((o & 128) > 0) {
            position -= 15;
            return;
          }
          return fromCharCode(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o);
        }
      }
    }
  }
}
function readOnlyJSString() {
  let token = src[position++];
  let length;
  if (token < 192) {
    length = token - 160;
  } else {
    switch (token) {
      case 217:
        length = src[position++];
        break;
      case 218:
        length = dataView.getUint16(position);
        position += 2;
        break;
      case 219:
        length = dataView.getUint32(position);
        position += 4;
        break;
      default:
        throw new Error("Expected string");
    }
  }
  return readStringJS(length);
}
function readBin(length) {
  return currentUnpackr.copyBuffers ? Uint8Array.prototype.slice.call(src, position, position += length) : src.subarray(position, position += length);
}
function readExt(length) {
  let type = src[position++];
  if (currentExtensions[type]) {
    let end;
    return currentExtensions[type](src.subarray(position, end = position += length), (readPosition) => {
      position = readPosition;
      try {
        return read();
      } finally {
        position = end;
      }
    });
  } else
    throw new Error("Unknown extension type " + type);
}
var keyCache = new Array(4096);
function readKey() {
  let length = src[position++];
  if (length >= 160 && length < 192) {
    length = length - 160;
    if (srcStringEnd >= position)
      return srcString.slice(position - srcStringStart, (position += length) - srcStringStart);
    else if (!(srcStringEnd == 0 && srcEnd < 180))
      return readFixedString(length);
  } else {
    position--;
    return asSafeString(read());
  }
  let key = (length << 5 ^ (length > 1 ? dataView.getUint16(position) : length > 0 ? src[position] : 0)) & 4095;
  let entry = keyCache[key];
  let checkPosition = position;
  let end = position + length - 3;
  let chunk;
  let i = 0;
  if (entry && entry.bytes == length) {
    while (checkPosition < end) {
      chunk = dataView.getUint32(checkPosition);
      if (chunk != entry[i++]) {
        checkPosition = 1879048192;
        break;
      }
      checkPosition += 4;
    }
    end += 3;
    while (checkPosition < end) {
      chunk = src[checkPosition++];
      if (chunk != entry[i++]) {
        checkPosition = 1879048192;
        break;
      }
    }
    if (checkPosition === end) {
      position = checkPosition;
      return entry.string;
    }
    end -= 3;
    checkPosition = position;
  }
  entry = [];
  keyCache[key] = entry;
  entry.bytes = length;
  while (checkPosition < end) {
    chunk = dataView.getUint32(checkPosition);
    entry.push(chunk);
    checkPosition += 4;
  }
  end += 3;
  while (checkPosition < end) {
    chunk = src[checkPosition++];
    entry.push(chunk);
  }
  let string = length < 16 ? shortStringInJS(length) : longStringInJS(length);
  if (string != null)
    return entry.string = string;
  return entry.string = readFixedString(length);
}
function asSafeString(property) {
  if (typeof property === "string")
    return property;
  if (typeof property === "number" || typeof property === "boolean" || typeof property === "bigint")
    return property.toString();
  if (property == null)
    return property + "";
  if (currentUnpackr.allowArraysInMapKeys && Array.isArray(property) && property.flat().every((item) => ["string", "number", "boolean", "bigint"].includes(typeof item))) {
    return property.flat().toString();
  }
  throw new Error(`Invalid property type for record: ${typeof property}`);
}
var recordDefinition = (id, highByte) => {
  let structure = read().map(asSafeString);
  let firstByte = id;
  if (highByte !== undefined) {
    id = id < 32 ? -((highByte << 5) + id) : (highByte << 5) + id;
    structure.highByte = highByte;
  }
  let existingStructure = currentStructures[id];
  if (existingStructure && (existingStructure.isShared || sequentialMode)) {
    (currentStructures.restoreStructures || (currentStructures.restoreStructures = []))[id] = existingStructure;
  }
  currentStructures[id] = structure;
  structure.read = createStructureReader(structure, firstByte);
  return structure.read();
};
currentExtensions[0] = () => {};
currentExtensions[0].noBuffer = true;
currentExtensions[66] = (data) => {
  let headLength = data.byteLength % 8 || 8;
  let head = BigInt(data[0] & 128 ? data[0] - 256 : data[0]);
  for (let i = 1;i < headLength; i++) {
    head <<= BigInt(8);
    head += BigInt(data[i]);
  }
  if (data.byteLength !== headLength) {
    let view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    let decode = (start, end) => {
      let length = end - start;
      if (length <= 40) {
        let out = view.getBigUint64(start);
        for (let i = start + 8;i < end; i += 8) {
          out <<= BigInt(64n);
          out |= view.getBigUint64(i);
        }
        return out;
      }
      let middle = start + (length >> 4 << 3);
      let left = decode(start, middle);
      let right = decode(middle, end);
      return left << BigInt((end - middle) * 8) | right;
    };
    head = head << BigInt((view.byteLength - headLength) * 8) | decode(headLength, view.byteLength);
  }
  return head;
};
var errors = {
  Error,
  EvalError,
  RangeError,
  ReferenceError,
  SyntaxError,
  TypeError,
  URIError,
  AggregateError: typeof AggregateError === "function" ? AggregateError : null
};
currentExtensions[101] = () => {
  let data = read();
  if (!errors[data[0]]) {
    let error = Error(data[1], { cause: data[2] });
    error.name = data[0];
    return error;
  }
  return errors[data[0]](data[1], { cause: data[2] });
};
currentExtensions[105] = (data) => {
  if (currentUnpackr.structuredClone === false)
    throw new Error("Structured clone extension is disabled");
  let id = dataView.getUint32(position - 4);
  if (!referenceMap)
    referenceMap = new Map;
  let token = src[position];
  let target;
  if (token >= 144 && token < 160 || token == 220 || token == 221)
    target = [];
  else if (token >= 128 && token < 144 || token == 222 || token == 223)
    target = new Map;
  else if ((token >= 199 && token <= 201 || token >= 212 && token <= 216) && src[position + 1] === 115)
    target = new Set;
  else
    target = {};
  let refEntry = { target };
  referenceMap.set(id, refEntry);
  let targetProperties = read();
  if (!refEntry.used) {
    return refEntry.target = targetProperties;
  } else {
    Object.assign(target, targetProperties);
  }
  if (target instanceof Map)
    for (let [k, v] of targetProperties.entries())
      target.set(k, v);
  if (target instanceof Set)
    for (let i of Array.from(targetProperties))
      target.add(i);
  return target;
};
currentExtensions[112] = (data) => {
  if (currentUnpackr.structuredClone === false)
    throw new Error("Structured clone extension is disabled");
  let id = dataView.getUint32(position - 4);
  let refEntry = referenceMap.get(id);
  refEntry.used = true;
  return refEntry.target;
};
currentExtensions[115] = () => new Set(read());
var typedArrays = ["Int8", "Uint8", "Uint8Clamped", "Int16", "Uint16", "Int32", "Uint32", "Float32", "Float64", "BigInt64", "BigUint64"].map((type) => type + "Array");
var glbl = typeof globalThis === "object" ? globalThis : window;
currentExtensions[116] = (data) => {
  let typeCode = data[0];
  let buffer = Uint8Array.prototype.slice.call(data, 1).buffer;
  let typedArrayName = typedArrays[typeCode];
  if (!typedArrayName) {
    if (typeCode === 16)
      return buffer;
    if (typeCode === 17)
      return new DataView(buffer);
    throw new Error("Could not find typed array for code " + typeCode);
  }
  return new glbl[typedArrayName](buffer);
};
currentExtensions[120] = () => {
  let data = read();
  return new RegExp(data[0], data[1]);
};
var TEMP_BUNDLE = [];
currentExtensions[98] = (data) => {
  let dataSize = (data[0] << 24) + (data[1] << 16) + (data[2] << 8) + data[3];
  let dataPosition = position;
  position += dataSize - data.length;
  bundledStrings = TEMP_BUNDLE;
  bundledStrings = [readOnlyJSString(), readOnlyJSString()];
  bundledStrings.position0 = 0;
  bundledStrings.position1 = 0;
  bundledStrings.postBundlePosition = position;
  position = dataPosition;
  return read();
};
currentExtensions[255] = (data) => {
  if (data.length == 4)
    return new Date((data[0] * 16777216 + (data[1] << 16) + (data[2] << 8) + data[3]) * 1000);
  else if (data.length == 8)
    return new Date(((data[0] << 22) + (data[1] << 14) + (data[2] << 6) + (data[3] >> 2)) / 1e6 + ((data[3] & 3) * 4294967296 + data[4] * 16777216 + (data[5] << 16) + (data[6] << 8) + data[7]) * 1000);
  else if (data.length == 12)
    return new Date(((data[0] << 24) + (data[1] << 16) + (data[2] << 8) + data[3]) / 1e6 + ((data[4] & 128 ? -281474976710656 : 0) + data[6] * 1099511627776 + data[7] * 4294967296 + data[8] * 16777216 + (data[9] << 16) + (data[10] << 8) + data[11]) * 1000);
  else
    return new Date("invalid");
};
function saveState(callback) {
  if (onSaveState)
    onSaveState();
  let savedSrcEnd = srcEnd;
  let savedPosition = position;
  let savedStringPosition = stringPosition;
  let savedSrcStringStart = srcStringStart;
  let savedSrcStringEnd = srcStringEnd;
  let savedSrcString = srcString;
  let savedStrings = strings;
  let savedReferenceMap = referenceMap;
  let savedBundledStrings = bundledStrings;
  let savedSrc = new Uint8Array(src.slice(0, srcEnd));
  let savedStructures = currentStructures;
  let savedStructuresContents = currentStructures.slice(0, currentStructures.length);
  let savedPackr = currentUnpackr;
  let savedSequentialMode = sequentialMode;
  let value = callback();
  srcEnd = savedSrcEnd;
  position = savedPosition;
  stringPosition = savedStringPosition;
  srcStringStart = savedSrcStringStart;
  srcStringEnd = savedSrcStringEnd;
  srcString = savedSrcString;
  strings = savedStrings;
  referenceMap = savedReferenceMap;
  bundledStrings = savedBundledStrings;
  src = savedSrc;
  sequentialMode = savedSequentialMode;
  currentStructures = savedStructures;
  currentStructures.splice(0, currentStructures.length, ...savedStructuresContents);
  currentUnpackr = savedPackr;
  dataView = new DataView(src.buffer, src.byteOffset, src.byteLength);
  return value;
}
function clearSource() {
  src = null;
  referenceMap = null;
  currentStructures = null;
}
var mult10 = new Array(147);
for (let i = 0;i < 256; i++) {
  mult10[i] = +("1e" + Math.floor(45.15 - i * 0.30103));
}
var defaultUnpackr = new Unpackr({ useRecords: false });
var unpack = defaultUnpackr.unpack;
var unpackMultiple = defaultUnpackr.unpackMultiple;
var decode = defaultUnpackr.unpack;
var f32Array = new Float32Array(1);
var u8Array = new Uint8Array(f32Array.buffer, 0, 4);
function setReadStruct(updatedReadStruct, loadedStructs, saveState2) {
  readStruct = updatedReadStruct;
  onLoadedStructures = loadedStructs;
  onSaveState = saveState2;
}
// ../../node_modules/.bun/msgpackr@1.11.8/node_modules/msgpackr/pack.js
var textEncoder;
try {
  textEncoder = new TextEncoder;
} catch (error) {}
var extensions;
var extensionClasses;
var hasNodeBuffer = typeof Buffer !== "undefined";
var ByteArrayAllocate = hasNodeBuffer ? function(length) {
  return Buffer.allocUnsafeSlow(length);
} : Uint8Array;
var ByteArray = hasNodeBuffer ? Buffer : Uint8Array;
var MAX_BUFFER_SIZE = hasNodeBuffer ? 4294967296 : 2144337920;
var target;
var keysTarget;
var targetView;
var position2 = 0;
var safeEnd;
var bundledStrings2 = null;
var writeStructSlots;
var MAX_BUNDLE_SIZE = 21760;
var hasNonLatin = /[\u0080-\uFFFF]/;
var RECORD_SYMBOL = Symbol("record-id");

class Packr extends Unpackr {
  constructor(options) {
    super(options);
    this.offset = 0;
    let typeBuffer;
    let start;
    let hasSharedUpdate;
    let structures;
    let referenceMap2;
    let encodeUtf8 = ByteArray.prototype.utf8Write ? function(string, position3) {
      return target.utf8Write(string, position3, target.byteLength - position3);
    } : textEncoder && textEncoder.encodeInto ? function(string, position3) {
      return textEncoder.encodeInto(string, target.subarray(position3)).written;
    } : false;
    let packr = this;
    if (!options)
      options = {};
    let isSequential = options && options.sequential;
    let hasSharedStructures = options.structures || options.saveStructures;
    let maxSharedStructures = options.maxSharedStructures;
    if (maxSharedStructures == null)
      maxSharedStructures = hasSharedStructures ? 32 : 0;
    if (maxSharedStructures > 8160)
      throw new Error("Maximum maxSharedStructure is 8160");
    if (options.structuredClone && options.moreTypes == undefined) {
      this.moreTypes = true;
    }
    let maxOwnStructures = options.maxOwnStructures;
    if (maxOwnStructures == null)
      maxOwnStructures = hasSharedStructures ? 32 : 64;
    if (!this.structures && options.useRecords != false)
      this.structures = [];
    let useTwoByteRecords = maxSharedStructures > 32 || maxOwnStructures + maxSharedStructures > 64;
    let sharedLimitId = maxSharedStructures + 64;
    let maxStructureId = maxSharedStructures + maxOwnStructures + 64;
    if (maxStructureId > 8256) {
      throw new Error("Maximum maxSharedStructure + maxOwnStructure is 8192");
    }
    let recordIdsToRemove = [];
    let transitionsCount = 0;
    let serializationsSinceTransitionRebuild = 0;
    this.pack = this.encode = function(value, encodeOptions) {
      if (!target) {
        target = new ByteArrayAllocate(8192);
        targetView = target.dataView || (target.dataView = new DataView(target.buffer, 0, 8192));
        position2 = 0;
      }
      safeEnd = target.length - 10;
      if (safeEnd - position2 < 2048) {
        target = new ByteArrayAllocate(target.length);
        targetView = target.dataView || (target.dataView = new DataView(target.buffer, 0, target.length));
        safeEnd = target.length - 10;
        position2 = 0;
      } else
        position2 = position2 + 7 & 2147483640;
      start = position2;
      if (encodeOptions & RESERVE_START_SPACE)
        position2 += encodeOptions & 255;
      referenceMap2 = packr.structuredClone ? new Map : null;
      if (packr.bundleStrings && typeof value !== "string") {
        bundledStrings2 = [];
        bundledStrings2.size = Infinity;
      } else
        bundledStrings2 = null;
      structures = packr.structures;
      if (structures) {
        if (structures.uninitialized)
          structures = packr._mergeStructures(packr.getStructures());
        let sharedLength = structures.sharedLength || 0;
        if (sharedLength > maxSharedStructures) {
          throw new Error("Shared structures is larger than maximum shared structures, try increasing maxSharedStructures to " + structures.sharedLength);
        }
        if (!structures.transitions) {
          structures.transitions = Object.create(null);
          for (let i = 0;i < sharedLength; i++) {
            let keys = structures[i];
            if (!keys)
              continue;
            let nextTransition, transition = structures.transitions;
            for (let j = 0, l = keys.length;j < l; j++) {
              let key = keys[j];
              nextTransition = transition[key];
              if (!nextTransition) {
                nextTransition = transition[key] = Object.create(null);
              }
              transition = nextTransition;
            }
            transition[RECORD_SYMBOL] = i + 64;
          }
          this.lastNamedStructuresLength = sharedLength;
        }
        if (!isSequential) {
          structures.nextId = sharedLength + 64;
        }
      }
      if (hasSharedUpdate)
        hasSharedUpdate = false;
      let encodingError;
      try {
        if (packr.randomAccessStructure && value && typeof value === "object") {
          if (value.constructor === Object)
            writeStruct(value);
          else if (value.constructor !== Map && !Array.isArray(value) && !extensionClasses.some((extClass) => value instanceof extClass)) {
            writeStruct(value.toJSON ? value.toJSON() : value);
          } else
            pack(value);
        } else
          pack(value);
        let lastBundle = bundledStrings2;
        if (bundledStrings2)
          writeBundles(start, pack, 0);
        if (referenceMap2 && referenceMap2.idsToInsert) {
          let idsToInsert = referenceMap2.idsToInsert.sort((a, b) => a.offset > b.offset ? 1 : -1);
          let i = idsToInsert.length;
          let incrementPosition = -1;
          while (lastBundle && i > 0) {
            let insertionPoint = idsToInsert[--i].offset + start;
            if (insertionPoint < lastBundle.stringsPosition + start && incrementPosition === -1)
              incrementPosition = 0;
            if (insertionPoint > lastBundle.position + start) {
              if (incrementPosition >= 0)
                incrementPosition += 6;
            } else {
              if (incrementPosition >= 0) {
                targetView.setUint32(lastBundle.position + start, targetView.getUint32(lastBundle.position + start) + incrementPosition);
                incrementPosition = -1;
              }
              lastBundle = lastBundle.previous;
              i++;
            }
          }
          if (incrementPosition >= 0 && lastBundle) {
            targetView.setUint32(lastBundle.position + start, targetView.getUint32(lastBundle.position + start) + incrementPosition);
          }
          position2 += idsToInsert.length * 6;
          if (position2 > safeEnd)
            makeRoom(position2);
          packr.offset = position2;
          let serialized = insertIds(target.subarray(start, position2), idsToInsert);
          referenceMap2 = null;
          return serialized;
        }
        packr.offset = position2;
        if (encodeOptions & REUSE_BUFFER_MODE) {
          target.start = start;
          target.end = position2;
          return target;
        }
        return target.subarray(start, position2);
      } catch (error) {
        encodingError = error;
        throw error;
      } finally {
        if (structures) {
          resetStructures();
          if (hasSharedUpdate && packr.saveStructures) {
            let sharedLength = structures.sharedLength || 0;
            let returnBuffer = target.subarray(start, position2);
            let newSharedData = prepareStructures(structures, packr);
            if (!encodingError) {
              if (packr.saveStructures(newSharedData, newSharedData.isCompatible) === false) {
                return packr.pack(value, encodeOptions);
              }
              packr.lastNamedStructuresLength = sharedLength;
              if (target.length > 1073741824)
                target = null;
              return returnBuffer;
            }
          }
        }
        if (target.length > 1073741824)
          target = null;
        if (encodeOptions & RESET_BUFFER_MODE)
          position2 = start;
      }
    };
    const resetStructures = () => {
      if (serializationsSinceTransitionRebuild < 10)
        serializationsSinceTransitionRebuild++;
      let sharedLength = structures.sharedLength || 0;
      if (structures.length > sharedLength && !isSequential)
        structures.length = sharedLength;
      if (transitionsCount > 1e4) {
        structures.transitions = null;
        serializationsSinceTransitionRebuild = 0;
        transitionsCount = 0;
        if (recordIdsToRemove.length > 0)
          recordIdsToRemove = [];
      } else if (recordIdsToRemove.length > 0 && !isSequential) {
        for (let i = 0, l = recordIdsToRemove.length;i < l; i++) {
          recordIdsToRemove[i][RECORD_SYMBOL] = 0;
        }
        recordIdsToRemove = [];
      }
    };
    const packArray = (value) => {
      var length = value.length;
      if (length < 16) {
        target[position2++] = 144 | length;
      } else if (length < 65536) {
        target[position2++] = 220;
        target[position2++] = length >> 8;
        target[position2++] = length & 255;
      } else {
        target[position2++] = 221;
        targetView.setUint32(position2, length);
        position2 += 4;
      }
      for (let i = 0;i < length; i++) {
        pack(value[i]);
      }
    };
    const pack = (value) => {
      if (position2 > safeEnd)
        target = makeRoom(position2);
      var type = typeof value;
      var length;
      if (type === "string") {
        let strLength = value.length;
        if (bundledStrings2 && strLength >= 4 && strLength < 4096) {
          if ((bundledStrings2.size += strLength) > MAX_BUNDLE_SIZE) {
            let extStart;
            let maxBytes2 = (bundledStrings2[0] ? bundledStrings2[0].length * 3 + bundledStrings2[1].length : 0) + 10;
            if (position2 + maxBytes2 > safeEnd)
              target = makeRoom(position2 + maxBytes2);
            let lastBundle;
            if (bundledStrings2.position) {
              lastBundle = bundledStrings2;
              target[position2] = 200;
              position2 += 3;
              target[position2++] = 98;
              extStart = position2 - start;
              position2 += 4;
              writeBundles(start, pack, 0);
              targetView.setUint16(extStart + start - 3, position2 - start - extStart);
            } else {
              target[position2++] = 214;
              target[position2++] = 98;
              extStart = position2 - start;
              position2 += 4;
            }
            bundledStrings2 = ["", ""];
            bundledStrings2.previous = lastBundle;
            bundledStrings2.size = 0;
            bundledStrings2.position = extStart;
          }
          let twoByte = hasNonLatin.test(value);
          bundledStrings2[twoByte ? 0 : 1] += value;
          target[position2++] = 193;
          pack(twoByte ? -strLength : strLength);
          return;
        }
        let headerSize;
        if (strLength < 32) {
          headerSize = 1;
        } else if (strLength < 256) {
          headerSize = 2;
        } else if (strLength < 65536) {
          headerSize = 3;
        } else {
          headerSize = 5;
        }
        let maxBytes = strLength * 3;
        if (position2 + maxBytes > safeEnd)
          target = makeRoom(position2 + maxBytes);
        if (strLength < 64 || !encodeUtf8) {
          let i, c1, c2, strPosition = position2 + headerSize;
          for (i = 0;i < strLength; i++) {
            c1 = value.charCodeAt(i);
            if (c1 < 128) {
              target[strPosition++] = c1;
            } else if (c1 < 2048) {
              target[strPosition++] = c1 >> 6 | 192;
              target[strPosition++] = c1 & 63 | 128;
            } else if ((c1 & 64512) === 55296 && ((c2 = value.charCodeAt(i + 1)) & 64512) === 56320) {
              c1 = 65536 + ((c1 & 1023) << 10) + (c2 & 1023);
              i++;
              target[strPosition++] = c1 >> 18 | 240;
              target[strPosition++] = c1 >> 12 & 63 | 128;
              target[strPosition++] = c1 >> 6 & 63 | 128;
              target[strPosition++] = c1 & 63 | 128;
            } else {
              target[strPosition++] = c1 >> 12 | 224;
              target[strPosition++] = c1 >> 6 & 63 | 128;
              target[strPosition++] = c1 & 63 | 128;
            }
          }
          length = strPosition - position2 - headerSize;
        } else {
          length = encodeUtf8(value, position2 + headerSize);
        }
        if (length < 32) {
          target[position2++] = 160 | length;
        } else if (length < 256) {
          if (headerSize < 2) {
            target.copyWithin(position2 + 2, position2 + 1, position2 + 1 + length);
          }
          target[position2++] = 217;
          target[position2++] = length;
        } else if (length < 65536) {
          if (headerSize < 3) {
            target.copyWithin(position2 + 3, position2 + 2, position2 + 2 + length);
          }
          target[position2++] = 218;
          target[position2++] = length >> 8;
          target[position2++] = length & 255;
        } else {
          if (headerSize < 5) {
            target.copyWithin(position2 + 5, position2 + 3, position2 + 3 + length);
          }
          target[position2++] = 219;
          targetView.setUint32(position2, length);
          position2 += 4;
        }
        position2 += length;
      } else if (type === "number") {
        if (value >>> 0 === value) {
          if (value < 32 || value < 128 && this.useRecords === false || value < 64 && !this.randomAccessStructure) {
            target[position2++] = value;
          } else if (value < 256) {
            target[position2++] = 204;
            target[position2++] = value;
          } else if (value < 65536) {
            target[position2++] = 205;
            target[position2++] = value >> 8;
            target[position2++] = value & 255;
          } else {
            target[position2++] = 206;
            targetView.setUint32(position2, value);
            position2 += 4;
          }
        } else if (value >> 0 === value) {
          if (value >= -32) {
            target[position2++] = 256 + value;
          } else if (value >= -128) {
            target[position2++] = 208;
            target[position2++] = value + 256;
          } else if (value >= -32768) {
            target[position2++] = 209;
            targetView.setInt16(position2, value);
            position2 += 2;
          } else {
            target[position2++] = 210;
            targetView.setInt32(position2, value);
            position2 += 4;
          }
        } else {
          let useFloat32;
          if ((useFloat32 = this.useFloat32) > 0 && value < 4294967296 && value >= -2147483648) {
            target[position2++] = 202;
            targetView.setFloat32(position2, value);
            let xShifted;
            if (useFloat32 < 4 || (xShifted = value * mult10[(target[position2] & 127) << 1 | target[position2 + 1] >> 7]) >> 0 === xShifted) {
              position2 += 4;
              return;
            } else
              position2--;
          }
          target[position2++] = 203;
          targetView.setFloat64(position2, value);
          position2 += 8;
        }
      } else if (type === "object" || type === "function") {
        if (!value)
          target[position2++] = 192;
        else {
          if (referenceMap2) {
            let referee = referenceMap2.get(value);
            if (referee) {
              if (!referee.id) {
                let idsToInsert = referenceMap2.idsToInsert || (referenceMap2.idsToInsert = []);
                referee.id = idsToInsert.push(referee);
              }
              target[position2++] = 214;
              target[position2++] = 112;
              targetView.setUint32(position2, referee.id);
              position2 += 4;
              return;
            } else
              referenceMap2.set(value, { offset: position2 - start });
          }
          let constructor = value.constructor;
          if (constructor === Object) {
            writeObject(value);
          } else if (constructor === Array) {
            packArray(value);
          } else if (constructor === Map) {
            if (this.mapAsEmptyObject)
              target[position2++] = 128;
            else {
              length = value.size;
              if (length < 16) {
                target[position2++] = 128 | length;
              } else if (length < 65536) {
                target[position2++] = 222;
                target[position2++] = length >> 8;
                target[position2++] = length & 255;
              } else {
                target[position2++] = 223;
                targetView.setUint32(position2, length);
                position2 += 4;
              }
              for (let [key, entryValue] of value) {
                pack(key);
                pack(entryValue);
              }
            }
          } else {
            for (let i = 0, l = extensions.length;i < l; i++) {
              let extensionClass = extensionClasses[i];
              if (value instanceof extensionClass) {
                let extension = extensions[i];
                if (extension.write) {
                  if (extension.type) {
                    target[position2++] = 212;
                    target[position2++] = extension.type;
                    target[position2++] = 0;
                  }
                  let writeResult = extension.write.call(this, value);
                  if (writeResult === value) {
                    if (Array.isArray(value)) {
                      packArray(value);
                    } else {
                      writeObject(value);
                    }
                  } else {
                    pack(writeResult);
                  }
                  return;
                }
                let currentTarget = target;
                let currentTargetView = targetView;
                let currentPosition = position2;
                target = null;
                let result;
                try {
                  result = extension.pack.call(this, value, (size) => {
                    target = currentTarget;
                    currentTarget = null;
                    position2 += size;
                    if (position2 > safeEnd)
                      makeRoom(position2);
                    return {
                      target,
                      targetView,
                      position: position2 - size
                    };
                  }, pack);
                } finally {
                  if (currentTarget) {
                    target = currentTarget;
                    targetView = currentTargetView;
                    position2 = currentPosition;
                    safeEnd = target.length - 10;
                  }
                }
                if (result) {
                  if (result.length + position2 > safeEnd)
                    makeRoom(result.length + position2);
                  position2 = writeExtensionData(result, target, position2, extension.type);
                }
                return;
              }
            }
            if (Array.isArray(value)) {
              packArray(value);
            } else {
              if (value.toJSON) {
                const json = value.toJSON();
                if (json !== value)
                  return pack(json);
              }
              if (type === "function")
                return pack(this.writeFunction && this.writeFunction(value));
              writeObject(value);
            }
          }
        }
      } else if (type === "boolean") {
        target[position2++] = value ? 195 : 194;
      } else if (type === "bigint") {
        if (value < 9223372036854776000 && value >= -9223372036854776000) {
          target[position2++] = 211;
          targetView.setBigInt64(position2, value);
        } else if (value < 18446744073709552000 && value > 0) {
          target[position2++] = 207;
          targetView.setBigUint64(position2, value);
        } else {
          if (this.largeBigIntToFloat) {
            target[position2++] = 203;
            targetView.setFloat64(position2, Number(value));
          } else if (this.largeBigIntToString) {
            return pack(value.toString());
          } else if (this.useBigIntExtension || this.moreTypes) {
            let empty = value < 0 ? BigInt(-1) : BigInt(0);
            let array;
            if (value >> BigInt(65536) === empty) {
              let mask = BigInt(18446744073709552000) - BigInt(1);
              let chunks = [];
              while (true) {
                chunks.push(value & mask);
                if (value >> BigInt(63) === empty)
                  break;
                value >>= BigInt(64);
              }
              array = new Uint8Array(new BigUint64Array(chunks).buffer);
              array.reverse();
            } else {
              let invert = value < 0;
              let string = (invert ? ~value : value).toString(16);
              if (string.length % 2) {
                string = "0" + string;
              } else if (parseInt(string.charAt(0), 16) >= 8) {
                string = "00" + string;
              }
              if (hasNodeBuffer) {
                array = Buffer.from(string, "hex");
              } else {
                array = new Uint8Array(string.length / 2);
                for (let i = 0;i < array.length; i++) {
                  array[i] = parseInt(string.slice(i * 2, i * 2 + 2), 16);
                }
              }
              if (invert) {
                for (let i = 0;i < array.length; i++)
                  array[i] = ~array[i];
              }
            }
            if (array.length + position2 > safeEnd)
              makeRoom(array.length + position2);
            position2 = writeExtensionData(array, target, position2, 66);
            return;
          } else {
            throw new RangeError(value + " was too large to fit in MessagePack 64-bit integer format, use" + " useBigIntExtension, or set largeBigIntToFloat to convert to float-64, or set" + " largeBigIntToString to convert to string");
          }
        }
        position2 += 8;
      } else if (type === "undefined") {
        if (this.encodeUndefinedAsNil)
          target[position2++] = 192;
        else {
          target[position2++] = 212;
          target[position2++] = 0;
          target[position2++] = 0;
        }
      } else {
        throw new Error("Unknown type: " + type);
      }
    };
    const writePlainObject = this.variableMapSize || this.coercibleKeyAsNumber || this.skipValues ? (object) => {
      let keys;
      if (this.skipValues) {
        keys = [];
        for (let key2 in object) {
          if ((typeof object.hasOwnProperty !== "function" || object.hasOwnProperty(key2)) && !this.skipValues.includes(object[key2]))
            keys.push(key2);
        }
      } else {
        keys = Object.keys(object);
      }
      let length = keys.length;
      if (length < 16) {
        target[position2++] = 128 | length;
      } else if (length < 65536) {
        target[position2++] = 222;
        target[position2++] = length >> 8;
        target[position2++] = length & 255;
      } else {
        target[position2++] = 223;
        targetView.setUint32(position2, length);
        position2 += 4;
      }
      let key;
      if (this.coercibleKeyAsNumber) {
        for (let i = 0;i < length; i++) {
          key = keys[i];
          let num = Number(key);
          pack(isNaN(num) ? key : num);
          pack(object[key]);
        }
      } else {
        for (let i = 0;i < length; i++) {
          pack(key = keys[i]);
          pack(object[key]);
        }
      }
    } : (object) => {
      target[position2++] = 222;
      let objectOffset = position2 - start;
      position2 += 2;
      let size = 0;
      for (let key in object) {
        if (typeof object.hasOwnProperty !== "function" || object.hasOwnProperty(key)) {
          pack(key);
          pack(object[key]);
          size++;
        }
      }
      if (size > 65535) {
        throw new Error("Object is too large to serialize with fast 16-bit map size," + ' use the "variableMapSize" option to serialize this object');
      }
      target[objectOffset++ + start] = size >> 8;
      target[objectOffset + start] = size & 255;
    };
    const writeRecord = this.useRecords === false ? writePlainObject : options.progressiveRecords && !useTwoByteRecords ? (object) => {
      let nextTransition, transition = structures.transitions || (structures.transitions = Object.create(null));
      let objectOffset = position2++ - start;
      let wroteKeys;
      for (let key in object) {
        if (typeof object.hasOwnProperty !== "function" || object.hasOwnProperty(key)) {
          nextTransition = transition[key];
          if (nextTransition)
            transition = nextTransition;
          else {
            let keys = Object.keys(object);
            let lastTransition = transition;
            transition = structures.transitions;
            let newTransitions = 0;
            for (let i = 0, l = keys.length;i < l; i++) {
              let key2 = keys[i];
              nextTransition = transition[key2];
              if (!nextTransition) {
                nextTransition = transition[key2] = Object.create(null);
                newTransitions++;
              }
              transition = nextTransition;
            }
            if (objectOffset + start + 1 == position2) {
              position2--;
              newRecord(transition, keys, newTransitions);
            } else
              insertNewRecord(transition, keys, objectOffset, newTransitions);
            wroteKeys = true;
            transition = lastTransition[key];
          }
          pack(object[key]);
        }
      }
      if (!wroteKeys) {
        let recordId = transition[RECORD_SYMBOL];
        if (recordId)
          target[objectOffset + start] = recordId;
        else
          insertNewRecord(transition, Object.keys(object), objectOffset, 0);
      }
    } : (object) => {
      let nextTransition, transition = structures.transitions || (structures.transitions = Object.create(null));
      let newTransitions = 0;
      for (let key in object)
        if (typeof object.hasOwnProperty !== "function" || object.hasOwnProperty(key)) {
          nextTransition = transition[key];
          if (!nextTransition) {
            nextTransition = transition[key] = Object.create(null);
            newTransitions++;
          }
          transition = nextTransition;
        }
      let recordId = transition[RECORD_SYMBOL];
      if (recordId) {
        if (recordId >= 96 && useTwoByteRecords) {
          target[position2++] = ((recordId -= 96) & 31) + 96;
          target[position2++] = recordId >> 5;
        } else
          target[position2++] = recordId;
      } else {
        newRecord(transition, transition.__keys__ || Object.keys(object), newTransitions);
      }
      for (let key in object)
        if (typeof object.hasOwnProperty !== "function" || object.hasOwnProperty(key)) {
          pack(object[key]);
        }
    };
    const checkUseRecords = typeof this.useRecords == "function" && this.useRecords;
    const writeObject = checkUseRecords ? (object) => {
      checkUseRecords(object) ? writeRecord(object) : writePlainObject(object);
    } : writeRecord;
    const makeRoom = (end) => {
      let newSize;
      if (end > 16777216) {
        if (end - start > MAX_BUFFER_SIZE)
          throw new Error("Packed buffer would be larger than maximum buffer size");
        newSize = Math.min(MAX_BUFFER_SIZE, Math.round(Math.max((end - start) * (end > 67108864 ? 1.25 : 2), 4194304) / 4096) * 4096);
      } else
        newSize = (Math.max(end - start << 2, target.length - 1) >> 12) + 1 << 12;
      let newBuffer = new ByteArrayAllocate(newSize);
      targetView = newBuffer.dataView || (newBuffer.dataView = new DataView(newBuffer.buffer, 0, newSize));
      end = Math.min(end, target.length);
      if (target.copy)
        target.copy(newBuffer, 0, start, end);
      else
        newBuffer.set(target.slice(start, end));
      position2 -= start;
      start = 0;
      safeEnd = newBuffer.length - 10;
      return target = newBuffer;
    };
    const newRecord = (transition, keys, newTransitions) => {
      let recordId = structures.nextId;
      if (!recordId)
        recordId = 64;
      if (recordId < sharedLimitId && this.shouldShareStructure && !this.shouldShareStructure(keys)) {
        recordId = structures.nextOwnId;
        if (!(recordId < maxStructureId))
          recordId = sharedLimitId;
        structures.nextOwnId = recordId + 1;
      } else {
        if (recordId >= maxStructureId)
          recordId = sharedLimitId;
        structures.nextId = recordId + 1;
      }
      let highByte = keys.highByte = recordId >= 96 && useTwoByteRecords ? recordId - 96 >> 5 : -1;
      transition[RECORD_SYMBOL] = recordId;
      transition.__keys__ = keys;
      structures[recordId - 64] = keys;
      if (recordId < sharedLimitId) {
        keys.isShared = true;
        structures.sharedLength = recordId - 63;
        hasSharedUpdate = true;
        if (highByte >= 0) {
          target[position2++] = (recordId & 31) + 96;
          target[position2++] = highByte;
        } else {
          target[position2++] = recordId;
        }
      } else {
        if (highByte >= 0) {
          target[position2++] = 213;
          target[position2++] = 114;
          target[position2++] = (recordId & 31) + 96;
          target[position2++] = highByte;
        } else {
          target[position2++] = 212;
          target[position2++] = 114;
          target[position2++] = recordId;
        }
        if (newTransitions)
          transitionsCount += serializationsSinceTransitionRebuild * newTransitions;
        if (recordIdsToRemove.length >= maxOwnStructures)
          recordIdsToRemove.shift()[RECORD_SYMBOL] = 0;
        recordIdsToRemove.push(transition);
        pack(keys);
      }
    };
    const insertNewRecord = (transition, keys, insertionOffset, newTransitions) => {
      let mainTarget = target;
      let mainPosition = position2;
      let mainSafeEnd = safeEnd;
      let mainStart = start;
      target = keysTarget;
      position2 = 0;
      start = 0;
      if (!target)
        keysTarget = target = new ByteArrayAllocate(8192);
      safeEnd = target.length - 10;
      newRecord(transition, keys, newTransitions);
      keysTarget = target;
      let keysPosition = position2;
      target = mainTarget;
      position2 = mainPosition;
      safeEnd = mainSafeEnd;
      start = mainStart;
      if (keysPosition > 1) {
        let newEnd = position2 + keysPosition - 1;
        if (newEnd > safeEnd)
          makeRoom(newEnd);
        let insertionPosition = insertionOffset + start;
        target.copyWithin(insertionPosition + keysPosition, insertionPosition + 1, position2);
        target.set(keysTarget.slice(0, keysPosition), insertionPosition);
        position2 = newEnd;
      } else {
        target[insertionOffset + start] = keysTarget[0];
      }
    };
    const writeStruct = (object) => {
      let newPosition = writeStructSlots(object, target, start, position2, structures, makeRoom, (value, newPosition2, notifySharedUpdate) => {
        if (notifySharedUpdate)
          return hasSharedUpdate = true;
        position2 = newPosition2;
        let startTarget = target;
        pack(value);
        resetStructures();
        if (startTarget !== target) {
          return { position: position2, targetView, target };
        }
        return position2;
      }, this);
      if (newPosition === 0)
        return writeObject(object);
      position2 = newPosition;
    };
  }
  useBuffer(buffer) {
    target = buffer;
    target.dataView || (target.dataView = new DataView(target.buffer, target.byteOffset, target.byteLength));
    targetView = target.dataView;
    position2 = 0;
  }
  set position(value) {
    position2 = value;
  }
  get position() {
    return position2;
  }
  clearSharedData() {
    if (this.structures)
      this.structures = [];
    if (this.typedStructs)
      this.typedStructs = [];
  }
}
extensionClasses = [Date, Set, Error, RegExp, ArrayBuffer, Object.getPrototypeOf(Uint8Array.prototype).constructor, DataView, C1Type];
extensions = [{
  pack(date, allocateForWrite, pack) {
    let seconds = date.getTime() / 1000;
    if ((this.useTimestamp32 || date.getMilliseconds() === 0) && seconds >= 0 && seconds < 4294967296) {
      let { target: target2, targetView: targetView2, position: position3 } = allocateForWrite(6);
      target2[position3++] = 214;
      target2[position3++] = 255;
      targetView2.setUint32(position3, seconds);
    } else if (seconds > 0 && seconds < 4294967296) {
      let { target: target2, targetView: targetView2, position: position3 } = allocateForWrite(10);
      target2[position3++] = 215;
      target2[position3++] = 255;
      targetView2.setUint32(position3, date.getMilliseconds() * 4000000 + (seconds / 1000 / 4294967296 >> 0));
      targetView2.setUint32(position3 + 4, seconds);
    } else if (isNaN(seconds)) {
      if (this.onInvalidDate) {
        allocateForWrite(0);
        return pack(this.onInvalidDate());
      }
      let { target: target2, targetView: targetView2, position: position3 } = allocateForWrite(3);
      target2[position3++] = 212;
      target2[position3++] = 255;
      target2[position3++] = 255;
    } else {
      let { target: target2, targetView: targetView2, position: position3 } = allocateForWrite(15);
      target2[position3++] = 199;
      target2[position3++] = 12;
      target2[position3++] = 255;
      targetView2.setUint32(position3, date.getMilliseconds() * 1e6);
      targetView2.setBigInt64(position3 + 4, BigInt(Math.floor(seconds)));
    }
  }
}, {
  pack(set, allocateForWrite, pack) {
    if (this.setAsEmptyObject) {
      allocateForWrite(0);
      return pack({});
    }
    let array = Array.from(set);
    let { target: target2, position: position3 } = allocateForWrite(this.moreTypes ? 3 : 0);
    if (this.moreTypes) {
      target2[position3++] = 212;
      target2[position3++] = 115;
      target2[position3++] = 0;
    }
    pack(array);
  }
}, {
  pack(error, allocateForWrite, pack) {
    let { target: target2, position: position3 } = allocateForWrite(this.moreTypes ? 3 : 0);
    if (this.moreTypes) {
      target2[position3++] = 212;
      target2[position3++] = 101;
      target2[position3++] = 0;
    }
    pack([error.name, error.message, error.cause]);
  }
}, {
  pack(regex, allocateForWrite, pack) {
    let { target: target2, position: position3 } = allocateForWrite(this.moreTypes ? 3 : 0);
    if (this.moreTypes) {
      target2[position3++] = 212;
      target2[position3++] = 120;
      target2[position3++] = 0;
    }
    pack([regex.source, regex.flags]);
  }
}, {
  pack(arrayBuffer, allocateForWrite) {
    if (this.moreTypes)
      writeExtBuffer(arrayBuffer, 16, allocateForWrite);
    else
      writeBuffer(hasNodeBuffer ? Buffer.from(arrayBuffer) : new Uint8Array(arrayBuffer), allocateForWrite);
  }
}, {
  pack(typedArray, allocateForWrite) {
    let constructor = typedArray.constructor;
    if (constructor !== ByteArray && this.moreTypes)
      writeExtBuffer(typedArray, typedArrays.indexOf(constructor.name), allocateForWrite);
    else
      writeBuffer(typedArray, allocateForWrite);
  }
}, {
  pack(arrayBuffer, allocateForWrite) {
    if (this.moreTypes)
      writeExtBuffer(arrayBuffer, 17, allocateForWrite);
    else
      writeBuffer(hasNodeBuffer ? Buffer.from(arrayBuffer) : new Uint8Array(arrayBuffer), allocateForWrite);
  }
}, {
  pack(c1, allocateForWrite) {
    let { target: target2, position: position3 } = allocateForWrite(1);
    target2[position3] = 193;
  }
}];
function writeExtBuffer(typedArray, type, allocateForWrite, encode) {
  let length = typedArray.byteLength;
  if (length + 1 < 256) {
    var { target: target2, position: position3 } = allocateForWrite(4 + length);
    target2[position3++] = 199;
    target2[position3++] = length + 1;
  } else if (length + 1 < 65536) {
    var { target: target2, position: position3 } = allocateForWrite(5 + length);
    target2[position3++] = 200;
    target2[position3++] = length + 1 >> 8;
    target2[position3++] = length + 1 & 255;
  } else {
    var { target: target2, position: position3, targetView: targetView2 } = allocateForWrite(7 + length);
    target2[position3++] = 201;
    targetView2.setUint32(position3, length + 1);
    position3 += 4;
  }
  target2[position3++] = 116;
  target2[position3++] = type;
  if (!typedArray.buffer)
    typedArray = new Uint8Array(typedArray);
  target2.set(new Uint8Array(typedArray.buffer, typedArray.byteOffset, typedArray.byteLength), position3);
}
function writeBuffer(buffer, allocateForWrite) {
  let length = buffer.byteLength;
  var target2, position3;
  if (length < 256) {
    var { target: target2, position: position3 } = allocateForWrite(length + 2);
    target2[position3++] = 196;
    target2[position3++] = length;
  } else if (length < 65536) {
    var { target: target2, position: position3 } = allocateForWrite(length + 3);
    target2[position3++] = 197;
    target2[position3++] = length >> 8;
    target2[position3++] = length & 255;
  } else {
    var { target: target2, position: position3, targetView: targetView2 } = allocateForWrite(length + 5);
    target2[position3++] = 198;
    targetView2.setUint32(position3, length);
    position3 += 4;
  }
  target2.set(buffer, position3);
}
function writeExtensionData(result, target2, position3, type) {
  let length = result.length;
  switch (length) {
    case 1:
      target2[position3++] = 212;
      break;
    case 2:
      target2[position3++] = 213;
      break;
    case 4:
      target2[position3++] = 214;
      break;
    case 8:
      target2[position3++] = 215;
      break;
    case 16:
      target2[position3++] = 216;
      break;
    default:
      if (length < 256) {
        target2[position3++] = 199;
        target2[position3++] = length;
      } else if (length < 65536) {
        target2[position3++] = 200;
        target2[position3++] = length >> 8;
        target2[position3++] = length & 255;
      } else {
        target2[position3++] = 201;
        target2[position3++] = length >> 24;
        target2[position3++] = length >> 16 & 255;
        target2[position3++] = length >> 8 & 255;
        target2[position3++] = length & 255;
      }
  }
  target2[position3++] = type;
  target2.set(result, position3);
  position3 += length;
  return position3;
}
function insertIds(serialized, idsToInsert) {
  let nextId;
  let distanceToMove = idsToInsert.length * 6;
  let lastEnd = serialized.length - distanceToMove;
  while (nextId = idsToInsert.pop()) {
    let offset = nextId.offset;
    let id = nextId.id;
    serialized.copyWithin(offset + distanceToMove, offset, lastEnd);
    distanceToMove -= 6;
    let position3 = offset + distanceToMove;
    serialized[position3++] = 214;
    serialized[position3++] = 105;
    serialized[position3++] = id >> 24;
    serialized[position3++] = id >> 16 & 255;
    serialized[position3++] = id >> 8 & 255;
    serialized[position3++] = id & 255;
    lastEnd = offset;
  }
  return serialized;
}
function writeBundles(start, pack, incrementPosition) {
  if (bundledStrings2.length > 0) {
    targetView.setUint32(bundledStrings2.position + start, position2 + incrementPosition - bundledStrings2.position - start);
    bundledStrings2.stringsPosition = position2 - start;
    let writeStrings = bundledStrings2;
    bundledStrings2 = null;
    pack(writeStrings[0]);
    pack(writeStrings[1]);
  }
}
function prepareStructures(structures, packr) {
  structures.isCompatible = (existingStructures) => {
    let compatible = !existingStructures || (packr.lastNamedStructuresLength || 0) === existingStructures.length;
    if (!compatible)
      packr._mergeStructures(existingStructures);
    return compatible;
  };
  return structures;
}
function setWriteStructSlots(writeSlots, makeStructures) {
  writeStructSlots = writeSlots;
  prepareStructures = makeStructures;
}
var defaultPackr = new Packr({ useRecords: false });
var pack = defaultPackr.pack;
var encode = defaultPackr.pack;
var REUSE_BUFFER_MODE = 512;
var RESET_BUFFER_MODE = 1024;
var RESERVE_START_SPACE = 2048;
// ../../node_modules/.bun/msgpackr@1.11.8/node_modules/msgpackr/struct.js
var ASCII = 3;
var NUMBER = 0;
var UTF8 = 2;
var OBJECT_DATA = 1;
var DATE = 16;
var TYPE_NAMES = ["num", "object", "string", "ascii"];
TYPE_NAMES[DATE] = "date";
var float32Headers = [false, true, true, false, false, true, true, false];
var evalSupported;
try {
  new Function("");
  evalSupported = true;
} catch (error) {}
var updatedPosition;
var hasNodeBuffer2 = typeof Buffer !== "undefined";
var textEncoder2;
var currentSource;
try {
  textEncoder2 = new TextEncoder;
} catch (error) {}
var encodeUtf8 = hasNodeBuffer2 ? function(target2, string, position3) {
  return target2.utf8Write(string, position3, target2.byteLength - position3);
} : textEncoder2 && textEncoder2.encodeInto ? function(target2, string, position3) {
  return textEncoder2.encodeInto(string, target2.subarray(position3)).written;
} : false;
var TYPE = Symbol("type");
var PARENT = Symbol("parent");
setWriteStructSlots(writeStruct, prepareStructures2);
function writeStruct(object, target2, encodingStart, position3, structures, makeRoom, pack2, packr) {
  let typedStructs = packr.typedStructs || (packr.typedStructs = []);
  let targetView2 = target2.dataView;
  let refsStartPosition = (typedStructs.lastStringStart || 100) + position3;
  let safeEnd2 = target2.length - 10;
  let start = position3;
  if (position3 > safeEnd2) {
    target2 = makeRoom(position3);
    targetView2 = target2.dataView;
    position3 -= encodingStart;
    start -= encodingStart;
    refsStartPosition -= encodingStart;
    encodingStart = 0;
    safeEnd2 = target2.length - 10;
  }
  let refOffset, refPosition = refsStartPosition;
  let transition = typedStructs.transitions || (typedStructs.transitions = Object.create(null));
  let nextId = typedStructs.nextId || typedStructs.length;
  let headerSize = nextId < 15 ? 1 : nextId < 240 ? 2 : nextId < 61440 ? 3 : nextId < 15728640 ? 4 : 0;
  if (headerSize === 0)
    return 0;
  position3 += headerSize;
  let queuedReferences = [];
  let usedAscii0;
  let keyIndex = 0;
  for (let key in object) {
    let value = object[key];
    let nextTransition = transition[key];
    if (!nextTransition) {
      transition[key] = nextTransition = {
        key,
        parent: transition,
        enumerationOffset: 0,
        ascii0: null,
        ascii8: null,
        num8: null,
        string16: null,
        object16: null,
        num32: null,
        float64: null,
        date64: null
      };
    }
    if (position3 > safeEnd2) {
      target2 = makeRoom(position3);
      targetView2 = target2.dataView;
      position3 -= encodingStart;
      start -= encodingStart;
      refsStartPosition -= encodingStart;
      refPosition -= encodingStart;
      encodingStart = 0;
      safeEnd2 = target2.length - 10;
    }
    switch (typeof value) {
      case "number":
        let number = value;
        if (nextId < 200 || !nextTransition.num64) {
          if (number >> 0 === number && number < 536870912 && number > -520093696) {
            if (number < 246 && number >= 0 && (nextTransition.num8 && !(nextId > 200 && nextTransition.num32) || number < 32 && !nextTransition.num32)) {
              transition = nextTransition.num8 || createTypeTransition(nextTransition, NUMBER, 1);
              target2[position3++] = number;
            } else {
              transition = nextTransition.num32 || createTypeTransition(nextTransition, NUMBER, 4);
              targetView2.setUint32(position3, number, true);
              position3 += 4;
            }
            break;
          } else if (number < 4294967296 && number >= -2147483648) {
            targetView2.setFloat32(position3, number, true);
            if (float32Headers[target2[position3 + 3] >>> 5]) {
              let xShifted;
              if ((xShifted = number * mult10[(target2[position3 + 3] & 127) << 1 | target2[position3 + 2] >> 7]) >> 0 === xShifted) {
                transition = nextTransition.num32 || createTypeTransition(nextTransition, NUMBER, 4);
                position3 += 4;
                break;
              }
            }
          }
        }
        transition = nextTransition.num64 || createTypeTransition(nextTransition, NUMBER, 8);
        targetView2.setFloat64(position3, number, true);
        position3 += 8;
        break;
      case "string":
        let strLength = value.length;
        refOffset = refPosition - refsStartPosition;
        if ((strLength << 2) + refPosition > safeEnd2) {
          target2 = makeRoom((strLength << 2) + refPosition);
          targetView2 = target2.dataView;
          position3 -= encodingStart;
          start -= encodingStart;
          refsStartPosition -= encodingStart;
          refPosition -= encodingStart;
          encodingStart = 0;
          safeEnd2 = target2.length - 10;
        }
        if (strLength > 65280 + refOffset >> 2) {
          queuedReferences.push(key, value, position3 - start);
          break;
        }
        let isNotAscii;
        let strStart = refPosition;
        if (strLength < 64) {
          let i, c1, c2;
          for (i = 0;i < strLength; i++) {
            c1 = value.charCodeAt(i);
            if (c1 < 128) {
              target2[refPosition++] = c1;
            } else if (c1 < 2048) {
              isNotAscii = true;
              target2[refPosition++] = c1 >> 6 | 192;
              target2[refPosition++] = c1 & 63 | 128;
            } else if ((c1 & 64512) === 55296 && ((c2 = value.charCodeAt(i + 1)) & 64512) === 56320) {
              isNotAscii = true;
              c1 = 65536 + ((c1 & 1023) << 10) + (c2 & 1023);
              i++;
              target2[refPosition++] = c1 >> 18 | 240;
              target2[refPosition++] = c1 >> 12 & 63 | 128;
              target2[refPosition++] = c1 >> 6 & 63 | 128;
              target2[refPosition++] = c1 & 63 | 128;
            } else {
              isNotAscii = true;
              target2[refPosition++] = c1 >> 12 | 224;
              target2[refPosition++] = c1 >> 6 & 63 | 128;
              target2[refPosition++] = c1 & 63 | 128;
            }
          }
        } else {
          refPosition += encodeUtf8(target2, value, refPosition);
          isNotAscii = refPosition - strStart > strLength;
        }
        if (refOffset < 160 || refOffset < 246 && (nextTransition.ascii8 || nextTransition.string8)) {
          if (isNotAscii) {
            if (!(transition = nextTransition.string8)) {
              if (typedStructs.length > 10 && (transition = nextTransition.ascii8)) {
                transition.__type = UTF8;
                nextTransition.ascii8 = null;
                nextTransition.string8 = transition;
                pack2(null, 0, true);
              } else {
                transition = createTypeTransition(nextTransition, UTF8, 1);
              }
            }
          } else if (refOffset === 0 && !usedAscii0) {
            usedAscii0 = true;
            transition = nextTransition.ascii0 || createTypeTransition(nextTransition, ASCII, 0);
            break;
          } else if (!(transition = nextTransition.ascii8) && !(typedStructs.length > 10 && (transition = nextTransition.string8)))
            transition = createTypeTransition(nextTransition, ASCII, 1);
          target2[position3++] = refOffset;
        } else {
          transition = nextTransition.string16 || createTypeTransition(nextTransition, UTF8, 2);
          targetView2.setUint16(position3, refOffset, true);
          position3 += 2;
        }
        break;
      case "object":
        if (value) {
          if (value.constructor === Date) {
            transition = nextTransition.date64 || createTypeTransition(nextTransition, DATE, 8);
            targetView2.setFloat64(position3, value.getTime(), true);
            position3 += 8;
          } else {
            queuedReferences.push(key, value, keyIndex);
          }
          break;
        } else {
          nextTransition = anyType(nextTransition, position3, targetView2, -10);
          if (nextTransition) {
            transition = nextTransition;
            position3 = updatedPosition;
          } else
            queuedReferences.push(key, value, keyIndex);
        }
        break;
      case "boolean":
        transition = nextTransition.num8 || nextTransition.ascii8 || createTypeTransition(nextTransition, NUMBER, 1);
        target2[position3++] = value ? 249 : 248;
        break;
      case "undefined":
        nextTransition = anyType(nextTransition, position3, targetView2, -9);
        if (nextTransition) {
          transition = nextTransition;
          position3 = updatedPosition;
        } else
          queuedReferences.push(key, value, keyIndex);
        break;
      default:
        queuedReferences.push(key, value, keyIndex);
    }
    keyIndex++;
  }
  for (let i = 0, l = queuedReferences.length;i < l; ) {
    let key = queuedReferences[i++];
    let value = queuedReferences[i++];
    let propertyIndex = queuedReferences[i++];
    let nextTransition = transition[key];
    if (!nextTransition) {
      transition[key] = nextTransition = {
        key,
        parent: transition,
        enumerationOffset: propertyIndex - keyIndex,
        ascii0: null,
        ascii8: null,
        num8: null,
        string16: null,
        object16: null,
        num32: null,
        float64: null
      };
    }
    let newPosition;
    if (value) {
      let size;
      refOffset = refPosition - refsStartPosition;
      if (refOffset < 65280) {
        transition = nextTransition.object16;
        if (transition)
          size = 2;
        else if (transition = nextTransition.object32)
          size = 4;
        else {
          transition = createTypeTransition(nextTransition, OBJECT_DATA, 2);
          size = 2;
        }
      } else {
        transition = nextTransition.object32 || createTypeTransition(nextTransition, OBJECT_DATA, 4);
        size = 4;
      }
      newPosition = pack2(value, refPosition);
      if (typeof newPosition === "object") {
        refPosition = newPosition.position;
        targetView2 = newPosition.targetView;
        target2 = newPosition.target;
        refsStartPosition -= encodingStart;
        position3 -= encodingStart;
        start -= encodingStart;
        encodingStart = 0;
      } else
        refPosition = newPosition;
      if (size === 2) {
        targetView2.setUint16(position3, refOffset, true);
        position3 += 2;
      } else {
        targetView2.setUint32(position3, refOffset, true);
        position3 += 4;
      }
    } else {
      transition = nextTransition.object16 || createTypeTransition(nextTransition, OBJECT_DATA, 2);
      targetView2.setInt16(position3, value === null ? -10 : -9, true);
      position3 += 2;
    }
    keyIndex++;
  }
  let recordId = transition[RECORD_SYMBOL];
  if (recordId == null) {
    recordId = packr.typedStructs.length;
    let structure = [];
    let nextTransition = transition;
    let key, type;
    while ((type = nextTransition.__type) !== undefined) {
      let size = nextTransition.__size;
      nextTransition = nextTransition.__parent;
      key = nextTransition.key;
      let property = [type, size, key];
      if (nextTransition.enumerationOffset)
        property.push(nextTransition.enumerationOffset);
      structure.push(property);
      nextTransition = nextTransition.parent;
    }
    structure.reverse();
    transition[RECORD_SYMBOL] = recordId;
    packr.typedStructs[recordId] = structure;
    pack2(null, 0, true);
  }
  switch (headerSize) {
    case 1:
      if (recordId >= 16)
        return 0;
      target2[start] = recordId + 32;
      break;
    case 2:
      if (recordId >= 256)
        return 0;
      target2[start] = 56;
      target2[start + 1] = recordId;
      break;
    case 3:
      if (recordId >= 65536)
        return 0;
      target2[start] = 57;
      targetView2.setUint16(start + 1, recordId, true);
      break;
    case 4:
      if (recordId >= 16777216)
        return 0;
      targetView2.setUint32(start, (recordId << 8) + 58, true);
      break;
  }
  if (position3 < refsStartPosition) {
    if (refsStartPosition === refPosition)
      return position3;
    target2.copyWithin(position3, refsStartPosition, refPosition);
    refPosition += position3 - refsStartPosition;
    typedStructs.lastStringStart = position3 - start;
  } else if (position3 > refsStartPosition) {
    if (refsStartPosition === refPosition)
      return position3;
    typedStructs.lastStringStart = position3 - start;
    return writeStruct(object, target2, encodingStart, start, structures, makeRoom, pack2, packr);
  }
  return refPosition;
}
function anyType(transition, position3, targetView2, value) {
  let nextTransition;
  if (nextTransition = transition.ascii8 || transition.num8) {
    targetView2.setInt8(position3, value, true);
    updatedPosition = position3 + 1;
    return nextTransition;
  }
  if (nextTransition = transition.string16 || transition.object16) {
    targetView2.setInt16(position3, value, true);
    updatedPosition = position3 + 2;
    return nextTransition;
  }
  if (nextTransition = transition.num32) {
    targetView2.setUint32(position3, 3758096640 + value, true);
    updatedPosition = position3 + 4;
    return nextTransition;
  }
  if (nextTransition = transition.num64) {
    targetView2.setFloat64(position3, NaN, true);
    targetView2.setInt8(position3, value);
    updatedPosition = position3 + 8;
    return nextTransition;
  }
  updatedPosition = position3;
  return;
}
function createTypeTransition(transition, type, size) {
  let typeName = TYPE_NAMES[type] + (size << 3);
  let newTransition = transition[typeName] || (transition[typeName] = Object.create(null));
  newTransition.__type = type;
  newTransition.__size = size;
  newTransition.__parent = transition;
  return newTransition;
}
function onLoadedStructures2(sharedData) {
  if (!(sharedData instanceof Map))
    return sharedData;
  let typed = sharedData.get("typed") || [];
  if (Object.isFrozen(typed))
    typed = typed.map((structure) => structure.slice(0));
  let named = sharedData.get("named");
  let transitions = Object.create(null);
  for (let i = 0, l = typed.length;i < l; i++) {
    let structure = typed[i];
    let transition = transitions;
    for (let [type, size, key] of structure) {
      let nextTransition = transition[key];
      if (!nextTransition) {
        transition[key] = nextTransition = {
          key,
          parent: transition,
          enumerationOffset: 0,
          ascii0: null,
          ascii8: null,
          num8: null,
          string16: null,
          object16: null,
          num32: null,
          float64: null,
          date64: null
        };
      }
      transition = createTypeTransition(nextTransition, type, size);
    }
    transition[RECORD_SYMBOL] = i;
  }
  typed.transitions = transitions;
  this.typedStructs = typed;
  this.lastTypedStructuresLength = typed.length;
  return named;
}
var sourceSymbol = Symbol.for("source");
function readStruct2(src2, position3, srcEnd2, unpackr) {
  let recordId = src2[position3++] - 32;
  if (recordId >= 24) {
    switch (recordId) {
      case 24:
        recordId = src2[position3++];
        break;
      case 25:
        recordId = src2[position3++] + (src2[position3++] << 8);
        break;
      case 26:
        recordId = src2[position3++] + (src2[position3++] << 8) + (src2[position3++] << 16);
        break;
      case 27:
        recordId = src2[position3++] + (src2[position3++] << 8) + (src2[position3++] << 16) + (src2[position3++] << 24);
        break;
    }
  }
  let structure = unpackr.typedStructs && unpackr.typedStructs[recordId];
  if (!structure) {
    src2 = Uint8Array.prototype.slice.call(src2, position3, srcEnd2);
    srcEnd2 -= position3;
    position3 = 0;
    if (!unpackr.getStructures)
      throw new Error(`Reference to shared structure ${recordId} without getStructures method`);
    unpackr._mergeStructures(unpackr.getStructures());
    if (!unpackr.typedStructs)
      throw new Error("Could not find any shared typed structures");
    unpackr.lastTypedStructuresLength = unpackr.typedStructs.length;
    structure = unpackr.typedStructs[recordId];
    if (!structure)
      throw new Error("Could not find typed structure " + recordId);
  }
  var construct = structure.construct;
  var fullConstruct = structure.fullConstruct;
  if (!construct) {
    construct = structure.construct = function LazyObject() {};
    fullConstruct = structure.fullConstruct = function LoadedObject() {};
    fullConstruct.prototype = unpackr.structPrototype || {};
    var prototype = construct.prototype = unpackr.structPrototype ? Object.create(unpackr.structPrototype) : {};
    let properties = [];
    let currentOffset = 0;
    let lastRefProperty;
    for (let i = 0, l = structure.length;i < l; i++) {
      let definition = structure[i];
      let [type, size, key, enumerationOffset] = definition;
      if (key === "__proto__")
        key = "__proto_";
      let property = {
        key,
        offset: currentOffset
      };
      if (enumerationOffset)
        properties.splice(i + enumerationOffset, 0, property);
      else
        properties.push(property);
      let getRef;
      switch (size) {
        case 0:
          getRef = () => 0;
          break;
        case 1:
          getRef = (source, position4) => {
            let ref = source.bytes[position4 + property.offset];
            return ref >= 246 ? toConstant(ref) : ref;
          };
          break;
        case 2:
          getRef = (source, position4) => {
            let src3 = source.bytes;
            let dataView2 = src3.dataView || (src3.dataView = new DataView(src3.buffer, src3.byteOffset, src3.byteLength));
            let ref = dataView2.getUint16(position4 + property.offset, true);
            return ref >= 65280 ? toConstant(ref & 255) : ref;
          };
          break;
        case 4:
          getRef = (source, position4) => {
            let src3 = source.bytes;
            let dataView2 = src3.dataView || (src3.dataView = new DataView(src3.buffer, src3.byteOffset, src3.byteLength));
            let ref = dataView2.getUint32(position4 + property.offset, true);
            return ref >= 4294967040 ? toConstant(ref & 255) : ref;
          };
          break;
      }
      property.getRef = getRef;
      currentOffset += size;
      let get;
      switch (type) {
        case ASCII:
          if (lastRefProperty && !lastRefProperty.next)
            lastRefProperty.next = property;
          lastRefProperty = property;
          property.multiGetCount = 0;
          get = function(source) {
            let src3 = source.bytes;
            let position4 = source.position;
            let refStart = currentOffset + position4;
            let ref = getRef(source, position4);
            if (typeof ref !== "number")
              return ref;
            let end, next = property.next;
            while (next) {
              end = next.getRef(source, position4);
              if (typeof end === "number")
                break;
              else
                end = null;
              next = next.next;
            }
            if (end == null)
              end = source.bytesEnd - refStart;
            if (source.srcString) {
              return source.srcString.slice(ref, end);
            }
            return readString(src3, ref + refStart, end - ref);
          };
          break;
        case UTF8:
        case OBJECT_DATA:
          if (lastRefProperty && !lastRefProperty.next)
            lastRefProperty.next = property;
          lastRefProperty = property;
          get = function(source) {
            let position4 = source.position;
            let refStart = currentOffset + position4;
            let ref = getRef(source, position4);
            if (typeof ref !== "number")
              return ref;
            let src3 = source.bytes;
            let end, next = property.next;
            while (next) {
              end = next.getRef(source, position4);
              if (typeof end === "number")
                break;
              else
                end = null;
              next = next.next;
            }
            if (end == null)
              end = source.bytesEnd - refStart;
            if (type === UTF8) {
              return src3.toString("utf8", ref + refStart, end + refStart);
            } else {
              currentSource = source;
              try {
                return unpackr.unpack(src3, { start: ref + refStart, end: end + refStart });
              } finally {
                currentSource = null;
              }
            }
          };
          break;
        case NUMBER:
          switch (size) {
            case 4:
              get = function(source) {
                let src3 = source.bytes;
                let dataView2 = src3.dataView || (src3.dataView = new DataView(src3.buffer, src3.byteOffset, src3.byteLength));
                let position4 = source.position + property.offset;
                let value = dataView2.getInt32(position4, true);
                if (value < 536870912) {
                  if (value > -520093696)
                    return value;
                  if (value > -536870912)
                    return toConstant(value & 255);
                }
                let fValue = dataView2.getFloat32(position4, true);
                let multiplier = mult10[(src3[position4 + 3] & 127) << 1 | src3[position4 + 2] >> 7];
                return (multiplier * fValue + (fValue > 0 ? 0.5 : -0.5) >> 0) / multiplier;
              };
              break;
            case 8:
              get = function(source) {
                let src3 = source.bytes;
                let dataView2 = src3.dataView || (src3.dataView = new DataView(src3.buffer, src3.byteOffset, src3.byteLength));
                let value = dataView2.getFloat64(source.position + property.offset, true);
                if (isNaN(value)) {
                  let byte = src3[source.position + property.offset];
                  if (byte >= 246)
                    return toConstant(byte);
                }
                return value;
              };
              break;
            case 1:
              get = function(source) {
                let src3 = source.bytes;
                let value = src3[source.position + property.offset];
                return value < 246 ? value : toConstant(value);
              };
              break;
          }
          break;
        case DATE:
          get = function(source) {
            let src3 = source.bytes;
            let dataView2 = src3.dataView || (src3.dataView = new DataView(src3.buffer, src3.byteOffset, src3.byteLength));
            return new Date(dataView2.getFloat64(source.position + property.offset, true));
          };
          break;
      }
      property.get = get;
    }
    if (evalSupported) {
      let objectLiteralProperties = [];
      let args = [];
      let i = 0;
      let hasInheritedProperties;
      for (let property of properties) {
        if (unpackr.alwaysLazyProperty && unpackr.alwaysLazyProperty(property.key)) {
          hasInheritedProperties = true;
          continue;
        }
        Object.defineProperty(prototype, property.key, { get: withSource(property.get), enumerable: true });
        let valueFunction = "v" + i++;
        args.push(valueFunction);
        objectLiteralProperties.push("o[" + JSON.stringify(property.key) + "]=" + valueFunction + "(s)");
      }
      if (hasInheritedProperties) {
        objectLiteralProperties.push("__proto__:this");
      }
      let toObject = new Function(...args, "var c=this;return function(s){var o=new c();" + objectLiteralProperties.join(";") + ";return o;}").apply(fullConstruct, properties.map((prop) => prop.get));
      Object.defineProperty(prototype, "toJSON", {
        value(omitUnderscoredProperties) {
          return toObject.call(this, this[sourceSymbol]);
        }
      });
    } else {
      Object.defineProperty(prototype, "toJSON", {
        value(omitUnderscoredProperties) {
          let resolved = {};
          for (let i = 0, l = properties.length;i < l; i++) {
            let key = properties[i].key;
            resolved[key] = this[key];
          }
          return resolved;
        }
      });
    }
  }
  var instance = new construct;
  instance[sourceSymbol] = {
    bytes: src2,
    position: position3,
    srcString: "",
    bytesEnd: srcEnd2
  };
  return instance;
}
function toConstant(code) {
  switch (code) {
    case 246:
      return null;
    case 247:
      return;
    case 248:
      return false;
    case 249:
      return true;
  }
  throw new Error("Unknown constant");
}
function withSource(get) {
  return function() {
    return get(this[sourceSymbol]);
  };
}
function saveState2() {
  if (currentSource) {
    currentSource.bytes = Uint8Array.prototype.slice.call(currentSource.bytes, currentSource.position, currentSource.bytesEnd);
    currentSource.position = 0;
    currentSource.bytesEnd = currentSource.bytes.length;
  }
}
function prepareStructures2(structures, packr) {
  if (packr.typedStructs) {
    let structMap = new Map;
    structMap.set("named", structures);
    structMap.set("typed", packr.typedStructs);
    structures = structMap;
  }
  let lastTypedStructuresLength = packr.lastTypedStructuresLength || 0;
  structures.isCompatible = (existing) => {
    let compatible = true;
    if (existing instanceof Map) {
      let named = existing.get("named") || [];
      if (named.length !== (packr.lastNamedStructuresLength || 0))
        compatible = false;
      let typed = existing.get("typed") || [];
      if (typed.length !== lastTypedStructuresLength)
        compatible = false;
    } else if (existing instanceof Array || Array.isArray(existing)) {
      if (existing.length !== (packr.lastNamedStructuresLength || 0))
        compatible = false;
    }
    if (!compatible)
      packr._mergeStructures(existing);
    return compatible;
  };
  packr.lastTypedStructuresLength = packr.typedStructs && packr.typedStructs.length;
  return structures;
}
setReadStruct(readStruct2, onLoadedStructures2, saveState2);
// ../../node_modules/.bun/msgpackr@1.11.8/node_modules/msgpackr/node-index.js
var nativeAccelerationDisabled = process.env.MSGPACKR_NATIVE_ACCELERATION_DISABLED !== undefined && process.env.MSGPACKR_NATIVE_ACCELERATION_DISABLED.toLowerCase() === "true";
if (!nativeAccelerationDisabled) {
  let extractor;
  try {
    if (true)
      extractor = require_msgpackr_extract();
    else
      ;
    if (extractor)
      setExtractor(extractor.extractStrings);
  } catch (error) {}
}

// src/infrastructure/serialization/msgpack-serializer.ts
class MsgpackSerializer {
  encode(value) {
    return pack(value);
  }
  decode(data) {
    return unpack(data);
  }
}

// src/infrastructure/serialization/binary-event-batch-serializer.ts
var BATCH_MAGIC2 = 1397773378;
var BATCH_VERSION = 1;
var BATCH_HEADER_SIZE3 = 12;
var NULL_LENGTH = 4294967295;
var textEncoder3 = new TextEncoder;
var textDecoder = new TextDecoder;
function isStoredEvent(value) {
  if (!value || typeof value !== "object")
    return false;
  const record = value;
  return typeof record["streamId"] === "string" && typeof record["type"] === "string" && typeof record["revision"] === "number" && typeof record["globalPosition"] === "number" && typeof record["timestamp"] === "number";
}
function ensureSafeUint(value, label) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${label} must be a non-negative safe integer`);
  }
}
function encodeEventBatch(events, packr) {
  const encoded = events.map((event) => {
    const streamId = textEncoder3.encode(event.streamId);
    const type = textEncoder3.encode(event.type);
    const tenantId = event.tenantId !== undefined ? textEncoder3.encode(event.tenantId) : null;
    const metadata = event.metadata !== undefined ? packr.pack(event.metadata) : null;
    const data = packr.pack(event.data);
    ensureSafeUint(event.revision, "Revision");
    ensureSafeUint(event.globalPosition, "Global position");
    ensureSafeUint(event.timestamp, "Timestamp");
    return {
      event,
      streamId,
      type,
      tenantId,
      metadata,
      data
    };
  });
  let totalSize = BATCH_HEADER_SIZE3;
  for (const entry of encoded) {
    totalSize += 4 + entry.streamId.length;
    totalSize += 4 + entry.type.length;
    totalSize += 4;
    totalSize += 8;
    totalSize += 8;
    totalSize += 4 + (entry.tenantId ? entry.tenantId.length : 0);
    totalSize += 4 + (entry.metadata ? entry.metadata.length : 0);
    totalSize += 4 + entry.data.length;
  }
  const buffer = new Uint8Array(totalSize);
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  view.setUint32(0, BATCH_MAGIC2, false);
  view.setUint16(4, BATCH_VERSION, false);
  view.setUint16(6, 0, false);
  view.setUint32(8, events.length, false);
  let offset = BATCH_HEADER_SIZE3;
  for (const entry of encoded) {
    view.setUint32(offset, entry.streamId.length, false);
    offset += 4;
    buffer.set(entry.streamId, offset);
    offset += entry.streamId.length;
    view.setUint32(offset, entry.type.length, false);
    offset += 4;
    buffer.set(entry.type, offset);
    offset += entry.type.length;
    view.setUint32(offset, entry.event.revision, false);
    offset += 4;
    view.setBigUint64(offset, BigInt(entry.event.globalPosition), false);
    offset += 8;
    view.setBigUint64(offset, BigInt(entry.event.timestamp), false);
    offset += 8;
    if (entry.tenantId) {
      view.setUint32(offset, entry.tenantId.length, false);
      offset += 4;
      buffer.set(entry.tenantId, offset);
      offset += entry.tenantId.length;
    } else {
      view.setUint32(offset, NULL_LENGTH, false);
      offset += 4;
    }
    if (entry.metadata) {
      view.setUint32(offset, entry.metadata.length, false);
      offset += 4;
      buffer.set(entry.metadata, offset);
      offset += entry.metadata.length;
    } else {
      view.setUint32(offset, NULL_LENGTH, false);
      offset += 4;
    }
    view.setUint32(offset, entry.data.length, false);
    offset += 4;
    buffer.set(entry.data, offset);
    offset += entry.data.length;
  }
  return buffer;
}
function decodeEventBatch(data, unpackr) {
  if (data.length < BATCH_HEADER_SIZE3) {
    throw new Error("Batch payload is too small");
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const magic = view.getUint32(0, false);
  if (magic !== BATCH_MAGIC2) {
    throw new Error("Invalid batch payload magic");
  }
  const version = view.getUint16(4, false);
  if (version !== BATCH_VERSION) {
    throw new Error(`Unsupported batch payload version: ${version}`);
  }
  const eventCount = view.getUint32(8, false);
  let offset = BATCH_HEADER_SIZE3;
  const events = [];
  for (let i = 0;i < eventCount; i += 1) {
    if (offset + 4 > data.length) {
      throw new Error("Batch payload truncated (streamId length)");
    }
    const streamIdLength = view.getUint32(offset, false);
    offset += 4;
    const streamIdEnd = offset + streamIdLength;
    if (streamIdEnd > data.length) {
      throw new Error("Batch payload truncated (streamId)");
    }
    const streamId = textDecoder.decode(data.subarray(offset, streamIdEnd));
    offset = streamIdEnd;
    if (offset + 4 > data.length) {
      throw new Error("Batch payload truncated (type length)");
    }
    const typeLength = view.getUint32(offset, false);
    offset += 4;
    const typeEnd = offset + typeLength;
    if (typeEnd > data.length) {
      throw new Error("Batch payload truncated (type)");
    }
    const type = textDecoder.decode(data.subarray(offset, typeEnd));
    offset = typeEnd;
    if (offset + 20 > data.length) {
      throw new Error("Batch payload truncated (position fields)");
    }
    const revision = view.getUint32(offset, false);
    offset += 4;
    const globalPosition = Number(view.getBigUint64(offset, false));
    offset += 8;
    const timestamp = Number(view.getBigUint64(offset, false));
    offset += 8;
    if (!Number.isSafeInteger(globalPosition) || !Number.isSafeInteger(timestamp)) {
      throw new Error("Batch payload contains unsafe integer positions");
    }
    if (offset + 4 > data.length) {
      throw new Error("Batch payload truncated (tenant length)");
    }
    const tenantLength = view.getUint32(offset, false);
    offset += 4;
    let tenantId;
    if (tenantLength !== NULL_LENGTH) {
      const tenantEnd = offset + tenantLength;
      if (tenantEnd > data.length) {
        throw new Error("Batch payload truncated (tenant id)");
      }
      tenantId = textDecoder.decode(data.subarray(offset, tenantEnd));
      offset = tenantEnd;
    }
    if (offset + 4 > data.length) {
      throw new Error("Batch payload truncated (metadata length)");
    }
    const metadataLength = view.getUint32(offset, false);
    offset += 4;
    let metadata;
    if (metadataLength !== NULL_LENGTH) {
      const metadataEnd = offset + metadataLength;
      if (metadataEnd > data.length) {
        throw new Error("Batch payload truncated (metadata)");
      }
      metadata = unpackr.unpack(data.subarray(offset, metadataEnd));
      offset = metadataEnd;
    }
    if (offset + 4 > data.length) {
      throw new Error("Batch payload truncated (data length)");
    }
    const dataLength = view.getUint32(offset, false);
    offset += 4;
    const dataEnd = offset + dataLength;
    if (dataEnd > data.length) {
      throw new Error("Batch payload truncated (data)");
    }
    const payload = unpackr.unpack(data.subarray(offset, dataEnd));
    offset = dataEnd;
    events.push({
      streamId,
      type,
      data: payload,
      metadata,
      revision,
      globalPosition,
      timestamp,
      tenantId: tenantId ?? "default"
    });
  }
  return events;
}

class BinaryEventBatchSerializer {
  packr = new Packr({ useRecords: true });
  unpackr = new Unpackr({ useRecords: true });
  encode(value) {
    if (!Array.isArray(value)) {
      throw new Error("BinaryEventBatchSerializer only supports event batches");
    }
    if (value.length > 0 && !isStoredEvent(value[0])) {
      throw new Error("BinaryEventBatchSerializer expects StoredEvent[]");
    }
    return encodeEventBatch(value, this.packr);
  }
  decode(data) {
    if (data.length < BATCH_HEADER_SIZE3) {
      throw new Error("Batch payload is too small");
    }
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    if (view.getUint32(0, false) !== BATCH_MAGIC2) {
      throw new Error("Invalid batch payload magic");
    }
    return decodeEventBatch(data, this.unpackr);
  }
}

// src/infrastructure/serialization/noop-compressor.ts
class NoopCompressor {
  compress(data) {
    return data;
  }
  decompress(data) {
    return data;
  }
}

// src/spitedb.ts
class SpiteDB {
  eventStore;
  coordinator;
  dataDir;
  projectionsStarted = false;
  backpressure;
  constructor(eventStore, coordinator, dataDir, backpressure) {
    this.eventStore = eventStore;
    this.coordinator = coordinator;
    this.dataDir = dataDir;
    this.backpressure = backpressure;
  }
  static async open(path, options = {}) {
    const fs = new BunFileSystem;
    const clock = new BunClock;
    const eventSerializer = options.eventSerializer ?? new BinaryEventBatchSerializer;
    const projectionSerializer = options.projectionSerializer ?? new MsgpackSerializer;
    const compressor = options.compressor ?? new NoopCompressor;
    const eventStoreConfig = {
      fs,
      clock,
      serializer: eventSerializer,
      compressor
    };
    if (options.maxSegmentSize !== undefined) {
      eventStoreConfig.maxSegmentSize = options.maxSegmentSize;
    }
    if (options.indexCacheSize !== undefined) {
      eventStoreConfig.indexCacheSize = options.indexCacheSize;
    }
    if (options.autoFlushCount !== undefined) {
      eventStoreConfig.autoFlushCount = options.autoFlushCount;
    }
    if (options.readProfiler !== undefined) {
      eventStoreConfig.readProfiler = options.readProfiler;
    }
    const eventStore = new EventStore(eventStoreConfig);
    const eventsDir = `${path}/events`;
    await eventStore.open(eventsDir);
    const coordinatorConfig = {
      eventStore,
      fs,
      serializer: projectionSerializer,
      clock,
      dataDir: `${path}/projections`
    };
    if (options.projectionPollingIntervalMs !== undefined) {
      coordinatorConfig.pollingIntervalMs = options.projectionPollingIntervalMs;
    }
    if (options.projectionCheckpointIntervalMs !== undefined) {
      coordinatorConfig.defaultCheckpointIntervalMs = options.projectionCheckpointIntervalMs;
    }
    if (options.projectionBatchSize !== undefined) {
      coordinatorConfig.defaultBatchSize = options.projectionBatchSize;
    }
    if (options.projectionSharedReader !== undefined) {
      coordinatorConfig.sharedReader = options.projectionSharedReader;
    }
    const coordinator = new ProjectionCoordinator(coordinatorConfig);
    const backpressure = resolveBackpressure(options.projectionBackpressure);
    return new SpiteDB(eventStore, coordinator, path, backpressure);
  }
  async close() {
    if (!this.eventStore.isOpen()) {
      return;
    }
    await this.stopProjections();
    await this.eventStore.close();
  }
  isOpen() {
    return this.eventStore.isOpen();
  }
  getDataDir() {
    return this.dataDir;
  }
  async append(streamId, events, options) {
    this.ensureOpen();
    await this.applyProjectionBackpressure();
    return this.eventStore.append(streamId, events, options);
  }
  async appendBatch(operations) {
    this.ensureOpen();
    await this.applyProjectionBackpressure();
    return this.eventStore.appendBatch(operations);
  }
  async readStream(streamId, options) {
    this.ensureOpen();
    return this.eventStore.readStream(streamId, options);
  }
  async readGlobal(fromPosition = 0, options) {
    this.ensureOpen();
    return this.eventStore.readGlobal(fromPosition, options);
  }
  async* streamGlobal(fromPosition = 0) {
    this.ensureOpen();
    yield* this.eventStore.streamGlobal(fromPosition);
  }
  getStreamRevision(streamId) {
    this.ensureOpen();
    return this.eventStore.getStreamRevision(streamId);
  }
  hasStream(streamId) {
    this.ensureOpen();
    return this.eventStore.hasStream(streamId);
  }
  async getStreamIds() {
    this.ensureOpen();
    return this.eventStore.getStreamIds();
  }
  getGlobalPosition() {
    this.ensureOpen();
    return this.eventStore.getGlobalPosition();
  }
  async flush() {
    this.ensureOpen();
    await this.eventStore.flush();
  }
  registerProjection(registration, options) {
    this.ensureOpen();
    this.coordinator.getRegistry().register(registration, options);
  }
  async startProjections() {
    this.ensureOpen();
    if (this.projectionsStarted) {
      return;
    }
    await this.coordinator.start();
    this.projectionsStarted = true;
  }
  async stopProjections() {
    if (!this.projectionsStarted) {
      return;
    }
    await this.coordinator.stop();
    this.projectionsStarted = false;
  }
  getProjection(name) {
    this.ensureOpen();
    return this.coordinator.getProjection(name);
  }
  requireProjection(name) {
    this.ensureOpen();
    if (!this.projectionsStarted) {
      throw new ProjectionsNotStartedError;
    }
    return this.coordinator.requireProjection(name);
  }
  async waitForProjections(timeoutMs = 30000) {
    this.ensureOpen();
    if (!this.projectionsStarted) {
      throw new ProjectionsNotStartedError;
    }
    await this.coordinator.waitForCatchUp(timeoutMs);
  }
  async forceProjectionCheckpoint() {
    this.ensureOpen();
    if (!this.projectionsStarted) {
      throw new ProjectionsNotStartedError;
    }
    await this.coordinator.forceCheckpoint();
  }
  getProjectionStatus() {
    this.ensureOpen();
    return this.coordinator.getStatus();
  }
  projectionsRunning() {
    return this.projectionsStarted;
  }
  ensureOpen() {
    if (!this.eventStore.isOpen()) {
      throw new SpiteDBNotOpenError;
    }
  }
  async applyProjectionBackpressure() {
    if (!this.backpressure || !this.projectionsStarted) {
      return;
    }
    const {
      maxLag,
      maxWaitMs,
      pollIntervalMs,
      mode
    } = this.backpressure;
    const start = Date.now();
    while (true) {
      const status = this.coordinator.getStatus();
      if (status.projections.length === 0) {
        return;
      }
      let slowest = status.projections[0];
      for (const projection of status.projections) {
        if (projection.currentPosition < slowest.currentPosition) {
          slowest = projection;
        }
      }
      const current = slowest.currentPosition < 0 ? 0 : slowest.currentPosition;
      const lag = status.globalPosition > current ? status.globalPosition - current : 0;
      if (lag <= maxLag) {
        return;
      }
      if (mode === "fail") {
        throw new ProjectionBackpressureError(slowest.name, lag, maxLag);
      }
      const waitedMs = Date.now() - start;
      if (waitedMs >= maxWaitMs) {
        throw new ProjectionBackpressureTimeoutError(slowest.name, lag, maxLag, waitedMs, maxWaitMs);
      }
      await new Promise((resolve) => {
        setTimeout(resolve, pollIntervalMs);
      });
    }
  }
}
function resolveBackpressure(options) {
  if (options === false) {
    return;
  }
  return {
    maxLag: options?.maxLag ?? 200000,
    maxWaitMs: options?.maxWaitMs ?? 5000,
    pollIntervalMs: options?.pollIntervalMs ?? 25,
    mode: options?.mode ?? "block"
  };
}
// src/domain/value-objects/stream-id.ts
var MAX_STREAM_ID_LENGTH = 256;
var STREAM_ID_PATTERN = /^[a-zA-Z0-9_\-:.]+$/;

class StreamId {
  value;
  constructor(value) {
    this.value = value;
  }
  static from(value) {
    if (!value) {
      throw new InvalidStreamIdError("StreamId cannot be empty");
    }
    if (value.length > MAX_STREAM_ID_LENGTH) {
      throw new InvalidStreamIdError(`StreamId cannot exceed ${MAX_STREAM_ID_LENGTH} characters`);
    }
    if (!STREAM_ID_PATTERN.test(value)) {
      throw new InvalidStreamIdError("StreamId can only contain alphanumeric characters, underscores, hyphens, colons, and dots");
    }
    return new StreamId(value);
  }
  toString() {
    return this.value;
  }
  equals(other) {
    return this.value === other.value;
  }
  hash() {
    let hash = 2166136261;
    for (let i = 0;i < this.value.length; i++) {
      hash ^= this.value.charCodeAt(i);
      hash = hash * 16777619 >>> 0;
    }
    return hash;
  }
}
// src/domain/value-objects/global-position.ts
class GlobalPosition {
  value;
  constructor(value) {
    this.value = value;
  }
  static from(value) {
    if (!Number.isSafeInteger(value)) {
      throw new InvalidPositionError("GlobalPosition must be a safe integer");
    }
    if (value < 0) {
      throw new InvalidPositionError("GlobalPosition cannot be negative");
    }
    return new GlobalPosition(value);
  }
  static BEGINNING = GlobalPosition.from(0);
  toNumber() {
    return this.value;
  }
  next() {
    return new GlobalPosition(this.value + 1);
  }
  advance(count) {
    return new GlobalPosition(this.value + count);
  }
  equals(other) {
    return this.value === other.value;
  }
  compareTo(other) {
    if (this.value < other.value)
      return -1;
    if (this.value > other.value)
      return 1;
    return 0;
  }
  isAfter(other) {
    return this.value > other.value;
  }
  isBefore(other) {
    return this.value < other.value;
  }
}
// src/domain/value-objects/revision.ts
class Revision {
  value;
  constructor(value) {
    this.value = value;
  }
  static from(value) {
    if (!Number.isInteger(value)) {
      throw new Error("Revision must be an integer");
    }
    return new Revision(value);
  }
  static NONE = new Revision(-1);
  static ANY = new Revision(-2);
  toNumber() {
    return this.value;
  }
  next() {
    return new Revision(this.value + 1);
  }
  isNone() {
    return this.value === -1;
  }
  isAny() {
    return this.value === -2;
  }
  equals(other) {
    return this.value === other.value;
  }
  compareTo(other) {
    if (this.value < other.value)
      return -1;
    if (this.value > other.value)
      return 1;
    return 0;
  }
}
// src/domain/value-objects/tenant-id.ts
var MAX_TENANT_ID_LENGTH = 128;
var TENANT_ID_PATTERN = /^[a-zA-Z0-9_\-]+$/;

class TenantId {
  value;
  constructor(value) {
    this.value = value;
  }
  static from(value) {
    if (!value) {
      throw new Error("TenantId cannot be empty");
    }
    if (value.length > MAX_TENANT_ID_LENGTH) {
      throw new Error(`TenantId cannot exceed ${MAX_TENANT_ID_LENGTH} characters`);
    }
    if (!TENANT_ID_PATTERN.test(value)) {
      throw new Error("TenantId can only contain alphanumeric characters, underscores, and hyphens");
    }
    return new TenantId(value);
  }
  static DEFAULT = new TenantId("default");
  toString() {
    return this.value;
  }
  equals(other) {
    return this.value === other.value;
  }
  isDefault() {
    return this.value === "default";
  }
}
// src/domain/value-objects/batch-id.ts
class BatchId {
  value;
  constructor(value) {
    this.value = value;
  }
  static from(value) {
    return new BatchId(BigInt(value));
  }
  static generate() {
    const timestamp = BigInt(Date.now()) << 20n;
    const random = BigInt(Math.floor(Math.random() * 1048575));
    return new BatchId(timestamp | random);
  }
  toBigInt() {
    return this.value;
  }
  toString() {
    return this.value.toString(16);
  }
  equals(other) {
    return this.value === other.value;
  }
}
// src/domain/value-objects/command-id.ts
class CommandId {
  value;
  constructor(value) {
    this.value = value;
  }
  static from(value) {
    if (!value) {
      throw new Error("CommandId cannot be empty");
    }
    if (value.length > 256) {
      throw new Error("CommandId cannot exceed 256 characters");
    }
    return new CommandId(value);
  }
  static generate() {
    return new CommandId(crypto.randomUUID());
  }
  toString() {
    return this.value;
  }
  equals(other) {
    return this.value === other.value;
  }
}
// src/infrastructure/serialization/fast-event-serializer.ts
var BATCH_MARKER = "__spite_batch";
var BATCH_VERSION2 = 1;
function isStoredEvent2(value) {
  if (!value || typeof value !== "object")
    return false;
  const record = value;
  return typeof record["streamId"] === "string" && typeof record["type"] === "string" && typeof record["revision"] === "number" && typeof record["globalPosition"] === "number" && typeof record["timestamp"] === "number";
}
function toColumnarBatch(events) {
  const len = events.length;
  const streamIds = new Array(len);
  const types = new Array(len);
  const data = new Array(len);
  const metadata = new Array(len);
  const revisions = new Array(len);
  const globalPositions = new Array(len);
  const timestamps = new Array(len);
  const tenantIds = new Array(len);
  for (let i = 0;i < len; i++) {
    const event = events[i];
    streamIds[i] = event.streamId;
    types[i] = event.type;
    data[i] = event.data;
    metadata[i] = event.metadata;
    revisions[i] = event.revision;
    globalPositions[i] = event.globalPosition;
    timestamps[i] = event.timestamp;
    tenantIds[i] = event.tenantId;
  }
  return {
    [BATCH_MARKER]: BATCH_VERSION2,
    streamIds,
    types,
    data,
    metadata,
    revisions,
    globalPositions,
    timestamps,
    tenantIds
  };
}
function fromColumnarBatch(batch) {
  const len = batch.streamIds.length;
  const events = new Array(len);
  for (let i = 0;i < len; i++) {
    events[i] = {
      streamId: batch.streamIds[i] ?? "",
      type: batch.types[i] ?? "",
      data: batch.data[i],
      metadata: batch.metadata[i],
      revision: batch.revisions[i] ?? 0,
      globalPosition: batch.globalPositions[i] ?? 0,
      timestamp: batch.timestamps[i] ?? 0,
      tenantId: batch.tenantIds[i] ?? "default"
    };
  }
  return events;
}

class FastEventSerializer {
  packr = new Packr({ useRecords: true });
  unpackr = new Unpackr({ useRecords: true });
  encode(value) {
    if (Array.isArray(value) && value.length > 0 && isStoredEvent2(value[0])) {
      return this.packr.pack(toColumnarBatch(value));
    }
    return this.packr.pack(value);
  }
  decode(data) {
    const decoded = this.unpackr.unpack(data);
    if (decoded && typeof decoded === "object" && decoded[BATCH_MARKER] === BATCH_VERSION2) {
      return fromColumnarBatch(decoded);
    }
    return decoded;
  }
}
// src/infrastructure/serialization/zstd-compressor.ts
class ZstdCompressor {
  static DEFAULT_LEVEL = 3;
  compress(data, level = ZstdCompressor.DEFAULT_LEVEL) {
    return Bun.zstdCompressSync(data, { level });
  }
  decompress(data) {
    return Bun.zstdDecompressSync(data);
  }
}
// src/infrastructure/projections/stores/disk-kv-store.ts
var TOMBSTONE_MARKER = 4294967295;

class DiskKeyValueStore {
  fs;
  serializer;
  path;
  index = new Map;
  writePosition = 0;
  liveBytes = 0;
  totalBytes = 0;
  writeHandle = null;
  opened = false;
  constructor(config) {
    this.fs = config.fs;
    this.serializer = config.serializer;
    this.path = config.path;
  }
  async open() {
    if (this.opened) {
      return;
    }
    const dir = this.path.substring(0, this.path.lastIndexOf("/"));
    if (dir && !await this.fs.exists(dir)) {
      await this.fs.mkdir(dir, { recursive: true });
    }
    const exists = await this.fs.exists(this.path);
    if (exists) {
      await this.rebuildIndex();
    }
    this.writeHandle = await this.fs.open(this.path, exists ? "append" : "write");
    this.opened = true;
  }
  async close() {
    if (!this.opened) {
      return;
    }
    if (this.writeHandle) {
      await this.fs.sync(this.writeHandle);
      await this.fs.close(this.writeHandle);
      this.writeHandle = null;
    }
    this.opened = false;
  }
  async get(key) {
    const entry = this.index.get(key);
    if (!entry) {
      return;
    }
    const record = await this.fs.readFileSlice(this.path, entry.offset, entry.offset + entry.length);
    const parsed = this.parseRecord(record);
    if (!parsed || parsed.isDeleted) {
      return;
    }
    return parsed.value;
  }
  async set(key, value) {
    this.ensureOpen();
    const valueData = this.serializer.encode(value);
    const record = this.buildRecord(key, valueData);
    const existingEntry = this.index.get(key);
    if (existingEntry) {
      this.liveBytes -= existingEntry.length;
    }
    const offset = this.writePosition;
    await writeAllBytes(this.fs, this.writeHandle, record);
    this.writePosition += record.length;
    const newEntry = { offset, length: record.length };
    this.index.set(key, newEntry);
    this.liveBytes += record.length;
    this.totalBytes = this.writePosition;
  }
  async delete(key) {
    const existingEntry = this.index.get(key);
    if (!existingEntry) {
      return false;
    }
    this.ensureOpen();
    const record = this.buildTombstone(key);
    await writeAllBytes(this.fs, this.writeHandle, record);
    this.writePosition += record.length;
    this.liveBytes -= existingEntry.length;
    this.totalBytes = this.writePosition;
    this.index.delete(key);
    return true;
  }
  has(key) {
    return this.index.has(key);
  }
  keys() {
    return this.index.keys();
  }
  get size() {
    return this.index.size;
  }
  async sync() {
    if (this.writeHandle) {
      await this.fs.sync(this.writeHandle);
    }
  }
  getGarbageRatio() {
    if (this.totalBytes === 0) {
      return 0;
    }
    return 1 - this.liveBytes / this.totalBytes;
  }
  getTotalBytes() {
    return this.totalBytes;
  }
  getLiveBytes() {
    return this.liveBytes;
  }
  async compact() {
    this.ensureOpen();
    const tempPath = this.path + ".compact";
    try {
      if (this.writeHandle) {
        await this.fs.sync(this.writeHandle);
        await this.fs.close(this.writeHandle);
        this.writeHandle = null;
      }
      const tempHandle = await this.fs.open(tempPath, "write");
      const newIndex = new Map;
      let newPosition = 0;
      for (const [key, entry] of this.index) {
        const record = await this.fs.readFileSlice(this.path, entry.offset, entry.offset + entry.length);
        await writeAllBytes(this.fs, tempHandle, record);
        newIndex.set(key, { offset: newPosition, length: record.length });
        newPosition += record.length;
      }
      await this.fs.sync(tempHandle);
      await this.fs.close(tempHandle);
      await this.fs.rename(tempPath, this.path);
      this.index = newIndex;
      this.writePosition = newPosition;
      this.liveBytes = newPosition;
      this.totalBytes = newPosition;
      this.writeHandle = await this.fs.open(this.path, "append");
    } catch (error) {
      try {
        if (await this.fs.exists(tempPath)) {
          await this.fs.unlink(tempPath);
        }
      } catch {}
      this.writeHandle = await this.fs.open(this.path, "append");
      throw error;
    }
  }
  async rebuildIndex() {
    const data = await this.fs.readFile(this.path);
    let offset = 0;
    let corruptionDetected = false;
    this.index.clear();
    this.liveBytes = 0;
    while (offset < data.length) {
      if (offset + 8 > data.length) {
        corruptionDetected = true;
        break;
      }
      const view = new DataView(data.buffer, data.byteOffset + offset, data.length - offset);
      const keyLen = view.getUint32(0, true);
      if (keyLen === 0 || keyLen > 65536) {
        corruptionDetected = true;
        break;
      }
      if (offset + 4 + keyLen + 4 > data.length) {
        corruptionDetected = true;
        break;
      }
      const valueLen = view.getUint32(4 + keyLen, true);
      const isDeleted = valueLen === TOMBSTONE_MARKER;
      const actualValueLen = isDeleted ? 0 : valueLen;
      const recordLen = 4 + keyLen + 4 + actualValueLen + 4;
      if (offset + recordLen > data.length) {
        corruptionDetected = true;
        break;
      }
      const recordData = data.subarray(offset, offset + recordLen - 4);
      const storedCrc = view.getUint32(recordLen - 4, true);
      const calculatedCrc = crc32(recordData);
      if (calculatedCrc !== storedCrc) {
        corruptionDetected = true;
        break;
      }
      const keyBytes = data.subarray(offset + 4, offset + 4 + keyLen);
      const key = new TextDecoder().decode(keyBytes);
      if (isDeleted) {
        const existing = this.index.get(key);
        if (existing) {
          this.liveBytes -= existing.length;
        }
        this.index.delete(key);
      } else {
        const existing = this.index.get(key);
        if (existing) {
          this.liveBytes -= existing.length;
        }
        this.index.set(key, { offset, length: recordLen });
        this.liveBytes += recordLen;
      }
      offset += recordLen;
    }
    if (corruptionDetected && offset < data.length) {
      console.warn(`[DiskKeyValueStore] Corruption detected at offset ${offset}, ` + `truncating file from ${data.length} to ${offset} bytes`);
      const handle = await this.fs.open(this.path, "readwrite");
      try {
        await this.fs.truncate(handle, offset);
        await this.fs.sync(handle);
      } finally {
        await this.fs.close(handle);
      }
    }
    this.writePosition = offset;
    this.totalBytes = offset;
  }
  buildRecord(key, valueData) {
    const encoder = new TextEncoder;
    const keyData = encoder.encode(key);
    const recordLen = 4 + keyData.length + 4 + valueData.length + 4;
    const buffer = new Uint8Array(recordLen);
    const view = new DataView(buffer.buffer);
    view.setUint32(0, keyData.length, true);
    buffer.set(keyData, 4);
    view.setUint32(4 + keyData.length, valueData.length, true);
    buffer.set(valueData, 4 + keyData.length + 4);
    const dataForCrc = buffer.subarray(0, recordLen - 4);
    const checksum = crc32(dataForCrc);
    view.setUint32(recordLen - 4, checksum, true);
    return buffer;
  }
  buildTombstone(key) {
    const encoder = new TextEncoder;
    const keyData = encoder.encode(key);
    const recordLen = 4 + keyData.length + 4 + 4;
    const buffer = new Uint8Array(recordLen);
    const view = new DataView(buffer.buffer);
    view.setUint32(0, keyData.length, true);
    buffer.set(keyData, 4);
    view.setUint32(4 + keyData.length, TOMBSTONE_MARKER, true);
    const dataForCrc = buffer.subarray(0, recordLen - 4);
    const checksum = crc32(dataForCrc);
    view.setUint32(recordLen - 4, checksum, true);
    return buffer;
  }
  parseRecord(data) {
    if (data.length < 8) {
      return null;
    }
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const keyLen = view.getUint32(0, true);
    if (data.length < 4 + keyLen + 4) {
      return null;
    }
    const valueLen = view.getUint32(4 + keyLen, true);
    const isDeleted = valueLen === TOMBSTONE_MARKER;
    const actualValueLen = isDeleted ? 0 : valueLen;
    const recordLen = 4 + keyLen + 4 + actualValueLen + 4;
    if (data.length < recordLen) {
      return null;
    }
    const recordData = data.subarray(0, recordLen - 4);
    const storedCrc = view.getUint32(recordLen - 4, true);
    const calculatedCrc = crc32(recordData);
    if (calculatedCrc !== storedCrc) {
      return null;
    }
    const keyBytes = data.subarray(4, 4 + keyLen);
    const key = new TextDecoder().decode(keyBytes);
    if (isDeleted) {
      return { key, value: undefined, isDeleted: true };
    }
    const valueBytes = data.subarray(4 + keyLen + 4, 4 + keyLen + 4 + valueLen);
    const value = this.serializer.decode(valueBytes);
    return { key, value, isDeleted: false };
  }
  ensureOpen() {
    if (!this.opened) {
      throw new Error("DiskKeyValueStore not open. Call open() first.");
    }
  }
}
export {
  crc32,
  ZstdCompressor,
  TenantId,
  StreamId,
  SpiteDBNotOpenError,
  SpiteDBError,
  SpiteDB,
  SortedIndex,
  SegmentWriter,
  SegmentReader,
  SegmentManager,
  SegmentIndex,
  SEGMENT_VERSION,
  SEGMENT_MAGIC,
  SEGMENT_HEADER_SIZE,
  Revision,
  ProjectionsNotStartedError,
  ProjectionRunner,
  ProjectionNotFoundError,
  ProjectionError,
  ProjectionDisabledError,
  ProjectionCoordinatorError,
  ProjectionCoordinator,
  ProjectionCatchUpTimeoutError,
  ProjectionBuildError,
  ProjectionBackpressureTimeoutError,
  ProjectionBackpressureError,
  ProjectionAlreadyRegisteredError,
  NoopCompressor,
  MsgpackSerializer,
  InvalidStreamIdError,
  InvalidSegmentHeaderError,
  InvalidPositionError,
  InvalidBatchError,
  IndexCollection,
  GlobalPosition,
  FastEventSerializer,
  EventStore,
  EqualityIndex,
  DiskKeyValueStore,
  DenormalizedViewStore,
  DefaultProjectionRegistry,
  ConcurrencyError,
  CommandId,
  CheckpointWriteError,
  CheckpointVersionError,
  CheckpointManager,
  CheckpointLoadError,
  CheckpointCorruptionError,
  CRC32Calculator,
  CHECKPOINT_VERSION,
  CHECKPOINT_MAGIC,
  CHECKPOINT_HEADER_SIZE,
  BunFileSystem,
  BunClock,
  BinaryEventBatchSerializer,
  BatchId,
  BatchChecksumError,
  BATCH_MAGIC,
  BATCH_HEADER_SIZE,
  AggregatorStore
};

//# debugId=5CDDB4466536CA6564756E2164756E21
