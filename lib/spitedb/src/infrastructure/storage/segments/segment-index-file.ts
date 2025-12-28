import type { FileSystem } from '../../../ports/storage/filesystem';
import { crc32 } from '../support/crc32';

/**
 * Compare two strings using byte-wise (code point) comparison.
 *
 * This ensures deterministic ordering across platforms and locales.
 * Using localeCompare with the ambient locale can cause inconsistent
 * index ordering if the index is written under one locale and read
 * under another.
 *
 * @param a - First string
 * @param b - Second string
 * @returns Negative if a < b, positive if a > b, zero if equal
 */
function compareStreamIds(a: string, b: string): number {
  const minLen = Math.min(a.length, b.length);
  for (let i = 0; i < minLen; i++) {
    const diff = a.charCodeAt(i) - b.charCodeAt(i);
    if (diff !== 0) return diff;
  }
  return a.length - b.length;
}

/**
 * Magic number for index files: "IDXC" in ASCII
 */
export const INDEX_MAGIC = 0x49445843;

/**
 * Current version of the index file format.
 */
export const INDEX_VERSION = 1;

/**
 * Size of the index header in bytes.
 */
export const INDEX_HEADER_SIZE = 32;

/**
 * Size of each index entry in bytes.
 */
export const INDEX_ENTRY_SIZE = 24;

/**
 * An entry in the segment index.
 */
export interface IndexEntry {
  /** Stream identifier */
  streamId: string;
  /** Event revision within the stream */
  revision: number;
  /** Global position across all streams */
  globalPosition: number;
  /** Byte offset of the batch in the segment file */
  batchOffset: number;
}

/**
 * Header structure for index files.
 */
export interface IndexHeader {
  magic: number;
  version: number;
  segmentId: bigint;
  entryCount: number;
  stringTableOffset: number;
  checksum: number;
}

/**
 * Error thrown when index file is corrupted.
 */
export class IndexCorruptedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'IndexCorruptedError';
  }
}

/**
 * Segment index file reader/writer.
 *
 * File format:
 * - 32 byte header (magic, version, segmentId, entryCount, stringTableOffset, checksum)
 * - Fixed-size entries (24 bytes each, sorted by streamId then revision)
 * - String table at end (deduplicated streamIds, null-terminated)
 *
 * This class supports two loading modes:
 * - mmap: Zero-copy, OS-managed caching (preferred)
 * - memory: Full load into memory (fallback)
 *
 * @example
 * ```ts
 * // Read an index file
 * const index = new SegmentIndexFile();
 * await index.load(fs, '/data/segment-00000001.idx');
 *
 * // Find events for a stream
 * const entries = index.findByStream('user-123');
 *
 * // Write an index file
 * const entries = [...];
 * await SegmentIndexFile.write(fs, '/data/segment-00000001.idx', 1n, entries);
 * ```
 */
export class SegmentIndexFile {
  private data: Uint8Array | null = null;
  private header: IndexHeader | null = null;
  private entries: IndexEntry[] = [];
  private entriesByGlobalPosition: IndexEntry[] | null = null;
  private stringTable: Map<number, string> = new Map();
  private loaded = false;

  /**
   * Load an index file using mmap with fallback to memory.
   */
  async load(fs: FileSystem, path: string): Promise<void> {
    try {
      this.data = await fs.mmap(path);
    } catch {
      this.data = await fs.readFile(path);
    }
    this.parse();
  }

  /**
   * Load an index file directly into memory.
   */
  async loadIntoMemory(fs: FileSystem, path: string): Promise<void> {
    this.data = await fs.readFile(path);
    this.parse();
  }

  /**
   * Parse the loaded data.
   */
  private parse(): void {
    if (!this.data) {
      throw new Error('No data loaded');
    }

    if (this.data.length < INDEX_HEADER_SIZE) {
      throw new IndexCorruptedError('File too small for header');
    }

    // Parse header
    const view = new DataView(this.data.buffer, this.data.byteOffset, this.data.byteLength);
    this.header = {
      magic: view.getUint32(0, true),
      version: view.getUint32(4, true),
      segmentId: view.getBigUint64(8, true),
      entryCount: view.getUint32(16, true),
      stringTableOffset: view.getUint32(20, true),
      checksum: view.getUint32(24, true),
    };

    // Validate magic
    if (this.header.magic !== INDEX_MAGIC) {
      throw new IndexCorruptedError(
        `Invalid magic: expected 0x${INDEX_MAGIC.toString(16)}, got 0x${this.header.magic.toString(16)}`
      );
    }

    // Validate version
    if (this.header.version !== INDEX_VERSION) {
      throw new IndexCorruptedError(
        `Unsupported version: expected ${INDEX_VERSION}, got ${this.header.version}`
      );
    }

    // Validate checksum
    if (!this.validateChecksum()) {
      throw new IndexCorruptedError('Checksum mismatch');
    }

    // Parse string table first (needed for entries)
    this.parseStringTable();

    // Parse entries
    this.parseEntries();

    this.entriesByGlobalPosition = null;
    this.loaded = true;
  }

  /**
   * Parse the string table at the end of the file.
   */
  private parseStringTable(): void {
    if (!this.data || !this.header) return;

    const decoder = new TextDecoder();
    let offset = this.header.stringTableOffset;

    // Read null-terminated strings (including empty strings)
    while (offset < this.data.length) {
      const start = offset;
      while (offset < this.data.length && this.data[offset] !== 0) {
        offset++;
      }
      // Store the string (empty strings are valid - just a null terminator)
      const str = decoder.decode(this.data.subarray(start, offset));
      this.stringTable.set(start - this.header.stringTableOffset, str);
      offset++; // Skip null terminator
    }
  }

  /**
   * Parse index entries.
   */
  private parseEntries(): void {
    if (!this.data || !this.header) return;

    const view = new DataView(this.data.buffer, this.data.byteOffset, this.data.byteLength);
    const entriesStart = INDEX_HEADER_SIZE;

    this.entries = [];

    for (let i = 0; i < this.header.entryCount; i++) {
      const entryOffset = entriesStart + i * INDEX_ENTRY_SIZE;

      const stringOffset = view.getUint32(entryOffset, true);
      const revision = view.getUint32(entryOffset + 4, true);
      const globalPosition = Number(view.getBigUint64(entryOffset + 8, true));
      if (!Number.isSafeInteger(globalPosition)) {
        throw new IndexCorruptedError('Global position exceeds safe integer range');
      }
      const batchOffset = view.getUint32(entryOffset + 16, true);

      // Validate string table offset - don't silently return empty string
      // as that would mask index corruption
      const streamId = this.stringTable.get(stringOffset);
      if (streamId === undefined) {
        throw new IndexCorruptedError(
          `Invalid string table offset ${stringOffset} at entry ${i}`
        );
      }

      this.entries.push({
        streamId,
        revision,
        globalPosition,
        batchOffset,
      });
    }
  }

  /**
   * Validate the file checksum.
   */
  validateChecksum(): boolean {
    if (!this.data || !this.header) return false;

    // Calculate CRC32 of file excluding the checksum field (bytes 24-27)
    const before = this.data.subarray(0, 24);
    const after = this.data.subarray(28);

    const combined = new Uint8Array(before.length + after.length);
    combined.set(before);
    combined.set(after, before.length);

    const calculated = crc32(combined);
    return calculated === this.header.checksum;
  }

  /**
   * Check if the index is loaded.
   */
  isLoaded(): boolean {
    return this.loaded;
  }

  /**
   * Get the segment ID from the header.
   */
  getSegmentId(): bigint {
    return this.header?.segmentId ?? 0n;
  }

  /**
   * Get the number of entries.
   */
  getEntryCount(): number {
    return this.entries.length;
  }

  /**
   * Get all entries (for testing/debugging).
   */
  getAllEntries(): IndexEntry[] {
    return [...this.entries];
  }

  /**
   * Find the batch offset for the first event at or after a global position.
   *
   * Returns null if no entries exist.
   */
  findBatchOffsetForGlobalPosition(position: number): number | null {
    if (this.entries.length === 0) {
      return null;
    }

    const sorted = this.getEntriesSortedByGlobalPosition();
    let left = 0;
    let right = sorted.length - 1;
    let matchIndex = sorted.length;

    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      const entry = sorted[mid]!;
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

    return sorted[matchIndex]!.batchOffset;
  }

  /**
   * Get all unique stream IDs in this index.
   */
  getAllStreamIds(): Set<string> {
    const ids = new Set<string>();
    for (const entry of this.entries) {
      ids.add(entry.streamId);
    }
    return ids;
  }

  /**
   * Find entries for a stream using binary search.
   *
   * Since entries are sorted by (streamId, revision), we can efficiently
   * find all entries for a stream and filter by revision range.
   */
  findByStream(
    streamId: string,
    fromRevision?: number,
    toRevision?: number
  ): IndexEntry[] {
    // Find the first entry for this stream using binary search
    let left = 0;
    let right = this.entries.length - 1;
    let firstIndex = -1;

    // Find leftmost entry for streamId
    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      const entry = this.entries[mid]!;
      const cmp = compareStreamIds(entry.streamId, streamId);

      if (cmp < 0) {
        left = mid + 1;
      } else if (cmp > 0) {
        right = mid - 1;
      } else {
        // Found a match, but we want the leftmost one
        firstIndex = mid;
        right = mid - 1;
      }
    }

    if (firstIndex === -1) {
      return []; // Stream not found
    }

    // Collect all entries for this stream
    const results: IndexEntry[] = [];
    for (let i = firstIndex; i < this.entries.length; i++) {
      const entry = this.entries[i]!;
      if (entry.streamId !== streamId) break;

      // Apply revision filters
      if (fromRevision !== undefined && entry.revision < fromRevision) continue;
      if (toRevision !== undefined && entry.revision > toRevision) continue;

      results.push(entry);
    }

    return results;
  }

  /**
   * Find an entry by global position using binary search.
   */
  findByGlobalPosition(position: number): IndexEntry | undefined {
    // Entries are sorted by (streamId, revision), not globalPosition
    // For now, do a linear scan. Could optimize with a secondary index.
    return this.entries.find((e) => e.globalPosition === position);
  }

  /**
   * Get the maximum revision for a stream.
   */
  getStreamMaxRevision(streamId: string): number {
    const entries = this.findByStream(streamId);
    if (entries.length === 0) return -1;
    return Math.max(...entries.map((e) => e.revision));
  }

  private getEntriesSortedByGlobalPosition(): IndexEntry[] {
    if (this.entriesByGlobalPosition) {
      return this.entriesByGlobalPosition;
    }

    this.entriesByGlobalPosition = [...this.entries].sort((a, b) =>
      a.globalPosition < b.globalPosition ? -1 : a.globalPosition > b.globalPosition ? 1 : 0
    );

    return this.entriesByGlobalPosition;
  }

  // ============================================================
  // Static Write Methods
  // ============================================================

  /**
   * Write an index file atomically.
   *
   * Uses write-tmp → fsync → rename pattern for crash safety.
   */
  static async write(
    fs: FileSystem,
    path: string,
    segmentId: bigint,
    entries: IndexEntry[]
  ): Promise<void> {
    const encoder = new TextEncoder();

    // Sort entries by (streamId, revision) using byte-wise comparison
    // for deterministic cross-platform ordering
    const sorted = [...entries].sort((a, b) => {
      const cmp = compareStreamIds(a.streamId, b.streamId);
      if (cmp !== 0) return cmp;
      return a.revision - b.revision;
    });

    // Build string table (need to encode to get actual byte lengths for unicode)
    const stringTable = new Map<string, number>();
    const encodedStrings: Uint8Array[] = [];
    let stringOffset = 0;

    for (const entry of sorted) {
      if (!stringTable.has(entry.streamId)) {
        stringTable.set(entry.streamId, stringOffset);
        const encoded = encoder.encode(entry.streamId);
        encodedStrings.push(encoded);
        stringOffset += encoded.length + 1; // +1 for null terminator
      }
    }

    // Calculate sizes
    const entriesSize = sorted.length * INDEX_ENTRY_SIZE;
    const stringTableStart = INDEX_HEADER_SIZE + entriesSize;
    const totalSize = stringTableStart + stringOffset;

    // Allocate buffer
    const buffer = new Uint8Array(totalSize);
    const view = new DataView(buffer.buffer);

    // Write header (checksum placeholder = 0)
    view.setUint32(0, INDEX_MAGIC, true);
    view.setUint32(4, INDEX_VERSION, true);
    view.setBigUint64(8, segmentId, true);
    view.setUint32(16, sorted.length, true);
    view.setUint32(20, stringTableStart, true);
    view.setUint32(24, 0, true); // Checksum placeholder
    view.setUint32(28, 0, true); // Reserved

    // Write entries
    for (let i = 0; i < sorted.length; i++) {
      const entry = sorted[i]!;
      if (!Number.isSafeInteger(entry.globalPosition) || entry.globalPosition < 0) {
        throw new Error('Global position must be a non-negative safe integer');
      }
      const entryOffset = INDEX_HEADER_SIZE + i * INDEX_ENTRY_SIZE;

      view.setUint32(entryOffset, stringTable.get(entry.streamId)!, true);
      view.setUint32(entryOffset + 4, entry.revision, true);
      view.setBigUint64(entryOffset + 8, BigInt(entry.globalPosition), true);
      view.setUint32(entryOffset + 16, entry.batchOffset, true);
      view.setUint32(entryOffset + 20, 0, true); // Reserved
    }

    // Write string table
    let strWriteOffset = stringTableStart;
    for (const encoded of encodedStrings) {
      buffer.set(encoded, strWriteOffset);
      strWriteOffset += encoded.length;
      buffer[strWriteOffset] = 0; // Null terminator
      strWriteOffset++;
    }

    // Calculate checksum (excluding bytes 24-27)
    const beforeChecksum = buffer.subarray(0, 24);
    const afterChecksum = buffer.subarray(28);
    const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
    checksumData.set(beforeChecksum);
    checksumData.set(afterChecksum, beforeChecksum.length);
    const checksum = crc32(checksumData);
    view.setUint32(24, checksum, true);

    // Write atomically: temp file → fsync → rename
    const tempPath = path + '.tmp';
    const handle = await fs.open(tempPath, 'write');
    await fs.write(handle, buffer);
    await fs.sync(handle);
    await fs.close(handle);
    await fs.rename(tempPath, path);
  }
}
