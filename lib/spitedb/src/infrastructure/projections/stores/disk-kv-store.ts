/**
 * Append-only disk key-value store for projection spillover.
 *
 * Uses the Bitcask pattern:
 * - Append-only log for writes
 * - In-memory index (key -> file offset)
 * - Compaction to reclaim space from deleted/updated keys
 *
 * Record format:
 * ```
 * [keyLen:u32][key:utf8][valueLen:u32][value:msgpack][crc:u32]
 * ```
 *
 * @example
 * ```ts
 * const store = new DiskKeyValueStore({
 *   fs,
 *   serializer,
 *   path: '/data/projections/users.dat',
 * });
 *
 * await store.open();
 *
 * await store.set('user-1', { name: 'Alice' });
 * const user = await store.get('user-1');
 *
 * // Compact when garbage ratio is high
 * if (store.getGarbageRatio() > 0.5) {
 *   await store.compact();
 * }
 *
 * await store.close();
 * ```
 */

import type { FileSystem, FileHandle } from '../../../ports/storage/filesystem';
import type { Serializer } from '../../../ports/serialization/serializer';
import { crc32 } from '../../storage/support/crc32';
import { writeAllBytes } from '../../storage/support/write-all-bytes';

/**
 * Record header size: keyLen(4) + crc(4) = 8 bytes
 * (valueLen is after key, so total overhead varies)
 */
const RECORD_OVERHEAD = 12; // keyLen + valueLen + crc

/**
 * Tombstone marker value length.
 * A record with this length indicates the key was deleted.
 */
const TOMBSTONE_MARKER = 0xffffffff;

/**
 * Configuration for disk KV store.
 */
export interface DiskKeyValueStoreConfig {
  /** Filesystem implementation */
  fs: FileSystem;
  /** Serializer for values */
  serializer: Serializer;
  /** Path to the data file */
  path: string;
}

/**
 * Index entry: offset and length of a record.
 */
interface IndexEntry {
  /** Byte offset in the file */
  offset: number;
  /** Total record length (for garbage calculation) */
  length: number;
}

/**
 * Append-only disk key-value store.
 *
 * Features:
 * - Sequential writes (fast, no seeks)
 * - In-memory index for O(1) key lookups
 * - CRC32 validation per record
 * - Compaction to reclaim space
 */
export class DiskKeyValueStore<TValue = unknown> {
  private readonly fs: FileSystem;
  private readonly serializer: Serializer;
  private readonly path: string;

  /** In-memory index: key -> { offset, length } */
  private index = new Map<string, IndexEntry>();

  /** Current write position */
  private writePosition = 0;

  /** Total bytes occupied by live records */
  private liveBytes = 0;

  /** Total file size (live + garbage) */
  private totalBytes = 0;

  /** Open file handle for writing */
  private writeHandle: FileHandle | null = null;

  /** Whether the store is open */
  private opened = false;

  constructor(config: DiskKeyValueStoreConfig) {
    this.fs = config.fs;
    this.serializer = config.serializer;
    this.path = config.path;
  }

  /**
   * Open the store.
   *
   * Creates file if it doesn't exist, rebuilds index from existing data.
   */
  async open(): Promise<void> {
    if (this.opened) {
      return;
    }

    // Ensure parent directory exists
    const dir = this.path.substring(0, this.path.lastIndexOf('/'));
    if (dir && !(await this.fs.exists(dir))) {
      await this.fs.mkdir(dir, { recursive: true });
    }

    // Check if file exists
    const exists = await this.fs.exists(this.path);

    if (exists) {
      // Rebuild index from existing file
      await this.rebuildIndex();
    }

    // Open for appending
    this.writeHandle = await this.fs.open(this.path, exists ? 'append' : 'write');
    this.opened = true;
  }

  /**
   * Close the store.
   */
  async close(): Promise<void> {
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

  /**
   * Get a value by key.
   *
   * @param key - Key to look up
   * @returns Value or undefined if not found
   */
  async get(key: string): Promise<TValue | undefined> {
    const entry = this.index.get(key);
    if (!entry) {
      return undefined;
    }

    // Read the record from disk
    const record = await this.fs.readFileSlice(this.path, entry.offset, entry.offset + entry.length);

    // Parse the record
    const parsed = this.parseRecord(record);
    if (!parsed || parsed.isDeleted) {
      return undefined;
    }

    return parsed.value as TValue;
  }

  /**
   * Set a value.
   *
   * Appends a new record. Old record (if exists) becomes garbage.
   *
   * @param key - Key
   * @param value - Value
   */
  async set(key: string, value: TValue): Promise<void> {
    this.ensureOpen();

    // Serialize value
    const valueData = this.serializer.encode(value);

    // Build record
    const record = this.buildRecord(key, valueData);

    // Update garbage tracking if key exists
    const existingEntry = this.index.get(key);
    if (existingEntry) {
      this.liveBytes -= existingEntry.length;
    }

    // Append to file - ensure all bytes are written (handle short writes)
    const offset = this.writePosition;
    await writeAllBytes(this.fs, this.writeHandle!, record);
    this.writePosition += record.length;

    // Update index
    const newEntry: IndexEntry = { offset, length: record.length };
    this.index.set(key, newEntry);

    // Update byte tracking
    this.liveBytes += record.length;
    this.totalBytes = this.writePosition;
  }

  /**
   * Delete a key.
   *
   * Appends a tombstone record. Old record becomes garbage.
   *
   * @param key - Key to delete
   * @returns true if key existed
   */
  async delete(key: string): Promise<boolean> {
    const existingEntry = this.index.get(key);
    if (!existingEntry) {
      return false;
    }

    this.ensureOpen();

    // Build tombstone record
    const record = this.buildTombstone(key);

    // Append to file - ensure all bytes are written (handle short writes)
    await writeAllBytes(this.fs, this.writeHandle!, record);
    this.writePosition += record.length;

    // Update tracking
    this.liveBytes -= existingEntry.length;
    this.totalBytes = this.writePosition;

    // Remove from index
    this.index.delete(key);

    return true;
  }

  /**
   * Check if a key exists.
   */
  has(key: string): boolean {
    return this.index.has(key);
  }

  /**
   * Get all keys.
   */
  keys(): IterableIterator<string> {
    return this.index.keys();
  }

  /**
   * Get number of keys.
   */
  get size(): number {
    return this.index.size;
  }

  /**
   * Flush writes to disk.
   */
  async sync(): Promise<void> {
    if (this.writeHandle) {
      await this.fs.sync(this.writeHandle);
    }
  }

  /**
   * Get ratio of garbage to total bytes.
   *
   * @returns Number between 0 and 1
   */
  getGarbageRatio(): number {
    if (this.totalBytes === 0) {
      return 0;
    }
    return 1 - this.liveBytes / this.totalBytes;
  }

  /**
   * Get total file size in bytes.
   */
  getTotalBytes(): number {
    return this.totalBytes;
  }

  /**
   * Get live data size in bytes.
   */
  getLiveBytes(): number {
    return this.liveBytes;
  }

  /**
   * Compact the file to reclaim garbage space.
   *
   * Creates a new file with only live records, then atomically replaces.
   */
  async compact(): Promise<void> {
    this.ensureOpen();

    const tempPath = this.path + '.compact';

    try {
      // Close current write handle
      if (this.writeHandle) {
        await this.fs.sync(this.writeHandle);
        await this.fs.close(this.writeHandle);
        this.writeHandle = null;
      }

      // Open temp file for compacted data
      const tempHandle = await this.fs.open(tempPath, 'write');
      const newIndex = new Map<string, IndexEntry>();
      let newPosition = 0;

      // Copy live records in index order
      for (const [key, entry] of this.index) {
        // Read original record
        const record = await this.fs.readFileSlice(this.path, entry.offset, entry.offset + entry.length);

        // Write to temp file - ensure all bytes are written (handle short writes)
        await writeAllBytes(this.fs, tempHandle, record);
        newIndex.set(key, { offset: newPosition, length: record.length });
        newPosition += record.length;
      }

      // Sync and close temp file
      await this.fs.sync(tempHandle);
      await this.fs.close(tempHandle);

      // Atomic replace
      await this.fs.rename(tempPath, this.path);

      // Update state
      this.index = newIndex;
      this.writePosition = newPosition;
      this.liveBytes = newPosition;
      this.totalBytes = newPosition;

      // Reopen write handle
      this.writeHandle = await this.fs.open(this.path, 'append');
    } catch (error) {
      // Clean up temp file on error
      try {
        if (await this.fs.exists(tempPath)) {
          await this.fs.unlink(tempPath);
        }
      } catch {
        // Ignore cleanup errors
      }

      // Reopen write handle
      this.writeHandle = await this.fs.open(this.path, 'append');

      throw error;
    }
  }

  /**
   * Rebuild index by scanning the file.
   *
   * If corruption is detected, the file is truncated to the last valid
   * record position. This prevents subsequent writes from being appended
   * after corrupt data (which would make them unreadable on future rebuilds).
   */
  private async rebuildIndex(): Promise<void> {
    const data = await this.fs.readFile(this.path);
    let offset = 0;
    let corruptionDetected = false;

    this.index.clear();
    this.liveBytes = 0;

    while (offset < data.length) {
      // Read record header
      if (offset + 8 > data.length) {
        // Truncated record header - corruption
        corruptionDetected = true;
        break;
      }

      const view = new DataView(data.buffer, data.byteOffset + offset, data.length - offset);
      const keyLen = view.getUint32(0, true);

      // Check for valid key length
      if (keyLen === 0 || keyLen > 65536) {
        // Invalid key length - corruption
        corruptionDetected = true;
        break;
      }

      if (offset + 4 + keyLen + 4 > data.length) {
        // Truncated key/value length - corruption
        corruptionDetected = true;
        break;
      }

      const valueLen = view.getUint32(4 + keyLen, true);
      const isDeleted = valueLen === TOMBSTONE_MARKER;
      const actualValueLen = isDeleted ? 0 : valueLen;
      const recordLen = 4 + keyLen + 4 + actualValueLen + 4;

      if (offset + recordLen > data.length) {
        // Truncated record - corruption
        corruptionDetected = true;
        break;
      }

      // Validate CRC
      const recordData = data.subarray(offset, offset + recordLen - 4);
      const storedCrc = view.getUint32(recordLen - 4, true);
      const calculatedCrc = crc32(recordData);

      if (calculatedCrc !== storedCrc) {
        // CRC mismatch - corruption
        corruptionDetected = true;
        break;
      }

      // Extract key
      const keyBytes = data.subarray(offset + 4, offset + 4 + keyLen);
      const key = new TextDecoder().decode(keyBytes);

      if (isDeleted) {
        // Remove from index
        const existing = this.index.get(key);
        if (existing) {
          this.liveBytes -= existing.length;
        }
        this.index.delete(key);
      } else {
        // Update index
        const existing = this.index.get(key);
        if (existing) {
          this.liveBytes -= existing.length;
        }
        this.index.set(key, { offset, length: recordLen });
        this.liveBytes += recordLen;
      }

      offset += recordLen;
    }

    // If corruption was detected, truncate the file to remove corrupt data
    // This ensures new writes will be readable on future rebuilds
    if (corruptionDetected && offset < data.length) {
      console.warn(
        `[DiskKeyValueStore] Corruption detected at offset ${offset}, ` +
          `truncating file from ${data.length} to ${offset} bytes`
      );

      // Open file in readwrite mode to truncate
      const handle = await this.fs.open(this.path, 'readwrite');
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

  /**
   * Build a record buffer.
   */
  private buildRecord(key: string, valueData: Uint8Array): Uint8Array {
    const encoder = new TextEncoder();
    const keyData = encoder.encode(key);

    const recordLen = 4 + keyData.length + 4 + valueData.length + 4;
    const buffer = new Uint8Array(recordLen);
    const view = new DataView(buffer.buffer);

    // Write header
    view.setUint32(0, keyData.length, true);

    // Write key
    buffer.set(keyData, 4);

    // Write value length
    view.setUint32(4 + keyData.length, valueData.length, true);

    // Write value
    buffer.set(valueData, 4 + keyData.length + 4);

    // Calculate and write CRC
    const dataForCrc = buffer.subarray(0, recordLen - 4);
    const checksum = crc32(dataForCrc);
    view.setUint32(recordLen - 4, checksum, true);

    return buffer;
  }

  /**
   * Build a tombstone record for deletion.
   */
  private buildTombstone(key: string): Uint8Array {
    const encoder = new TextEncoder();
    const keyData = encoder.encode(key);

    const recordLen = 4 + keyData.length + 4 + 4; // No value, just tombstone marker
    const buffer = new Uint8Array(recordLen);
    const view = new DataView(buffer.buffer);

    // Write header
    view.setUint32(0, keyData.length, true);

    // Write key
    buffer.set(keyData, 4);

    // Write tombstone marker
    view.setUint32(4 + keyData.length, TOMBSTONE_MARKER, true);

    // Calculate and write CRC
    const dataForCrc = buffer.subarray(0, recordLen - 4);
    const checksum = crc32(dataForCrc);
    view.setUint32(recordLen - 4, checksum, true);

    return buffer;
  }

  /**
   * Parse a record from bytes.
   */
  private parseRecord(data: Uint8Array): { key: string; value: unknown; isDeleted: boolean } | null {
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

    // Validate CRC
    const recordData = data.subarray(0, recordLen - 4);
    const storedCrc = view.getUint32(recordLen - 4, true);
    const calculatedCrc = crc32(recordData);

    if (calculatedCrc !== storedCrc) {
      return null;
    }

    // Extract key
    const keyBytes = data.subarray(4, 4 + keyLen);
    const key = new TextDecoder().decode(keyBytes);

    if (isDeleted) {
      return { key, value: undefined, isDeleted: true };
    }

    // Extract value
    const valueBytes = data.subarray(4 + keyLen + 4, 4 + keyLen + 4 + valueLen);
    const value = this.serializer.decode(valueBytes);

    return { key, value, isDeleted: false };
  }

  /**
   * Ensure the store is open.
   */
  private ensureOpen(): void {
    if (!this.opened) {
      throw new Error('DiskKeyValueStore not open. Call open() first.');
    }
  }
}
