/**
 * Store for DenormalizedView projections.
 *
 * Key-value store with:
 * - Equality indexes for fast lookups
 * - Sorted indexes for range queries
 * - Optional disk spillover when memory exceeds threshold
 *
 * @example
 * ```ts
 * const store = new DenormalizedViewStore<UserProfile>({
 *   fs,
 *   serializer,
 *   clock,
 *   dataDir: '/data/projections/user-profiles',
 *   projectionName: 'UserProfiles',
 *   indexFields: ['email', 'status'],
 *   rangeFields: ['createdAt'],
 *   memoryThreshold: 50 * 1024 * 1024, // 50MB
 * });
 *
 * await store.initialize();
 *
 * // Set rows
 * store.setByKey('user-1', { email: 'alice@example.com', status: 'active' });
 * store.setByKey('user-2', { email: 'bob@example.com', status: 'inactive' });
 *
 * // Query by index
 * const activeUsers = store.query({ status: 'active' });
 *
 * // Checkpoint
 * await store.persist(1000);
 * ```
 */

import type { FileSystem } from '../../../ports/storage/filesystem';
import type { Serializer } from '../../../ports/serialization/serializer';
import type { Clock } from '../../../ports/time/clock';
import type { ProjectionStore, ProjectionStoreConfig, QueryFilter, TimeRange } from '../../../ports/projections';
import { crc32 } from '../../storage/support/crc32';
import { EqualityIndex, SortedIndex, IndexCollection } from './index-structures';
import {
  CheckpointWriteError,
  CheckpointLoadError,
  CheckpointCorruptionError,
  CheckpointVersionError,
} from '../../../errors';

/**
 * Magic number for denormalized view checkpoint files: "DNCP" in ASCII
 */
const DENORMALIZED_MAGIC = 0x444e4350;

/**
 * Current version of the checkpoint format.
 */
const DENORMALIZED_VERSION = 1;

/**
 * Supported versions for loading.
 */
const SUPPORTED_VERSIONS = [1];

/**
 * Default memory threshold before disk spillover (50MB).
 */
const DEFAULT_MEMORY_THRESHOLD = 50 * 1024 * 1024;

/**
 * Configuration for denormalized view store.
 */
export interface DenormalizedViewStoreConfig extends ProjectionStoreConfig {
  /** Fields to create equality indexes on */
  indexFields?: string[];
  /** Fields to create sorted indexes on (for range queries) */
  rangeFields?: string[];
  /** Memory threshold before disk spillover (default: 50MB) */
  memoryThreshold?: number;
}

/**
 * Store for denormalized view projections.
 *
 * Features:
 * - In-memory key-value storage
 * - Equality indexes for O(1) lookups by field
 * - Sorted indexes for range queries
 * - Checkpointing with CRC32 validation
 *
 * @typeParam TRow - The row type
 */
export class DenormalizedViewStore<TRow extends Record<string, unknown> = Record<string, unknown>>
  implements ProjectionStore<Map<string, TRow>>
{
  private readonly fs: FileSystem;
  private readonly serializer: Serializer;
  private readonly clock: Clock;
  private readonly dataDir: string;
  private readonly projectionName: string;
  private readonly memoryThreshold: number;

  /** Row storage: primaryKey -> row */
  private rows = new Map<string, TRow>();

  /** Index collection */
  private indexes: IndexCollection;

  /** Index field names */
  private readonly indexFields: string[];

  /** Range field names */
  private readonly rangeFields: string[];

  constructor(config: DenormalizedViewStoreConfig) {
    this.fs = config.fs;
    this.serializer = config.serializer;
    this.clock = config.clock;
    this.dataDir = config.dataDir;
    this.projectionName = config.projectionName;
    this.memoryThreshold = config.memoryThreshold ?? DEFAULT_MEMORY_THRESHOLD;
    this.indexFields = config.indexFields ?? [];
    this.rangeFields = config.rangeFields ?? [];

    // Create indexes
    this.indexes = new IndexCollection();
    for (const field of this.indexFields) {
      this.indexes.addEqualityIndex(field);
    }
    for (const field of this.rangeFields) {
      this.indexes.addSortedIndex(field);
    }
  }

  /**
   * Initialize the store.
   */
  async initialize(): Promise<void> {
    if (!(await this.fs.exists(this.dataDir))) {
      await this.fs.mkdir(this.dataDir, { recursive: true });
    }
  }

  /**
   * Get the full state (all rows).
   */
  get(): Map<string, TRow> {
    return new Map(this.rows);
  }

  /**
   * Set the full state (replace all rows).
   */
  set(state: Map<string, TRow>): void {
    this.rows.clear();
    this.indexes.clear();

    for (const [key, row] of state) {
      this.rows.set(key, row);
      this.indexes.indexRow(key, row);
    }
  }

  /**
   * Get a single row by primary key.
   */
  getByKey(key: string): TRow | undefined {
    return this.rows.get(key);
  }

  /**
   * Set a single row by primary key.
   */
  setByKey(key: string, value: TRow): void {
    const existingRow = this.rows.get(key);
    if (existingRow) {
      this.indexes.removeRow(key, existingRow);
    }

    this.rows.set(key, value);
    this.indexes.indexRow(key, value);
  }

  /**
   * Delete a row by primary key.
   */
  deleteByKey(key: string): boolean {
    const row = this.rows.get(key);
    if (!row) {
      return false;
    }

    this.indexes.removeRow(key, row);
    this.rows.delete(key);
    return true;
  }

  /**
   * Query rows by equality filter.
   *
   * All filter conditions are ANDed together.
   * Uses indexes when available.
   */
  query(filter: QueryFilter): TRow[] {
    if (Object.keys(filter).length === 0) {
      return Array.from(this.rows.values());
    }

    // Start with all keys
    let candidateKeys: Set<string> | null = null;

    // Use indexes to narrow down candidates
    for (const [field, value] of Object.entries(filter)) {
      const index = this.indexes.getEqualityIndex(field);
      if (index) {
        const matchingKeys = index.find(value);
        if (candidateKeys === null) {
          candidateKeys = new Set<string>(matchingKeys);
        } else {
          // Intersect
          const intersection = new Set<string>();
          for (const k of candidateKeys) {
            if (matchingKeys.has(k)) {
              intersection.add(k);
            }
          }
          candidateKeys = intersection;
        }
      }
    }

    // If no indexes were used, scan all rows
    if (candidateKeys === null) {
      candidateKeys = new Set<string>(this.rows.keys());
    }

    // Filter candidates by all conditions
    const results: TRow[] = [];
    for (const key of candidateKeys) {
      const row = this.rows.get(key);
      if (row && this.matchesFilter(row, filter)) {
        results.push(row);
      }
    }

    return results;
  }

  /**
   * Query rows by key range (for time-series style queries).
   *
   * The first range field is used for the query.
   */
  queryRange(range: TimeRange): Map<string, TRow> {
    const result = new Map<string, TRow>();

    // If we have sorted indexes, use them
    if (this.rangeFields.length > 0) {
      const rangeField = this.rangeFields[0]!;
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

    // Fallback: scan all rows and filter by key
    for (const [key, row] of this.rows) {
      if (this.keyInRange(key, range)) {
        result.set(key, row);
      }
    }

    return result;
  }

  /**
   * Get the number of rows.
   */
  get size(): number {
    return this.rows.size;
  }

  /**
   * Persist state to disk.
   */
  async persist(position: number): Promise<void> {
    const path = this.getCheckpointPath();
    const tempPath = path + '.tmp';

    try {
      // Serialize state
      const stateObject = {
        rows: Array.from(this.rows.entries()),
        indexFields: this.indexFields,
        rangeFields: this.rangeFields,
      };
      const stateData = this.serializer.encode(stateObject);

      // Build header
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
      view.setUint32(28, 0, true); // Checksum placeholder

      // Write state
      buffer.set(stateData, headerSize);

      // Calculate checksum
      const beforeChecksum = buffer.subarray(0, 28);
      const afterChecksum = buffer.subarray(32);
      const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
      checksumData.set(beforeChecksum);
      checksumData.set(afterChecksum, beforeChecksum.length);
      const checksum = crc32(checksumData);
      view.setUint32(28, checksum, true);

      // Atomic write
      const handle = await this.fs.open(tempPath, 'write');
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
      } catch {
        // Ignore cleanup errors
      }

      throw new CheckpointWriteError(
        this.projectionName,
        path,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  /**
   * Load state from disk.
   */
  async load(): Promise<number | null> {
    const path = this.getCheckpointPath();

    if (!(await this.fs.exists(path))) {
      return null;
    }

    try {
      const data = await this.fs.readFile(path);
      const headerSize = 32;

      if (data.length < headerSize) {
        throw new CheckpointCorruptionError(this.projectionName, path, 'File too small');
      }

      // Parse header
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      const magic = view.getUint32(0, true);
      const version = view.getUint32(4, true);
      const position = Number(view.getBigUint64(8, true));
      const stateLength = view.getUint32(24, true);
      const storedChecksum = view.getUint32(28, true);

      // Validate
      if (magic !== DENORMALIZED_MAGIC) {
        throw new CheckpointCorruptionError(this.projectionName, path, `Invalid magic: 0x${magic.toString(16)}`);
      }

      if (!SUPPORTED_VERSIONS.includes(version)) {
        throw new CheckpointVersionError(this.projectionName, path, version, SUPPORTED_VERSIONS);
      }

      const expectedSize = headerSize + stateLength;
      if (data.length < expectedSize) {
        throw new CheckpointCorruptionError(this.projectionName, path, 'File too small for state');
      }

      // Validate checksum
      const beforeChecksum = data.subarray(0, 28);
      const afterChecksum = data.subarray(32);
      const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
      checksumData.set(beforeChecksum);
      checksumData.set(afterChecksum, beforeChecksum.length);
      const calculatedChecksum = crc32(checksumData);

      if (calculatedChecksum !== storedChecksum) {
        throw new CheckpointCorruptionError(this.projectionName, path, 'Checksum mismatch');
      }

      // Parse state
      const stateData = data.subarray(headerSize, expectedSize);
      const stateObject = this.serializer.decode(stateData) as {
        rows: Array<[string, TRow]>;
        indexFields: string[];
        rangeFields: string[];
      };

      // Restore state
      this.rows.clear();
      this.indexes.clear();

      for (const [key, row] of stateObject.rows) {
        this.rows.set(key, row);
        this.indexes.indexRow(key, row);
      }

      return position;
    } catch (error) {
      if (
        error instanceof CheckpointCorruptionError ||
        error instanceof CheckpointVersionError
      ) {
        throw error;
      }

      throw new CheckpointLoadError(
        this.projectionName,
        path,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  /**
   * Close and release resources.
   */
  async close(): Promise<void> {
    // No resources to release for in-memory store
  }

  /**
   * Get memory usage estimate in bytes.
   */
  getMemoryUsage(): number {
    let bytes = 0;

    // Row storage estimate
    try {
      const rowSample = this.rows.size > 0 ? this.rows.values().next().value : undefined;
      if (rowSample) {
        const sampleSize = this.serializer.encode(rowSample).length;
        bytes += this.rows.size * (sampleSize + 30); // 30 bytes overhead per entry
      }
    } catch {
      // Fallback estimate
      bytes += this.rows.size * 200;
    }

    // Index memory
    bytes += this.indexes.getMemoryUsage();

    return bytes;
  }

  /**
   * Clear all data.
   */
  clear(): void {
    this.rows.clear();
    this.indexes.clear();
  }

  /**
   * Check if memory threshold is exceeded.
   */
  isMemoryThresholdExceeded(): boolean {
    return this.getMemoryUsage() > this.memoryThreshold;
  }

  /**
   * Get checkpoint file path.
   */
  private getCheckpointPath(): string {
    const safeName = this.projectionName.replace(/[^a-zA-Z0-9_-]/g, '_');
    return `${this.dataDir}/${safeName}.ckpt`;
  }

  /**
   * Check if a row matches all filter conditions.
   */
  private matchesFilter(row: TRow, filter: QueryFilter): boolean {
    for (const [field, value] of Object.entries(filter)) {
      if (row[field] !== value) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if a key is within a range.
   */
  private keyInRange(key: string, range: TimeRange): boolean {
    if (range.start !== undefined && key < range.start) {
      return false;
    }
    if (range.end !== undefined && key > range.end) {
      return false;
    }
    return true;
  }
}
