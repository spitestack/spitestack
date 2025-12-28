/**
 * In-memory store for Aggregator projections.
 *
 * Aggregator projections maintain simple value state like totals, averages,
 * or counts. State is kept entirely in memory for fast reads, with periodic
 * checkpointing for durability.
 *
 * @example
 * ```ts
 * const store = new AggregatorStore<number>({
 *   fs,
 *   serializer,
 *   clock,
 *   dataDir: '/data/checkpoints',
 *   projectionName: 'TotalRevenue',
 * }, 0);
 *
 * await store.initialize();
 *
 * // Update state
 * store.set(store.get() + 100);
 *
 * // Checkpoint
 * await store.persist(1000);
 *
 * // Later, load from checkpoint
 * const position = await store.load();
 * ```
 */

import type { FileSystem } from '../../../ports/storage/filesystem';
import type { Serializer } from '../../../ports/serialization/serializer';
import type { Clock } from '../../../ports/time/clock';
import type { ProjectionStore, ProjectionStoreConfig } from '../../../ports/projections';
import { crc32 } from '../../storage/support/crc32';
import {
  CheckpointWriteError,
  CheckpointLoadError,
  CheckpointCorruptionError,
  CheckpointVersionError,
} from '../../../errors';

/**
 * Magic number for aggregator checkpoint files: "AGCP" in ASCII
 */
const AGGREGATOR_MAGIC = 0x41474350;

/**
 * Current version of the checkpoint format.
 */
const AGGREGATOR_VERSION = 1;

/**
 * Supported versions for loading.
 */
const SUPPORTED_VERSIONS = [1];

/**
 * Size of the checkpoint header in bytes.
 */
const HEADER_SIZE = 32;

/**
 * Configuration for aggregator store.
 */
export interface AggregatorStoreConfig extends ProjectionStoreConfig {
  // No additional config needed
}

/**
 * In-memory store for aggregator projections.
 *
 * Features:
 * - Pure in-memory state for fast reads
 * - Checkpointing with CRC32 validation
 * - Atomic writes (temp file → fsync → rename)
 *
 * @typeParam TState - The state type (e.g., number, { count: number, sum: number })
 */
export class AggregatorStore<TState> implements ProjectionStore<TState> {
  private readonly fs: FileSystem;
  private readonly serializer: Serializer;
  private readonly clock: Clock;
  private readonly dataDir: string;
  private readonly projectionName: string;

  private state: TState;
  private readonly initialState: TState;

  constructor(config: AggregatorStoreConfig, initialState: TState) {
    this.fs = config.fs;
    this.serializer = config.serializer;
    this.clock = config.clock;
    this.dataDir = config.dataDir;
    this.projectionName = config.projectionName;

    // Deep clone initial state if it's an object
    this.initialState =
      typeof initialState === 'object' && initialState !== null
        ? JSON.parse(JSON.stringify(initialState))
        : initialState;

    this.state = this.cloneState(this.initialState);
  }

  /**
   * Initialize the store.
   *
   * Creates the checkpoint directory if needed.
   */
  async initialize(): Promise<void> {
    if (!(await this.fs.exists(this.dataDir))) {
      await this.fs.mkdir(this.dataDir, { recursive: true });
    }
  }

  /**
   * Get current state.
   */
  get(): TState {
    return this.state;
  }

  /**
   * Set state (used during build and recovery).
   */
  set(state: TState): void {
    this.state = state;
  }

  /**
   * Persist state to disk.
   *
   * Checkpoint format:
   * - Header (32 bytes): magic, version, position, timestamp, stateLength, checksum, reserved
   * - State: msgpack-encoded state
   *
   * @param position - Global position to checkpoint at
   */
  async persist(position: number): Promise<void> {
    const path = this.getCheckpointPath();
    const tempPath = path + '.tmp';

    try {
      // Serialize state
      const stateData = this.serializer.encode(this.state);

      // Build file content
      const totalSize = HEADER_SIZE + stateData.length;
      const buffer = new Uint8Array(totalSize);
      const view = new DataView(buffer.buffer);

      const timestamp = BigInt(this.clock.now());

      // Write header
      view.setUint32(0, AGGREGATOR_MAGIC, true);
      view.setUint32(4, AGGREGATOR_VERSION, true);
      view.setBigUint64(8, BigInt(position), true);
      view.setBigUint64(16, timestamp, true);
      view.setUint32(24, stateData.length, true);
      view.setUint32(28, 0, true); // Checksum placeholder

      // Write state
      buffer.set(stateData, HEADER_SIZE);

      // Calculate checksum (excluding bytes 28-31)
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
      // Clean up temp file
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
   *
   * @returns Checkpoint position if loaded, or null if no checkpoint exists
   */
  async load(): Promise<number | null> {
    const path = this.getCheckpointPath();

    if (!(await this.fs.exists(path))) {
      return null;
    }

    try {
      const data = await this.fs.readFile(path);

      // Validate minimum size
      if (data.length < HEADER_SIZE) {
        throw new CheckpointCorruptionError(
          this.projectionName,
          path,
          `File too small: ${data.length} bytes`
        );
      }

      // Parse header
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      const magic = view.getUint32(0, true);
      const version = view.getUint32(4, true);
      const position = Number(view.getBigUint64(8, true));
      const stateLength = view.getUint32(24, true);
      const storedChecksum = view.getUint32(28, true);

      // Validate magic
      if (magic !== AGGREGATOR_MAGIC) {
        throw new CheckpointCorruptionError(
          this.projectionName,
          path,
          `Invalid magic: 0x${magic.toString(16)}`
        );
      }

      // Validate version
      if (!SUPPORTED_VERSIONS.includes(version)) {
        throw new CheckpointVersionError(this.projectionName, path, version, SUPPORTED_VERSIONS);
      }

      // Validate size
      const expectedSize = HEADER_SIZE + stateLength;
      if (data.length < expectedSize) {
        throw new CheckpointCorruptionError(
          this.projectionName,
          path,
          `File too small for state: ${data.length} bytes, expected ${expectedSize}`
        );
      }

      // Validate checksum
      const beforeChecksum = data.subarray(0, 28);
      const afterChecksum = data.subarray(32);
      const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
      checksumData.set(beforeChecksum);
      checksumData.set(afterChecksum, beforeChecksum.length);
      const calculatedChecksum = crc32(checksumData);

      if (calculatedChecksum !== storedChecksum) {
        throw new CheckpointCorruptionError(
          this.projectionName,
          path,
          `Checksum mismatch: expected 0x${storedChecksum.toString(16)}, got 0x${calculatedChecksum.toString(16)}`
        );
      }

      // Parse state
      const stateData = data.subarray(HEADER_SIZE, expectedSize);
      this.state = this.serializer.decode(stateData) as TState;

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
    // Rough estimate: serialize and measure
    try {
      const encoded = this.serializer.encode(this.state);
      return encoded.length;
    } catch {
      return 0;
    }
  }

  /**
   * Reset to initial state.
   */
  reset(): void {
    this.state = this.cloneState(this.initialState);
  }

  /**
   * Get checkpoint file path.
   */
  private getCheckpointPath(): string {
    const safeName = this.projectionName.replace(/[^a-zA-Z0-9_-]/g, '_');
    return `${this.dataDir}/${safeName}.ckpt`;
  }

  /**
   * Clone state for isolation.
   */
  private cloneState(state: TState): TState {
    if (typeof state === 'object' && state !== null) {
      return JSON.parse(JSON.stringify(state));
    }
    return state;
  }
}
