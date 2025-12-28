/**
 * Checkpoint manager for projection state persistence.
 *
 * Handles writing and loading checkpoint files with:
 * - Atomic writes (temp file → fsync → rename)
 * - CRC32 checksums for integrity
 * - Jitter scheduling to avoid write storms
 *
 * Checkpoint file format (32 byte header + state blob):
 * ```
 * Header (32 bytes):
 *   magic: u32 = 0x43484B50 ("CHKP")
 *   version: u32 = 1
 *   position: u64 (globalPosition)
 *   timestamp: u64 (Unix ms)
 *   stateLength: u32
 *   checksum: u32 (CRC32 of header + state)
 *   reserved: u32
 *
 * State: [msgpack-encoded state]
 * ```
 */

import type { FileSystem } from '../../ports/storage/filesystem';
import type { Serializer } from '../../ports/serialization/serializer';
import type { Clock } from '../../ports/time/clock';
import { crc32 } from '../../infrastructure/storage/support/crc32';
import {
  CheckpointWriteError,
  CheckpointLoadError,
  CheckpointCorruptionError,
  CheckpointVersionError,
} from '../../errors';

/**
 * Magic number for checkpoint files: "CHKP" in ASCII
 */
export const CHECKPOINT_MAGIC = 0x43484b50;

/**
 * Current version of the checkpoint file format.
 */
export const CHECKPOINT_VERSION = 1;

/**
 * Supported versions for loading (for forward compatibility).
 */
export const SUPPORTED_VERSIONS = [1];

/**
 * Size of the checkpoint header in bytes.
 */
export const CHECKPOINT_HEADER_SIZE = 32;

/**
 * Checkpoint data structure.
 */
export interface Checkpoint {
  /** Projection name */
  projectionName: string;
  /** Global position processed up to */
  position: number;
  /** Serialized projection state */
  state: unknown;
  /** Timestamp of checkpoint */
  timestamp: number;
}

/**
 * Checkpoint header structure.
 */
interface CheckpointHeader {
  magic: number;
  version: number;
  position: number;
  timestamp: number;
  stateLength: number;
  checksum: number;
}

/**
 * Configuration for checkpoint manager.
 */
export interface CheckpointManagerConfig {
  /** Filesystem implementation */
  fs: FileSystem;
  /** Serializer for state */
  serializer: Serializer;
  /** Clock for timestamps */
  clock: Clock;
  /** Directory for checkpoint files */
  dataDir: string;
  /** Jitter range in ms (default: 1000) */
  jitterMs?: number;
}

/**
 * Manages checkpoint persistence for projections.
 *
 * Features:
 * - Atomic writes using temp file + rename pattern
 * - CRC32 checksums for corruption detection
 * - Jittered scheduling to avoid write storms
 *
 * @example
 * ```ts
 * const manager = new CheckpointManager({
 *   fs: new BunFileSystem(),
 *   serializer: new MsgpackSerializer(),
 *   clock: new BunClock(),
 *   dataDir: '/data/checkpoints',
 * });
 *
 * // Write a checkpoint
 * await manager.writeCheckpoint({
 *   projectionName: 'TotalRevenue',
 *   position: 1000,
 *   state: { total: 50000 },
 *   timestamp: Date.now(),
 * });
 *
 * // Load a checkpoint
 * const checkpoint = await manager.loadCheckpoint('TotalRevenue');
 * ```
 */
export class CheckpointManager {
  private readonly fs: FileSystem;
  private readonly serializer: Serializer;
  private readonly clock: Clock;
  private readonly dataDir: string;
  private readonly jitterMs: number;

  /** Track next scheduled checkpoint times per projection */
  private readonly scheduledTimes = new Map<string, number>();

  constructor(config: CheckpointManagerConfig) {
    this.fs = config.fs;
    this.serializer = config.serializer;
    this.clock = config.clock;
    this.dataDir = config.dataDir;
    this.jitterMs = config.jitterMs ?? 1000;
  }

  /**
   * Initialize the checkpoint manager.
   *
   * Creates the checkpoint directory if it doesn't exist.
   */
  async initialize(): Promise<void> {
    if (!(await this.fs.exists(this.dataDir))) {
      await this.fs.mkdir(this.dataDir, { recursive: true });
    }
  }

  /**
   * Schedule a checkpoint with jitter.
   *
   * Returns the scheduled time. The actual checkpoint should be written
   * when the clock reaches this time.
   *
   * @param projectionName - Projection name
   * @param baseIntervalMs - Base interval between checkpoints
   * @returns Scheduled time in milliseconds since epoch
   */
  scheduleCheckpoint(projectionName: string, baseIntervalMs: number): number {
    const now = this.clock.now();
    const lastScheduled = this.scheduledTimes.get(projectionName) ?? now;

    // Calculate jitter: random value in [-jitterMs, +jitterMs]
    const jitter = (Math.random() * 2 - 1) * this.jitterMs;

    // Schedule next checkpoint
    const nextTime = Math.max(now, lastScheduled) + baseIntervalMs + jitter;
    this.scheduledTimes.set(projectionName, nextTime);

    return nextTime;
  }

  /**
   * Check if it's time to checkpoint.
   *
   * @param projectionName - Projection name
   * @returns true if current time >= scheduled time
   */
  shouldCheckpoint(projectionName: string): boolean {
    const scheduled = this.scheduledTimes.get(projectionName);
    if (scheduled === undefined) return false;
    return this.clock.now() >= scheduled;
  }

  /**
   * Write a checkpoint atomically.
   *
   * Uses write-to-temp → fsync → rename pattern for crash safety.
   *
   * @param checkpoint - Checkpoint data
   * @throws {CheckpointWriteError} if write fails
   */
  async writeCheckpoint(checkpoint: Checkpoint): Promise<void> {
    const path = this.getCheckpointPath(checkpoint.projectionName);
    const tempPath = path + '.tmp';

    try {
      if (!Number.isSafeInteger(checkpoint.position) || checkpoint.position < 0) {
        throw new Error('Checkpoint position must be a non-negative safe integer');
      }
      if (!Number.isSafeInteger(checkpoint.timestamp) || checkpoint.timestamp < 0) {
        throw new Error('Checkpoint timestamp must be a non-negative safe integer');
      }

      // Serialize state
      const stateData = this.serializer.encode(checkpoint.state);

      // Build file content
      const totalSize = CHECKPOINT_HEADER_SIZE + stateData.length;
      const buffer = new Uint8Array(totalSize);
      const view = new DataView(buffer.buffer);

      // Write header (checksum placeholder = 0)
      view.setUint32(0, CHECKPOINT_MAGIC, true);
      view.setUint32(4, CHECKPOINT_VERSION, true);
      view.setBigUint64(8, BigInt(checkpoint.position), true);
      view.setBigUint64(16, BigInt(checkpoint.timestamp), true);
      view.setUint32(24, stateData.length, true);
      view.setUint32(28, 0, true); // Checksum placeholder

      // Write state
      buffer.set(stateData, CHECKPOINT_HEADER_SIZE);

      // Calculate checksum (excluding bytes 28-31)
      const beforeChecksum = buffer.subarray(0, 28);
      const afterChecksum = buffer.subarray(32);
      const checksumData = new Uint8Array(beforeChecksum.length + afterChecksum.length);
      checksumData.set(beforeChecksum);
      checksumData.set(afterChecksum, beforeChecksum.length);
      const checksum = crc32(checksumData);
      view.setUint32(28, checksum, true);

      // Atomic write: temp file → fsync → rename
      const handle = await this.fs.open(tempPath, 'write');
      try {
        await this.fs.write(handle, buffer);
        await this.fs.sync(handle);
      } finally {
        await this.fs.close(handle);
      }

      await this.fs.rename(tempPath, path);
    } catch (error) {
      // Clean up temp file if it exists
      try {
        if (await this.fs.exists(tempPath)) {
          await this.fs.unlink(tempPath);
        }
      } catch {
        // Ignore cleanup errors
      }

      throw new CheckpointWriteError(
        checkpoint.projectionName,
        path,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  /**
   * Load a checkpoint for a projection.
   *
   * @param projectionName - Projection name
   * @returns Checkpoint or null if not found
   * @throws {CheckpointLoadError} if read fails
   * @throws {CheckpointCorruptionError} if file is corrupted
   * @throws {CheckpointVersionError} if version is unsupported
   */
  async loadCheckpoint(projectionName: string): Promise<Checkpoint | null> {
    const path = this.getCheckpointPath(projectionName);

    if (!(await this.fs.exists(path))) {
      return null;
    }

    try {
      const data = await this.fs.readFile(path);

      // Validate minimum size
      if (data.length < CHECKPOINT_HEADER_SIZE) {
        throw new CheckpointCorruptionError(
          projectionName,
          path,
          `File too small: ${data.length} bytes, expected at least ${CHECKPOINT_HEADER_SIZE}`
        );
      }

      // Parse header
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      const header: CheckpointHeader = {
        magic: view.getUint32(0, true),
        version: view.getUint32(4, true),
        position: Number(view.getBigUint64(8, true)),
        timestamp: Number(view.getBigUint64(16, true)),
        stateLength: view.getUint32(24, true),
        checksum: view.getUint32(28, true),
      };

      // Validate magic
      if (header.magic !== CHECKPOINT_MAGIC) {
        throw new CheckpointCorruptionError(
          projectionName,
          path,
          `Invalid magic: expected 0x${CHECKPOINT_MAGIC.toString(16)}, got 0x${header.magic.toString(16)}`
        );
      }

      // Validate version
      if (!SUPPORTED_VERSIONS.includes(header.version)) {
        throw new CheckpointVersionError(projectionName, path, header.version, SUPPORTED_VERSIONS);
      }

      // Validate state length
      const expectedSize = CHECKPOINT_HEADER_SIZE + header.stateLength;
      if (data.length < expectedSize) {
        throw new CheckpointCorruptionError(
          projectionName,
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

      if (calculatedChecksum !== header.checksum) {
        throw new CheckpointCorruptionError(
          projectionName,
          path,
          `Checksum mismatch: expected 0x${header.checksum.toString(16)}, calculated 0x${calculatedChecksum.toString(16)}`
        );
      }

      // Parse state
      const stateData = data.subarray(CHECKPOINT_HEADER_SIZE, expectedSize);
      const state = this.serializer.decode(stateData);

      return {
        projectionName,
        position: header.position,
        state,
        timestamp: Number(header.timestamp),
      };
    } catch (error) {
      // Re-throw our custom errors
      if (
        error instanceof CheckpointCorruptionError ||
        error instanceof CheckpointVersionError
      ) {
        throw error;
      }

      throw new CheckpointLoadError(
        projectionName,
        path,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  /**
   * Delete a checkpoint file.
   *
   * @param projectionName - Projection name
   * @returns true if file existed and was deleted
   */
  async deleteCheckpoint(projectionName: string): Promise<boolean> {
    const path = this.getCheckpointPath(projectionName);

    if (!(await this.fs.exists(path))) {
      return false;
    }

    await this.fs.unlink(path);
    this.scheduledTimes.delete(projectionName);
    return true;
  }

  /**
   * List all checkpoint files.
   *
   * @returns Array of projection names with checkpoints
   */
  async listCheckpoints(): Promise<string[]> {
    if (!(await this.fs.exists(this.dataDir))) {
      return [];
    }

    const files = await this.fs.readdir(this.dataDir);
    return files
      .filter((f) => f.endsWith('.ckpt'))
      .map((f) => f.slice(0, -5)); // Remove .ckpt extension
  }

  /**
   * Get the file path for a projection's checkpoint.
   */
  private getCheckpointPath(projectionName: string): string {
    // Sanitize projection name for filesystem
    const safeName = projectionName.replace(/[^a-zA-Z0-9_-]/g, '_');
    return `${this.dataDir}/${safeName}.ckpt`;
  }
}
