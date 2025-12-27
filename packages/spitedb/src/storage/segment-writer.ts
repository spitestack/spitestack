/**
 * Segment file writer for appending batches of events.
 *
 * Handles:
 * - Writing segment header on open
 * - Appending compressed batches
 * - Managing file position
 * - Syncing data to disk for durability
 *
 * @example
 * ```ts
 * const writer = new SegmentWriter(fs, serializer, compressor);
 * await writer.open('/data/segment-0001.log', 1n, 0n);
 *
 * const result = await writer.appendBatch(events);
 * await writer.sync();
 * await writer.close();
 * ```
 */

import type { FileSystem, FileHandle } from '../interfaces/filesystem';
import type { Serializer } from '../interfaces/serializer';
import type { Compressor } from '../interfaces/compressor';
import { encodeSegmentHeader, SEGMENT_HEADER_SIZE } from './segment-header';
import { encodeBatchRecord, BATCH_HEADER_SIZE } from './batch-record';
import type { StoredEvent } from './stored-event';
import { SegmentIndexFile, type IndexEntry } from './segment-index-file';

/**
 * Result of writing a batch to a segment.
 */
export interface BatchWriteResult {
  /** Unique batch identifier */
  batchId: bigint;
  /** File offset where the batch starts */
  offset: number;
  /** Total bytes written (header + payload) */
  length: number;
  /** Number of events in the batch */
  eventCount: number;
}

/**
 * Writes events to segment files.
 *
 * Thread-safety: This class is NOT thread-safe. Use a single writer
 * per segment and coordinate access externally if needed.
 */
export class SegmentWriter {
  private handle: FileHandle | null = null;
  private currentOffset: number = 0;
  private batchIdCounter: bigint = 0n;
  private path: string = '';
  private segmentId: bigint = 0n;
  private indexEntries: IndexEntry[] = [];

  constructor(
    private readonly fs: FileSystem,
    private readonly serializer: Serializer,
    private readonly compressor: Compressor
  ) {}

  /**
   * Open a new segment file for writing.
   *
   * Creates the file and writes the segment header.
   *
   * @param path - Path to the segment file
   * @param segmentId - Unique segment identifier
   * @param basePosition - Global position of first event in this segment
   */
  async open(path: string, segmentId: bigint, basePosition: bigint): Promise<void> {
    if (this.handle) {
      throw new Error('SegmentWriter already has an open file');
    }

    this.path = path;
    this.segmentId = segmentId;
    this.indexEntries = [];
    this.handle = await this.fs.open(path, 'write');

    // Write segment header
    const header = encodeSegmentHeader({
      flags: 0,
      segmentId,
      basePosition,
    });

    await this.fs.write(this.handle, header, 0);
    this.currentOffset = SEGMENT_HEADER_SIZE;

    // Sync header to ensure segment is valid even if we crash
    await this.fs.sync(this.handle);
  }

  /**
   * Resume writing to an existing segment file.
   *
   * Opens the file in append mode at the given offset.
   *
   * @param path - Path to the segment file
   * @param offset - File offset to resume from
   * @param lastBatchId - Last batch ID written (to continue numbering)
   */
  async resume(path: string, offset: number, lastBatchId: bigint): Promise<void> {
    if (this.handle) {
      throw new Error('SegmentWriter already has an open file');
    }

    this.path = path;
    this.handle = await this.fs.open(path, 'readwrite');
    this.currentOffset = offset;
    this.batchIdCounter = lastBatchId + 1n;
  }

  /**
   * Append a batch of events to the segment.
   *
   * Events are serialized together, compressed, and written atomically.
   * The data is NOT durable until sync() is called.
   *
   * @param events - Events to write
   * @returns Batch write result with position info
   */
  async appendBatch(events: StoredEvent[]): Promise<BatchWriteResult> {
    if (!this.handle) {
      throw new Error('SegmentWriter is not open');
    }

    if (events.length === 0) {
      throw new Error('Cannot write empty batch');
    }

    const batchId = this.batchIdCounter++;
    const batchOffset = this.currentOffset;

    // Serialize events to msgpack
    const serialized = this.serializer.encode(events);

    // Compress the serialized data
    const compressed = this.compressor.compress(serialized);

    // Encode the batch record
    const record = encodeBatchRecord(batchId, events.length, compressed, serialized.length);

    // Write to file
    await this.fs.write(this.handle, record, batchOffset);
    this.currentOffset += record.length;

    // Track index entries for each event
    for (const event of events) {
      this.indexEntries.push({
        streamId: event.streamId,
        revision: event.revision,
        globalPosition: event.globalPosition,
        batchOffset,
      });
    }

    return {
      batchId,
      offset: batchOffset,
      length: record.length,
      eventCount: events.length,
    };
  }

  /**
   * Sync written data to disk.
   *
   * After this returns successfully, all previously written
   * batches are guaranteed to survive a crash.
   */
  async sync(): Promise<void> {
    if (!this.handle) {
      throw new Error('SegmentWriter is not open');
    }
    await this.fs.sync(this.handle);
  }

  /**
   * Close the segment file and write the index file.
   *
   * Note: This does NOT automatically sync the .log file. Call sync() before
   * close() if durability is required.
   *
   * The .idx file is written atomically (temp file → fsync → rename).
   */
  async close(): Promise<void> {
    if (!this.handle) {
      return; // Already closed or never opened
    }

    // Write index file if we have any entries
    if (this.indexEntries.length > 0) {
      const idxPath = this.path.replace('.log', '.idx');
      await SegmentIndexFile.write(this.fs, idxPath, this.segmentId, this.indexEntries);
    }

    await this.fs.close(this.handle);
    this.handle = null;
    this.path = '';
    this.indexEntries = [];
  }

  /**
   * Get the current file offset (end of written data).
   */
  get offset(): number {
    return this.currentOffset;
  }

  /**
   * Check if the writer has an open file.
   */
  get isOpen(): boolean {
    return this.handle !== null;
  }

  /**
   * Get the path to the current segment file.
   */
  get filePath(): string {
    return this.path;
  }

  /**
   * Get the next batch ID that will be assigned.
   */
  get nextBatchId(): bigint {
    return this.batchIdCounter;
  }
}
