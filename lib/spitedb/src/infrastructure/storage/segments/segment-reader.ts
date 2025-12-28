/**
 * Segment file reader for reading batches of events.
 *
 * Handles:
 * - Reading and validating segment headers
 * - Reading individual batches by offset
 * - Iterating through all batches
 * - Segment validation and recovery
 *
 * @example
 * ```ts
 * const reader = new SegmentReader(fs, serializer, compressor);
 *
 * // Read header
 * const header = await reader.readHeader('/data/segment-0001.log');
 *
 * // Read a specific batch
 * const events = await reader.readBatch('/data/segment-0001.log', 32);
 *
 * // Iterate all batches
 * for await (const batch of reader.readAllBatches(path)) {
 *   for (const event of batch) {
 *     console.log(event.type);
 *   }
 * }
 * ```
 */

import type { FileSystem } from '../../../ports/storage/filesystem';
import type { Serializer } from '../../../ports/serialization/serializer';
import type { Compressor } from '../../../ports/serialization/compressor';
import {
  decodeSegmentHeader,
  isValidSegmentHeader,
  SEGMENT_HEADER_SIZE,
  type SegmentHeader,
} from './segment-header';
import {
  decodeBatchHeader,
  extractBatchPayload,
  isValidBatchMagic,
  BATCH_HEADER_SIZE,
  InvalidBatchError,
  BatchChecksumError,
} from '../batch/batch-record';
import type { StoredEvent } from '../../../domain/events/stored-event';

export interface ReadBatchProfileSample {
  headerReadMs: number;
  payloadReadMs: number;
  decompressMs: number;
  decodeMs: number;
  totalMs: number;
  eventCount: number;
  compressedBytes: number;
  uncompressedBytes: number;
}

export interface ReadBatchProfiler {
  record(sample: ReadBatchProfileSample): void;
}

/**
 * Result of validating a segment file.
 */
export interface ValidationResult {
  /** Whether the segment is fully valid */
  valid: boolean;
  /** Offset of the last valid batch end (useful for truncation during recovery) */
  lastValidOffset: number;
  /** Number of valid batches found */
  batchCount: number;
  /** Total number of events across all valid batches */
  eventCount: number;
  /** List of errors encountered (if any) */
  errors: string[];
}

/**
 * Reads events from segment files.
 *
 * Thread-safety: This class is thread-safe for concurrent reads
 * as it doesn't maintain mutable state.
 */
export class SegmentReader {
  constructor(
    private readonly fs: FileSystem,
    private readonly serializer: Serializer,
    private readonly compressor: Compressor,
    private readonly profiler?: ReadBatchProfiler
  ) {}

  /**
   * Read and validate the segment header.
   *
   * @param path - Path to the segment file
   * @returns Parsed segment header
   * @throws {InvalidSegmentHeaderError} if header is invalid
   */
  async readHeader(path: string): Promise<SegmentHeader> {
    const data = await this.fs.readFileSlice(path, 0, SEGMENT_HEADER_SIZE);
    return decodeSegmentHeader(data);
  }

  /**
   * Read a single batch at the given offset.
   *
   * @param path - Path to the segment file
   * @param offset - File offset where batch starts
   * @returns Array of events in the batch
   * @throws {InvalidBatchError} if batch is invalid
   * @throws {BatchChecksumError} if checksum doesn't match
   */
  async readBatch(path: string, offset: number): Promise<StoredEvent[]> {
    const totalStart = performance.now();
    // First read just the header to get payload size
    const headerStart = performance.now();
    const headerData = await this.fs.readFileSlice(path, offset, offset + BATCH_HEADER_SIZE);
    const headerReadMs = performance.now() - headerStart;
    const header = decodeBatchHeader(headerData, false); // Don't validate checksum yet

    // Now read the full batch including payload
    const fullBatchSize = BATCH_HEADER_SIZE + header.compressedLength;
    const payloadStart = performance.now();
    const batchData = await this.fs.readFileSlice(path, offset, offset + fullBatchSize);
    const payloadReadMs = performance.now() - payloadStart;

    // Validate checksum on full batch
    decodeBatchHeader(batchData, true);

    // Extract and decompress payload
    const payload = extractBatchPayload(batchData, header);
    const decompressStart = performance.now();
    const decompressed = this.compressor.decompress(payload);
    const decompressMs = performance.now() - decompressStart;

    // Deserialize events
    const decodeStart = performance.now();
    const events = this.serializer.decode<StoredEvent[]>(decompressed);
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
      uncompressedBytes: header.uncompressedLength,
    });

    return events;
  }

  /**
   * Read a batch and return metadata along with events.
   *
   * @param path - Path to the segment file
   * @param offset - File offset where batch starts
   * @returns Batch info including events and metadata
   */
  async readBatchWithMetadata(
    path: string,
    offset: number
  ): Promise<{
    events: StoredEvent[];
    batchId: bigint;
    nextOffset: number;
  }> {
    const totalStart = performance.now();
    // First read just the header to get payload size
    const headerStart = performance.now();
    const headerData = await this.fs.readFileSlice(path, offset, offset + BATCH_HEADER_SIZE);
    const headerReadMs = performance.now() - headerStart;
    const header = decodeBatchHeader(headerData, false);

    // Now read the full batch including payload
    const fullBatchSize = BATCH_HEADER_SIZE + header.compressedLength;
    const payloadStart = performance.now();
    const batchData = await this.fs.readFileSlice(path, offset, offset + fullBatchSize);
    const payloadReadMs = performance.now() - payloadStart;

    // Validate checksum on full batch
    decodeBatchHeader(batchData, true);

    // Extract and decompress payload
    const payload = extractBatchPayload(batchData, header);
    const decompressStart = performance.now();
    const decompressed = this.compressor.decompress(payload);
    const decompressMs = performance.now() - decompressStart;

    const decodeStart = performance.now();
    const events = this.serializer.decode<StoredEvent[]>(decompressed);
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
      uncompressedBytes: header.uncompressedLength,
    });

    return {
      events,
      batchId: header.batchId,
      nextOffset: offset + fullBatchSize,
    };
  }

  /**
   * Iterate through all batches in a segment file.
   *
   * @param path - Path to the segment file
   * @yields Arrays of events, one per batch
   */
  async *readAllBatches(
    path: string,
    startOffset: number = SEGMENT_HEADER_SIZE
  ): AsyncGenerator<StoredEvent[]> {
    const fileStat = await this.fs.stat(path);
    let offset = Math.max(startOffset, SEGMENT_HEADER_SIZE);

    while (offset < fileStat.size) {
      // Try to read header first to check if there's a valid batch
      const remaining = fileStat.size - offset;
      if (remaining < BATCH_HEADER_SIZE) {
        break; // Not enough data for another batch header
      }

      let headerData: Uint8Array;
      try {
        headerData = await this.fs.readFileSlice(path, offset, offset + BATCH_HEADER_SIZE);
      } catch (error) {
        throw error;
      }

      if (!isValidBatchMagic(headerData)) {
        break; // No more valid batches
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
        break; // Incomplete batch at end of file
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

  /**
   * Validate a segment file and find the last valid offset.
   *
   * This is used during crash recovery to determine how much
   * of a segment can be trusted.
   *
   * @param path - Path to the segment file
   * @returns Validation result
   */
  async validateSegment(path: string): Promise<ValidationResult> {
    const errors: string[] = [];
    let batchCount = 0;
    let eventCount = 0;
    let lastValidOffset = 0;

    // Check if file exists
    if (!(await this.fs.exists(path))) {
      return {
        valid: false,
        lastValidOffset: 0,
        batchCount: 0,
        eventCount: 0,
        errors: ['File does not exist'],
      };
    }

    const fileStat = await this.fs.stat(path);

    // Check minimum size for header
    if (fileStat.size < SEGMENT_HEADER_SIZE) {
      return {
        valid: false,
        lastValidOffset: 0,
        batchCount: 0,
        eventCount: 0,
        errors: [`File too small: ${fileStat.size} bytes (need at least ${SEGMENT_HEADER_SIZE})`],
      };
    }

    // Validate header
    const headerData = await this.fs.readFileSlice(path, 0, SEGMENT_HEADER_SIZE);
    if (!isValidSegmentHeader(headerData)) {
      return {
        valid: false,
        lastValidOffset: 0,
        batchCount: 0,
        eventCount: 0,
        errors: ['Invalid segment header'],
      };
    }

    lastValidOffset = SEGMENT_HEADER_SIZE;
    let offset = SEGMENT_HEADER_SIZE;

    // Validate each batch
    while (offset < fileStat.size) {
      const remaining = fileStat.size - offset;

      if (remaining < BATCH_HEADER_SIZE) {
        errors.push(`Incomplete batch header at offset ${offset}: only ${remaining} bytes remaining`);
        break;
      }

      try {
        const headerData = await this.fs.readFileSlice(path, offset, offset + BATCH_HEADER_SIZE);

        if (!isValidBatchMagic(headerData)) {
          errors.push(`Invalid batch magic at offset ${offset}`);
          break;
        }

        const header = decodeBatchHeader(headerData, false);
        const fullBatchSize = BATCH_HEADER_SIZE + header.compressedLength;

        if (offset + fullBatchSize > fileStat.size) {
          errors.push(
            `Incomplete batch at offset ${offset}: need ${fullBatchSize} bytes, have ${remaining}`
          );
          break;
        }

        // Read full batch and validate checksum
        const batchData = await this.fs.readFileSlice(path, offset, offset + fullBatchSize);
        decodeBatchHeader(batchData, true); // Throws on checksum failure

        // Try to decompress and deserialize to verify data integrity
        const payload = extractBatchPayload(batchData, header);
        const decompressed = this.compressor.decompress(payload);
        const events = this.serializer.decode<StoredEvent[]>(decompressed);

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
      errors,
    };
  }

  /**
   * Get the file size of a segment.
   *
   * @param path - Path to the segment file
   * @returns File size in bytes
   */
  async getFileSize(path: string): Promise<number> {
    const stat = await this.fs.stat(path);
    return stat.size;
  }
}
