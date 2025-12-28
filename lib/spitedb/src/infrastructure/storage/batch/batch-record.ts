/**
 * Batch record encoding and decoding for segment files.
 *
 * Each batch contains one or more events compressed together.
 *
 * Batch Record Format (28 + N bytes):
 * ```
 * Offset  Size  Field
 * 0       4     Magic number (0x42415443 = "BATC")
 * 4       4     CRC32 checksum (of bytes 8 to end)
 * 8       4     Compressed payload length
 * 12      4     Uncompressed payload length
 * 16      8     Batch ID
 * 24      4     Event count in batch
 * 28      N     Compressed payload (msgpack-encoded events)
 * ```
 */

import { crc32 } from '../support/crc32';

/** Magic number for batch records: "BATC" in big-endian */
export const BATCH_MAGIC = 0x42415443;

/** Size of the batch record header (before payload) */
export const BATCH_HEADER_SIZE = 28;

/**
 * Metadata for a batch record.
 */
export interface BatchHeader {
  /** Magic number (should always be BATCH_MAGIC) */
  magic: number;
  /** CRC32 checksum of bytes after this field */
  checksum: number;
  /** Length of compressed payload in bytes */
  compressedLength: number;
  /** Length of uncompressed payload in bytes */
  uncompressedLength: number;
  /** Unique batch identifier */
  batchId: bigint;
  /** Number of events in this batch */
  eventCount: number;
}

/**
 * Error thrown when batch validation fails.
 */
export class InvalidBatchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidBatchError';
  }
}

/**
 * Error thrown when batch checksum doesn't match.
 */
export class BatchChecksumError extends InvalidBatchError {
  constructor(expected: number, actual: number) {
    super(
      `Checksum mismatch: expected 0x${expected.toString(16).padStart(8, '0')}, ` +
        `got 0x${actual.toString(16).padStart(8, '0')}`
    );
    this.name = 'BatchChecksumError';
  }
}

/**
 * Encode a batch record to bytes.
 *
 * @param batchId - Unique batch identifier
 * @param eventCount - Number of events in this batch
 * @param compressedPayload - Compressed event data
 * @param uncompressedLength - Original uncompressed size
 * @returns Byte array containing the complete batch record
 */
export function encodeBatchRecord(
  batchId: bigint,
  eventCount: number,
  compressedPayload: Uint8Array,
  uncompressedLength: number
): Uint8Array {
  const totalSize = BATCH_HEADER_SIZE + compressedPayload.length;
  const buffer = new ArrayBuffer(totalSize);
  const view = new DataView(buffer);
  const bytes = new Uint8Array(buffer);

  // Magic number (4 bytes, big-endian)
  view.setUint32(0, BATCH_MAGIC, false);

  // Skip checksum for now (offset 4-8), will compute after filling rest

  // Compressed payload length (4 bytes, big-endian)
  view.setUint32(8, compressedPayload.length, false);

  // Uncompressed payload length (4 bytes, big-endian)
  view.setUint32(12, uncompressedLength, false);

  // Batch ID (8 bytes, big-endian)
  view.setBigUint64(16, batchId, false);

  // Event count (4 bytes, big-endian)
  view.setUint32(24, eventCount, false);

  // Compressed payload
  bytes.set(compressedPayload, BATCH_HEADER_SIZE);

  // Compute checksum of everything from offset 8 onwards
  const checksumData = bytes.subarray(8);
  const checksum = crc32(checksumData);
  view.setUint32(4, checksum, false);

  return bytes;
}

/**
 * Decode the header of a batch record.
 *
 * @param data - Byte array containing at least the header
 * @param validateChecksum - Whether to validate the CRC32 checksum
 * @returns Parsed header
 * @throws {InvalidBatchError} if header is invalid
 * @throws {BatchChecksumError} if checksum doesn't match (when validateChecksum=true)
 */
export function decodeBatchHeader(data: Uint8Array, validateChecksum = true): BatchHeader {
  if (data.length < BATCH_HEADER_SIZE) {
    throw new InvalidBatchError(
      `Header too short: expected at least ${BATCH_HEADER_SIZE} bytes, got ${data.length}`
    );
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  const magic = view.getUint32(0, false);
  if (magic !== BATCH_MAGIC) {
    throw new InvalidBatchError(
      `Invalid magic number: expected 0x${BATCH_MAGIC.toString(16).toUpperCase()}, ` +
        `got 0x${magic.toString(16).toUpperCase()}`
    );
  }

  const storedChecksum = view.getUint32(4, false);
  const compressedLength = view.getUint32(8, false);
  const uncompressedLength = view.getUint32(12, false);
  const batchId = view.getBigUint64(16, false);
  const eventCount = view.getUint32(24, false);

  // Validate checksum if requested and we have the full record
  if (validateChecksum) {
    const expectedSize = BATCH_HEADER_SIZE + compressedLength;
    if (data.length < expectedSize) {
      throw new InvalidBatchError(
        `Incomplete batch: expected ${expectedSize} bytes, got ${data.length}`
      );
    }

    // Checksum is computed over bytes 8 to end (including payload)
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
    eventCount,
  };
}

/**
 * Extract the compressed payload from a batch record.
 *
 * @param data - Complete batch record data
 * @param header - Pre-decoded header (optional, will decode if not provided)
 * @returns Compressed payload bytes
 */
export function extractBatchPayload(data: Uint8Array, header?: BatchHeader): Uint8Array {
  const h = header ?? decodeBatchHeader(data, false);
  return data.subarray(BATCH_HEADER_SIZE, BATCH_HEADER_SIZE + h.compressedLength);
}

/**
 * Calculate the total size of a batch record given the compressed payload length.
 */
export function getBatchRecordSize(compressedLength: number): number {
  return BATCH_HEADER_SIZE + compressedLength;
}

/**
 * Check if data starts with a valid batch magic number.
 */
export function isValidBatchMagic(data: Uint8Array): boolean {
  if (data.length < 4) {
    return false;
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  return view.getUint32(0, false) === BATCH_MAGIC;
}
