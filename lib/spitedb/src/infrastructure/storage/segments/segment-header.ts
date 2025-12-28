/**
 * Segment file header encoding and decoding.
 *
 * Header format (32 bytes):
 * ```
 * Offset  Size  Field
 * 0       4     Magic number (0x53504954 = "SPIT")
 * 4       1     Version (1)
 * 5       1     Flags (reserved)
 * 6       2     Reserved
 * 8       8     Segment ID (bigint)
 * 16      8     Base position (first event's global position, u64)
 * 24      8     Reserved
 * ```
 */

/** Magic number for segment files: "SPIT" in big-endian */
export const SEGMENT_MAGIC = 0x53504954;

/** Current segment file version */
export const SEGMENT_VERSION = 1;

/** Size of the segment header in bytes */
export const SEGMENT_HEADER_SIZE = 32;

/**
 * Parsed segment header.
 */
export interface SegmentHeader {
  /** Magic number (should always be SEGMENT_MAGIC) */
  magic: number;
  /** File format version */
  version: number;
  /** Reserved flags for future use */
  flags: number;
  /** Unique segment identifier */
  segmentId: bigint;
  /** Global position of the first event in this segment */
  basePosition: number;
}

/**
 * Error thrown when segment header validation fails.
 */
export class InvalidSegmentHeaderError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidSegmentHeaderError';
  }
}

/**
 * Encode a segment header to bytes.
 *
 * @param header - Header data to encode
 * @returns 32-byte Uint8Array containing the encoded header
 */
export function encodeSegmentHeader(header: Omit<SegmentHeader, 'magic' | 'version'>): Uint8Array {
  if (!Number.isSafeInteger(header.basePosition) || header.basePosition < 0) {
    throw new InvalidSegmentHeaderError('Base position must be a non-negative safe integer');
  }

  const buffer = new ArrayBuffer(SEGMENT_HEADER_SIZE);
  const view = new DataView(buffer);

  // Magic number (4 bytes, big-endian)
  view.setUint32(0, SEGMENT_MAGIC, false);

  // Version (1 byte)
  view.setUint8(4, SEGMENT_VERSION);

  // Flags (1 byte)
  view.setUint8(5, header.flags);

  // Reserved (2 bytes) - leave as zeros
  view.setUint16(6, 0, false);

  // Segment ID (8 bytes, big-endian)
  view.setBigUint64(8, header.segmentId, false);

  // Base position (8 bytes, big-endian)
  view.setBigUint64(16, BigInt(header.basePosition), false);

  // Reserved (8 bytes) - leave as zeros

  return new Uint8Array(buffer);
}

/**
 * Decode a segment header from bytes.
 *
 * @param data - Byte array containing the header (must be at least 32 bytes)
 * @returns Parsed header
 * @throws {InvalidSegmentHeaderError} if header is invalid
 */
export function decodeSegmentHeader(data: Uint8Array): SegmentHeader {
  if (data.length < SEGMENT_HEADER_SIZE) {
    throw new InvalidSegmentHeaderError(
      `Header too short: expected ${SEGMENT_HEADER_SIZE} bytes, got ${data.length}`
    );
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  const magic = view.getUint32(0, false);
  if (magic !== SEGMENT_MAGIC) {
    throw new InvalidSegmentHeaderError(
      `Invalid magic number: expected 0x${SEGMENT_MAGIC.toString(16).toUpperCase()}, got 0x${magic.toString(16).toUpperCase()}`
    );
  }

  const version = view.getUint8(4);
  if (version !== SEGMENT_VERSION) {
    throw new InvalidSegmentHeaderError(
      `Unsupported version: expected ${SEGMENT_VERSION}, got ${version}`
    );
  }

  const basePosition = Number(view.getBigUint64(16, false));
  if (!Number.isSafeInteger(basePosition)) {
    throw new InvalidSegmentHeaderError('Base position exceeds safe integer range');
  }

  return {
    magic,
    version,
    flags: view.getUint8(5),
    segmentId: view.getBigUint64(8, false),
    basePosition,
  };
}

/**
 * Validate that data starts with a valid segment header.
 *
 * @param data - Data to validate
 * @returns true if valid, false otherwise
 */
export function isValidSegmentHeader(data: Uint8Array): boolean {
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
