import { InvalidStreamIdError } from '../errors/invalid-stream-id.error';

const MAX_STREAM_ID_LENGTH = 256;
const STREAM_ID_PATTERN = /^[a-zA-Z0-9_\-:.]+$/;

/**
 * Value object representing a unique identifier for an event stream.
 *
 * StreamIds are immutable and validated on creation. They can contain
 * alphanumeric characters, underscores, hyphens, colons, and dots.
 *
 * @example
 * ```ts
 * const streamId = StreamId.from('user-123');
 * const orderId = StreamId.from('order:456');
 * ```
 */
export class StreamId {
  private constructor(private readonly value: string) {}

  /**
   * Create a StreamId from a string value.
   * @throws {InvalidStreamIdError} if the value is invalid
   */
  static from(value: string): StreamId {
    if (!value) {
      throw new InvalidStreamIdError('StreamId cannot be empty');
    }
    if (value.length > MAX_STREAM_ID_LENGTH) {
      throw new InvalidStreamIdError(
        `StreamId cannot exceed ${MAX_STREAM_ID_LENGTH} characters`,
      );
    }
    if (!STREAM_ID_PATTERN.test(value)) {
      throw new InvalidStreamIdError(
        'StreamId can only contain alphanumeric characters, underscores, hyphens, colons, and dots',
      );
    }
    return new StreamId(value);
  }

  /**
   * Get the string representation of this StreamId
   */
  toString(): string {
    return this.value;
  }

  /**
   * Check equality with another StreamId
   */
  equals(other: StreamId): boolean {
    return this.value === other.value;
  }

  /**
   * Compute a hash of this StreamId using FNV-1a algorithm.
   * Useful for consistent bucketing/partitioning.
   */
  hash(): number {
    let hash = 2166136261;
    for (let i = 0; i < this.value.length; i++) {
      hash ^= this.value.charCodeAt(i);
      hash = (hash * 16777619) >>> 0;
    }
    return hash;
  }
}
