import { InvalidPositionError } from '../errors/invalid-position.error';

/**
 * Value object representing a position in the global event log.
 *
 * GlobalPosition is a monotonically increasing value that uniquely
 * identifies an event's position across all streams. Uses bigint
 * internally to support very large event stores.
 *
 * @example
 * ```ts
 * const pos = GlobalPosition.from(1000n);
 * const next = pos.next();
 * ```
 */
export class GlobalPosition {
  private constructor(private readonly value: bigint) {}

  /**
   * Create a GlobalPosition from a bigint or number value.
   * @throws {InvalidPositionError} if the value is negative
   */
  static from(value: bigint | number): GlobalPosition {
    const bigintValue = BigInt(value);
    if (bigintValue < 0n) {
      throw new InvalidPositionError('GlobalPosition cannot be negative');
    }
    return new GlobalPosition(bigintValue);
  }

  /**
   * The beginning of the log (position 0)
   */
  static readonly BEGINNING = GlobalPosition.from(0n);

  /**
   * Get the position as a bigint
   */
  toBigInt(): bigint {
    return this.value;
  }

  /**
   * Get the position as a number.
   * @throws {Error} if the position exceeds Number.MAX_SAFE_INTEGER
   */
  toNumber(): number {
    if (this.value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error('GlobalPosition too large to convert to number');
    }
    return Number(this.value);
  }

  /**
   * Get the next position (current + 1)
   */
  next(): GlobalPosition {
    return new GlobalPosition(this.value + 1n);
  }

  /**
   * Advance by a specified count
   */
  advance(count: number): GlobalPosition {
    return new GlobalPosition(this.value + BigInt(count));
  }

  /**
   * Check equality with another GlobalPosition
   */
  equals(other: GlobalPosition): boolean {
    return this.value === other.value;
  }

  /**
   * Compare with another GlobalPosition
   * @returns -1 if this < other, 0 if equal, 1 if this > other
   */
  compareTo(other: GlobalPosition): -1 | 0 | 1 {
    if (this.value < other.value) return -1;
    if (this.value > other.value) return 1;
    return 0;
  }

  /**
   * Check if this position is after another
   */
  isAfter(other: GlobalPosition): boolean {
    return this.value > other.value;
  }

  /**
   * Check if this position is before another
   */
  isBefore(other: GlobalPosition): boolean {
    return this.value < other.value;
  }
}
