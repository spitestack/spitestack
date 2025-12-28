import { InvalidPositionError } from '../errors/invalid-position.error';

/**
 * Value object representing a position in the global event log.
 *
 * GlobalPosition is a monotonically increasing value that uniquely
 * identifies an event's position across all streams.
 *
 * @example
 * ```ts
 * const pos = GlobalPosition.from(1000);
 * const next = pos.next();
 * ```
 */
export class GlobalPosition {
  private constructor(private readonly value: number) {}

  /**
   * Create a GlobalPosition from a number value.
   * @throws {InvalidPositionError} if the value is negative
   */
  static from(value: number): GlobalPosition {
    if (!Number.isSafeInteger(value)) {
      throw new InvalidPositionError('GlobalPosition must be a safe integer');
    }
    if (value < 0) {
      throw new InvalidPositionError('GlobalPosition cannot be negative');
    }
    return new GlobalPosition(value);
  }

  /**
   * The beginning of the log (position 0)
   */
  static readonly BEGINNING = GlobalPosition.from(0);

  /**
   * Get the position as a number
   */
  toNumber(): number {
    return this.value;
  }

  /**
   * Get the next position (current + 1)
   */
  next(): GlobalPosition {
    return new GlobalPosition(this.value + 1);
  }

  /**
   * Advance by a specified count
   */
  advance(count: number): GlobalPosition {
    return new GlobalPosition(this.value + count);
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
