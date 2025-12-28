/**
 * Value object representing a stream-specific revision number.
 *
 * Revision tracks the position of an event within a single stream.
 * It's used for optimistic concurrency control - you can specify
 * an expected revision when appending to ensure no concurrent writes.
 *
 * Special values:
 * - Revision.NONE (-1): Stream does not exist yet
 * - Revision.ANY (-2): Skip optimistic concurrency check
 *
 * @example
 * ```ts
 * // Expect stream to not exist
 * await store.append(streamId, events, Revision.NONE);
 *
 * // Expect stream to be at revision 5
 * await store.append(streamId, events, Revision.from(5));
 *
 * // Don't check revision
 * await store.append(streamId, events, Revision.ANY);
 * ```
 */
export class Revision {
  private constructor(private readonly value: number) {}

  /**
   * Create a Revision from a number value.
   * @throws {Error} if the value is not an integer
   */
  static from(value: number): Revision {
    if (!Number.isInteger(value)) {
      throw new Error('Revision must be an integer');
    }
    return new Revision(value);
  }

  /**
   * Indicates the stream does not exist yet.
   * Use when creating a new stream.
   */
  static readonly NONE = new Revision(-1);

  /**
   * Skip the optimistic concurrency check.
   * Use when you don't care about concurrent writes.
   */
  static readonly ANY = new Revision(-2);

  /**
   * Get the revision as a number
   */
  toNumber(): number {
    return this.value;
  }

  /**
   * Get the next revision (current + 1)
   */
  next(): Revision {
    return new Revision(this.value + 1);
  }

  /**
   * Check if this is the NONE revision
   */
  isNone(): boolean {
    return this.value === -1;
  }

  /**
   * Check if this is the ANY revision
   */
  isAny(): boolean {
    return this.value === -2;
  }

  /**
   * Check equality with another Revision
   */
  equals(other: Revision): boolean {
    return this.value === other.value;
  }

  /**
   * Compare with another Revision
   * @returns -1 if this < other, 0 if equal, 1 if this > other
   */
  compareTo(other: Revision): -1 | 0 | 1 {
    if (this.value < other.value) return -1;
    if (this.value > other.value) return 1;
    return 0;
  }
}
