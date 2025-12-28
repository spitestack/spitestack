/**
 * Value object representing a unique identifier for a batch of events.
 *
 * BatchIds are used internally to track batches of events that are
 * written together in a single atomic operation.
 *
 * @example
 * ```ts
 * const batchId = BatchId.generate();
 * const fromValue = BatchId.from(12345n);
 * ```
 */
export class BatchId {
  private constructor(private readonly value: bigint) {}

  /**
   * Create a BatchId from a bigint or number value.
   */
  static from(value: bigint | number): BatchId {
    return new BatchId(BigInt(value));
  }

  /**
   * Generate a new unique BatchId.
   * Uses timestamp + random bits for uniqueness.
   */
  static generate(): BatchId {
    // Combine timestamp with random for uniqueness
    const timestamp = BigInt(Date.now()) << 20n;
    const random = BigInt(Math.floor(Math.random() * 0xfffff));
    return new BatchId(timestamp | random);
  }

  /**
   * Get the batch ID as a bigint
   */
  toBigInt(): bigint {
    return this.value;
  }

  /**
   * Get the batch ID as a hex string
   */
  toString(): string {
    return this.value.toString(16);
  }

  /**
   * Check equality with another BatchId
   */
  equals(other: BatchId): boolean {
    return this.value === other.value;
  }
}
