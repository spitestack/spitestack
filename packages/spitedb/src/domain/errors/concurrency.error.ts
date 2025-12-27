/**
 * Thrown when an optimistic concurrency check fails
 */
export class ConcurrencyError extends Error {
  constructor(
    public readonly streamId: string,
    public readonly expectedRevision: number,
    public readonly actualRevision: number,
  ) {
    super(
      `Concurrency conflict on stream '${streamId}': ` +
        `expected revision ${expectedRevision}, ` +
        `but actual revision is ${actualRevision}`,
    );
    this.name = 'ConcurrencyError';
    Object.setPrototypeOf(this, ConcurrencyError.prototype);
  }
}
