/**
 * Base error for SpiteDB operations.
 *
 * All SpiteDB-specific errors extend this class for easy catching.
 *
 * @example
 * ```ts
 * try {
 *   await db.append('stream', events);
 * } catch (error) {
 *   if (error instanceof SpiteDBError) {
 *     // Handle SpiteDB-specific errors
 *   }
 * }
 * ```
 */
export class SpiteDBError extends Error {
  constructor(message: string) {
    super(`[SpiteDB] ${message}`);
    this.name = 'SpiteDBError';
    Object.setPrototypeOf(this, SpiteDBError.prototype);
  }
}

/**
 * Thrown when SpiteDB is used before open or after close.
 *
 * @example
 * ```ts
 * const db = new SpiteDB(); // Wrong! Use SpiteDB.open()
 * db.append('stream', [event]); // throws SpiteDBNotOpenError
 * ```
 */
export class SpiteDBNotOpenError extends SpiteDBError {
  constructor() {
    super('Database is not open. Call SpiteDB.open() first.');
    this.name = 'SpiteDBNotOpenError';
    Object.setPrototypeOf(this, SpiteDBNotOpenError.prototype);
  }
}

/**
 * Thrown when projections are accessed before being started.
 *
 * @example
 * ```ts
 * const db = await SpiteDB.open('./data');
 * db.requireProjection('MyProjection'); // throws ProjectionsNotStartedError
 *
 * // Fix: start projections first
 * await db.startProjections();
 * db.requireProjection('MyProjection'); // works
 * ```
 */
export class ProjectionsNotStartedError extends SpiteDBError {
  constructor() {
    super('Projections not started. Call startProjections() first.');
    this.name = 'ProjectionsNotStartedError';
    Object.setPrototypeOf(this, ProjectionsNotStartedError.prototype);
  }
}

/**
 * Thrown when projection backpressure blocks appends.
 */
export class ProjectionBackpressureError extends SpiteDBError {
  constructor(
    public readonly projectionName: string,
    public readonly lag: number,
    public readonly maxLag: number
  ) {
    super(
      `Append blocked: projection '${projectionName}' is lagging by ${lag} (max ${maxLag}).`
    );
    this.name = 'ProjectionBackpressureError';
    Object.setPrototypeOf(this, ProjectionBackpressureError.prototype);
  }
}

/**
 * Thrown when append backpressure waits too long.
 */
export class ProjectionBackpressureTimeoutError extends ProjectionBackpressureError {
  constructor(
    projectionName: string,
    lag: number,
    maxLag: number,
    public readonly waitedMs: number,
    public readonly maxWaitMs: number
  ) {
    super(projectionName, lag, maxLag);
    this.name = 'ProjectionBackpressureTimeoutError';
    Object.setPrototypeOf(this, ProjectionBackpressureTimeoutError.prototype);
  }
}
