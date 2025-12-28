/**
 * Base error for projection-related failures.
 */
export class ProjectionError extends Error {
  constructor(
    message: string,
    public readonly projectionName: string
  ) {
    super(`[Projection: ${projectionName}] ${message}`);
    this.name = 'ProjectionError';
    Object.setPrototypeOf(this, ProjectionError.prototype);
  }
}

/**
 * Thrown when a projection fails to process an event.
 */
export class ProjectionBuildError extends ProjectionError {
  constructor(
    projectionName: string,
    public readonly eventType: string,
    public readonly globalPosition: number,
    public readonly cause: Error
  ) {
    super(
      `Failed to process event '${eventType}' at position ${globalPosition}: ${cause.message}`,
      projectionName
    );
    this.name = 'ProjectionBuildError';
    Object.setPrototypeOf(this, ProjectionBuildError.prototype);
  }
}

/**
 * Thrown when a projection is not found.
 */
export class ProjectionNotFoundError extends ProjectionError {
  constructor(projectionName: string) {
    super(`Projection not found`, projectionName);
    this.name = 'ProjectionNotFoundError';
    Object.setPrototypeOf(this, ProjectionNotFoundError.prototype);
  }
}

/**
 * Thrown when a projection is already registered.
 */
export class ProjectionAlreadyRegisteredError extends ProjectionError {
  constructor(projectionName: string) {
    super(`Projection is already registered`, projectionName);
    this.name = 'ProjectionAlreadyRegisteredError';
    Object.setPrototypeOf(this, ProjectionAlreadyRegisteredError.prototype);
  }
}

/**
 * Thrown when a projection is not enabled.
 */
export class ProjectionDisabledError extends ProjectionError {
  constructor(projectionName: string) {
    super(`Projection is disabled`, projectionName);
    this.name = 'ProjectionDisabledError';
    Object.setPrototypeOf(this, ProjectionDisabledError.prototype);
  }
}

/**
 * Thrown when a projection coordinator operation fails.
 */
export class ProjectionCoordinatorError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ProjectionCoordinatorError';
    Object.setPrototypeOf(this, ProjectionCoordinatorError.prototype);
  }
}

/**
 * Thrown when waiting for catch-up times out.
 */
export class ProjectionCatchUpTimeoutError extends ProjectionCoordinatorError {
  constructor(
    public readonly projectionName: string,
    public readonly currentPosition: number,
    public readonly targetPosition: number,
    public readonly timeoutMs: number
  ) {
    super(
      `Projection '${projectionName}' failed to catch up: ` +
        `at position ${currentPosition}, target ${targetPosition}, timeout ${timeoutMs}ms`
    );
    this.name = 'ProjectionCatchUpTimeoutError';
    Object.setPrototypeOf(this, ProjectionCatchUpTimeoutError.prototype);
  }
}
