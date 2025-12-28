/**
 * Checkpoint-related errors.
 */

/**
 * Thrown when checkpoint persistence fails.
 */
export class CheckpointWriteError extends Error {
  constructor(
    public readonly projectionName: string,
    public readonly path: string,
    public readonly cause: Error
  ) {
    super(
      `Failed to write checkpoint for '${projectionName}' to ${path}: ${cause.message}`
    );
    this.name = 'CheckpointWriteError';
    Object.setPrototypeOf(this, CheckpointWriteError.prototype);
  }
}

/**
 * Thrown when checkpoint loading fails.
 */
export class CheckpointLoadError extends Error {
  constructor(
    public readonly projectionName: string,
    public readonly path: string,
    public readonly cause: Error
  ) {
    super(
      `Failed to load checkpoint for '${projectionName}' from ${path}: ${cause.message}`
    );
    this.name = 'CheckpointLoadError';
    Object.setPrototypeOf(this, CheckpointLoadError.prototype);
  }
}

/**
 * Thrown when checkpoint is corrupted.
 */
export class CheckpointCorruptionError extends Error {
  constructor(
    public readonly projectionName: string,
    public readonly path: string,
    public readonly reason: string
  ) {
    super(
      `Corrupted checkpoint for '${projectionName}' at ${path}: ${reason}`
    );
    this.name = 'CheckpointCorruptionError';
    Object.setPrototypeOf(this, CheckpointCorruptionError.prototype);
  }
}

/**
 * Thrown when checkpoint version is unsupported.
 */
export class CheckpointVersionError extends Error {
  constructor(
    public readonly projectionName: string,
    public readonly path: string,
    public readonly version: number,
    public readonly supportedVersions: number[]
  ) {
    super(
      `Unsupported checkpoint version ${version} for '${projectionName}' at ${path}. ` +
        `Supported versions: ${supportedVersions.join(', ')}`
    );
    this.name = 'CheckpointVersionError';
    Object.setPrototypeOf(this, CheckpointVersionError.prototype);
  }
}
