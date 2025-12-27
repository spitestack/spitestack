/**
 * Thrown when a stream ID fails validation
 */
export class InvalidStreamIdError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidStreamIdError';
    Object.setPrototypeOf(this, InvalidStreamIdError.prototype);
  }
}
