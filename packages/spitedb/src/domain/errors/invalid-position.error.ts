/**
 * Thrown when a global position is invalid
 */
export class InvalidPositionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidPositionError';
    Object.setPrototypeOf(this, InvalidPositionError.prototype);
  }
}
