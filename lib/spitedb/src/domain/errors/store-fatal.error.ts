/**
 * Thrown when the EventStore enters an unrecoverable state.
 *
 * This typically occurs when a sync operation fails after data has been
 * written to disk. The store must be closed and reopened to recover.
 *
 * @example
 * ```ts
 * try {
 *   await store.flush();
 * } catch (error) {
 *   if (error instanceof StoreFatalError) {
 *     console.error('Store is in failed state, must reopen:', error.cause);
 *     await store.close();
 *     // Reopen and handle recovery...
 *   }
 * }
 * ```
 */
export class StoreFatalError extends Error {
  constructor(
    message: string,
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = 'StoreFatalError';
    Object.setPrototypeOf(this, StoreFatalError.prototype);
  }
}
