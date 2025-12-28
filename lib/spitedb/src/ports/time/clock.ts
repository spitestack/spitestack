/**
 * Handle to a scheduled timer that can be cancelled.
 */
export interface Timer {
  /** Cancel the timer */
  cancel(): void;
}

/**
 * Abstract clock interface for dependency injection.
 *
 * This abstraction allows us to:
 * - Use real time in production (BunClock)
 * - Use simulated time in tests (SimulatedClock)
 * - Control time advancement for deterministic tests
 *
 * @example
 * ```ts
 * // Production
 * const clock = new BunClock();
 *
 * // Testing
 * const clock = new SimulatedClock();
 * clock.tick(1000); // Advance 1 second
 * ```
 */
export interface Clock {
  /**
   * Returns current timestamp in milliseconds since Unix epoch.
   */
  now(): number;

  /**
   * Sleep for specified duration.
   * @param ms - Duration to sleep in milliseconds
   */
  sleep(ms: number): Promise<void>;

  /**
   * Schedule a callback to run after a delay.
   * @param callback - Function to call
   * @param ms - Delay in milliseconds
   * @returns Timer handle that can be cancelled
   */
  setTimeout(callback: () => void, ms: number): Timer;

  /**
   * Schedule a callback to run repeatedly at an interval.
   * @param callback - Function to call
   * @param ms - Interval in milliseconds
   * @returns Timer handle that can be cancelled
   */
  setInterval(callback: () => void, ms: number): Timer;
}
