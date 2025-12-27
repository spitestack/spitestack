import type { Clock, Timer } from '../interfaces/clock';

/**
 * Timer implementation wrapping Node.js timer handle.
 */
class BunTimer implements Timer {
  constructor(private readonly handle: ReturnType<typeof setTimeout>) {}

  cancel(): void {
    clearTimeout(this.handle);
  }
}

/**
 * Production clock implementation using real time.
 *
 * Uses Date.now() for timestamps and Bun.sleep() for async delays.
 *
 * @example
 * ```ts
 * const clock = new BunClock();
 * console.log(clock.now()); // Current timestamp
 * await clock.sleep(1000); // Wait 1 second
 * ```
 */
export class BunClock implements Clock {
  now(): number {
    return Date.now();
  }

  async sleep(ms: number): Promise<void> {
    return Bun.sleep(ms);
  }

  setTimeout(callback: () => void, ms: number): Timer {
    return new BunTimer(setTimeout(callback, ms));
  }

  setInterval(callback: () => void, ms: number): Timer {
    return new BunTimer(setInterval(callback, ms));
  }
}
