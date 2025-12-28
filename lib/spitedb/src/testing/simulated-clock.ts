import type { Clock, Timer } from '../ports/time/clock';

/**
 * Internal state for a scheduled task.
 */
interface ScheduledTask {
  /** Scheduled execution time (simulated) */
  time: number;
  /** Callback to execute (may be async) */
  callback: () => void | Promise<void>;
  /** If set, this is an interval timer */
  interval?: number;
  /** If true, timer has been cancelled */
  cancelled: boolean;
}

/**
 * Timer implementation for simulated clock.
 */
class SimulatedTimer implements Timer {
  constructor(private readonly task: ScheduledTask) {}

  cancel(): void {
    this.task.cancelled = true;
  }
}

/**
 * Simulated clock for deterministic testing.
 *
 * This implementation provides:
 * - Controllable time advancement
 * - Deterministic timer execution
 * - Inspection APIs for time-based assertions
 *
 * @example
 * ```ts
 * const clock = new SimulatedClock();
 *
 * // Schedule something
 * clock.setTimeout(() => console.log('fired!'), 1000);
 *
 * // Nothing happens yet
 * console.log(clock.now()); // 0
 *
 * // Advance time
 * clock.tick(1000);
 * // Output: fired!
 * console.log(clock.now()); // 1000
 * ```
 */
export class SimulatedClock implements Clock {
  /** Current simulated time in milliseconds */
  private currentTime = 0;

  /** Scheduled tasks (sorted by time) */
  private tasks: ScheduledTask[] = [];

  /** Pending sleep resolvers */
  private sleepResolvers: Array<{ time: number; resolve: () => void }> = [];

  /** Pending promises from async callbacks */
  private pendingAsyncCallbacks: Promise<void>[] = [];

  now(): number {
    return this.currentTime;
  }

  sleep(ms: number): Promise<void> {
    if (ms <= 0) {
      return Promise.resolve();
    }

    return new Promise((resolve) => {
      this.sleepResolvers.push({
        time: this.currentTime + ms,
        resolve,
      });
      // Keep sorted by time
      this.sleepResolvers.sort((a, b) => a.time - b.time);
    });
  }

  setTimeout(callback: () => void, ms: number): Timer {
    const task: ScheduledTask = {
      time: this.currentTime + ms,
      callback,
      cancelled: false,
    };

    this.tasks.push(task);
    this.tasks.sort((a, b) => a.time - b.time);

    return new SimulatedTimer(task);
  }

  setInterval(callback: () => void, ms: number): Timer {
    const task: ScheduledTask = {
      time: this.currentTime + ms,
      callback,
      interval: ms,
      cancelled: false,
    };

    this.tasks.push(task);
    this.tasks.sort((a, b) => a.time - b.time);

    return new SimulatedTimer(task);
  }

  // ============================================================
  // Test Control API
  // ============================================================

  /**
   * Advance time by specified milliseconds, executing any scheduled tasks.
   * @param ms - Duration to advance in milliseconds
   */
  tick(ms: number): void {
    if (ms < 0) {
      throw new Error('Cannot tick negative time');
    }
    const targetTime = this.currentTime + ms;
    this.advanceTo(targetTime);
  }

  /**
   * Advance time to a specific timestamp, executing any scheduled tasks.
   * @param targetTime - Target timestamp in milliseconds
   */
  advanceTo(targetTime: number): void {
    if (targetTime < this.currentTime) {
      throw new Error('Cannot go back in time');
    }

    while (this.currentTime < targetTime) {
      // Find the next event (task or sleep resolver)
      const nextTaskTime = this.getNextTaskTime();
      const nextSleepTime = this.sleepResolvers[0]?.time ?? Infinity;
      const nextTime = Math.min(nextTaskTime, nextSleepTime, targetTime);

      // Advance to next event
      this.currentTime = nextTime;

      // Resolve any sleeps that are due
      this.resolveSleeps();

      // Execute any tasks that are due
      this.executeTasks();
    }
  }

  /**
   * Run all pending timers immediately.
   * Useful for draining async operations in tests.
   */
  runAllTimers(): void {
    const maxIterations = 10000; // Prevent infinite loops
    let iterations = 0;

    while (iterations < maxIterations) {
      const nextTaskTime = this.getNextTaskTime();
      const nextSleepTime = this.sleepResolvers[0]?.time ?? Infinity;
      const nextTime = Math.min(nextTaskTime, nextSleepTime);

      if (nextTime === Infinity) {
        break;
      }

      this.advanceTo(nextTime);
      iterations++;
    }

    if (iterations >= maxIterations) {
      throw new Error('runAllTimers: exceeded maximum iterations - possible infinite loop');
    }
  }

  /**
   * Run only pending timers (no intervals).
   * Advances to the time of the last pending timer.
   */
  runPendingTimers(): void {
    const pendingTasks = this.tasks.filter((t) => !t.cancelled && !t.interval);
    const pendingSleeps = this.sleepResolvers;

    if (pendingTasks.length === 0 && pendingSleeps.length === 0) {
      return;
    }

    const maxTaskTime = Math.max(...pendingTasks.map((t) => t.time), 0);
    const maxSleepTime = Math.max(...pendingSleeps.map((s) => s.time), 0);
    const targetTime = Math.max(maxTaskTime, maxSleepTime);

    if (targetTime > this.currentTime) {
      this.advanceTo(targetTime);
    }
  }

  /**
   * Advance time and await all async callbacks.
   * Use this when timers may have async callbacks that need to complete.
   * @param ms - Duration to advance in milliseconds
   */
  async tickAsync(ms: number): Promise<void> {
    this.tick(ms);
    await this.flushAsyncCallbacks();
  }

  /**
   * Await all pending async callbacks without advancing time.
   * Call this after tick() to ensure async callbacks have completed.
   */
  async flushAsyncCallbacks(): Promise<void> {
    while (this.pendingAsyncCallbacks.length > 0) {
      const pending = [...this.pendingAsyncCallbacks];
      this.pendingAsyncCallbacks = [];
      await Promise.all(pending);
    }
  }

  /**
   * Get the current simulated time.
   */
  getCurrentTime(): number {
    return this.currentTime;
  }

  /**
   * Get the number of pending timers.
   */
  getPendingTimerCount(): number {
    return this.tasks.filter((t) => !t.cancelled).length;
  }

  /**
   * Get the number of pending sleep resolvers.
   */
  getPendingSleepCount(): number {
    return this.sleepResolvers.length;
  }

  /**
   * Reset the clock to initial state.
   */
  reset(): void {
    this.currentTime = 0;
    this.tasks = [];
    this.sleepResolvers = [];
    this.pendingAsyncCallbacks = [];
  }

  /**
   * Set the current time without executing timers.
   * Useful for test setup.
   */
  setTime(time: number): void {
    if (time < 0) {
      throw new Error('Time cannot be negative');
    }
    this.currentTime = time;
  }

  // ============================================================
  // Private Helpers
  // ============================================================

  private getNextTaskTime(): number {
    for (const task of this.tasks) {
      if (!task.cancelled) {
        return task.time;
      }
    }
    return Infinity;
  }

  private resolveSleeps(): void {
    while (
      this.sleepResolvers.length > 0 &&
      this.sleepResolvers[0]!.time <= this.currentTime
    ) {
      const resolver = this.sleepResolvers.shift()!;
      resolver.resolve();
    }
  }

  private executeTasks(): void {
    while (this.tasks.length > 0) {
      const task = this.tasks[0]!;

      // Skip cancelled tasks
      if (task.cancelled) {
        this.tasks.shift();
        continue;
      }

      // Stop if task is in the future
      if (task.time > this.currentTime) {
        break;
      }

      // Remove task from queue
      this.tasks.shift();

      // Execute callback and capture promise if async
      const result = task.callback();
      if (result instanceof Promise) {
        this.pendingAsyncCallbacks.push(result);
      }

      // Reschedule interval tasks
      if (task.interval && !task.cancelled) {
        task.time = this.currentTime + task.interval;
        this.tasks.push(task);
        this.tasks.sort((a, b) => a.time - b.time);
      }
    }
  }
}
