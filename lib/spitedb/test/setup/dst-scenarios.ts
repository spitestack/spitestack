/**
 * DST scenario runners and utilities.
 *
 * Provides high-level abstractions for running deterministic
 * simulation tests with crash recovery, fault injection, and
 * timed operations.
 *
 * @example
 * ```ts
 * const ctx = createDSTContext(12345);
 * const runner = new DSTScenarioRunner(ctx);
 *
 * await runner.runWithCrashAt(
 *   async () => { ... }, // setup
 *   async () => { ... }, // operation that may crash
 *   () => operationCount > 5, // crash condition
 *   async () => { ... } // recovery and verification
 * );
 * ```
 */

import type { DSTContext, TestEnvironment } from './test-helpers';
import type { FaultScheduler } from './fault-scheduler';
import type { SimulatedFileSystem, FaultConfig } from '../../src/testing';
import type { SeededRandom } from './seeded-random';

/**
 * DST scenario runner for complex test scenarios.
 */
export class DSTScenarioRunner {
  constructor(private readonly ctx: DSTContext) {}

  /**
   * Run an operation with a crash at a specified condition.
   *
   * @param setup - Setup function to prepare state
   * @param operation - Operation that will be interrupted by crash
   * @param crashCondition - Function that returns true when crash should occur
   * @param recovery - Recovery function to run after crash
   */
  async runWithCrashAt(
    setup: () => Promise<void>,
    operation: () => Promise<void>,
    crashCondition: () => boolean,
    recovery: () => Promise<void>
  ): Promise<void> {
    // Run setup
    await setup();

    // Run operation with crash monitoring
    try {
      // In real DST, we'd interleave the operation with crash checks
      // For simplicity, we check the condition before operation
      if (crashCondition()) {
        this.ctx.fs.crash();
        throw new CrashSimulatedError('Crash condition met');
      }

      await operation();

      // Check again after operation
      if (crashCondition()) {
        this.ctx.fs.crash();
        throw new CrashSimulatedError('Crash condition met after operation');
      }
    } catch (error) {
      if (!(error instanceof CrashSimulatedError)) {
        throw error;
      }
    }

    // Run recovery
    await recovery();
  }

  /**
   * Run an operation with fault injection.
   *
   * @param fault - Fault configuration to inject
   * @param operation - Operation to run under fault
   * @param expectError - Optional: expected error type
   */
  async runWithFault<T>(
    fault: Partial<FaultConfig>,
    operation: () => Promise<T>,
    expectError?: new (...args: unknown[]) => Error
  ): Promise<T | undefined> {
    // Inject fault
    this.ctx.fs.injectFault(fault as FaultConfig);

    try {
      const result = await operation();
      this.ctx.fs.clearFaults();

      if (expectError) {
        throw new Error(`Expected error ${expectError.name} but operation succeeded`);
      }

      return result;
    } catch (error) {
      this.ctx.fs.clearFaults();

      if (expectError && error instanceof expectError) {
        return undefined;
      }

      throw error;
    }
  }

  /**
   * Run operations at specified times.
   *
   * @param operations - Array of timed operations
   */
  async runWithTimedOperations(
    operations: Array<{ time: number; op: () => Promise<void> }>
  ): Promise<void> {
    // Sort by time
    const sorted = [...operations].sort((a, b) => a.time - b.time);

    let currentTime = 0;

    for (const { time, op } of sorted) {
      // Advance clock to operation time
      if (time > currentTime) {
        this.ctx.clock.tick(time - currentTime);
        currentTime = time;
      }

      await op();
    }
  }

  /**
   * Run multiple operations in random order.
   *
   * @param operations - Operations to run
   */
  async runInRandomOrder(operations: Array<() => Promise<void>>): Promise<void> {
    const shuffled = this.ctx.random.shuffle([...operations]);
    for (const op of shuffled) {
      await op();
    }
  }

  /**
   * Get the context.
   */
  getContext(): DSTContext {
    return this.ctx;
  }
}

/**
 * Error thrown to simulate a crash.
 */
export class CrashSimulatedError extends Error {
  constructor(message: string = 'Crash simulated') {
    super(message);
    this.name = 'CrashSimulatedError';
  }
}

/**
 * Crash scenario definition.
 */
export interface CrashScenario {
  /** Scenario name */
  name: string;
  /** Crash point identifier */
  crashPoint: 'beforeFlush' | 'duringFlush' | 'afterFlush' | 'duringCheckpoint' | 'random';
  /** Setup function */
  setup: (ctx: DSTContext) => Promise<void>;
  /** Operation that may crash */
  operation: (ctx: DSTContext) => Promise<void>;
  /** Verification after recovery */
  verify: (ctx: DSTContext) => Promise<void>;
}

/**
 * Run a crash scenario.
 */
export async function runCrashScenario(
  ctx: DSTContext,
  scenario: CrashScenario
): Promise<void> {
  // Setup
  await scenario.setup(ctx);

  // Determine crash timing
  let crashed = false;

  switch (scenario.crashPoint) {
    case 'beforeFlush':
      ctx.fs.crash();
      crashed = true;
      break;

    case 'random':
      // Crash at random point during operation
      const crashAfterMs = ctx.random.int(0, 100);
      setTimeout(() => {
        if (!crashed) {
          ctx.fs.crash();
          crashed = true;
        }
      }, crashAfterMs);
      break;

    default:
      // Run operation normally
      try {
        await scenario.operation(ctx);
      } catch {
        // Expected during crash scenarios
      }
  }

  if (!crashed) {
    ctx.fs.crash();
  }

  // Verify recovery
  await scenario.verify(ctx);
}

/**
 * I/O failure scenario definition.
 */
export interface IOFailureScenario {
  /** Scenario name */
  name: string;
  /** Fault to inject */
  fault: Partial<FaultConfig>;
  /** Operation to run */
  operation: (ctx: DSTContext) => Promise<void>;
  /** Expected behavior: 'error' or 'recover' */
  expected: 'error' | 'recover';
  /** Optional: expected error type */
  expectedErrorType?: new (...args: unknown[]) => Error;
}

/**
 * Run an I/O failure scenario.
 */
export async function runIOFailureScenario(
  ctx: DSTContext,
  scenario: IOFailureScenario
): Promise<{ success: boolean; error?: Error }> {
  ctx.fs.injectFault(scenario.fault as FaultConfig);

  try {
    await scenario.operation(ctx);
    ctx.fs.clearFaults();

    if (scenario.expected === 'error') {
      return { success: false, error: new Error('Expected error but operation succeeded') };
    }

    return { success: true };
  } catch (error) {
    ctx.fs.clearFaults();

    if (scenario.expected === 'recover') {
      return { success: false, error: error as Error };
    }

    if (scenario.expectedErrorType && !(error instanceof scenario.expectedErrorType)) {
      return {
        success: false,
        error: new Error(
          `Expected ${scenario.expectedErrorType.name} but got ${(error as Error).name}`
        ),
      };
    }

    return { success: true };
  }
}

/**
 * Invariant checker for DST tests.
 */
export interface Invariant<T> {
  /** Invariant name */
  name: string;
  /** Check function returns true if invariant holds */
  check: (state: T) => boolean;
  /** Optional: message on failure */
  message?: (state: T) => string;
}

/**
 * Check invariants on a state.
 */
export function checkInvariants<T>(state: T, invariants: Invariant<T>[]): void {
  for (const invariant of invariants) {
    if (!invariant.check(state)) {
      const message = invariant.message
        ? invariant.message(state)
        : `Invariant "${invariant.name}" failed`;
      throw new InvariantViolationError(invariant.name, message);
    }
  }
}

/**
 * Error thrown when an invariant is violated.
 */
export class InvariantViolationError extends Error {
  constructor(
    public readonly invariantName: string,
    message: string
  ) {
    super(message);
    this.name = 'InvariantViolationError';
  }
}

/**
 * Common invariants for event stores.
 */
export const EventStoreInvariants = {
  /**
   * Global positions must be monotonically increasing.
   */
  monotonicPositions: {
    name: 'monotonic_positions',
    check: (events: Array<{ globalPosition: number }>) => {
      for (let i = 1; i < events.length; i++) {
        if (events[i]!.globalPosition <= events[i - 1]!.globalPosition) {
          return false;
        }
      }
      return true;
    },
    message: () => 'Global positions are not monotonically increasing',
  },

  /**
   * Stream revisions must be sequential.
   */
  sequentialRevisions: {
    name: 'sequential_revisions',
    check: (events: Array<{ streamId: string; revision: number }>) => {
      const byStream = new Map<string, number[]>();
      for (const event of events) {
        const revisions = byStream.get(event.streamId) ?? [];
        revisions.push(event.revision);
        byStream.set(event.streamId, revisions);
      }

      for (const revisions of byStream.values()) {
        for (let i = 1; i < revisions.length; i++) {
          if (revisions[i] !== revisions[i - 1]! + 1) {
            return false;
          }
        }
      }
      return true;
    },
    message: () => 'Stream revisions are not sequential',
  },

  /**
   * No duplicate global positions allowed.
   *
   * This invariant detects data corruption from retry bugs where
   * the same events are written multiple times.
   */
  noDuplicatePositions: {
    name: 'no_duplicate_positions',
    check: (events: Array<{ globalPosition: number }>) => {
      const seen = new Set<number>();
      for (const event of events) {
        if (seen.has(event.globalPosition)) {
          return false;
        }
        seen.add(event.globalPosition);
      }
      return true;
    },
    message: (events: Array<{ globalPosition: number }>) => {
      const counts = new Map<number, number>();
      for (const event of events) {
        counts.set(event.globalPosition, (counts.get(event.globalPosition) ?? 0) + 1);
      }
      const duplicates = [...counts.entries()].filter(([_, count]) => count > 1);
      return `Duplicate global positions found: ${duplicates.map(([pos, count]) => `${pos} (${count}x)`).join(', ')}`;
    },
  },
};

/**
 * Common invariants for projections.
 */
export const ProjectionInvariants = {
  /**
   * Checkpoint position never exceeds processed events.
   */
  checkpointNotAhead: {
    name: 'checkpoint_not_ahead',
    check: (state: { checkpointPosition: number; eventsProcessed: number }) => {
      return state.checkpointPosition <= state.eventsProcessed;
    },
    message: (state: { checkpointPosition: number; eventsProcessed: number }) =>
      `Checkpoint ${state.checkpointPosition} is ahead of processed events ${state.eventsProcessed}`,
  },
};
