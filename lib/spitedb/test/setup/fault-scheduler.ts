/**
 * Randomized fault injection scheduler for DST.
 *
 * Coordinates with SeededRandom to inject faults at deterministic
 * but random times, enabling comprehensive failure testing.
 *
 * @example
 * ```ts
 * const random = new SeededRandom(12345);
 * const fs = new SimulatedFileSystem();
 * const scheduler = new FaultScheduler(random, fs);
 *
 * // 10% chance of fault on each operation
 * for (let i = 0; i < 100; i++) {
 *   scheduler.maybeInjectFault(0.1);
 *   await doOperation();
 *   scheduler.clearFaults();
 * }
 * ```
 */

import type { SeededRandom } from './seeded-random';
import type { SimulatedFileSystem, FaultConfig } from '../../src/testing';

/**
 * Configuration for intermittent failures.
 */
export interface IntermittentConfig {
  /** Probability of failure on each operation (0-1) */
  failProbability: number;
  /** Probability of recovery after failure (0-1) */
  recoveryProbability: number;
  /** Types of faults to inject */
  faultTypes: ('sync' | 'read' | 'write' | 'partial')[];
}

/**
 * Tracks operation counts for conditional fault injection.
 */
interface OperationCounter {
  read: number;
  write: number;
  sync: number;
}

/**
 * Schedules faults based on seeded random decisions.
 */
export type FaultType = 'sync' | 'read' | 'write' | 'partial';

export class FaultScheduler {
  private operationCount: OperationCounter = { read: 0, write: 0, sync: 0 };
  private scheduledFaults: Map<string, number> = new Map();
  private inFailureState = false;
  private intermittentConfig: IntermittentConfig | null = null;
  private activeFaultType: FaultType | null = null;

  constructor(
    private readonly random: SeededRandom,
    private readonly fs: SimulatedFileSystem
  ) {}

  /**
   * Maybe inject a fault based on probability.
   *
   * @param probability - Probability of fault injection (0-1)
   * @returns true if fault was injected
   */
  maybeInjectFault(probability: number): boolean {
    if (this.random.bool(probability)) {
      const faultType = this.random.choice(['sync', 'read', 'write', 'partial'] as const);
      this.injectFault(faultType);
      return true;
    }
    return false;
  }

  /**
   * Inject a specific fault type.
   */
  injectFault(type: FaultType): void {
    const config: FaultConfig = {};
    this.activeFaultType = type;

    switch (type) {
      case 'sync':
        config.syncFails = true;
        break;
      case 'read':
        config.readFails = true;
        break;
      case 'write':
        config.writeFails = true;
        break;
      case 'partial':
        config.partialWrite = true;
        break;
    }

    this.fs.injectFault(config);
  }

  /**
   * Get the currently active fault type.
   */
  getActiveFaultType(): FaultType | null {
    return this.activeFaultType;
  }

  /**
   * Clear all injected faults.
   */
  clearFaults(): void {
    this.fs.clearFaults();
    this.inFailureState = false;
    this.activeFaultType = null;
  }

  /**
   * Schedule a fault to occur on the Nth operation of a type.
   *
   * @param op - Operation type
   * @param n - Trigger on Nth operation (1-based)
   */
  injectOnNthOperation(op: 'read' | 'write' | 'sync', n: number): void {
    this.scheduledFaults.set(op, n);
  }

  /**
   * Track an operation and check if scheduled fault should trigger.
   *
   * @param op - Operation type that occurred
   * @returns true if fault was triggered
   */
  trackOperation(op: 'read' | 'write' | 'sync'): boolean {
    this.operationCount[op]++;

    const scheduledN = this.scheduledFaults.get(op);
    if (scheduledN !== undefined && this.operationCount[op] === scheduledN) {
      this.scheduledFaults.delete(op);
      this.injectFault(op);
      return true;
    }

    return false;
  }

  /**
   * Schedule a random crash after between min and max operations.
   *
   * @param options - Min and max operation counts
   * @returns The operation number when crash will occur
   */
  scheduleRandomCrash(options: { min: number; max: number }): number {
    const crashAt = this.random.int(options.min, options.max);
    // Store crash point for later checking
    return crashAt;
  }

  /**
   * Start intermittent failure mode.
   *
   * In this mode, faults come and go randomly based on config.
   */
  startIntermittentFailures(config: IntermittentConfig): void {
    this.intermittentConfig = config;
    this.inFailureState = false;
  }

  /**
   * Stop intermittent failure mode.
   */
  stopIntermittentFailures(): void {
    this.intermittentConfig = null;
    this.clearFaults();
  }

  /**
   * Tick the intermittent failure state machine.
   * Call this before each operation.
   *
   * @returns true if currently in failure state
   */
  tickIntermittent(): boolean {
    if (!this.intermittentConfig) {
      return false;
    }

    const config = this.intermittentConfig;

    if (this.inFailureState) {
      // Maybe recover
      if (this.random.bool(config.recoveryProbability)) {
        this.clearFaults();
        this.inFailureState = false;
      }
    } else {
      // Maybe fail
      if (this.random.bool(config.failProbability)) {
        const faultType = this.random.choice(config.faultTypes);
        this.injectFault(faultType);
        this.inFailureState = true;
      }
    }

    return this.inFailureState;
  }

  /**
   * Inject partial write on next write operation.
   */
  injectPartialWriteOnNext(): void {
    this.fs.injectFault({ partialWrite: true });
  }

  /**
   * Inject sync delay.
   *
   * @param ms - Delay in milliseconds
   */
  injectSyncDelay(ms: number): void {
    this.fs.injectFault({ syncDelayMs: ms });
  }

  /**
   * Get current operation counts.
   */
  getOperationCounts(): Readonly<OperationCounter> {
    return { ...this.operationCount };
  }

  /**
   * Reset operation counts.
   */
  resetOperationCounts(): void {
    this.operationCount = { read: 0, write: 0, sync: 0 };
  }

  /**
   * Check if currently in failure state (for intermittent mode).
   */
  isInFailureState(): boolean {
    return this.inFailureState;
  }

  /**
   * Generate a random fault injection plan for N operations.
   *
   * @param totalOps - Total number of operations
   * @param faultProbability - Probability of fault per operation
   * @returns Array of operation indices where faults should occur
   */
  generateFaultPlan(totalOps: number, faultProbability: number): number[] {
    const faultPoints: number[] = [];

    for (let i = 0; i < totalOps; i++) {
      if (this.random.bool(faultProbability)) {
        faultPoints.push(i);
      }
    }

    return faultPoints;
  }
}

/**
 * Create a fault scheduler with common DST setup.
 */
export function createFaultScheduler(
  random: SeededRandom,
  fs: SimulatedFileSystem
): FaultScheduler {
  return new FaultScheduler(random, fs);
}

/**
 * Check if an error is expected based on the active fault type.
 *
 * This helps distinguish between:
 * - Expected errors from injected faults (should be swallowed)
 * - Unexpected errors that indicate real bugs (should be thrown)
 *
 * @param error - The error that was thrown
 * @param faultType - The type of fault that was injected (or null)
 * @returns true if the error matches the expected fault pattern
 */
export function isExpectedFaultError(error: unknown, faultType: FaultType | null): boolean {
  if (faultType === null) {
    return false; // No fault was injected, error is unexpected
  }

  const message = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();

  switch (faultType) {
    case 'sync':
      // StoreFatalError is thrown after sync failure - includes messages like
      // "Sync failed after write" or "failed state"
      return (
        message.includes('sync') ||
        message.includes('fsync') ||
        message.includes('failed state') ||
        (error instanceof Error && error.name === 'StoreFatalError')
      );
    case 'read':
      return message.includes('read') || message.includes('eio') || message.includes('input');
    case 'write':
      return message.includes('write') || message.includes('enospc') || message.includes('output');
    case 'partial':
      // Partial writes might cause CRC errors, corruption errors, or truncation errors
      return (
        message.includes('crc') ||
        message.includes('corrupt') ||
        message.includes('truncat') ||
        message.includes('invalid') ||
        message.includes('incomplete')
      );
    default:
      return false;
  }
}
