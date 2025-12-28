/**
 * Test environment helpers for DST testing.
 *
 * Provides factory functions to create fully configured test
 * environments with simulated filesystem, clock, and seeded randomness.
 *
 * @example
 * ```ts
 * const env = createTestEnvironment();
 * const store = await createTestEventStore(env);
 *
 * await store.append('stream-1', [{ type: 'Created', data: {} }]);
 *
 * // On failure, seed is printed for reproduction
 * ```
 */

import { SimulatedFileSystem, SimulatedClock } from '../../src/testing';
import { MsgpackSerializer } from '../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../src/infrastructure/serialization/zstd-compressor';
import { EventStore, type EventStoreConfig } from '../../src/application/event-store';
import { ProjectionCoordinator, type ProjectionCoordinatorConfig } from '../../src/application/projections';
import { SeededRandom, getSeedFromEnv } from './seeded-random';
import { FaultScheduler } from './fault-scheduler';

/**
 * Complete test environment with all dependencies.
 */
export interface TestEnvironment {
  /** Simulated clock for time control */
  clock: SimulatedClock;
  /** Simulated filesystem with fault injection */
  fs: SimulatedFileSystem;
  /** MessagePack serializer */
  serializer: MsgpackSerializer;
  /** Zstd compressor */
  compressor: ZstdCompressor;
  /** Seeded random generator */
  random: SeededRandom;
  /** Seed used (for reproduction) */
  seed: number;
  /** Optional fault scheduler */
  faultScheduler?: FaultScheduler;
}

/**
 * DST-aware test context with additional utilities.
 */
export interface DSTContext extends TestEnvironment {
  /** Fault scheduler for random fault injection */
  faultScheduler: FaultScheduler;
}

/**
 * Create a test environment with all dependencies.
 *
 * @param seed - Optional seed for reproducibility
 * @returns Fully configured test environment
 */
export function createTestEnvironment(seed?: number): TestEnvironment {
  const actualSeed = seed ?? getSeedFromEnv();
  const random = new SeededRandom(actualSeed);
  const clock = new SimulatedClock();
  const fs = new SimulatedFileSystem(clock);
  const serializer = new MsgpackSerializer();
  const compressor = new ZstdCompressor();

  return {
    clock,
    fs,
    serializer,
    compressor,
    random,
    seed: actualSeed,
  };
}

/**
 * Create a DST context with fault scheduler.
 *
 * @param seed - Optional seed for reproducibility
 * @returns DST context with all utilities
 */
export function createDSTContext(seed?: number): DSTContext {
  const env = createTestEnvironment(seed);
  const faultScheduler = new FaultScheduler(env.random, env.fs);

  return {
    ...env,
    faultScheduler,
  };
}

/**
 * Create an EventStore configured for testing.
 *
 * @param env - Test environment
 * @param dir - Optional data directory (default: '/test-data')
 * @returns Configured EventStore (not yet opened)
 */
export function createTestEventStoreConfig(
  env: TestEnvironment,
  dir: string = '/test-data'
): { store: EventStore; dir: string } {
  const config: EventStoreConfig = {
    fs: env.fs,
    clock: env.clock,
    serializer: env.serializer,
    compressor: env.compressor,
    maxSegmentSize: 1024 * 1024, // 1MB for faster tests
    indexCacheSize: 5,
    autoFlushCount: 10, // Flush more often in tests
  };

  const store = new EventStore(config);
  return { store, dir };
}

/**
 * Create and open an EventStore for testing.
 *
 * @param env - Test environment
 * @param dir - Optional data directory
 * @returns Opened EventStore ready for use
 */
export async function createTestEventStore(
  env: TestEnvironment,
  dir: string = '/test-data'
): Promise<EventStore> {
  const { store, dir: dataDir } = createTestEventStoreConfig(env, dir);
  await store.open(dataDir);
  return store;
}

/**
 * Create a ProjectionCoordinator configured for testing.
 *
 * @param env - Test environment
 * @param eventStore - EventStore to read from
 * @param dir - Optional data directory
 * @returns Configured ProjectionCoordinator
 */
export function createTestCoordinator(
  env: TestEnvironment,
  eventStore: EventStore,
  dir: string = '/test-projections'
): ProjectionCoordinator {
  const config: ProjectionCoordinatorConfig = {
    eventStore,
    fs: env.fs,
    serializer: env.serializer,
    clock: env.clock,
    dataDir: dir,
    pollingIntervalMs: 10, // Fast polling for tests
    defaultCheckpointIntervalMs: 100, // Quick checkpoints for tests
    checkpointJitterMs: 10,
    defaultBatchSize: 10,
  };

  return new ProjectionCoordinator(config);
}

/**
 * Run a test with seed reporting on failure.
 *
 * @param env - Test environment
 * @param testName - Name of the test (for error reporting)
 * @param fn - Test function to run
 */
export async function runWithSeedReporting<T>(
  env: TestEnvironment,
  testName: string,
  fn: () => Promise<T>
): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    console.error(`\n[DST] Test failed: ${testName}`);
    console.error(`[DST] Seed: ${env.seed}`);
    console.error(`[DST] To reproduce: SEED=${env.seed} bun test\n`);
    throw error;
  }
}

/**
 * Helper to advance clock and run timers.
 *
 * @param env - Test environment
 * @param ms - Milliseconds to advance
 */
export function advanceTime(env: TestEnvironment, ms: number): void {
  env.clock.tick(ms);
}

/**
 * Helper to advance clock and await all async callbacks.
 * Use this when timers have async callbacks (e.g., projection polling).
 *
 * @param env - Test environment
 * @param ms - Milliseconds to advance
 */
export async function advanceTimeAsync(env: TestEnvironment, ms: number): Promise<void> {
  await env.clock.tickAsync(ms);
}

/**
 * Helper to simulate a crash and recovery.
 *
 * @param env - Test environment
 */
export function simulateCrash(env: TestEnvironment): void {
  env.fs.crash();
}

/**
 * Clean up test environment.
 * Call in afterEach to ensure clean state.
 */
export async function cleanupTestEnvironment(env: TestEnvironment): Promise<void> {
  env.fs.clearFaults();
  // No need to clear filesystem - it's in-memory and will be GC'd
}

/**
 * Get the number of fuzz iterations based on environment.
 * - CI: 100 iterations
 * - FUZZ_ITERATIONS env var: specified number
 * - Default: 10 iterations
 */
export function getFuzzIterations(): number {
  const fuzzIterations = process.env['FUZZ_ITERATIONS'];
  if (fuzzIterations) {
    return parseInt(fuzzIterations, 10);
  }
  if (process.env['CI']) {
    return 100;
  }
  return 10;
}

/**
 * Generate a unique test directory path.
 *
 * @param env - Test environment
 * @param prefix - Optional prefix for the directory
 */
export function generateTestDir(env: TestEnvironment, prefix: string = 'test'): string {
  return `/${prefix}-${env.random.string(8)}`;
}
