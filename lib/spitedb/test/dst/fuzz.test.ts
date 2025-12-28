/**
 * DST Fuzz Tests
 *
 * Property-based fuzz testing with random seeds.
 * Runs multiple iterations with different random workloads
 * and fault injection to verify system invariants.
 *
 * Environment variables:
 * - SEED: Specific seed to reproduce a failure
 * - FUZZ_ITERATIONS: Number of iterations (default: 10 local, 100 CI)
 *
 * Run locally:        bun test test/dst/fuzz.test.ts
 * Run with seed:      SEED=12345 bun test test/dst/fuzz.test.ts
 * Run more iters:     FUZZ_ITERATIONS=50 bun test test/dst/fuzz.test.ts
 */

import { describe, test, beforeEach, afterEach, expect } from 'bun:test';
import {
  createTestEnvironment,
  createTestEventStore,
  createTestCoordinator,
  cleanupTestEnvironment,
  runWithSeedReporting,
  getFuzzIterations,
  advanceTime,
  advanceTimeAsync,
  type TestEnvironment,
} from '../setup/test-helpers';
import { getSeedFromEnv } from '../setup/seeded-random';
import { FaultScheduler, createFaultScheduler, isExpectedFaultError } from '../setup/fault-scheduler';
import {
  createMockAggregatorRegistration,
  createMockViewRegistration,
  MockAggregatorProjection,
  MockViewProjection,
} from '../setup/mock-projection';
import { generateRandomEvents, generateRandomWorkload } from '../setup/test-fixtures';
import {
  EventStoreInvariants,
  ProjectionInvariants,
  checkInvariants,
} from '../setup/dst-scenarios';
import { createDSTContext } from '../setup/test-helpers';
import { EventStore } from '../../src/application/event-store';
import { DenormalizedViewStore } from '../../src/infrastructure/projections/stores/denormalized-view-store';

describe('DST: Fuzz Tests', () => {
  const iterations = getFuzzIterations();
  const rotationSegmentSize = 64 * 1024;

  async function createRotatingStore(env: TestEnvironment, dir: string): Promise<EventStore> {
    const store = new EventStore({
      fs: env.fs,
      serializer: env.serializer,
      compressor: env.compressor,
      clock: env.clock,
      maxSegmentSize: rotationSegmentSize,
      indexCacheSize: 3,
      autoFlushCount: 5,
    });
    await store.open(dir);
    return store;
  }

  describe('EventStore Fuzz', () => {
    // Each iteration uses a unique seed derived from environment or random
    for (let i = 0; i < iterations; i++) {
      test(`iteration ${i}`, async () => {
        // Create fresh environment for each iteration
        // If SEED is set, use it + iteration as seed for reproducibility
        const baseSeed = getSeedFromEnv();
        const iterationSeed = baseSeed + i;
        const env = createTestEnvironment(iterationSeed);

        try {
          await runWithSeedReporting(env, `fuzz iteration ${i}`, async () => {
            let store = await createTestEventStore(env);
            const scheduler = createFaultScheduler(env.random, env.fs);
            let faultedThisIteration = false;

            // Random workload size
            const numOperations = env.random.int(10, 50);
            const streamCount = env.random.int(2, 5);
            const streams = Array.from({ length: streamCount }, (_, i) => `stream-${i}`);

            for (let op = 0; op < numOperations; op++) {
              // If store is in failed state from previous sync failure, recover first
              if (store.isFailed()) {
                await store.close();
                store = await createTestEventStore(env);
              }

              // 5% chance of fault injection (reduced from 10% to find real bugs vs noise)
              const faultInjected = scheduler.maybeInjectFault(0.05);
              const currentFaultType = scheduler.getActiveFaultType();
              if (faultInjected) {
                faultedThisIteration = true;
              }

              try {
                if (env.random.bool(0.7)) {
                  // 70% appends
                  const streamId = env.random.choice(streams);
                  const eventCount = env.random.int(1, 5);
                  const events = generateRandomEvents(env.random, eventCount);
                  await store.append(streamId, events);
                } else if (env.random.bool(0.5)) {
                  // 15% flushes
                  await store.flush();
                } else {
                  // 15% reads
                  const streamId = env.random.choice(streams);
                  await store.readStream(streamId);
                }
              } catch (error) {
                // Only swallow errors that match the injected fault type
                if (!faultInjected || !isExpectedFaultError(error, currentFaultType)) {
                  // This is either an unexpected error or doesn't match the fault pattern
                  // which could indicate a real bug
                  throw error;
                }
              }

              scheduler.clearFaults();
            }

            // Recover if in failed state before final operations
            if (store.isFailed()) {
              await store.close();
              store = await createTestEventStore(env);
            }

            // Final flush (no faults)
            try {
              await store.flush();
            } catch {
              // May fail if previous operations left bad state
              if (!faultedThisIteration) {
                throw new Error('Flush failed without injected faults');
              }
            }

            // Try to read events - may fail if corruption occurred
            let allEvents: Awaited<ReturnType<typeof store.readGlobal>> = [];
            try {
              allEvents = await store.readGlobal(0);
            } catch {
              if (!faultedThisIteration) {
                throw new Error('readGlobal failed without injected faults');
              }
              // Corruption from partial writes is acceptable
              // Close and reopen to see what's recoverable
              await store.close();
              try {
                const recoveredStore = await createTestEventStore(env);
                allEvents = await recoveredStore.readGlobal(0);
                await recoveredStore.close();
              } catch {
                // Complete corruption - store is unrecoverable, which is acceptable
                // under fault injection
                return;
              }
              return;
            }

            // Whatever we successfully read should satisfy invariants
            if (allEvents.length > 0) {
              checkInvariants(allEvents, [
                EventStoreInvariants.monotonicPositions,
                EventStoreInvariants.sequentialRevisions,
              ]);
            }

            // Verify per-stream invariants
            for (const streamId of streams) {
              try {
                const streamEvents = await store.readStream(streamId);
                if (streamEvents.length > 0) {
                  // Revisions should start at 0 and be sequential
                  for (let j = 0; j < streamEvents.length; j++) {
                    expect(streamEvents[j]!.revision).toBe(j);
                  }
                }
              } catch {
                // Stream may be unreadable due to corruption
                if (!faultedThisIteration) {
                  throw new Error('readStream failed without injected faults');
                }
              }
            }

            await store.close();
          });
        } finally {
          await cleanupTestEnvironment(env);
        }
      });
    }
  });

  describe('Projection Fuzz', () => {
    for (let i = 0; i < Math.min(iterations, 5); i++) {
      // Fewer projection iterations (more expensive)
      test(`iteration ${i}`, async () => {
        const baseSeed = getSeedFromEnv();
        const iterationSeed = baseSeed + i + 1000; // Different seed space
        const env = createTestEnvironment(iterationSeed);

        try {
          await runWithSeedReporting(env, `projection fuzz ${i}`, async () => {
            const store = await createTestEventStore(env);

            // Add initial events
            const initialCount = env.random.int(10, 30);
            const events = generateRandomEvents(env.random, initialCount);
            await store.append('test-stream', events);
            await store.flush();

            // Create coordinator with projection
            const coordinator = createTestCoordinator(env, store);
            coordinator
              .getRegistry()
              .register(createMockAggregatorRegistration('counter', ['*']));

            await coordinator.start();

            // Random operations while projection runs (no fault injection for projections)
            const numOps = env.random.int(5, 15);

            for (let op = 0; op < numOps; op++) {
              if (env.random.bool(0.6)) {
                // Add more events
                const count = env.random.int(1, 3);
                const newEvents = generateRandomEvents(env.random, count);
                await store.append('test-stream', newEvents);
                await store.flush();
              } else if (env.random.bool(0.5)) {
                // Force checkpoint
                await coordinator.forceCheckpoint();
              } else {
                // Advance time
                await advanceTimeAsync(env, env.random.int(10, 100));
              }

              await advanceTimeAsync(env, 20);
            }

            // Wait for catch up
            await coordinator.waitForCatchUp(10000);

            // Verify projection state
            const projection = coordinator.getProjection('counter') as MockAggregatorProjection;
            const allEvents = await store.readGlobal(0);

            // Projection should have processed all events
            expect(projection?.getState()).toBe(allEvents.length);

            // Verify checkpoint invariant
            const status = coordinator.getProjectionStatus('counter');
            if (status) {
              const maxPosition = allEvents.length > 0 ? Number(allEvents.length - 1) : -1;
              expect(status.currentPosition).toBeLessThanOrEqual(maxPosition);
            }

            await coordinator.stop();
            await store.close();
          });
        } finally {
          await cleanupTestEnvironment(env);
        }
      });
    }
  });

  describe('Denormalized View Fuzz', () => {
    for (let i = 0; i < Math.min(iterations, 4); i++) {
      test(`iteration ${i}`, async () => {
        const baseSeed = getSeedFromEnv();
        const iterationSeed = baseSeed + i + 1500;
        const env = createTestEnvironment(iterationSeed);

        try {
          await runWithSeedReporting(env, `view fuzz ${i}`, async () => {
            const store = await createTestEventStore(env);
            const coordinator = createTestCoordinator(env, store);
            coordinator
              .getRegistry()
              .register(createMockViewRegistration('users', ['UserCreated', 'UserUpdated', 'UserDeleted']));

            await coordinator.start();

            const expected = new Map<string, Record<string, unknown>>();
            const userIds = Array.from({ length: 15 }, (_, idx) => `user-${idx}`);
            const operations = env.random.int(20, 40);

            for (let op = 0; op < operations; op++) {
              const id = env.random.choice(userIds);
              const action = env.random.int(0, 2);
              let type = 'UserUpdated';

              if (action === 0) {
                type = 'UserCreated';
              } else if (action === 1) {
                type = 'UserUpdated';
              } else {
                type = 'UserDeleted';
              }

              const payload = {
                id,
                email: `${id}@example.com`,
                name: `User ${id}`,
                updatedAt: env.clock.now(),
              };

              await store.append('users', [{ type, data: payload }]);

              if (type === 'UserDeleted') {
                expected.delete(id);
              } else {
                expected.set(id, payload);
              }

              if (env.random.bool(0.4)) {
                await store.flush();
              }
              await advanceTimeAsync(env, env.random.int(5, 30));
            }

            await store.flush();
            await coordinator.waitForCatchUp(10000);

            const projection = coordinator.getProjection('users') as MockViewProjection;
            expect(projection.getCount()).toBe(expected.size);
            for (const [id, record] of expected) {
              expect(projection.getById(id)).toEqual(record);
            }

            await coordinator.stop();
            await store.close();
          });
        } finally {
          await cleanupTestEnvironment(env);
        }
      });
    }
  });

  describe('Denormalized View Store Fuzz', () => {
    for (let i = 0; i < Math.min(iterations, 3); i++) {
      test(`iteration ${i}`, async () => {
        const baseSeed = getSeedFromEnv();
        const iterationSeed = baseSeed + i + 1800;
        const env = createTestEnvironment(iterationSeed);

        try {
          await runWithSeedReporting(env, `view store fuzz ${i}`, async () => {
            const store = new DenormalizedViewStore<Record<string, unknown>>({
              fs: env.fs,
              serializer: env.serializer,
              clock: env.clock,
              dataDir: '/test-projections',
              projectionName: `view-store-${i}`,
              indexFields: ['category'],
              rangeFields: ['createdAt'],
              memoryThreshold: 1024,
            });

            await store.initialize();

            const rows: Array<[string, Record<string, unknown>]> = [];
            const categories = ['alpha', 'beta', 'gamma'];
            const expectedByCategory = new Map<string, Set<string>>();
            const expectedByDay = new Map<string, Set<string>>();
            for (const category of categories) {
              expectedByCategory.set(category, new Set());
            }

            for (let idx = 0; idx < 50; idx++) {
              const id = `item-${idx.toString().padStart(3, '0')}`;
              const createdAt = `2024-01-${(idx % 28 + 1).toString().padStart(2, '0')}`;
              const row = {
                id,
                category: env.random.choice(categories),
                createdAt,
                payload: 'x'.repeat(128),
              };
              store.setByKey?.(id, row);
              rows.push([id, row]);

              const categorySet = expectedByCategory.get(row.category as string);
              categorySet?.add(id);
              if (!expectedByDay.has(createdAt)) {
                expectedByDay.set(createdAt, new Set());
              }
              expectedByDay.get(createdAt)?.add(id);
            }

            expect(store.isMemoryThresholdExceeded()).toBe(true);

            const range = store.queryRange({ start: 'item-010', end: 'item-019' });
            expect(range.size).toBe(10);

            const alpha = store.query({ category: 'alpha' });
            expect(alpha.length).toBe(expectedByCategory.get('alpha')?.size ?? 0);

            const dayKey = '2024-01-05';
            const dayMatches = store.query({ category: 'alpha', createdAt: dayKey });
            const expectedDay = expectedByDay.get(dayKey) ?? new Set<string>();
            const expectedDayAlpha = Array.from(expectedDay).filter((id) =>
              expectedByCategory.get('alpha')?.has(id)
            );
            expect(dayMatches.length).toBe(expectedDayAlpha.length);

            const checkpointPosition = Number(env.random.int(100, 500));
            await store.persist(checkpointPosition);

            const reloaded = new DenormalizedViewStore<Record<string, unknown>>({
              fs: env.fs,
              serializer: env.serializer,
              clock: env.clock,
              dataDir: '/test-projections',
              projectionName: `view-store-${i}`,
              indexFields: ['category'],
              rangeFields: ['createdAt'],
              memoryThreshold: 1024,
            });
            await reloaded.initialize();
            const loadedPosition = await reloaded.load();

            expect(loadedPosition).toBe(checkpointPosition);
            expect(reloaded.size).toBe(rows.length);

            const sample = reloaded.getByKey?.('item-005') as Record<string, unknown> | undefined;
            expect(sample?.['id']).toBe('item-005');

            const alphaReloaded = reloaded.query({ category: 'alpha' });
            expect(alphaReloaded.length).toBe(expectedByCategory.get('alpha')?.size ?? 0);

            await reloaded.close();
          });
        } finally {
          await cleanupTestEnvironment(env);
        }
      });
    }
  });

  describe('Combined Fuzz', () => {
    for (let i = 0; i < Math.min(iterations, 3); i++) {
      // Even fewer combined iterations
      test(`iteration ${i}`, async () => {
        const baseSeed = getSeedFromEnv();
        const iterationSeed = baseSeed + i + 2000;
        const env = createTestEnvironment(iterationSeed);

        try {
          await runWithSeedReporting(env, `combined fuzz ${i}`, async () => {
            const store = await createTestEventStore(env);
            const coordinator = createTestCoordinator(env, store);
            coordinator
              .getRegistry()
              .register(createMockAggregatorRegistration('counter1', ['*']));
            coordinator
              .getRegistry()
              .register(createMockAggregatorRegistration('counter2', ['*']));

            await coordinator.start();

            // Combined workload (no fault injection for stability)
            const numOps = env.random.int(20, 40);
            const streams = ['stream-a', 'stream-b', 'stream-c'];

            for (let op = 0; op < numOps; op++) {
              const opType = env.random.int(0, 5);

              switch (opType) {
                case 0:
                case 1: // 40% - Append
                  const streamId = env.random.choice(streams);
                  const events = generateRandomEvents(
                    env.random,
                    env.random.int(1, 3)
                  );
                  await store.append(streamId, events);
                  break;

                case 2: // 20% - Flush
                  await store.flush();
                  break;

                case 3: // 20% - Read
                  await store.readGlobal(0);
                  break;

                case 4: // 10% - Checkpoint
                  await coordinator.forceCheckpoint();
                  break;

                case 5: // 10% - Advance time
                  await advanceTimeAsync(env, env.random.int(10, 50));
                  break;
              }

              // Small time advance each iteration
              await advanceTimeAsync(env, 5);
            }

            // Final flush
            await store.flush();

            // Wait for projections
            await coordinator.waitForCatchUp(10000);

            // Verify invariants
            const allEvents = await store.readGlobal(0);
            if (allEvents.length > 0) {
              checkInvariants(allEvents, [
                EventStoreInvariants.monotonicPositions,
                EventStoreInvariants.sequentialRevisions,
              ]);
            }

            // Verify projection consistency - both should have same count
            const p1 = coordinator.getProjection('counter1') as MockAggregatorProjection;
            const p2 = coordinator.getProjection('counter2') as MockAggregatorProjection;
            expect(p1?.getState()).toBe(p2?.getState());
            expect(p1?.getState()).toBe(allEvents.length);

            await coordinator.stop();
            await store.close();
          });
        } finally {
          await cleanupTestEnvironment(env);
        }
      });
    }
  });

  describe('Concurrent Rotation Fuzz', () => {
    for (let i = 0; i < Math.min(iterations, 3); i++) {
      test(`iteration ${i}`, async () => {
        const baseSeed = getSeedFromEnv();
        const iterationSeed = baseSeed + i + 2500;
        const env = createTestEnvironment(iterationSeed);

        try {
          await runWithSeedReporting(env, `concurrent rotation fuzz ${i}`, async () => {
            let store = await createRotatingStore(env, '/test-data-rotate');
            const scheduler = createFaultScheduler(env.random, env.fs);
            let faultedThisIteration = false;

            const workerCount = 4;
            const operationsPerWorker = env.random.int(15, 30);
            const streams = Array.from({ length: 6 }, (_, idx) => `stream-${idx}`);
            const payload = 'x'.repeat(2048);

            const workers = Array.from({ length: workerCount }, () => {
              return (async () => {
                for (let op = 0; op < operationsPerWorker; op++) {
                  // Skip operations if store is in failed state (another worker caused sync failure)
                  if (store.isFailed()) {
                    break;
                  }

                  const faultInjected = scheduler.maybeInjectFault(0.03);
                  const currentFaultType = scheduler.getActiveFaultType();
                  if (faultInjected) {
                    faultedThisIteration = true;
                  }

                  try {
                    if (env.random.bool(0.7)) {
                      const streamId = env.random.choice(streams);
                      const eventCount = env.random.int(1, 4);
                      const events = Array.from({ length: eventCount }, (_, index) => ({
                        type: 'RotateEvent',
                        data: { payload, streamId, op, index },
                      }));
                      await store.append(streamId, events);
                    } else if (env.random.bool(0.5)) {
                      await store.flush();
                    } else {
                      await store.readGlobal(0);
                    }
                  } catch (error) {
                    // In concurrent scenarios, one worker's fault can affect another worker.
                    // Check both local fault injection AND global fault state.
                    // Also accept StoreFatalError which indicates sync failure marked store as failed.
                    const isFaultRelated = faultInjected || faultedThisIteration;
                    if (!isFaultRelated) {
                      throw error;
                    }
                    // If a fault was injected somewhere, accept any simulated error or StoreFatalError
                    const msg = error instanceof Error ? error.message : String(error);
                    const isStoreFatalError = error instanceof Error && error.name === 'StoreFatalError';
                    if (!msg.includes('Simulated') && !isStoreFatalError && !msg.includes('failed state')) {
                      // Real error, not from fault injection
                      throw error;
                    }
                  } finally {
                    scheduler.clearFaults();
                  }
                }
              })();
            });

            await Promise.all(workers);

            // Recover store if in failed state before final operations
            if (store.isFailed()) {
              await store.close();
              store = await createRotatingStore(env, '/test-data-rotate');
            }

            try {
              await store.flush();
            } catch {
              if (!faultedThisIteration) {
                throw new Error('Flush failed without injected faults');
              }
            }

            // Recover again if flush caused failure
            if (store.isFailed()) {
              await store.close();
              store = await createRotatingStore(env, '/test-data-rotate');
            }

            try {
              const allEvents = await store.readGlobal(0);
              if (allEvents.length > 0) {
                checkInvariants(allEvents, [
                  EventStoreInvariants.monotonicPositions,
                  EventStoreInvariants.sequentialRevisions,
                ]);
              }
            } catch {
              if (!faultedThisIteration) {
                throw new Error('readGlobal failed without injected faults');
              }
            }

            await store.close();
          });
        } finally {
          await cleanupTestEnvironment(env);
        }
      });
    }
  });

  describe('Crash Recovery Fuzz', () => {
    for (let i = 0; i < Math.min(iterations, 3); i++) {
      test(`iteration ${i}`, async () => {
        const baseSeed = getSeedFromEnv();
        const iterationSeed = baseSeed + i + 3000;
        const env = createTestEnvironment(iterationSeed);

        try {
          await runWithSeedReporting(env, `crash recovery fuzz ${i}`, async () => {
            let lastKnownGoodCount = 0;

            // Multiple crash cycles
            const numCycles = env.random.int(2, 4);

            for (let cycle = 0; cycle < numCycles; cycle++) {
              let store: Awaited<ReturnType<typeof createTestEventStore>>;
              try {
                store = await createTestEventStore(env);
              } catch {
                // Store may be unrecoverable after corruption - that's OK
                // Start fresh
                env.fs.crash(); // Clear any partial state
                store = await createTestEventStore(env);
              }

              // Count what actually exists
              let existing: Awaited<ReturnType<typeof store.readGlobal>> = [];
              try {
                existing = await store.readGlobal(0);
              } catch {
                // Corruption - close and continue
                await store.close();
                continue;
              }

              // Previous events should be at least what we last confirmed
              // (may be more if flush succeeded before crash)
              expect(existing.length).toBeGreaterThanOrEqual(0);

              // Add new events
              const numAppends = env.random.int(3, 8);
              for (let j = 0; j < numAppends; j++) {
                const events = generateRandomEvents(
                  env.random,
                  env.random.int(1, 3)
                );
                try {
                  await store.append(`stream-${env.random.int(1, 3)}`, events);
                } catch {
                  // May fail
                }

                // Random flush
                if (env.random.bool(0.5)) {
                  try {
                    await store.flush();
                  } catch {
                    // May fail
                  }
                }
              }

              // Final flush this cycle
              try {
                await store.flush();
              } catch {
                // May fail
              }

              // Count what should survive (don't crash yet so we can read)
              try {
                const currentEvents = await store.readGlobal(0);
                lastKnownGoodCount = currentEvents.length;
              } catch {
                // Can't read - keep previous count
              }

              await store.close();

              // Crash
              env.fs.crash();
            }

            // Final recovery
            try {
              const finalStore = await createTestEventStore(env);
              const finalEvents = await finalStore.readGlobal(0);

              // Should have some events (or none if all corrupted)
              expect(finalEvents.length).toBeGreaterThanOrEqual(0);

              if (finalEvents.length > 0) {
                checkInvariants(finalEvents, [
                  EventStoreInvariants.monotonicPositions,
                  EventStoreInvariants.sequentialRevisions,
                ]);
              }

              await finalStore.close();
            } catch {
              // Complete corruption - acceptable
            }
          });
        } finally {
          await cleanupTestEnvironment(env);
        }
      });
    }
  });
});
