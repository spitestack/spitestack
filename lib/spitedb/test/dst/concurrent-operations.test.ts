/**
 * DST Concurrent Operations
 *
 * Tests concurrent operation ordering and consistency:
 * - Stream contention (multiple appends to same stream)
 * - Projection coordination (appends during processing)
 * - Random operation ordering
 *
 * Run with specific seed: SEED=12345 bun test test/dst/concurrent-operations.test.ts
 */

import { describe, test, beforeEach, afterEach, expect } from 'bun:test';
import {
  createTestEnvironment,
  createTestEventStore,
  createTestCoordinator,
  cleanupTestEnvironment,
  runWithSeedReporting,
  advanceTime,
  advanceTimeAsync,
  type TestEnvironment,
} from '../setup/test-helpers';
import { getSeedFromEnv } from '../setup/seeded-random';
import { createMockAggregatorRegistration, MockAggregatorProjection } from '../setup/mock-projection';
import { generateRandomEvents, generateRandomWorkload, type WorkloadOperation } from '../setup/test-fixtures';
import {
  EventStoreInvariants,
  checkInvariants,
  DSTScenarioRunner,
} from '../setup/dst-scenarios';
import { createDSTContext } from '../setup/test-helpers';

describe('DST: Concurrent Operations', () => {
  let env: TestEnvironment;

  beforeEach(() => {
    env = createTestEnvironment(getSeedFromEnv());
  });

  afterEach(async () => {
    await cleanupTestEnvironment(env);
  });

  // ============================================================
  // Stream Contention Tests
  // ============================================================

  describe('Stream Contention', () => {
    test('sequential appends to same stream increment revisions correctly', async () => {
      await runWithSeedReporting(env, 'sequential appends same stream', async () => {
        const store = await createTestEventStore(env);

        // Multiple sequential appends to same stream
        const streamId = 'contention-stream';
        let expectedRevision = -1;

        for (let i = 0; i < 10; i++) {
          const events = generateRandomEvents(env.random, env.random.int(1, 3));
          const result = await store.append(streamId, events);

          // Revision should increment correctly
          expect(result.streamRevision).toBe(expectedRevision + events.length);
          expectedRevision = result.streamRevision;
        }

        await store.flush();

        // Verify all events have sequential revisions
        const allEvents = await store.readStream(streamId);
        for (let i = 0; i < allEvents.length; i++) {
          expect(allEvents[i]!.revision).toBe(i);
        }

        await store.close();
      });
    });

    test('concurrent reads during writes see consistent state', async () => {
      await runWithSeedReporting(env, 'concurrent reads during writes', async () => {
        const store = await createTestEventStore(env);

        // Initial events
        const initialEvents = generateRandomEvents(env.random, 5);
        await store.append('test-stream', initialEvents);
        await store.flush();

        // Start adding more events
        const moreEvents = generateRandomEvents(env.random, 5);
        await store.append('test-stream', moreEvents);
        // Don't flush yet

        // Read should see all events (flushed + buffered)
        const duringWrite = await store.readStream('test-stream');
        expect(duringWrite).toHaveLength(10);

        // Invariants should hold
        checkInvariants(duringWrite, [EventStoreInvariants.sequentialRevisions]);

        await store.flush();
        await store.close();
      });
    });

    test('expectedRevision enforces optimistic concurrency', async () => {
      await runWithSeedReporting(env, 'expectedRevision conflicts', async () => {
        const store = await createTestEventStore(env);

        // Initial append
        const events1 = generateRandomEvents(env.random, 3);
        await store.append('test-stream', events1);

        // Second append with correct expected revision
        const events2 = generateRandomEvents(env.random, 2);
        await store.append('test-stream', events2, { expectedRevision: 2 });

        // Third append with wrong expected revision should fail
        const events3 = generateRandomEvents(env.random, 1);
        await expect(
          store.append('test-stream', events3, { expectedRevision: 0 })
        ).rejects.toThrow();

        // Correct expected revision should work
        await store.append('test-stream', events3, { expectedRevision: 4 });

        const all = await store.readStream('test-stream');
        expect(all).toHaveLength(6);

        await store.close();
      });
    });

    test('appends to different streams are independent', async () => {
      await runWithSeedReporting(env, 'independent stream appends', async () => {
        const store = await createTestEventStore(env);

        const streams = ['stream-a', 'stream-b', 'stream-c'];
        const eventCounts = new Map<string, number>();

        // Interleaved appends to different streams
        for (let i = 0; i < 15; i++) {
          const streamId = env.random.choice(streams);
          const count = env.random.int(1, 3);
          const events = generateRandomEvents(env.random, count);

          await store.append(streamId, events);
          eventCounts.set(streamId, (eventCounts.get(streamId) ?? 0) + count);
        }

        await store.flush();

        // Each stream should have correct count and sequential revisions
        for (const [streamId, expectedCount] of eventCounts) {
          const events = await store.readStream(streamId);
          expect(events).toHaveLength(expectedCount);
          checkInvariants(events, [EventStoreInvariants.sequentialRevisions]);
        }

        await store.close();
      });
    });
  });

  // ============================================================
  // Projection Coordination Tests
  // ============================================================

  describe('Projection Coordination', () => {
    test('appends during projection processing are caught up', async () => {
      await runWithSeedReporting(env, 'append during projection processing', async () => {
        const store = await createTestEventStore(env);

        // Initial events
        const initialEvents = generateRandomEvents(env.random, 10);
        await store.append('test-stream', initialEvents);
        await store.flush();

        // Start coordinator
        const coordinator = createTestCoordinator(env, store);
        coordinator.getRegistry().register(createMockAggregatorRegistration('counter', ['*']));
        await coordinator.start();

        // Add more events while projection is processing
        for (let i = 0; i < 5; i++) {
          const events = generateRandomEvents(env.random, 2);
          await store.append('test-stream', events);
          await store.flush();

          // Advance time to allow polling (must use async to flush poll callbacks)
          await advanceTimeAsync(env, 20);
        }

        // Wait for all events to be processed
        await coordinator.waitForCatchUp(5000);

        const projection = coordinator.getProjection('counter') as MockAggregatorProjection;
        expect(projection?.getState()).toBe(20); // 10 + (5 * 2)

        await coordinator.stop();
        await store.close();
      });
    });

    test('multiple projections process same events', async () => {
      await runWithSeedReporting(env, 'multiple projections same events', async () => {
        const store = await createTestEventStore(env);

        // Add events
        const events = generateRandomEvents(env.random, 15);
        await store.append('test-stream', events);
        await store.flush();

        // Create coordinator with multiple projections
        const coordinator = createTestCoordinator(env, store);
        coordinator.getRegistry().register(createMockAggregatorRegistration('counter1', ['*']));
        coordinator.getRegistry().register(createMockAggregatorRegistration('counter2', ['*']));
        coordinator.getRegistry().register(createMockAggregatorRegistration('counter3', ['*']));

        await coordinator.start();
        await coordinator.waitForCatchUp(5000);

        // All projections should have same count
        const p1 = coordinator.getProjection('counter1') as MockAggregatorProjection;
        const p2 = coordinator.getProjection('counter2') as MockAggregatorProjection;
        const p3 = coordinator.getProjection('counter3') as MockAggregatorProjection;

        expect(p1?.getState()).toBe(15);
        expect(p2?.getState()).toBe(15);
        expect(p3?.getState()).toBe(15);

        await coordinator.stop();
        await store.close();
      });
    });

    test('waitForCatchUp with ongoing appends eventually succeeds', async () => {
      await runWithSeedReporting(env, 'waitForCatchUp with ongoing appends', async () => {
        const store = await createTestEventStore(env);

        // Start coordinator
        const coordinator = createTestCoordinator(env, store);
        coordinator.getRegistry().register(createMockAggregatorRegistration('counter', ['*']));
        await coordinator.start();

        // Concurrent append loop
        let appendDone = false;
        const appendLoop = (async () => {
          for (let i = 0; i < 10; i++) {
            const events = generateRandomEvents(env.random, 1);
            await store.append('test-stream', events);
            await store.flush();
            advanceTime(env, 10);
          }
          appendDone = true;
        })();

        // Wait for catch up - should eventually succeed after appends stop
        let caughtUp = false;
        while (!caughtUp && !appendDone) {
          try {
            await coordinator.waitForCatchUp(100);
            caughtUp = true;
          } catch {
            // Timeout, try again
          }
          advanceTime(env, 50);
        }

        await appendLoop;

        // Final catch up
        await coordinator.waitForCatchUp(5000);

        const projection = coordinator.getProjection('counter') as MockAggregatorProjection;
        expect(projection?.getState()).toBe(10);

        await coordinator.stop();
        await store.close();
      });
    });

    test('forceCheckpoint during processing', async () => {
      await runWithSeedReporting(env, 'forceCheckpoint during processing', async () => {
        const store = await createTestEventStore(env);

        // Add events
        const events = generateRandomEvents(env.random, 20);
        await store.append('test-stream', events);
        await store.flush();

        // Start coordinator
        const coordinator = createTestCoordinator(env, store);
        coordinator.getRegistry().register(createMockAggregatorRegistration('counter', ['*']));
        await coordinator.start();

        // Force checkpoint periodically during processing
        for (let i = 0; i < 5; i++) {
          advanceTime(env, 50);
          await coordinator.forceCheckpoint();
        }

        await coordinator.waitForCatchUp(5000);

        // Verify final state
        const projection = coordinator.getProjection('counter') as MockAggregatorProjection;
        expect(projection?.getState()).toBe(20);

        await coordinator.stop();
        await store.close();
      });
    });
  });

  // ============================================================
  // Random Ordering Tests
  // ============================================================

  describe('Random Ordering', () => {
    test('shuffled operation sequence preserves invariants', async () => {
      await runWithSeedReporting(env, 'shuffled operation sequence', async () => {
        const ctx = createDSTContext(env.seed);
        const runner = new DSTScenarioRunner(ctx);
        const store = await createTestEventStore(env);

        // Create operations
        const operations: Array<() => Promise<void>> = [];

        // Add appends
        for (let i = 0; i < 10; i++) {
          const streamId = `stream-${ctx.random.int(1, 3)}`;
          const events = generateRandomEvents(ctx.random, ctx.random.int(1, 3));
          operations.push(async () => {
            await store.append(streamId, events);
          });
        }

        // Add reads
        for (let i = 0; i < 5; i++) {
          const streamId = `stream-${ctx.random.int(1, 3)}`;
          operations.push(async () => {
            await store.readStream(streamId);
          });
        }

        // Add flushes
        for (let i = 0; i < 3; i++) {
          operations.push(async () => {
            await store.flush();
          });
        }

        // Run in random order
        await runner.runInRandomOrder(operations);

        // Final flush to persist
        await store.flush();

        // Verify invariants
        const allEvents = await store.readGlobal(0);
        checkInvariants(allEvents, [
          EventStoreInvariants.monotonicPositions,
          EventStoreInvariants.sequentialRevisions,
        ]);

        await store.close();
      });
    });

    test('timed operation interleaving', async () => {
      await runWithSeedReporting(env, 'timed operation interleaving', async () => {
        const ctx = createDSTContext(env.seed);
        const runner = new DSTScenarioRunner(ctx);
        const store = await createTestEventStore(env);

        // Create timed operations
        const operations: Array<{ time: number; op: () => Promise<void> }> = [];

        // Appends at various times
        for (let i = 0; i < 10; i++) {
          const time = ctx.random.int(0, 1000);
          const events = generateRandomEvents(ctx.random, 1);
          operations.push({
            time,
            op: async () => {
              await store.append(`stream-${i % 3}`, events);
            },
          });
        }

        // Flushes at various times
        operations.push({ time: 200, op: () => store.flush() });
        operations.push({ time: 500, op: () => store.flush() });
        operations.push({ time: 900, op: () => store.flush() });

        // Run with timed scheduling
        await runner.runWithTimedOperations(operations);

        // Final flush
        await store.flush();

        // Verify invariants
        const allEvents = await store.readGlobal(0);
        checkInvariants(allEvents, [
          EventStoreInvariants.monotonicPositions,
          EventStoreInvariants.sequentialRevisions,
        ]);

        await store.close();
      });
    });

    test('random workload maintains consistency', async () => {
      await runWithSeedReporting(env, 'random workload consistency', async () => {
        const store = await createTestEventStore(env);

        // Generate random workload
        const workload = generateRandomWorkload(env.random, 30, {
          appendProbability: 0.7,
          streamCount: 4,
          eventsPerAppend: { min: 1, max: 5 },
        });

        // Execute workload
        for (const op of workload) {
          switch (op.type) {
            case 'append':
              if (op.streamId && op.events) {
                await store.append(op.streamId, op.events);
              }
              break;
            case 'read':
              if (op.streamId) {
                await store.readStream(op.streamId);
              }
              break;
            case 'readGlobal':
              await store.readGlobal(op.fromPosition ?? 0);
              break;
          }

          // Random flush
          if (env.random.bool(0.2)) {
            await store.flush();
          }
        }

        // Final flush
        await store.flush();

        // Verify invariants
        const allEvents = await store.readGlobal(0);
        expect(allEvents.length).toBeGreaterThan(0);
        checkInvariants(allEvents, [
          EventStoreInvariants.monotonicPositions,
          EventStoreInvariants.sequentialRevisions,
        ]);

        await store.close();
      });
    });

    test('mixed EventStore and Projection operations', async () => {
      await runWithSeedReporting(env, 'mixed EventStore and Projection ops', async () => {
        const store = await createTestEventStore(env);
        const coordinator = createTestCoordinator(env, store);
        coordinator.getRegistry().register(createMockAggregatorRegistration('counter', ['*']));

        await coordinator.start();

        // Interleave appends, reads, and projection operations
        for (let i = 0; i < 15; i++) {
          const opType = env.random.int(0, 3);

          switch (opType) {
            case 0: // Append
              const events = generateRandomEvents(env.random, env.random.int(1, 3));
              await store.append(`stream-${env.random.int(1, 3)}`, events);
              await store.flush();
              break;

            case 1: // Read
              await store.readGlobal(0);
              break;

            case 2: // Advance time (for projection polling)
              advanceTime(env, env.random.int(10, 100));
              break;

            case 3: // Force checkpoint
              await coordinator.forceCheckpoint();
              break;
          }
        }

        // Wait for projection to catch up
        await coordinator.waitForCatchUp(5000);

        // Verify projection processed all events
        const projection = coordinator.getProjection('counter') as MockAggregatorProjection;
        const allEvents = await store.readGlobal(0);

        expect(projection?.getState()).toBe(allEvents.length);

        await coordinator.stop();
        await store.close();
      });
    });
  });
});
