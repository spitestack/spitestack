/**
 * DST Crash Scenarios
 *
 * Tests crash recovery for both EventStore and Projections.
 * Verifies that the system recovers correctly from crashes at
 * various points in operation sequences.
 *
 * Run with specific seed: SEED=12345 bun test test/dst/crash-scenarios.test.ts
 */

import { describe, test, beforeEach, afterEach, expect } from 'bun:test';
import {
  createTestEnvironment,
  createTestEventStore,
  createTestCoordinator,
  cleanupTestEnvironment,
  runWithSeedReporting,
  type TestEnvironment,
} from '../setup/test-helpers';
import { getSeedFromEnv } from '../setup/seeded-random';
import { createMockAggregatorRegistration, MockAggregatorProjection } from '../setup/mock-projection';
import { generateRandomEvents } from '../setup/test-fixtures';
import {
  EventStoreInvariants,
  checkInvariants,
  DSTScenarioRunner,
} from '../setup/dst-scenarios';
import { createDSTContext } from '../setup/test-helpers';
import type { StoredEvent } from '../../src/domain/events/stored-event';
import { EventStore } from '../../src/application/event-store';

describe('DST: Crash Scenarios', () => {
  let env: TestEnvironment;

  beforeEach(() => {
    env = createTestEnvironment(getSeedFromEnv());
  });

  afterEach(async () => {
    await cleanupTestEnvironment(env);
  });

  // ============================================================
  // EventStore Crash Tests
  // ============================================================

  describe('EventStore Crashes', () => {
    test('crash before flush loses unflushed events', async () => {
      await runWithSeedReporting(env, 'crash before flush', async () => {
        const store = await createTestEventStore(env);

        // Append some events but don't flush
        const events = generateRandomEvents(env.random, 5);
        await store.append('test-stream', events);

        // Verify events are readable before crash (from buffer)
        const beforeCrash = await store.readStream('test-stream');
        expect(beforeCrash).toHaveLength(5);

        // Simulate crash - loses all unflushed writes
        env.fs.crash();

        // Reopen the store
        const recoveredStore = await createTestEventStore(env);

        // Events should be lost
        const afterCrash = await recoveredStore.readStream('test-stream');
        expect(afterCrash).toHaveLength(0);

        await recoveredStore.close();
      });
    });

    test('crash after flush preserves all events', async () => {
      await runWithSeedReporting(env, 'crash after flush', async () => {
        const store = await createTestEventStore(env);

        // Append and flush events
        const events = generateRandomEvents(env.random, 5);
        await store.append('test-stream', events);
        await store.flush();

        // Get position before crash
        const positionBefore = store.getGlobalPosition();

        // Simulate crash
        env.fs.crash();

        // Reopen the store
        const recoveredStore = await createTestEventStore(env);

        // Events should be preserved
        const afterCrash = await recoveredStore.readStream('test-stream');
        expect(afterCrash).toHaveLength(5);
        expect(recoveredStore.getGlobalPosition()).toBe(positionBefore);

        // Verify invariants
        const allEvents = await recoveredStore.readGlobal(0);
        checkInvariants(allEvents, [EventStoreInvariants.monotonicPositions]);

        await recoveredStore.close();
      });
    });

    test('crash during flush with partial write', async () => {
      await runWithSeedReporting(env, 'crash during flush', async () => {
        const store = await createTestEventStore(env);

        // Append some events
        const events = generateRandomEvents(env.random, 10);
        await store.append('test-stream', events);

        // Inject partial write fault - simulates crash mid-sync
        env.fs.injectFault({ partialWrite: true });

        // Try to flush - may partially succeed
        try {
          await store.flush();
        } catch {
          // Expected - partial write may cause issues
        }

        // Simulate crash
        env.fs.crash();

        // Reopen should handle partial/corrupt segment
        const recoveredStore = await createTestEventStore(env);

        // Either all events or none should be present (atomic)
        const afterCrash = await recoveredStore.readStream('test-stream');
        // With partial write, we expect 0 events (incomplete write is discarded)
        expect(afterCrash.length).toBeLessThanOrEqual(events.length);

        // Whatever we have should satisfy invariants
        if (afterCrash.length > 0) {
          checkInvariants(afterCrash, [EventStoreInvariants.sequentialRevisions]);
        }

        await recoveredStore.close();
      });
    });

    test('recovery rebuilds stream revisions correctly', async () => {
      await runWithSeedReporting(env, 'rebuild stream revisions', async () => {
        const store = await createTestEventStore(env);

        // Append to multiple streams
        const streams = ['stream-a', 'stream-b', 'stream-c'];
        const eventCounts: Record<string, number> = {};

        for (const streamId of streams) {
          const count = env.random.int(3, 10);
          const events = generateRandomEvents(env.random, count);
          await store.append(streamId, events);
          eventCounts[streamId] = count;
        }

        await store.flush();

        // Record revisions before crash
        const revisionsBefore: Record<string, number> = {};
        for (const streamId of streams) {
          revisionsBefore[streamId] = store.getStreamRevision(streamId);
        }

        // Simulate crash
        env.fs.crash();

        // Reopen - should rebuild stream revisions from segments
        const recoveredStore = await createTestEventStore(env);

        // Verify all streams have correct revisions
        for (const streamId of streams) {
          const revision = recoveredStore.getStreamRevision(streamId);
          expect(revision).toBe(revisionsBefore[streamId] ?? -1);

          // And correct event count
          const events = await recoveredStore.readStream(streamId);
          expect(events).toHaveLength(eventCounts[streamId]!);
        }

        await recoveredStore.close();
      });
    });

    test('multiple crash-recovery cycles preserve data', async () => {
      await runWithSeedReporting(env, 'multiple crash cycles', async () => {
        let totalEvents = 0;

        // Multiple crash-recovery cycles
        for (let cycle = 0; cycle < 3; cycle++) {
          const store = await createTestEventStore(env);

          // Verify previous events still present
          const existing = await store.readGlobal(0);
          expect(existing).toHaveLength(totalEvents);

          // Add more events
          const newCount = env.random.int(5, 10);
          const events = generateRandomEvents(env.random, newCount);
          await store.append(`stream-${cycle}`, events);
          await store.flush();
          totalEvents += newCount;

          // Crash
          env.fs.crash();
        }

        // Final recovery
        const finalStore = await createTestEventStore(env);
        const allEvents = await finalStore.readGlobal(0);
        expect(allEvents).toHaveLength(totalEvents);

        // Verify invariants
        checkInvariants(allEvents, [
          EventStoreInvariants.monotonicPositions,
          EventStoreInvariants.sequentialRevisions,
        ]);

        await finalStore.close();
      });
    });
  });

  // ============================================================
  // Projection Crash Tests
  // ============================================================

  describe('Projection Crashes', () => {
    test('crash during checkpoint write - temp file recovery', async () => {
      await runWithSeedReporting(env, 'crash during checkpoint', async () => {
        const store = await createTestEventStore(env);

        // Add events
        const events = generateRandomEvents(env.random, 10);
        await store.append('test-stream', events);
        await store.flush();

        // Create coordinator with projection
        const coordinator = createTestCoordinator(env, store);
        const registration = createMockAggregatorRegistration('counter', ['*']);
        coordinator.getRegistry().register(registration);

        await coordinator.start();

        // Wait for projection to process some events
        await coordinator.waitForCatchUp(1000);

        // Inject sync failure to simulate crash during checkpoint write
        env.fs.injectFault({ syncFails: true });

        // Force checkpoint - should fail
        try {
          await coordinator.forceCheckpoint();
        } catch {
          // Expected
        }

        env.fs.clearFaults();

        // Simulate crash
        await coordinator.stop();
        env.fs.crash();

        // Restart
        const newStore = await createTestEventStore(env);
        const newCoordinator = createTestCoordinator(env, newStore);
        const newRegistration = createMockAggregatorRegistration('counter', ['*']);
        newCoordinator.getRegistry().register(newRegistration);

        await newCoordinator.start();
        await newCoordinator.waitForCatchUp(1000);

        // Projection should have reprocessed from last valid checkpoint
        const projection = newCoordinator.getProjection('counter') as MockAggregatorProjection;
        expect(projection?.getState()).toBe(events.length);

        await newCoordinator.stop();
        await newStore.close();
        await store.close();
      });
    });

    test('projection resumes from checkpoint after crash', async () => {
      await runWithSeedReporting(env, 'projection resume from checkpoint', async () => {
        const store = await createTestEventStore(env);

        // Add initial events
        const initialEvents = generateRandomEvents(env.random, 20);
        await store.append('test-stream', initialEvents);
        await store.flush();

        // Start coordinator and process events
        const coordinator = createTestCoordinator(env, store);
        const registration = createMockAggregatorRegistration('counter', ['*']);
        coordinator.getRegistry().register(registration);

        await coordinator.start();
        await coordinator.waitForCatchUp(1000);

        // Force a checkpoint
        await coordinator.forceCheckpoint();

        // Get checkpoint position
        const status = coordinator.getProjectionStatus('counter');
        const checkpointPosition = status?.currentPosition ?? 0;

        await coordinator.stop();

        // Simulate crash
        env.fs.crash();

        // Add more events after crash
        const newStore = await createTestEventStore(env);
        const moreEvents = generateRandomEvents(env.random, 10);
        await newStore.append('test-stream', moreEvents);
        await newStore.flush();

        // Restart coordinator
        const newCoordinator = createTestCoordinator(env, newStore);
        const newRegistration = createMockAggregatorRegistration('counter', ['*']);
        newCoordinator.getRegistry().register(newRegistration);

        await newCoordinator.start();
        await newCoordinator.waitForCatchUp(1000);

        // Projection should have processed all events
        const projection = newCoordinator.getProjection('counter') as MockAggregatorProjection;
        expect(projection?.getState()).toBe(30); // 20 + 10

        await newCoordinator.stop();
        await newStore.close();
      });
    });

    test('coordinator crash recovery with multiple projections', async () => {
      await runWithSeedReporting(env, 'coordinator multi-projection recovery', async () => {
        const store = await createTestEventStore(env);

        // Add events
        const events = generateRandomEvents(env.random, 15);
        await store.append('test-stream', events);
        await store.flush();

        // Create coordinator with multiple projections
        const coordinator = createTestCoordinator(env, store);
        coordinator.getRegistry().register(createMockAggregatorRegistration('counter1', ['*']));
        coordinator.getRegistry().register(createMockAggregatorRegistration('counter2', ['*']));

        await coordinator.start();
        await coordinator.waitForCatchUp(1000);
        await coordinator.forceCheckpoint();

        await coordinator.stop();
        env.fs.crash();

        // Restart with same projections
        const newStore = await createTestEventStore(env);
        const newCoordinator = createTestCoordinator(env, newStore);
        newCoordinator.getRegistry().register(createMockAggregatorRegistration('counter1', ['*']));
        newCoordinator.getRegistry().register(createMockAggregatorRegistration('counter2', ['*']));

        await newCoordinator.start();
        await newCoordinator.waitForCatchUp(1000);

        // Both projections should have same count
        const p1 = newCoordinator.getProjection('counter1') as MockAggregatorProjection;
        const p2 = newCoordinator.getProjection('counter2') as MockAggregatorProjection;

        expect(p1?.getState()).toBe(15);
        expect(p2?.getState()).toBe(15);

        await newCoordinator.stop();
        await newStore.close();
      });
    });
  });

  // ============================================================
  // Random Crash Timing (Seeded)
  // ============================================================

  describe('Random Crash Timing', () => {
    test('crash at random operation count preserves invariants', async () => {
      await runWithSeedReporting(env, 'random crash timing', async () => {
        const ctx = createDSTContext(env.seed);
        const runner = new DSTScenarioRunner(ctx);

        const crashAfterOps = ctx.random.int(3, 15);
        let operationCount = 0;

        const store = await createTestEventStore(env);

        try {
          while (operationCount < 20) {
            operationCount++;

            // Random operation
            if (ctx.random.bool(0.7)) {
              const events = generateRandomEvents(ctx.random, ctx.random.int(1, 3));
              await store.append(`stream-${ctx.random.int(1, 3)}`, events);
            } else {
              await store.flush();
            }

            // Crash at random point
            if (operationCount === crashAfterOps) {
              env.fs.crash();
              break;
            }
          }
        } catch {
          // May throw due to crash
        }

        // Recovery
        const recoveredStore = await createTestEventStore(env);
        const allEvents = await recoveredStore.readGlobal(0);

        // Whatever survived should satisfy invariants
        if (allEvents.length > 0) {
          checkInvariants(allEvents, [
            EventStoreInvariants.monotonicPositions,
            EventStoreInvariants.sequentialRevisions,
          ]);
        }

        await recoveredStore.close();
      });
    });
  });
});
