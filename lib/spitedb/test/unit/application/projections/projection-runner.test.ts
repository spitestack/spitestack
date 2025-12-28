/**
 * Unit tests for ProjectionRunner.
 *
 * Tests the projection runner with:
 * - Lifecycle (start/stop)
 * - Event processing and filtering
 * - Checkpointing with jitter
 * - waitForCatchUp behavior
 * - Error handling
 * - DST scenarios
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { ProjectionRunner, type ProjectionRunnerConfig } from '../../../../src/application/projections/projection-runner';
import { AggregatorStore } from '../../../../src/infrastructure/projections/stores/aggregator-store';
import { createTestEnvironment, createTestEventStore, type TestEnvironment } from '../../../setup/test-helpers';
import { MockAggregatorProjection, TimingRecordingProjection } from '../../../setup/mock-projection';
import { FaultScheduler } from '../../../setup/fault-scheduler';
import { SeededRandom } from '../../../setup/seeded-random';
import type { EventStore } from '../../../../src/application/event-store';
import type { Projection, ProjectionMetadata } from '../../../../src/ports/projections';
import type { ProjectionStore } from '../../../../src/ports/projections/projection-store';
import type { StoredEvent } from '../../../../src/domain/events/stored-event';

describe('ProjectionRunner', () => {
  let env: TestEnvironment;
  let eventStore: EventStore;
  let runner: ProjectionRunner;
  let projection: MockAggregatorProjection;
  let store: AggregatorStore<number>;
  const dataDir = '/test-runner';
  const projectionName = 'TestRunner';

  const createMetadata = (
    name: string = projectionName,
    subscribedEvents: string[] = ['*']
  ): ProjectionMetadata => ({
    name,
    kind: 'aggregator',
    subscribedEvents,
    accessPatterns: [],
  });

  const createRunner = (
    config: Partial<ProjectionRunnerConfig> = {}
  ): ProjectionRunner => {
    const metadata = createMetadata(config.metadata?.name, config.metadata?.subscribedEvents);
    return new ProjectionRunner({
      eventStore,
      projection: config.projection ?? projection,
      store: config.store ?? store,
      metadata,
      clock: env.clock,
      pollingIntervalMs: config.pollingIntervalMs ?? 10,
      checkpointIntervalMs: config.checkpointIntervalMs ?? 100,
      jitterMs: config.jitterMs ?? 10,
      eventFilter: config.eventFilter,
      batchSize: config.batchSize ?? 10,
    });
  };

  const appendEvent = async (
    type: string,
    data: Record<string, unknown> = {},
    streamId: string = 'test-stream'
  ): Promise<void> => {
    await eventStore.append(streamId, [{ type, data }]);
    await eventStore.flush();
  };

  /**
   * Helper to advance time and await all async callbacks.
   * Uses SimulatedClock's tickAsync which properly handles async callbacks.
   */
  const advanceTimeAndSettle = async (ms: number): Promise<void> => {
    await env.clock.tickAsync(ms);
  };

  beforeEach(async () => {
    env = createTestEnvironment(12345);
    eventStore = await createTestEventStore(env, '/test-events');

    projection = new MockAggregatorProjection(['*']);

    store = new AggregatorStore({
      fs: env.fs,
      serializer: env.serializer,
      clock: env.clock,
      dataDir,
      projectionName,
    }, 0);
    await store.initialize();

    runner = createRunner();
  });

  afterEach(async () => {
    if (runner.getStatus().running) {
      await runner.stop();
    }
    await eventStore.close();
  });

  describe('start', () => {
    test('should start polling', async () => {
      await runner.start();

      expect(runner.getStatus().running).toBe(true);
    });

    test('should load checkpoint on start', async () => {
      // Persist a checkpoint at position 5
      store.set(5);
      await store.persist(5);

      runner = createRunner();
      await runner.start();

      expect(runner.getCurrentPosition()).toBe(5);
    });

    test('should be idempotent', async () => {
      await runner.start();
      await runner.start(); // Second call should be no-op

      expect(runner.getStatus().running).toBe(true);
    });

    test('should begin processing events after start', async () => {
      await appendEvent('TestEvent', { id: 1 });
      await appendEvent('TestEvent', { id: 2 });

      await runner.start();

      // Advance clock through multiple poll cycles to ensure processing
      // pollingIntervalMs=10, so advance in steps to trigger polls
      for (let i = 0; i < 5; i++) {
        await advanceTimeAndSettle(15);
      }

      // Eventually processes events
      expect(projection.getProcessedCount()).toBeGreaterThanOrEqual(1);
    });
  });

  describe('stop', () => {
    test('should stop polling', async () => {
      await runner.start();
      await runner.stop();

      expect(runner.getStatus().running).toBe(false);
    });

    test('should persist final checkpoint on stop', async () => {
      await appendEvent('TestEvent', { id: 1 });

      await runner.start();

      // Process events - need multiple poll cycles
      for (let i = 0; i < 5; i++) {
        await advanceTimeAndSettle(15);
      }

      const positionBeforeStop = runner.getCurrentPosition();
      await runner.stop();

      // Create new store and verify checkpoint
      const newStore = new AggregatorStore({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
      }, 0);
      const position = await newStore.load();

      expect(position).toBe(positionBeforeStop);
    });

    test('should be idempotent', async () => {
      await runner.start();
      await runner.stop();
      await runner.stop(); // Second call should be no-op

      expect(runner.getStatus().running).toBe(false);
    });

    test('should cancel pending timers', async () => {
      await runner.start();
      await runner.stop();

      // Advance time - should not process any events
      const countBefore = projection.getProcessedCount();
      env.clock.tick(1000);

      expect(projection.getProcessedCount()).toBe(countBefore);
    });
  });

  describe('event processing', () => {
    test('should process all subscribed events', async () => {
      await appendEvent('EventA', { id: 1 });
      await appendEvent('EventB', { id: 2 });
      await appendEvent('EventC', { id: 3 });

      await runner.start();

      // Process events - need multiple poll cycles
      for (let i = 0; i < 5; i++) {
        await advanceTimeAndSettle(15);
      }

      expect(projection.getProcessedCount()).toBe(3);
    });

    test('should filter events by subscribedEvents', async () => {
      const filteredProjection = new MockAggregatorProjection(['EventA']);
      runner = createRunner({
        projection: filteredProjection,
        metadata: createMetadata('FilteredRunner', ['EventA']),
      });

      await appendEvent('EventA', { id: 1 });
      await appendEvent('EventB', { id: 2 }); // Should be skipped
      await appendEvent('EventA', { id: 3 });

      await runner.start();

      // Need multiple poll cycles
      for (let i = 0; i < 5; i++) {
        await advanceTimeAndSettle(15);
      }

      expect(filteredProjection.getProcessedCount()).toBe(2);
    });

    test('should apply custom eventFilter', async () => {
      const eventFilter = (event: StoredEvent) => event.data != null && typeof event.data === 'object' && 'important' in event.data && (event.data as Record<string, unknown>)['important'] === true;

      runner = createRunner({ eventFilter });

      await appendEvent('TestEvent', { important: true });
      await appendEvent('TestEvent', { important: false }); // Should be skipped
      await appendEvent('TestEvent', { important: true });

      await runner.start();

      // Need multiple poll cycles
      for (let i = 0; i < 5; i++) {
        await advanceTimeAndSettle(15);
      }

      expect(projection.getProcessedCount()).toBe(2);
    });

    test('should apply incremental store updates when available', async () => {
      class IncrementalStore implements ProjectionStore<Map<string, number>> {
        private rows = new Map<string, number>();
        setCalls = 0;
        setByKeyCalls = 0;

        async initialize(): Promise<void> {
          return undefined;
        }

        get(): Map<string, number> {
          return new Map(this.rows);
        }

        set(state: Map<string, number>): void {
          this.setCalls += 1;
          this.rows = new Map(state);
        }

        setByKey(key: string, value: number): void {
          this.setByKeyCalls += 1;
          this.rows.set(key, value);
        }

        deleteByKey(key: string): boolean {
          return this.rows.delete(key);
        }

        async persist(): Promise<void> {
          return undefined;
        }

        async load(): Promise<number | null> {
          return null;
        }

        async close(): Promise<void> {
          return undefined;
        }

        getMemoryUsage(): number {
          return 0;
        }
      }

      class IncrementalProjection implements Projection<Map<string, number>> {
        private state = new Map<string, number>();

        build(event: StoredEvent): void {
          if (event.type !== 'Set') return;
          const data = event.data as { key: string; value: number };
          this.state.set(data.key, data.value);
        }

        applyToStore(event: StoredEvent, store: ProjectionStore<Map<string, number>>): void {
          if (event.type !== 'Set') return;
          const data = event.data as { key: string; value: number };
          store.setByKey?.(data.key, data.value);
        }

        getState(): Map<string, number> {
          return new Map(this.state);
        }

        setState(state: Map<string, number>): void {
          this.state = new Map(state);
        }

        reset(): void {
          this.state.clear();
        }
      }

      const store = new IncrementalStore();
      await store.initialize();
      const projectionInstance = new IncrementalProjection();

      runner = new ProjectionRunner({
        eventStore,
        projection: projectionInstance,
        store,
        metadata: createMetadata('IncrementalRunner', ['Set']),
        clock: env.clock,
        pollingIntervalMs: 10,
        checkpointIntervalMs: 1000,
        jitterMs: 0,
        batchSize: 10,
      });

      await eventStore.append('test-stream', [
        { type: 'Set', data: { key: 'alpha', value: 42 } },
      ]);
      await eventStore.flush();

      await runner.start();

      for (let i = 0; i < 5; i++) {
        await advanceTimeAndSettle(15);
      }

      expect(store.setByKeyCalls).toBeGreaterThan(0);
      expect(store.setCalls).toBe(0);
      expect(store.get().get('alpha')).toBe(42);
    });

    test('should update currentPosition as events are processed', async () => {
      await appendEvent('TestEvent', { id: 1 });
      await appendEvent('TestEvent', { id: 2 });

      await runner.start();

      // Need multiple poll cycles
      for (let i = 0; i < 5; i++) {
        await advanceTimeAndSettle(15);
      }

      const globalPosition = eventStore.getGlobalPosition();
      expect(runner.getCurrentPosition()).toBeGreaterThanOrEqual(globalPosition - 1);
    });

    test('should drain multiple batches in a single poll', async () => {
      // Add many events
      for (let i = 0; i < 25; i++) {
        await appendEvent('TestEvent', { id: i });
      }

      runner = createRunner({ batchSize: 5 });
      await runner.start();

      // One poll cycle should drain multiple batches.
      await advanceTimeAndSettle(15);

      expect(projection.getProcessedCount()).toBe(25);
    });

    test('should increment eventsProcessed counter', async () => {
      await appendEvent('TestEvent', { id: 1 });
      await appendEvent('TestEvent', { id: 2 });

      await runner.start();
      await advanceTimeAndSettle(50);

      expect(runner.getStatus().eventsProcessed).toBeGreaterThanOrEqual(2);
    });
  });

  describe('checkpointing', () => {
    test('should checkpoint at configured interval', async () => {
      runner = createRunner({ checkpointIntervalMs: 50, jitterMs: 0 });

      await appendEvent('TestEvent', { id: 1 });

      await runner.start();
      await advanceTimeAndSettle(20);

      const positionBeforeCheckpoint = runner.getCurrentPosition();

      // Advance past checkpoint interval
      await advanceTimeAndSettle(60);

      expect(runner.getStatus().lastCheckpointPosition).toBe(positionBeforeCheckpoint);
    });

    test('should apply jitter to checkpoint timing', async () => {
      const checkpointTimes: number[] = [];

      // Run multiple cycles and record checkpoint times
      for (let i = 0; i < 5; i++) {
        const testEnv = createTestEnvironment(i * 12345);
        const testStore = await createTestEventStore(testEnv, `/test-events-${i}`);
        const testProjection = new MockAggregatorProjection(['*']);
        const testStoreState = new AggregatorStore({
          fs: testEnv.fs,
          serializer: testEnv.serializer,
          clock: testEnv.clock,
          dataDir: `/test-runner-${i}`,
          projectionName: `TestRunner${i}`,
        }, 0);
        await testStoreState.initialize();

        const testRunner = new ProjectionRunner({
          eventStore: testStore,
          projection: testProjection,
          store: testStoreState,
          metadata: createMetadata(`TestRunner${i}`),
          clock: testEnv.clock,
          pollingIntervalMs: 10,
          checkpointIntervalMs: 100,
          jitterMs: 50, // High jitter for observable variation
        });

        await testStore.append('stream', [{ type: 'Test', data: {} }]);
        await testStore.flush();

        await testRunner.start();
        checkpointTimes.push(testRunner.getStatus().nextCheckpointTime);
        await testRunner.stop();
        await testStore.close();
      }

      // With jitter, checkpoint times should vary
      const uniqueTimes = new Set(checkpointTimes);
      expect(uniqueTimes.size).toBeGreaterThan(1);
    });

    test('should skip checkpoint if no events processed', async () => {
      await runner.start();

      const initialLastCheckpoint = runner.getStatus().lastCheckpointTime;

      // Advance past checkpoint interval without any events
      await advanceTimeAndSettle(200);

      // Last checkpoint time should not have changed significantly
      // (no actual checkpoint persisted, but time tracking continues)
      // -1 means no events have been processed yet
      expect(runner.getStatus().lastCheckpointPosition).toBe(-1);
    });

    test('should force checkpoint when requested', async () => {
      await appendEvent('TestEvent', { id: 1 });

      await runner.start();
      await advanceTimeAndSettle(20);

      const positionBeforeForce = runner.getCurrentPosition();

      await runner.forceCheckpoint();

      expect(runner.getStatus().lastCheckpointPosition).toBe(positionBeforeForce);
    });
  });

  describe('waitForCatchUp', () => {
    test('should resolve when caught up', async () => {
      await appendEvent('TestEvent', { id: 1 });
      await appendEvent('TestEvent', { id: 2 });

      // globalPosition is the NEXT position (e.g., 2 after events at 0, 1)
      // Last event is at globalPosition - 1
      const globalPosition = eventStore.getGlobalPosition();
      const lastEventPosition = globalPosition - 1;

      await runner.start();

      // Start waitForCatchUp without blocking - wait for last event's position
      const catchUpPromise = runner.waitForCatchUp(lastEventPosition, 5000);

      // Advance time to allow polling and sleep resolution
      for (let i = 0; i < 10; i++) {
        await advanceTimeAndSettle(20);
      }

      const result = await catchUpPromise;

      expect(result).toBe(true);
      expect(runner.getCurrentPosition()).toBeGreaterThanOrEqual(lastEventPosition);
    });

    test('should timeout and return false', async () => {
      // Create many events to ensure we can't catch up in time
      for (let i = 0; i < 100; i++) {
        await appendEvent('TestEvent', { id: i });
      }

      const globalPosition = eventStore.getGlobalPosition();

      // Use a very slow runner with small batch size
      runner = createRunner({ pollingIntervalMs: 1000, batchSize: 1 });
      await runner.start();

      // Start waitForCatchUp with an impossibly high target position
      const catchUpPromise = runner.waitForCatchUp(globalPosition + 1000, 100);

      // Advance time past the timeout duration
      for (let i = 0; i < 20; i++) {
        await advanceTimeAndSettle(20);
      }

      const result = await catchUpPromise;

      expect(result).toBe(false);
    });

    test('should process events while waiting', async () => {
      await appendEvent('TestEvent', { id: 1 });

      await runner.start();

      // Wait for the last event's position
      const targetPosition = eventStore.getGlobalPosition() - 1;

      // Start waitForCatchUp without blocking
      const catchUpPromise = runner.waitForCatchUp(targetPosition);

      // Advance time to allow processing
      for (let i = 0; i < 10; i++) {
        await advanceTimeAndSettle(20);
      }

      await catchUpPromise;

      expect(projection.getProcessedCount()).toBeGreaterThanOrEqual(1);
    });
  });

  describe('error handling', () => {
    test('should increment error count on build error', async () => {
      await appendEvent('TestEvent', { id: 1 });

      projection.injectBuildError(new Error('Build failed'));

      await runner.start();

      // Process with error
      await advanceTimeAndSettle(20);

      expect(runner.getStatus().errorsCount).toBeGreaterThanOrEqual(1);
    });

    test('should continue polling after error', async () => {
      await appendEvent('TestEvent', { id: 1 });

      projection.injectBuildError(new Error('Build failed'));

      await runner.start();
      await advanceTimeAndSettle(20);

      // Clear error and add more events
      projection.clearBuildError();
      await appendEvent('TestEvent', { id: 2 });

      await advanceTimeAndSettle(50);

      // Should still be running
      expect(runner.getStatus().running).toBe(true);
    });
  });

  describe('getStatus', () => {
    test('should return complete status', async () => {
      await runner.start();

      const status = runner.getStatus();

      expect(status.name).toBe(projectionName);
      expect(status.kind).toBe('aggregator');
      expect(status.running).toBe(true);
      expect(typeof status.currentPosition).toBe('number');
      expect(typeof status.eventsProcessed).toBe('number');
      expect(typeof status.errorsCount).toBe('number');
      expect(typeof status.lastCheckpointPosition).toBe('number');
      expect(typeof status.lastCheckpointTime).toBe('number');
      expect(typeof status.nextCheckpointTime).toBe('number');
    });

    test('should reflect running state correctly', async () => {
      expect(runner.getStatus().running).toBe(false);

      await runner.start();
      expect(runner.getStatus().running).toBe(true);

      await runner.stop();
      expect(runner.getStatus().running).toBe(false);
    });
  });

  describe('DST scenarios', () => {
    test('should resume from checkpoint after restart', async () => {
      await appendEvent('TestEvent', { id: 1 });
      await appendEvent('TestEvent', { id: 2 });

      await runner.start();
      await advanceTimeAndSettle(50);

      await runner.forceCheckpoint();
      const checkpointPosition = runner.getCurrentPosition();
      await runner.stop();

      // Create new runner with same store
      const newProjection = new MockAggregatorProjection(['*']);
      const newRunner = createRunner({ projection: newProjection });

      await newRunner.start();

      // Should start from checkpoint position
      expect(newRunner.getCurrentPosition()).toBe(checkpointPosition);

      // Add more events
      await appendEvent('TestEvent', { id: 3 });

      await advanceTimeAndSettle(50);

      // Should only process the new event
      expect(newProjection.getProcessedCount()).toBe(1);

      await newRunner.stop();
    });

    test('should handle checkpoint write failure', async () => {
      const faultScheduler = new FaultScheduler(env.random, env.fs);

      await appendEvent('TestEvent', { id: 1 });

      await runner.start();
      await advanceTimeAndSettle(20);

      // Inject fault before checkpoint
      faultScheduler.injectFault('sync');

      // Force checkpoint - should fail but continue running
      await runner.forceCheckpoint();

      // Should still be running
      expect(runner.getStatus().running).toBe(true);
      expect(runner.getStatus().errorsCount).toBeGreaterThanOrEqual(1);

      faultScheduler.clearFaults();
      await runner.stop();
    });

    test('should maintain consistency through multiple poll cycles', async () => {
      // Add events in batches
      for (let batch = 0; batch < 5; batch++) {
        for (let i = 0; i < 10; i++) {
          await appendEvent('TestEvent', { batch, id: i });
        }
      }

      await runner.start();

      // Process all events - need enough poll cycles
      // With batchSize=10 and 50 events, need at least 5 polls
      // Adding extra iterations for safety
      for (let i = 0; i < 10; i++) {
        await advanceTimeAndSettle(15);
      }

      // Verify all events processed
      expect(projection.getProcessedCount()).toBe(50);
      // globalPosition is 1-indexed from the last event, currentPosition matches
      expect(runner.getCurrentPosition()).toBe(eventStore.getGlobalPosition() - 1);

      await runner.stop();
    });

    test('should handle random events with filtering (fuzz)', async () => {
      const random = new SeededRandom(42);
      const eventTypes = ['TypeA', 'TypeB', 'TypeC', 'TypeD'];
      const subscribedTypes = ['TypeA', 'TypeC'];

      const filteredProjection = new MockAggregatorProjection(subscribedTypes);
      runner = createRunner({
        projection: filteredProjection,
        metadata: createMetadata('FuzzRunner', subscribedTypes),
      });

      // Generate random events
      let expectedCount = 0;
      for (let i = 0; i < 100; i++) {
        const eventType = random.choice(eventTypes);
        await appendEvent(eventType, { id: i });
        if (subscribedTypes.includes(eventType)) {
          expectedCount++;
        }
      }

      await runner.start();

      // Process all events
      for (let i = 0; i < 30; i++) {
        await advanceTimeAndSettle(20);
      }

      expect(filteredProjection.getProcessedCount()).toBe(expectedCount);

      await runner.stop();
    });
  });

  describe('edge cases', () => {
    test('should handle empty event store', async () => {
      await runner.start();
      await advanceTimeAndSettle(50);

      expect(runner.getStatus().running).toBe(true);
      // -1 indicates no events have been processed yet
      expect(runner.getCurrentPosition()).toBe(-1);
      expect(projection.getProcessedCount()).toBe(0);

      await runner.stop();
    });

    test('should handle events added while running', async () => {
      await runner.start();

      // Add events after start
      await appendEvent('TestEvent', { id: 1 });
      await advanceTimeAndSettle(50);

      await appendEvent('TestEvent', { id: 2 });
      await advanceTimeAndSettle(50);

      expect(projection.getProcessedCount()).toBe(2);

      await runner.stop();
    });

    test('should handle wildcard event subscription', async () => {
      await appendEvent('TypeA', {});
      await appendEvent('TypeB', {});
      await appendEvent('TypeC', {});
      await appendEvent('AnyOtherType', {});

      await runner.start();
      await advanceTimeAndSettle(50);

      expect(projection.getProcessedCount()).toBe(4);

      await runner.stop();
    });

    test('should handle very small checkpoint interval', async () => {
      runner = createRunner({ checkpointIntervalMs: 1, jitterMs: 0 });

      await appendEvent('TestEvent', { id: 1 });

      await runner.start();

      // Need multiple poll cycles to process event and trigger checkpoint
      for (let i = 0; i < 5; i++) {
        await advanceTimeAndSettle(15);
      }

      // Should have checkpointed (position >= 0 since we processed at least one event)
      expect(runner.getStatus().lastCheckpointPosition).toBeGreaterThanOrEqual(0);

      await runner.stop();
    });

    test('should handle multiple streams', async () => {
      await appendEvent('EventA', { id: 1 }, 'stream-1');
      await appendEvent('EventB', { id: 2 }, 'stream-2');
      await appendEvent('EventC', { id: 3 }, 'stream-3');
      await appendEvent('EventD', { id: 4 }, 'stream-1');

      await runner.start();
      await advanceTimeAndSettle(50);

      // Should process all events regardless of stream
      expect(projection.getProcessedCount()).toBe(4);

      await runner.stop();
    });
  });

  describe('timing with SimulatedClock', () => {
    test('should respect pollingIntervalMs', async () => {
      runner = createRunner({ pollingIntervalMs: 100 });

      await appendEvent('TestEvent', { id: 1 });

      await runner.start();

      // Before first poll interval
      await advanceTimeAndSettle(50);

      const countAtFirstCheck = projection.getProcessedCount();

      // After poll interval
      await advanceTimeAndSettle(60);

      // Should have polled and processed
      expect(projection.getProcessedCount()).toBeGreaterThan(countAtFirstCheck);

      await runner.stop();
    });

    test('should use clock.setTimeout for scheduling', async () => {
      let setTimeoutCalled = false;
      const originalSetTimeout = env.clock.setTimeout.bind(env.clock);
      env.clock.setTimeout = (fn, delay) => {
        setTimeoutCalled = true;
        return originalSetTimeout(fn, delay);
      };

      await runner.start();
      await advanceTimeAndSettle(20);

      expect(setTimeoutCalled).toBe(true);

      await runner.stop();
    });
  });
});
