/**
 * Unit tests for ProjectionCoordinator.
 *
 * Tests the coordinator with:
 * - Lifecycle (start/stop)
 * - Projection registration and retrieval
 * - Status reporting
 * - waitForCatchUp and forceCheckpoint
 * - Lock management
 * - DST scenarios (crash recovery, lock contention)
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import {
  ProjectionCoordinator,
  type ProjectionCoordinatorConfig,
} from '../../../../src/application/projections/projection-coordinator';
import {
  createTestEnvironment,
  createTestEventStore,
  createTestCoordinator,
  type TestEnvironment,
} from '../../../setup/test-helpers';
import {
  createMockAggregatorRegistration,
  createMockViewRegistration,
  MockAggregatorProjection,
  MockViewProjection,
} from '../../../setup/mock-projection';
import { FaultScheduler } from '../../../setup/fault-scheduler';
import { SeededRandom } from '../../../setup/seeded-random';
import type { EventStore } from '../../../../src/application/event-store';
import {
  ProjectionCoordinatorError,
  ProjectionNotFoundError,
  ProjectionDisabledError,
  ProjectionCatchUpTimeoutError,
} from '../../../../src/errors';

describe('ProjectionCoordinator', () => {
  let env: TestEnvironment;
  let eventStore: EventStore;
  let coordinator: ProjectionCoordinator;
  const dataDir = '/test-projections';

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
   */
  const advanceTimeAndSettle = async (ms: number): Promise<void> => {
    await env.clock.tickAsync(ms);
  };

  beforeEach(async () => {
    env = createTestEnvironment(12345);
    eventStore = await createTestEventStore(env, '/test-events');
    coordinator = createTestCoordinator(env, eventStore, dataDir);
  });

  afterEach(async () => {
    if (coordinator.isRunning()) {
      await coordinator.stop();
    }
    await eventStore.close();
  });

  describe('start', () => {
    test('should start all enabled projections', async () => {
      const registration1 = createMockAggregatorRegistration('Counter1', ['*']);
      const registration2 = createMockAggregatorRegistration('Counter2', ['*']);

      coordinator.getRegistry().register(registration1);
      coordinator.getRegistry().register(registration2);

      await coordinator.start();

      expect(coordinator.isRunning()).toBe(true);
      expect(coordinator.getStatus().enabledCount).toBe(2);
    });

    test('should create data directory if it does not exist', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      expect(await env.fs.exists(dataDir)).toBe(false);

      await coordinator.start();

      expect(await env.fs.exists(dataDir)).toBe(true);
    });

    test('should create checkpoint directory', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();

      expect(await env.fs.exists(`${dataDir}/checkpoints`)).toBe(true);
    });

    test('should acquire exclusive lock', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();

      // Try to acquire lock again should fail
      const lockPath = `${dataDir}/.lock`;
      expect(await env.fs.exists(lockPath)).toBe(true);
    });

    test('should throw if already running', async () => {
      await coordinator.start();

      await expect(coordinator.start()).rejects.toThrow(ProjectionCoordinatorError);
      await expect(coordinator.start()).rejects.toThrow('already running');
    });

    test('should throw if EventStore is not open', async () => {
      await eventStore.close();

      await expect(coordinator.start()).rejects.toThrow(ProjectionCoordinatorError);
      await expect(coordinator.start()).rejects.toThrow('not open');
    });

    test('should not start disabled projections', async () => {
      const enabledReg = createMockAggregatorRegistration('Enabled', ['*']);
      const disabledReg = createMockAggregatorRegistration('Disabled', ['*']);

      coordinator.getRegistry().register(enabledReg);
      coordinator.getRegistry().register(disabledReg, { enabled: false });

      await coordinator.start();

      expect(coordinator.getStatus().registeredCount).toBe(2);
      expect(coordinator.getStatus().enabledCount).toBe(1);
      expect(coordinator.getProjection('Enabled')).toBeDefined();
      expect(coordinator.getProjection('Disabled')).toBeUndefined();
    });

    test('should process events after start', async () => {
      await appendEvent('TestEvent', { id: 1 });
      await appendEvent('TestEvent', { id: 2 });

      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();

      // Advance time for polling
      await advanceTimeAndSettle(50);

      const projection = coordinator.getProjection<MockAggregatorProjection>('Counter');
      expect(projection?.getProcessedCount()).toBeGreaterThanOrEqual(1);
    });

    test('should start with no projections registered', async () => {
      await coordinator.start();

      expect(coordinator.isRunning()).toBe(true);
      expect(coordinator.getStatus().registeredCount).toBe(0);
    });
  });

  describe('stop', () => {
    test('should stop all projections', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();
      await coordinator.stop();

      expect(coordinator.isRunning()).toBe(false);
    });

    test('should be idempotent (no-op if not running)', async () => {
      await coordinator.stop();
      await coordinator.stop();

      expect(coordinator.isRunning()).toBe(false);
    });

    test('should persist final checkpoints', async () => {
      await appendEvent('TestEvent', { id: 1 });

      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();
      await advanceTimeAndSettle(50);
      await coordinator.stop();

      // Create new coordinator and verify checkpoint was persisted
      // Event store is still open from beforeEach
      const newCoordinator = createTestCoordinator(env, eventStore, dataDir);
      newCoordinator.getRegistry().register(createMockAggregatorRegistration('Counter', ['*']));

      await newCoordinator.start();

      // Should resume from checkpoint (not reprocess events)
      const status = newCoordinator.getProjectionStatus('Counter');
      expect(status?.currentPosition).toBeGreaterThanOrEqual(0);

      await newCoordinator.stop();
    });

    test('should release lock after stop', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();
      await coordinator.stop();

      // Second coordinator should be able to acquire lock
      const coordinator2 = createTestCoordinator(env, eventStore, dataDir);
      coordinator2.getRegistry().register(createMockAggregatorRegistration('Counter', ['*']));

      await expect(coordinator2.start()).resolves.toBeUndefined();

      await coordinator2.stop();
    });

    test('should clear projection instances on stop', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();
      expect(coordinator.getProjection('Counter')).toBeDefined();

      await coordinator.stop();
      expect(coordinator.getProjection('Counter')).toBeUndefined();
    });
  });

  describe('getRegistry', () => {
    test('should return registry', () => {
      const registry = coordinator.getRegistry();

      expect(registry).toBeDefined();
      expect(registry.count()).toBe(0);
    });

    test('should allow registration before start', () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);

      coordinator.getRegistry().register(registration);

      expect(coordinator.getRegistry().count()).toBe(1);
      expect(coordinator.getRegistry().has('Counter')).toBe(true);
    });
  });

  describe('getProjection', () => {
    test('should return projection instance by name', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();

      const projection = coordinator.getProjection('Counter');
      expect(projection).toBeDefined();
    });

    test('should return undefined for unknown projection', async () => {
      await coordinator.start();

      const projection = coordinator.getProjection('Unknown');
      expect(projection).toBeUndefined();
    });

    test('should return undefined for disabled projection', async () => {
      const registration = createMockAggregatorRegistration('Disabled', ['*']);
      coordinator.getRegistry().register(registration, { enabled: false });

      await coordinator.start();

      const projection = coordinator.getProjection('Disabled');
      expect(projection).toBeUndefined();
    });

    test('should return typed projection', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();

      const projection = coordinator.getProjection<MockAggregatorProjection>('Counter');
      // Should have MockAggregatorProjection methods
      expect(projection?.getProcessedCount).toBeDefined();
    });
  });

  describe('requireProjection', () => {
    test('should return projection instance', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();

      const projection = coordinator.requireProjection('Counter');
      expect(projection).toBeDefined();
    });

    test('should throw ProjectionNotFoundError for unknown projection', async () => {
      await coordinator.start();

      expect(() => coordinator.requireProjection('Unknown')).toThrow(
        ProjectionNotFoundError
      );
    });

    test('should throw ProjectionDisabledError for disabled projection', async () => {
      const registration = createMockAggregatorRegistration('Disabled', ['*']);
      coordinator.getRegistry().register(registration, { enabled: false });

      await coordinator.start();

      expect(() => coordinator.requireProjection('Disabled')).toThrow(
        ProjectionDisabledError
      );
    });
  });

  describe('waitForCatchUp', () => {
    test('should resolve when all projections caught up', async () => {
      await appendEvent('TestEvent', { id: 1 });
      await appendEvent('TestEvent', { id: 2 });

      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();

      // Start waitForCatchUp
      const catchUpPromise = coordinator.waitForCatchUp(5000);

      // Advance time for processing
      for (let i = 0; i < 10; i++) {
        await advanceTimeAndSettle(20);
      }

      await expect(catchUpPromise).resolves.toBeUndefined();
    });

    test('should wait for multiple projections', async () => {
      await appendEvent('TypeA', { id: 1 });
      await appendEvent('TypeB', { id: 2 });

      const reg1 = createMockAggregatorRegistration('Counter1', ['TypeA']);
      const reg2 = createMockAggregatorRegistration('Counter2', ['TypeB']);

      coordinator.getRegistry().register(reg1);
      coordinator.getRegistry().register(reg2);

      await coordinator.start();

      const catchUpPromise = coordinator.waitForCatchUp(5000);

      for (let i = 0; i < 10; i++) {
        await advanceTimeAndSettle(20);
      }

      await expect(catchUpPromise).resolves.toBeUndefined();
    });

    test('should timeout with ProjectionCatchUpTimeoutError', async () => {
      // Create a few events
      for (let i = 0; i < 10; i++) {
        await appendEvent('TestEvent', { id: i });
      }

      // Use slow polling to ensure timeout
      const slowConfig: ProjectionCoordinatorConfig = {
        eventStore,
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir: '/test-projections-slow',
        pollingIntervalMs: 500, // Slow polling
        defaultBatchSize: 1,
      };
      const slowCoordinator = new ProjectionCoordinator(slowConfig);
      slowCoordinator.getRegistry().register(createMockAggregatorRegistration('Slow', ['*']));

      await slowCoordinator.start();

      // Very short timeout that will expire before catch-up
      const catchUpPromise = slowCoordinator.waitForCatchUp(20);

      // Advance time past timeout but not enough for catch-up
      await advanceTimeAndSettle(50);

      await expect(catchUpPromise).rejects.toThrow(ProjectionCatchUpTimeoutError);

      await slowCoordinator.stop();
    });
  });

  describe('forceCheckpoint', () => {
    test('should checkpoint all projections', async () => {
      await appendEvent('TestEvent', { id: 1 });

      const reg1 = createMockAggregatorRegistration('Counter1', ['*']);
      const reg2 = createMockAggregatorRegistration('Counter2', ['*']);

      coordinator.getRegistry().register(reg1);
      coordinator.getRegistry().register(reg2);

      await coordinator.start();
      await advanceTimeAndSettle(50);

      await coordinator.forceCheckpoint();

      const status1 = coordinator.getProjectionStatus('Counter1');
      const status2 = coordinator.getProjectionStatus('Counter2');

      // Both should have checkpointed
      expect(status1?.lastCheckpointPosition).toBeGreaterThanOrEqual(0);
      expect(status2?.lastCheckpointPosition).toBeGreaterThanOrEqual(0);
    });
  });

  describe('getStatus', () => {
    test('should return complete status', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();

      const status = coordinator.getStatus();

      expect(status.running).toBe(true);
      expect(status.registeredCount).toBe(1);
      expect(status.enabledCount).toBe(1);
      expect(status.projections).toHaveLength(1);
      expect(typeof status.globalPosition).toBe('number');
    });

    test('should reflect running state', async () => {
      expect(coordinator.getStatus().running).toBe(false);

      await coordinator.start();
      expect(coordinator.getStatus().running).toBe(true);

      await coordinator.stop();
      expect(coordinator.getStatus().running).toBe(false);
    });

    test('should track global position from event store', async () => {
      await appendEvent('TestEvent', { id: 1 });
      await appendEvent('TestEvent', { id: 2 });

      await coordinator.start();

      const status = coordinator.getStatus();
      expect(status.globalPosition).toBeGreaterThanOrEqual(1);
    });

    test('should include all projection statuses', async () => {
      const reg1 = createMockAggregatorRegistration('Counter1', ['*']);
      const reg2 = createMockAggregatorRegistration('Counter2', ['*']);

      coordinator.getRegistry().register(reg1);
      coordinator.getRegistry().register(reg2);

      await coordinator.start();

      const status = coordinator.getStatus();

      expect(status.projections).toHaveLength(2);
      expect(status.projections.map((p) => p.name)).toContain('Counter1');
      expect(status.projections.map((p) => p.name)).toContain('Counter2');
    });
  });

  describe('getProjectionStatus', () => {
    test('should return status for specific projection', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();

      const status = coordinator.getProjectionStatus('Counter');

      expect(status).toBeDefined();
      expect(status?.name).toBe('Counter');
      expect(status?.kind).toBe('aggregator');
      expect(status?.running).toBe(true);
    });

    test('should return undefined for unknown projection', async () => {
      await coordinator.start();

      const status = coordinator.getProjectionStatus('Unknown');
      expect(status).toBeUndefined();
    });
  });

  describe('isRunning', () => {
    test('should reflect running state', async () => {
      expect(coordinator.isRunning()).toBe(false);

      await coordinator.start();
      expect(coordinator.isRunning()).toBe(true);

      await coordinator.stop();
      expect(coordinator.isRunning()).toBe(false);
    });
  });

  describe('projection kinds', () => {
    test('should handle aggregator projections', async () => {
      await appendEvent('TestEvent', { id: 1 });
      await appendEvent('TestEvent', { id: 2 });

      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();
      await advanceTimeAndSettle(50);

      const projection = coordinator.getProjection<MockAggregatorProjection>('Counter');
      expect(projection?.getProcessedCount()).toBeGreaterThanOrEqual(1);
    });

    test('should handle denormalized_view projections', async () => {
      await appendEvent('UserCreated', { id: 'user-1', name: 'Alice' });
      await appendEvent('UserCreated', { id: 'user-2', name: 'Bob' });

      const registration = createMockViewRegistration('Users', ['UserCreated'], ['id']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();
      await advanceTimeAndSettle(50);

      const projection = coordinator.getProjection<MockViewProjection>('Users');
      expect(projection).toBeDefined();
    });

    test('should handle mixed projection kinds', async () => {
      await appendEvent('TestEvent', { id: 1 });

      const aggregatorReg = createMockAggregatorRegistration('Counter', ['*']);
      const viewReg = createMockViewRegistration('Items', ['*'], ['id']);

      coordinator.getRegistry().register(aggregatorReg);
      coordinator.getRegistry().register(viewReg);

      await coordinator.start();

      expect(coordinator.getProjectionStatus('Counter')?.kind).toBe('aggregator');
      expect(coordinator.getProjectionStatus('Items')?.kind).toBe('denormalized_view');
    });
  });

  describe('DST scenarios', () => {
    test('should prevent second coordinator from starting (lock contention)', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();

      // Try to start second coordinator
      const coordinator2 = createTestCoordinator(env, eventStore, dataDir);
      coordinator2.getRegistry().register(createMockAggregatorRegistration('Counter', ['*']));

      await expect(coordinator2.start()).rejects.toThrow(ProjectionCoordinatorError);
      await expect(coordinator2.start()).rejects.toThrow('lock');
    });

    test('should recover after restart', async () => {
      await appendEvent('TestEvent', { id: 1 });
      await appendEvent('TestEvent', { id: 2 });

      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();
      await advanceTimeAndSettle(50);
      await coordinator.forceCheckpoint();

      const positionBefore = coordinator.getProjectionStatus('Counter')?.currentPosition;
      await coordinator.stop();

      // Simulate restart
      const newCoordinator = createTestCoordinator(env, eventStore, dataDir);
      newCoordinator.getRegistry().register(createMockAggregatorRegistration('Counter', ['*']));

      await newCoordinator.start();

      // Should resume from checkpoint
      const positionAfter = newCoordinator.getProjectionStatus('Counter')?.currentPosition;
      expect(positionAfter).toBe(positionBefore);

      // Add new event
      await appendEvent('TestEvent', { id: 3 });
      await advanceTimeAndSettle(50);

      // Should process new event
      const newProjection = newCoordinator.getProjection<MockAggregatorProjection>('Counter');
      expect(newProjection?.getProcessedCount()).toBe(1); // Only the new event

      await newCoordinator.stop();
    });

    test('should coordinate multiple projections', async () => {
      const random = new SeededRandom(42);
      const eventTypes = ['TypeA', 'TypeB', 'TypeC'];

      // Generate random events
      for (let i = 0; i < 15; i++) {
        const eventType = random.choice(eventTypes);
        await appendEvent(eventType, { id: i });
      }

      const regA = createMockAggregatorRegistration('CounterA', ['TypeA']);
      const regB = createMockAggregatorRegistration('CounterB', ['TypeB']);
      const regAll = createMockAggregatorRegistration('CounterAll', ['*']);

      coordinator.getRegistry().register(regA);
      coordinator.getRegistry().register(regB);
      coordinator.getRegistry().register(regAll);

      await coordinator.start();

      // Wait for all to catch up
      const catchUpPromise = coordinator.waitForCatchUp(5000);
      for (let i = 0; i < 10; i++) {
        await advanceTimeAndSettle(20);
      }
      await catchUpPromise;

      // All projections should have caught up
      const statusA = coordinator.getProjectionStatus('CounterA');
      const statusB = coordinator.getProjectionStatus('CounterB');
      const statusAll = coordinator.getProjectionStatus('CounterAll');

      expect(statusA?.currentPosition).toBeGreaterThanOrEqual(14n);
      expect(statusB?.currentPosition).toBeGreaterThanOrEqual(14n);
      expect(statusAll?.currentPosition).toBeGreaterThanOrEqual(14n);
    });

    test('should handle checkpoint write failure gracefully', async () => {
      const faultScheduler = new FaultScheduler(env.random, env.fs);

      await appendEvent('TestEvent', { id: 1 });

      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();
      await advanceTimeAndSettle(50);

      // Inject fault before checkpoint
      faultScheduler.injectFault('sync');

      // Force checkpoint - should fail but continue running
      await coordinator.forceCheckpoint();

      // Should still be running
      expect(coordinator.isRunning()).toBe(true);

      faultScheduler.clearFaults();
    });

    test('should handle events added during startup', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      // Start coordinator
      await coordinator.start();

      // Add events while running
      await appendEvent('TestEvent', { id: 1 });
      await appendEvent('TestEvent', { id: 2 });
      await appendEvent('TestEvent', { id: 3 });

      await advanceTimeAndSettle(100);

      const projection = coordinator.getProjection<MockAggregatorProjection>('Counter');
      expect(projection?.getProcessedCount()).toBe(3);
    });

    test('should properly clean up on failed start', async () => {
      // Close event store to cause failure
      await eventStore.close();

      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await expect(coordinator.start()).rejects.toThrow();

      // Should not be running
      expect(coordinator.isRunning()).toBe(false);
    });
  });

  describe('runtime options', () => {
    test('should respect custom checkpoint interval', async () => {
      await appendEvent('TestEvent', { id: 1 });

      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration, { checkpointIntervalMs: 10 });

      await coordinator.start();

      // Advance time to trigger checkpoint
      await advanceTimeAndSettle(50);

      const status = coordinator.getProjectionStatus('Counter');
      // With short checkpoint interval, should have checkpointed
      expect(status?.lastCheckpointPosition).toBeGreaterThanOrEqual(0);
    });

    test('should apply event filter', async () => {
      await appendEvent('TestEvent', { important: true, id: 1 });
      await appendEvent('TestEvent', { important: false, id: 2 });
      await appendEvent('TestEvent', { important: true, id: 3 });

      const registration = createMockAggregatorRegistration('FilteredCounter', ['*']);
      coordinator.getRegistry().register(registration, {
        eventFilter: (event) =>
          event.data != null &&
          typeof event.data === 'object' &&
          'important' in event.data &&
          (event.data as Record<string, unknown>)['important'] === true,
      });

      await coordinator.start();
      await advanceTimeAndSettle(50);

      const projection = coordinator.getProjection<MockAggregatorProjection>('FilteredCounter');
      // Should only process events where important === true
      expect(projection?.getProcessedCount()).toBe(2);
    });
  });

  describe('edge cases', () => {
    test('should handle empty event store', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();
      await advanceTimeAndSettle(50);

      expect(coordinator.isRunning()).toBe(true);
      expect(coordinator.getProjectionStatus('Counter')?.currentPosition).toBe(-1);
    });

    test('should handle rapid start/stop cycles', async () => {
      const registration = createMockAggregatorRegistration('Counter', ['*']);
      coordinator.getRegistry().register(registration);

      for (let i = 0; i < 3; i++) {
        await coordinator.start();
        expect(coordinator.isRunning()).toBe(true);

        await coordinator.stop();
        expect(coordinator.isRunning()).toBe(false);
      }
    });

    test('should handle projection with no matching events', async () => {
      await appendEvent('TypeA', { id: 1 });
      await appendEvent('TypeA', { id: 2 });

      // Register projection that subscribes to different event type
      const registration = createMockAggregatorRegistration('Counter', ['TypeB']);
      coordinator.getRegistry().register(registration);

      await coordinator.start();
      await advanceTimeAndSettle(50);

      const projection = coordinator.getProjection<MockAggregatorProjection>('Counter');
      expect(projection?.getProcessedCount()).toBe(0);
    });
  });
});
