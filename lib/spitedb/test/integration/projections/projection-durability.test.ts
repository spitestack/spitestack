/**
 * Integration tests for projection checkpoint durability.
 *
 * These tests verify the critical fix for the bug where projections
 * could checkpoint beyond durable events, leading to state drift
 * after crash recovery.
 *
 * Bug scenario before fix:
 * 1. Events appended but NOT flushed
 * 2. Projection processes pending events, checkpoints position X
 * 3. CRASH before event store flush
 * 4. On restart: events lost, but projection checkpoint at X
 * 5. Event store allocates new positions starting before X
 * 6. Projection skips events (thinks it already processed them)
 *
 * After fix:
 * - Projections only see durable (flushed) events
 * - Checkpoint position never exceeds lastFlushedGlobalPosition
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { EventStore } from '../../../src/application/event-store';
import { createTestEnvironment, type TestEnvironment } from '../../setup/test-helpers';

describe('Projection Durability', () => {
  let env: TestEnvironment;
  let eventStore: EventStore;
  const dataDir = '/test-durability';

  beforeEach(async () => {
    env = createTestEnvironment(42);
  });

  afterEach(async () => {
    if (eventStore?.isOpen()) {
      await eventStore.close();
    }
  });

  function createEventStore(autoFlushCount = 0): EventStore {
    return new EventStore({
      fs: env.fs,
      serializer: env.serializer,
      compressor: env.compressor,
      clock: env.clock,
      autoFlushCount,
    });
  }

  describe('getDurableGlobalPosition', () => {
    test('should return -1 when no events have been flushed', async () => {
      eventStore = createEventStore();
      await eventStore.open(dataDir);

      expect(eventStore.getDurableGlobalPosition()).toBe(-1);
    });

    test('should return position of last flushed event', async () => {
      eventStore = createEventStore();
      await eventStore.open(dataDir);

      await eventStore.append('stream-1', [
        { type: 'Event1', data: {} },
        { type: 'Event2', data: {} },
      ]);

      // Before flush, durable position should still be -1
      expect(eventStore.getDurableGlobalPosition()).toBe(-1);

      await eventStore.flush();

      // After flush, durable position should be 1 (0-indexed, 2 events)
      expect(eventStore.getDurableGlobalPosition()).toBe(1);
    });

    test('should not include pending events in durable position', async () => {
      eventStore = createEventStore();
      await eventStore.open(dataDir);

      // First batch - flush
      await eventStore.append('stream-1', [{ type: 'Event1', data: {} }]);
      await eventStore.flush();
      expect(eventStore.getDurableGlobalPosition()).toBe(0);

      // Second batch - don't flush
      await eventStore.append('stream-1', [{ type: 'Event2', data: {} }]);

      // Durable position should still be 0, even though getGlobalPosition is 2
      expect(eventStore.getDurableGlobalPosition()).toBe(0);
      expect(eventStore.getGlobalPosition()).toBe(2);
    });
  });

  describe('readGlobalDurable', () => {
    test('should only return flushed events', async () => {
      eventStore = createEventStore();
      await eventStore.open(dataDir);

      // Append and flush first batch
      await eventStore.append('stream-1', [
        { type: 'Event1', data: {} },
        { type: 'Event2', data: {} },
      ]);
      await eventStore.flush();

      // Append second batch without flush
      await eventStore.append('stream-1', [
        { type: 'Event3', data: {} },
      ]);

      // readGlobalDurable should only return the flushed events
      const durableEvents = await eventStore.readGlobalDurable(0);
      expect(durableEvents).toHaveLength(2);
      expect(durableEvents[0]!.type).toBe('Event1');
      expect(durableEvents[1]!.type).toBe('Event2');

      // Regular readGlobal should return all events including pending
      const allEvents = await eventStore.readGlobal(0);
      expect(allEvents).toHaveLength(3);
    });

    test('should return empty array when no durable events', async () => {
      eventStore = createEventStore();
      await eventStore.open(dataDir);

      // Append without flush
      await eventStore.append('stream-1', [{ type: 'Event1', data: {} }]);

      const durableEvents = await eventStore.readGlobalDurable(0);
      expect(durableEvents).toHaveLength(0);
    });

    test('should respect fromPosition parameter', async () => {
      eventStore = createEventStore();
      await eventStore.open(dataDir);

      await eventStore.append('stream-1', [
        { type: 'Event1', data: {} },
        { type: 'Event2', data: {} },
        { type: 'Event3', data: {} },
      ]);
      await eventStore.flush();

      // Read starting from position 1
      const events = await eventStore.readGlobalDurable(1);
      expect(events).toHaveLength(2);
      expect(events[0]!.type).toBe('Event2');
      expect(events[1]!.type).toBe('Event3');
    });
  });

  describe('streamGlobalDurable', () => {
    test('should yield only flushed events', async () => {
      eventStore = createEventStore();
      await eventStore.open(dataDir);

      // Append and flush first batch
      await eventStore.append('stream-1', [
        { type: 'Event1', data: {} },
      ]);
      await eventStore.flush();

      // Append second batch without flush
      await eventStore.append('stream-1', [
        { type: 'Event2', data: {} },
      ]);

      // streamGlobalDurable should only yield the flushed event
      const events: string[] = [];
      for await (const event of eventStore.streamGlobalDurable(0)) {
        events.push(event.type);
      }
      expect(events).toEqual(['Event1']);
    });

    test('should stop at durable boundary even with pending events', async () => {
      eventStore = createEventStore();
      await eventStore.open(dataDir);

      // Flush 3 events
      await eventStore.append('stream-1', [
        { type: 'Event1', data: {} },
        { type: 'Event2', data: {} },
        { type: 'Event3', data: {} },
      ]);
      await eventStore.flush();

      // Add 2 pending events
      await eventStore.append('stream-1', [
        { type: 'Event4', data: {} },
        { type: 'Event5', data: {} },
      ]);

      // Count events from durable stream
      let count = 0;
      for await (const _event of eventStore.streamGlobalDurable(0)) {
        count++;
      }
      expect(count).toBe(3);
    });
  });

  describe('durability boundary consistency', () => {
    test('durable position should track flush operations', async () => {
      eventStore = createEventStore();
      await eventStore.open(dataDir);

      // Initial state
      expect(eventStore.getDurableGlobalPosition()).toBe(-1);

      // After append but before flush
      await eventStore.append('stream-1', [{ type: 'Event1', data: {} }]);
      expect(eventStore.getDurableGlobalPosition()).toBe(-1);

      // After first flush
      await eventStore.flush();
      expect(eventStore.getDurableGlobalPosition()).toBe(0);

      // Append more
      await eventStore.append('stream-1', [
        { type: 'Event2', data: {} },
        { type: 'Event3', data: {} },
      ]);
      expect(eventStore.getDurableGlobalPosition()).toBe(0);

      // After second flush
      await eventStore.flush();
      expect(eventStore.getDurableGlobalPosition()).toBe(2);
    });

    test('readGlobalDurable results should be consistent with getDurableGlobalPosition', async () => {
      eventStore = createEventStore();
      await eventStore.open(dataDir);

      // Create mixed durable and pending events
      await eventStore.append('stream-1', [
        { type: 'Event1', data: {} },
        { type: 'Event2', data: {} },
      ]);
      await eventStore.flush();
      await eventStore.append('stream-1', [
        { type: 'Event3', data: {} },
      ]);

      const durablePos = eventStore.getDurableGlobalPosition();
      const durableEvents = await eventStore.readGlobalDurable(0);

      // The last event's position should match durable position
      expect(durableEvents.length).toBeGreaterThan(0);
      expect(durableEvents[durableEvents.length - 1]!.globalPosition).toBe(durablePos);
    });
  });
});
