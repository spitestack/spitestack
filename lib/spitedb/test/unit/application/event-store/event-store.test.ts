import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { EventStore } from '../../../../src/application/event-store';
import { SimulatedFileSystem } from '../../../../src/testing/simulated-filesystem';
import { SimulatedClock } from '../../../../src/testing/simulated-clock';
import { MsgpackSerializer } from '../../../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../../../src/infrastructure/serialization/zstd-compressor';
import { ConcurrencyError } from '../../../../src/domain/errors';

describe('EventStore', () => {
  let fs: SimulatedFileSystem;
  let clock: SimulatedClock;
  let serializer: MsgpackSerializer;
  let compressor: ZstdCompressor;
  let store: EventStore;

  beforeEach(async () => {
    fs = new SimulatedFileSystem();
    clock = new SimulatedClock();
    serializer = new MsgpackSerializer();
    compressor = new ZstdCompressor();
    store = new EventStore({
      fs,
      serializer,
      compressor,
      clock,
      autoFlushCount: 0, // Disable auto-flush for most tests
    });
    await store.open('/data/events');
  });

  afterEach(async () => {
    await store.close();
  });

  describe('lifecycle', () => {
    test('should open and close', async () => {
      expect(store.isOpen()).toBe(true);
      await store.close();
      expect(store.isOpen()).toBe(false);
    });

    test('should throw if already open', async () => {
      await expect(store.open('/data/other')).rejects.toThrow('already open');
    });

    test('should allow reopen after close', async () => {
      await store.close();
      await store.open('/data/events');
      expect(store.isOpen()).toBe(true);
    });

    test('should persist data between sessions', async () => {
      await store.append('stream-1', [{ type: 'Created', data: { name: 'test' } }]);
      await store.flush();
      await store.close();

      // Reopen
      const store2 = new EventStore({ fs, serializer, compressor, clock });
      await store2.open('/data/events');

      const events = await store2.readStream('stream-1');
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toEqual({ name: 'test' });

      await store2.close();
    });
  });

  describe('append', () => {
    test('should append single event', async () => {
      const result = await store.append('stream-1', [
        { type: 'UserCreated', data: { name: 'Alice' } },
      ]);
      await store.flush();

      expect(result.eventCount).toBe(1);
      expect(result.streamRevision).toBe(0);
      expect(result.globalPosition).toBe(0);
    });

    test('should append multiple events', async () => {
      const result = await store.append('stream-1', [
        { type: 'Event1', data: {} },
        { type: 'Event2', data: {} },
        { type: 'Event3', data: {} },
      ]);
      await store.flush();

      expect(result.eventCount).toBe(3);
      expect(result.streamRevision).toBe(2);
    });

    test('should increment revision', async () => {
      await store.append('stream-1', [{ type: 'Event', data: {} }]);
      await store.append('stream-1', [{ type: 'Event', data: {} }]);
      const result = await store.append('stream-1', [{ type: 'Event', data: {} }]);

      expect(result.streamRevision).toBe(2);
    });

    test('should increment global position', async () => {
      const r1 = await store.append('stream-1', [{ type: 'Event', data: {} }]);
      const r2 = await store.append('stream-2', [{ type: 'Event', data: {} }]);
      const r3 = await store.append('stream-1', [{ type: 'Event', data: {} }]);

      expect(r1.globalPosition).toBe(0);
      expect(r2.globalPosition).toBe(1);
      expect(r3.globalPosition).toBe(2);
    });

    test('should throw on empty events', async () => {
      await expect(store.append('stream-1', [])).rejects.toThrow('empty');
    });

    test('should throw if not open', async () => {
      await store.close();
      await expect(
        store.append('stream-1', [{ type: 'Event', data: {} }])
      ).rejects.toThrow('not open');
    });
  });

  describe('optimistic concurrency', () => {
    test('should accept matching expected revision', async () => {
      await store.append('stream-1', [{ type: 'Event', data: {} }]);
      await store.flush();

      // Should not throw
      await store.append('stream-1', [{ type: 'Event', data: {} }], {
        expectedRevision: 0,
      });
    });

    test('should throw on mismatched expected revision', async () => {
      await store.append('stream-1', [{ type: 'Event', data: {} }]);
      await store.append('stream-1', [{ type: 'Event', data: {} }]);
      await store.flush();

      await expect(
        store.append('stream-1', [{ type: 'Event', data: {} }], {
          expectedRevision: 0,
        })
      ).rejects.toThrow(ConcurrencyError);
    });

    test('should enforce stream-does-not-exist with -1', async () => {
      await store.append('stream-1', [{ type: 'Event', data: {} }]);
      await store.flush();

      await expect(
        store.append('stream-1', [{ type: 'Event', data: {} }], {
          expectedRevision: -1,
        })
      ).rejects.toThrow(ConcurrencyError);
    });

    test('should allow -1 for new stream', async () => {
      const result = await store.append(
        'new-stream',
        [{ type: 'Event', data: {} }],
        { expectedRevision: -1 }
      );

      expect(result.streamRevision).toBe(0);
    });
  });

  describe('readStream', () => {
    beforeEach(async () => {
      await store.append('user-123', [
        { type: 'Created', data: { name: 'Alice' } },
        { type: 'Updated', data: { name: 'Alice Smith' } },
        { type: 'Deleted', data: {} },
      ]);
      await store.flush();
    });

    test('should read all events from stream', async () => {
      const events = await store.readStream('user-123');

      expect(events).toHaveLength(3);
      expect(events[0]!.type).toBe('Created');
      expect(events[1]!.type).toBe('Updated');
      expect(events[2]!.type).toBe('Deleted');
    });

    test('should return empty for non-existent stream', async () => {
      const events = await store.readStream('unknown');

      expect(events).toHaveLength(0);
    });

    test('should filter by fromRevision', async () => {
      const events = await store.readStream('user-123', { fromRevision: 1 });

      expect(events).toHaveLength(2);
      expect(events[0]!.revision).toBe(1);
    });

    test('should filter by toRevision', async () => {
      const events = await store.readStream('user-123', { toRevision: 1 });

      expect(events).toHaveLength(2);
      expect(events[1]!.revision).toBe(1);
    });

    test('should limit with maxCount', async () => {
      const events = await store.readStream('user-123', { maxCount: 2 });

      expect(events).toHaveLength(2);
    });

    test('should read backward', async () => {
      const events = await store.readStream('user-123', { direction: 'backward' });

      expect(events).toHaveLength(3);
      expect(events[0]!.revision).toBe(2);
      expect(events[2]!.revision).toBe(0);
    });
  });

  describe('readGlobal', () => {
    beforeEach(async () => {
      await store.append('stream-a', [{ type: 'EventA', data: {} }]);
      await store.append('stream-b', [{ type: 'EventB', data: {} }]);
      await store.append('stream-a', [{ type: 'EventC', data: {} }]);
      await store.flush();
    });

    test('should read all events in global order', async () => {
      const events = await store.readGlobal();

      expect(events).toHaveLength(3);
      expect(events[0]!.type).toBe('EventA');
      expect(events[1]!.type).toBe('EventB');
      expect(events[2]!.type).toBe('EventC');
    });

    test('should filter by fromPosition', async () => {
      const events = await store.readGlobal(1);

      expect(events).toHaveLength(2);
      expect(events[0]!.globalPosition).toBe(1);
    });

    test('should limit with maxCount', async () => {
      const events = await store.readGlobal(0, { maxCount: 2 });

      expect(events).toHaveLength(2);
    });
  });

  describe('streamEvents', () => {
    test('should yield events one at a time', async () => {
      await store.append('stream-1', [
        { type: 'Event1', data: {} },
        { type: 'Event2', data: {} },
      ]);
      await store.flush();

      const events = [];
      for await (const event of store.streamEvents('stream-1')) {
        events.push(event.type);
      }

      expect(events).toEqual(['Event1', 'Event2']);
    });
  });

  describe('streamGlobal', () => {
    test('should yield events one at a time', async () => {
      await store.append('stream-a', [{ type: 'A', data: {} }]);
      await store.append('stream-b', [{ type: 'B', data: {} }]);
      await store.flush();

      const types = [];
      for await (const event of store.streamGlobal()) {
        types.push(event.type);
      }

      expect(types).toEqual(['A', 'B']);
    });
  });

  describe('getStreamRevision', () => {
    test('should return -1 for non-existent stream', () => {
      expect(store.getStreamRevision('unknown')).toBe(-1);
    });

    test('should return current revision', async () => {
      await store.append('stream-1', [{ type: 'E', data: {} }]);
      await store.append('stream-1', [{ type: 'E', data: {} }]);

      expect(store.getStreamRevision('stream-1')).toBe(1);
    });

    test('should include pending events', async () => {
      // Append but don't flush
      await store.append('stream-1', [{ type: 'E', data: {} }]);

      expect(store.getStreamRevision('stream-1')).toBe(0);
    });
  });

  describe('hasStream', () => {
    test('should return false for non-existent stream', () => {
      expect(store.hasStream('unknown')).toBe(false);
    });

    test('should return true for existing stream', async () => {
      await store.append('stream-1', [{ type: 'E', data: {} }]);

      expect(store.hasStream('stream-1')).toBe(true);
    });
  });

  describe('getStreamIds', () => {
    test('should return all stream IDs', async () => {
      await store.append('stream-c', [{ type: 'E', data: {} }]);
      await store.append('stream-a', [{ type: 'E', data: {} }]);
      await store.append('stream-b', [{ type: 'E', data: {} }]);
      await store.flush();

      const ids = await store.getStreamIds();

      expect(ids).toEqual(['stream-a', 'stream-b', 'stream-c']);
    });
  });

  describe('auto-flush', () => {
    test('should auto-flush when threshold reached', async () => {
      const autoStore = new EventStore({
        fs,
        serializer,
        compressor,
        clock,
        autoFlushCount: 3,
      });
      await autoStore.open('/data/auto');

      await autoStore.append('s', [{ type: 'E1', data: {} }]);
      await autoStore.append('s', [{ type: 'E2', data: {} }]);

      // Auto-flush should happen when we hit 3 events
      await autoStore.append('s', [{ type: 'E3', data: {} }]);

      // Verify events are persisted (would survive crash)
      const content = fs.getFileContent('/data/auto/segment-00000000.log');
      expect(content).toBeDefined();
      expect(content!.length).toBeGreaterThan(32); // More than just header

      await autoStore.close();
    });
  });

  describe('multi-tenancy', () => {
    test('should store tenant ID with events', async () => {
      await store.append(
        'stream-1',
        [{ type: 'Event', data: {} }],
        { tenantId: 'tenant-a' }
      );
      await store.flush();

      const events = await store.readStream('stream-1');
      expect(events[0]!.tenantId).toBe('tenant-a');
    });

    test('should default tenant ID to "default"', async () => {
      await store.append('stream-1', [{ type: 'Event', data: {} }]);
      await store.flush();

      const events = await store.readStream('stream-1');
      expect(events[0]!.tenantId).toBe('default');
    });
  });

  describe('metadata', () => {
    test('should store event metadata', async () => {
      await store.append('stream-1', [
        {
          type: 'Event',
          data: { value: 42 },
          metadata: { correlationId: 'abc-123', userId: 'user-1' },
        },
      ]);
      await store.flush();

      const events = await store.readStream('stream-1');
      expect(events[0]!.metadata).toEqual({
        correlationId: 'abc-123',
        userId: 'user-1',
      });
    });
  });

  describe('crash recovery', () => {
    test('should preserve flushed data after crash', async () => {
      // Use fresh instances to avoid afterEach cleanup issues
      const crashFs = new SimulatedFileSystem();
      const crashStore = new EventStore({
        fs: crashFs,
        serializer,
        compressor,
        clock,
        autoFlushCount: 0,
      });
      await crashStore.open('/data/events');

      await crashStore.append('stream-1', [{ type: 'Safe', data: {} }]);
      await crashStore.flush();

      // Simulate crash (this invalidates all handles)
      crashFs.crash();

      // Reopen with new store (old store is "dead" after crash)
      const store2 = new EventStore({ fs: crashFs, serializer, compressor, clock });
      await store2.open('/data/events');

      const events = await store2.readStream('stream-1');
      expect(events).toHaveLength(1);
      expect(events[0]!.type).toBe('Safe');

      await store2.close();
    });

    test('should lose unflushed data after crash', async () => {
      // Use fresh instances to avoid afterEach cleanup issues
      const crashFs = new SimulatedFileSystem();
      const crashStore = new EventStore({
        fs: crashFs,
        serializer,
        compressor,
        clock,
        autoFlushCount: 0,
      });
      await crashStore.open('/data/events');

      await crashStore.append('stream-1', [{ type: 'Safe', data: {} }]);
      await crashStore.flush();
      await crashStore.append('stream-1', [{ type: 'Lost', data: {} }]);
      // Don't flush

      // Simulate crash
      crashFs.crash();

      // Reopen
      const store2 = new EventStore({ fs: crashFs, serializer, compressor, clock });
      await store2.open('/data/events');

      const events = await store2.readStream('stream-1');
      expect(events).toHaveLength(1);
      expect(events[0]!.type).toBe('Safe');

      await store2.close();
    });

    test('readGlobal returns pending events without persisting them', async () => {
      const crashFs = new SimulatedFileSystem();
      const crashStore = new EventStore({
        fs: crashFs,
        serializer,
        compressor,
        clock,
        autoFlushCount: 0,
      });
      await crashStore.open('/data/events');

      await crashStore.append('stream-1', [{ type: 'Pending', data: {} }]);

      const events = await crashStore.readGlobal(0);
      expect(events).toHaveLength(1);

      expect(crashFs.getFileContent('/data/events/segment-00000000.log')).toBeUndefined();

      crashFs.crash();

      const store2 = new EventStore({ fs: crashFs, serializer, compressor, clock });
      await store2.open('/data/events');

      const recovered = await store2.readGlobal(0);
      expect(recovered).toHaveLength(0);

      await store2.close();
    });
  });
});
