/**
 * DST Edge Case Tests
 *
 * Tests boundary conditions and edge cases that might not be covered
 * by fuzz testing:
 * - Unicode stream IDs (emojis, CJK characters, RTL text)
 * - Empty event payloads
 * - Very large event payloads (near segment boundary)
 * - Very long stream IDs
 * - Exact segment boundary conditions
 * - Special characters in stream IDs
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { EventStore } from '../../src/application/event-store';
import { SimulatedFileSystem } from '../../src/testing/simulated-filesystem';
import { SimulatedClock } from '../../src/testing/simulated-clock';
import { MsgpackSerializer } from '../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../src/infrastructure/serialization/zstd-compressor';

describe('Edge Cases DST', () => {
  let fs: SimulatedFileSystem;
  let clock: SimulatedClock;
  let serializer: MsgpackSerializer;
  let compressor: ZstdCompressor;

  beforeEach(() => {
    fs = new SimulatedFileSystem();
    clock = new SimulatedClock();
    serializer = new MsgpackSerializer();
    compressor = new ZstdCompressor();
  });

  function createStore(options?: { maxSegmentSize?: number }): EventStore {
    return new EventStore({
      fs,
      serializer,
      compressor,
      clock,
      autoFlushCount: 0,
      maxSegmentSize: options?.maxSegmentSize,
    });
  }

  describe('Unicode stream IDs', () => {
    test('should handle emoji stream IDs', async () => {
      const store = createStore();
      await store.open('/data/events');

      const streamId = '🚀-user-123-🎉';
      await store.append(streamId, [
        { type: 'Created', data: { name: 'Test' } },
        { type: 'Updated', data: { name: 'Test Updated' } },
      ]);
      await store.flush();

      const events = await store.readStream(streamId);
      expect(events).toHaveLength(2);
      expect(events[0]!.streamId).toBe(streamId);
      expect(events[1]!.streamId).toBe(streamId);

      // Verify stream ID roundtrips through index
      const ids = await store.getStreamIds();
      expect(ids).toContain(streamId);

      await store.close();
    });

    test('should handle CJK characters in stream IDs', async () => {
      const store = createStore();
      await store.open('/data/events');

      const streamIds = [
        '用户-12345', // Chinese
        'ユーザー-67890', // Japanese
        '사용자-11111', // Korean
      ];

      for (const streamId of streamIds) {
        await store.append(streamId, [
          { type: 'Created', data: { id: streamId } },
        ]);
      }
      await store.flush();

      for (const streamId of streamIds) {
        const events = await store.readStream(streamId);
        expect(events).toHaveLength(1);
        expect(events[0]!.streamId).toBe(streamId);
      }

      await store.close();
    });

    test('should handle RTL text in stream IDs', async () => {
      const store = createStore();
      await store.open('/data/events');

      const streamId = 'مستخدم-123'; // Arabic
      await store.append(streamId, [
        { type: 'Created', data: {} },
      ]);
      await store.flush();

      const events = await store.readStream(streamId);
      expect(events).toHaveLength(1);
      expect(events[0]!.streamId).toBe(streamId);

      await store.close();
    });

    test('should handle mixed Unicode and ASCII', async () => {
      const store = createStore();
      await store.open('/data/events');

      const streamId = 'user_αβγ_123_δεζ';
      await store.append(streamId, [{ type: 'Test', data: {} }]);
      await store.flush();

      const events = await store.readStream(streamId);
      expect(events).toHaveLength(1);

      await store.close();
    });
  });

  describe('Empty and null payloads', () => {
    test('should handle empty object payload', async () => {
      const store = createStore();
      await store.open('/data/events');

      await store.append('stream', [{ type: 'EmptyData', data: {} }]);
      await store.flush();

      const events = await store.readStream('stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toEqual({});

      await store.close();
    });

    test('should handle null payload', async () => {
      const store = createStore();
      await store.open('/data/events');

      await store.append('stream', [{ type: 'NullData', data: null }]);
      await store.flush();

      const events = await store.readStream('stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toBeNull();

      await store.close();
    });

    test('should handle undefined metadata', async () => {
      const store = createStore();
      await store.open('/data/events');

      await store.append('stream', [
        { type: 'NoMeta', data: { value: 1 } },
        { type: 'WithMeta', data: { value: 2 }, metadata: { key: 'value' } },
      ]);
      await store.flush();

      const events = await store.readStream('stream');
      expect(events).toHaveLength(2);
      expect(events[0]!.metadata).toBeUndefined();
      expect(events[1]!.metadata).toEqual({ key: 'value' });

      await store.close();
    });
  });

  describe('Large payloads', () => {
    test('should handle large event payload (100KB)', async () => {
      const store = createStore();
      await store.open('/data/events');

      const largePayload = {
        data: 'x'.repeat(100 * 1024),
        nested: {
          array: Array(1000).fill('item'),
        },
      };

      await store.append('stream', [{ type: 'LargeEvent', data: largePayload }]);
      await store.flush();

      const events = await store.readStream('stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toEqual(largePayload);

      await store.close();
    });

    test('should handle many small events in one batch', async () => {
      const store = createStore();
      await store.open('/data/events');

      const events = Array.from({ length: 1000 }, (_, i) => ({
        type: 'SmallEvent',
        data: { index: i },
      }));

      await store.append('stream', events);
      await store.flush();

      const readEvents = await store.readStream('stream');
      expect(readEvents).toHaveLength(1000);

      // Verify ordering
      for (let i = 0; i < 1000; i++) {
        expect((readEvents[i]!.data as { index: number }).index).toBe(i);
      }

      await store.close();
    });
  });

  describe('Long stream IDs', () => {
    test('should handle very long stream ID (1000 chars)', async () => {
      const store = createStore();
      await store.open('/data/events');

      const streamId = 'stream-' + 'x'.repeat(993); // 1000 total
      await store.append(streamId, [{ type: 'Test', data: {} }]);
      await store.flush();

      const events = await store.readStream(streamId);
      expect(events).toHaveLength(1);
      expect(events[0]!.streamId).toBe(streamId);

      await store.close();
    });

    test('should handle stream ID with special characters', async () => {
      const store = createStore();
      await store.open('/data/events');

      const specialIds = [
        'stream/with/slashes',
        'stream:with:colons',
        'stream.with.dots',
        'stream-with-dashes',
        'stream_with_underscores',
        'stream@with@at',
        'stream#with#hash',
      ];

      for (const streamId of specialIds) {
        await store.append(streamId, [{ type: 'Test', data: { id: streamId } }]);
      }
      await store.flush();

      for (const streamId of specialIds) {
        const events = await store.readStream(streamId);
        expect(events).toHaveLength(1);
        expect(events[0]!.streamId).toBe(streamId);
      }

      await store.close();
    });
  });

  describe('Segment boundary conditions', () => {
    test('should handle event at exact segment boundary', async () => {
      // Use small segment size to trigger rotation
      const store = createStore({ maxSegmentSize: 4096 });
      await store.open('/data/events');

      // Write events until we're close to segment boundary
      let totalWritten = 0;
      const targetSize = 4000; // Just under 4KB

      while (totalWritten < targetSize) {
        const payload = 'x'.repeat(200);
        await store.append('stream', [{ type: 'FillEvent', data: { payload } }]);
        totalWritten += 300; // Approximate size with overhead
      }
      await store.flush();

      // Now write one more event that should trigger rotation
      await store.append('stream', [{ type: 'BoundaryEvent', data: { large: 'y'.repeat(500) } }]);
      await store.flush();

      // Verify all events readable
      const events = await store.readStream('stream');
      expect(events.length).toBeGreaterThan(0);
      expect(events[events.length - 1]!.type).toBe('BoundaryEvent');

      await store.close();
    });

    test('should handle segment rotation mid-batch', async () => {
      const store = createStore({ maxSegmentSize: 2048 });
      await store.open('/data/events');

      // Write batch that should span segment boundary
      const events = Array.from({ length: 50 }, (_, i) => ({
        type: 'RotationEvent',
        data: { index: i, payload: 'x'.repeat(100) },
      }));

      await store.append('stream', events);
      await store.flush();

      // Verify all events
      const readEvents = await store.readStream('stream');
      expect(readEvents).toHaveLength(50);

      await store.close();
    });
  });

  describe('Recovery after boundary scenarios', () => {
    test('should recover unicode stream IDs after crash', async () => {
      const store1 = createStore();
      await store1.open('/data/events');

      const unicodeIds = ['🔥-stream', '日本語', 'العربية'];
      for (const id of unicodeIds) {
        await store1.append(id, [{ type: 'Test', data: {} }]);
      }
      await store1.flush();
      await store1.close();

      // Reopen and verify
      const store2 = createStore();
      await store2.open('/data/events');

      for (const id of unicodeIds) {
        const events = await store2.readStream(id);
        expect(events).toHaveLength(1);
      }

      await store2.close();
    });

    test('should recover after writing large payload', async () => {
      const store1 = createStore();
      await store1.open('/data/events');

      const largeData = { payload: 'x'.repeat(50000) };
      await store1.append('stream', [{ type: 'Large', data: largeData }]);
      await store1.flush();
      await store1.close();

      // Reopen and verify
      const store2 = createStore();
      await store2.open('/data/events');

      const events = await store2.readStream('stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toEqual(largeData);

      await store2.close();
    });
  });

  describe('Global position consistency', () => {
    test('should maintain consistent global position with unicode streams', async () => {
      const store = createStore();
      await store.open('/data/events');

      // Write to multiple unicode streams
      await store.append('stream-α', [{ type: 'A', data: {} }]);
      await store.append('stream-β', [{ type: 'B', data: {} }]);
      await store.append('stream-γ', [{ type: 'C', data: {} }]);
      await store.flush();

      const global = await store.readGlobal(0);
      expect(global).toHaveLength(3);

      // Verify global position ordering
      expect(global[0]!.globalPosition).toBe(0);
      expect(global[1]!.globalPosition).toBe(1);
      expect(global[2]!.globalPosition).toBe(2);

      await store.close();
    });

    test('should maintain global position across segment rotation', async () => {
      const store = createStore({ maxSegmentSize: 2048 });
      await store.open('/data/events');

      // Write enough to trigger multiple rotations
      for (let i = 0; i < 100; i++) {
        await store.append(`stream-${i % 5}`, [
          { type: 'Event', data: { index: i, padding: 'x'.repeat(50) } },
        ]);
      }
      await store.flush();

      const global = await store.readGlobal(0);
      expect(global).toHaveLength(100);

      // Verify strict ordering
      for (let i = 0; i < 100; i++) {
        expect(global[i]!.globalPosition).toBe(i);
      }

      await store.close();
    });
  });

  describe('Event type edge cases', () => {
    test('should handle empty event type', async () => {
      const store = createStore();
      await store.open('/data/events');

      await store.append('stream', [{ type: '', data: { value: 1 } }]);
      await store.flush();

      const events = await store.readStream('stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.type).toBe('');

      await store.close();
    });

    test('should handle very long event type', async () => {
      const store = createStore();
      await store.open('/data/events');

      const longType = 'EventType_' + 'x'.repeat(500);
      await store.append('stream', [{ type: longType, data: {} }]);
      await store.flush();

      const events = await store.readStream('stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.type).toBe(longType);

      await store.close();
    });

    test('should handle unicode event types', async () => {
      const store = createStore();
      await store.open('/data/events');

      await store.append('stream', [
        { type: '用户创建', data: {} },
        { type: 'ユーザー更新', data: {} },
        { type: '🎉PartyEvent🎊', data: {} },
      ]);
      await store.flush();

      const events = await store.readStream('stream');
      expect(events).toHaveLength(3);
      expect(events[0]!.type).toBe('用户创建');
      expect(events[1]!.type).toBe('ユーザー更新');
      expect(events[2]!.type).toBe('🎉PartyEvent🎊');

      await store.close();
    });
  });

  describe('Nested and complex data structures', () => {
    test('should handle deeply nested objects', async () => {
      const store = createStore();
      await store.open('/data/events');

      const deeplyNested = {
        level1: {
          level2: {
            level3: {
              level4: {
                level5: {
                  value: 'deep',
                },
              },
            },
          },
        },
      };

      await store.append('stream', [{ type: 'DeepEvent', data: deeplyNested }]);
      await store.flush();

      const events = await store.readStream('stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toEqual(deeplyNested);

      await store.close();
    });

    test('should handle arrays with mixed types', async () => {
      const store = createStore();
      await store.open('/data/events');

      const mixedArray = {
        items: [
          1,
          'string',
          true,
          null,
          { nested: 'object' },
          [1, 2, 3],
        ],
      };

      await store.append('stream', [{ type: 'MixedEvent', data: mixedArray }]);
      await store.flush();

      const events = await store.readStream('stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toEqual(mixedArray);

      await store.close();
    });

    test('should handle binary-like data in base64', async () => {
      const store = createStore();
      await store.open('/data/events');

      // Simulate binary data as base64 string
      const binaryData = {
        content: Buffer.from('Hello, World! 🌍').toString('base64'),
        type: 'base64',
      };

      await store.append('stream', [{ type: 'BinaryEvent', data: binaryData }]);
      await store.flush();

      const events = await store.readStream('stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toEqual(binaryData);

      await store.close();
    });
  });
});
