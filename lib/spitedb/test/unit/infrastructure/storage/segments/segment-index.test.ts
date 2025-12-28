import { describe, test, expect, beforeEach } from 'bun:test';
import { SegmentIndex } from '../../../../../src/infrastructure/storage/segments/segment-index';
import { SegmentWriter } from '../../../../../src/infrastructure/storage/segments/segment-writer';
import { SegmentReader } from '../../../../../src/infrastructure/storage/segments/segment-reader';
import { SimulatedFileSystem } from '../../../../../src/testing/simulated-filesystem';
import { MsgpackSerializer } from '../../../../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../../../../src/infrastructure/serialization/zstd-compressor';
import type { StoredEvent } from '../../../../../src/domain/events/stored-event';

describe('SegmentIndex', () => {
  let index: SegmentIndex;

  beforeEach(() => {
    index = new SegmentIndex();
  });

  function createEvent(overrides: Partial<StoredEvent> = {}): StoredEvent {
    return {
      streamId: 'test-stream',
      type: 'TestEvent',
      data: { message: 'hello' },
      revision: 0,
      globalPosition: 0,
      timestamp: Date.now(),
      tenantId: 'default',
      ...overrides,
    };
  }

  describe('addBatch', () => {
    test('should add events to stream index', () => {
      const events = [
        createEvent({ streamId: 'stream-1', revision: 0, globalPosition: 0 }),
        createEvent({ streamId: 'stream-1', revision: 1, globalPosition: 1 }),
      ];

      index.addBatch(1n, 100, events);

      expect(index.hasStream('stream-1')).toBe(true);
      expect(index.getStreamEventCount('stream-1')).toBe(2);
    });

    test('should add events to position index', () => {
      const events = [
        createEvent({ streamId: 'stream-1', revision: 0, globalPosition: 42 }),
      ];

      index.addBatch(1n, 100, events);

      const location = index.findByPosition(42);
      expect(location).toBeDefined();
      expect(location!.segmentId).toBe(1n);
      expect(location!.batchOffset).toBe(100);
      expect(location!.streamId).toBe('stream-1');
    });

    test('should track indexed segments', () => {
      index.addBatch(1n, 100, [createEvent()]);

      expect(index.isSegmentIndexed(1n)).toBe(true);
      expect(index.isSegmentIndexed(2n)).toBe(false);
    });
  });

  describe('findByStream', () => {
    beforeEach(() => {
      // Add events across multiple batches
      index.addBatch(1n, 100, [
        createEvent({ streamId: 'user-123', revision: 0, globalPosition: 0 }),
        createEvent({ streamId: 'user-123', revision: 1, globalPosition: 1 }),
      ]);
      index.addBatch(1n, 200, [
        createEvent({ streamId: 'user-123', revision: 2, globalPosition: 2 }),
        createEvent({ streamId: 'user-456', revision: 0, globalPosition: 3 }),
      ]);
      index.addBatch(2n, 100, [
        createEvent({ streamId: 'user-123', revision: 3, globalPosition: 4 }),
      ]);
    });

    test('should find all events for a stream', () => {
      const entries = index.findByStream('user-123');

      expect(entries).toHaveLength(4);
      expect(entries.map((e) => e.revision)).toEqual([0, 1, 2, 3]);
    });

    test('should return empty array for unknown stream', () => {
      const entries = index.findByStream('unknown');

      expect(entries).toHaveLength(0);
    });

    test('should filter by fromRevision', () => {
      const entries = index.findByStream('user-123', { fromRevision: 2 });

      expect(entries).toHaveLength(2);
      expect(entries.map((e) => e.revision)).toEqual([2, 3]);
    });

    test('should filter by toRevision', () => {
      const entries = index.findByStream('user-123', { toRevision: 1 });

      expect(entries).toHaveLength(2);
      expect(entries.map((e) => e.revision)).toEqual([0, 1]);
    });

    test('should filter by revision range', () => {
      const entries = index.findByStream('user-123', { fromRevision: 1, toRevision: 2 });

      expect(entries).toHaveLength(2);
      expect(entries.map((e) => e.revision)).toEqual([1, 2]);
    });

    test('should support backward direction', () => {
      const entries = index.findByStream('user-123', { direction: 'backward' });

      expect(entries).toHaveLength(4);
      expect(entries.map((e) => e.revision)).toEqual([3, 2, 1, 0]);
    });

    test('should combine filters with direction', () => {
      const entries = index.findByStream('user-123', {
        fromRevision: 1,
        toRevision: 2,
        direction: 'backward',
      });

      expect(entries).toHaveLength(2);
      expect(entries.map((e) => e.revision)).toEqual([2, 1]);
    });
  });

  describe('findByPosition', () => {
    test('should find event by global position', () => {
      index.addBatch(1n, 100, [
        createEvent({ streamId: 'stream-1', globalPosition: 42 }),
      ]);

      const location = index.findByPosition(42);

      expect(location).toBeDefined();
      expect(location!.segmentId).toBe(1n);
      expect(location!.batchOffset).toBe(100);
    });

    test('should return undefined for unknown position', () => {
      const location = index.findByPosition(999);

      expect(location).toBeUndefined();
    });
  });

  describe('getStreamRevision', () => {
    test('should return latest revision', () => {
      index.addBatch(1n, 100, [
        createEvent({ streamId: 'stream-1', revision: 0 }),
        createEvent({ streamId: 'stream-1', revision: 1 }),
        createEvent({ streamId: 'stream-1', revision: 2 }),
      ]);

      expect(index.getStreamRevision('stream-1')).toBe(2);
    });

    test('should return -1 for unknown stream', () => {
      expect(index.getStreamRevision('unknown')).toBe(-1);
    });
  });

  describe('evictSegment', () => {
    beforeEach(() => {
      index.addBatch(1n, 100, [
        createEvent({ streamId: 'stream-1', revision: 0, globalPosition: 0 }),
        createEvent({ streamId: 'stream-1', revision: 1, globalPosition: 1 }),
      ]);
      index.addBatch(2n, 100, [
        createEvent({ streamId: 'stream-1', revision: 2, globalPosition: 2 }),
        createEvent({ streamId: 'stream-2', revision: 0, globalPosition: 3 }),
      ]);
    });

    test('should remove entries from stream index', () => {
      expect(index.getStreamEventCount('stream-1')).toBe(3);

      index.evictSegment(1n);

      expect(index.getStreamEventCount('stream-1')).toBe(1);
      expect(index.findByStream('stream-1')[0]!.revision).toBe(2);
    });

    test('should remove entries from position index', () => {
      expect(index.findByPosition(0)).toBeDefined();
      expect(index.findByPosition(2)).toBeDefined();

      index.evictSegment(1n);

      expect(index.findByPosition(0)).toBeUndefined();
      expect(index.findByPosition(1)).toBeUndefined();
      expect(index.findByPosition(2)).toBeDefined();
    });

    test('should remove empty streams', () => {
      // stream-2 only has events in segment 2
      expect(index.hasStream('stream-2')).toBe(true);

      index.evictSegment(2n);

      expect(index.hasStream('stream-2')).toBe(false);
    });

    test('should update indexed segments set', () => {
      expect(index.isSegmentIndexed(1n)).toBe(true);

      index.evictSegment(1n);

      expect(index.isSegmentIndexed(1n)).toBe(false);
    });

    test('should handle evicting non-indexed segment', () => {
      // Should not throw
      index.evictSegment(999n);

      expect(index.getStats().totalEntries).toBe(4);
    });
  });

  describe('getStreamIds', () => {
    test('should return all stream IDs', () => {
      index.addBatch(1n, 100, [
        createEvent({ streamId: 'stream-a' }),
        createEvent({ streamId: 'stream-b' }),
        createEvent({ streamId: 'stream-c' }),
      ]);

      const ids = index.getStreamIds();

      expect(ids.sort()).toEqual(['stream-a', 'stream-b', 'stream-c']);
    });
  });

  describe('getStats', () => {
    test('should return correct statistics', () => {
      index.addBatch(1n, 100, [
        createEvent({ streamId: 'stream-1' }),
        createEvent({ streamId: 'stream-1' }),
      ]);
      index.addBatch(2n, 100, [
        createEvent({ streamId: 'stream-2' }),
      ]);

      const stats = index.getStats();

      expect(stats.streamCount).toBe(2);
      expect(stats.totalEntries).toBe(3);
      expect(stats.segmentCount).toBe(2);
    });
  });

  describe('clear', () => {
    test('should remove all data', () => {
      index.addBatch(1n, 100, [
        createEvent({ streamId: 'stream-1', globalPosition: 0 }),
      ]);

      index.clear();

      expect(index.hasStream('stream-1')).toBe(false);
      expect(index.findByPosition(0)).toBeUndefined();
      expect(index.isSegmentIndexed(1n)).toBe(false);
      expect(index.getStats().totalEntries).toBe(0);
    });
  });

  describe('rebuildFromSegment', () => {
    let fs: SimulatedFileSystem;
    let serializer: MsgpackSerializer;
    let compressor: ZstdCompressor;
    let writer: SegmentWriter;
    let reader: SegmentReader;

    beforeEach(() => {
      fs = new SimulatedFileSystem();
      serializer = new MsgpackSerializer();
      compressor = new ZstdCompressor();
      writer = new SegmentWriter(fs, serializer, compressor);
      reader = new SegmentReader(fs, serializer, compressor);
    });

    test('should rebuild index from segment file', async () => {
      // Write a segment with multiple batches
      await writer.open('/data/segment.log', 1n, 0);
      await writer.appendBatch([
        createEvent({ streamId: 'stream-1', revision: 0, globalPosition: 0 }),
        createEvent({ streamId: 'stream-1', revision: 1, globalPosition: 1 }),
      ]);
      await writer.appendBatch([
        createEvent({ streamId: 'stream-2', revision: 0, globalPosition: 2 }),
      ]);
      await writer.sync();
      await writer.close();

      // Rebuild index
      const result = await index.rebuildFromSegment(reader, '/data/segment.log', 1n);

      expect(result.batchCount).toBe(2);
      expect(result.eventCount).toBe(3);
      expect(index.hasStream('stream-1')).toBe(true);
      expect(index.hasStream('stream-2')).toBe(true);
      expect(index.findByPosition(0)).toBeDefined();
      expect(index.findByPosition(2)).toBeDefined();
    });

    test('should evict existing entries before rebuilding', async () => {
      // Pre-populate index with different data
      index.addBatch(1n, 999, [
        createEvent({ streamId: 'old-stream', revision: 0, globalPosition: 999 }),
      ]);

      // Write new segment
      await writer.open('/data/segment.log', 1n, 0);
      await writer.appendBatch([
        createEvent({ streamId: 'new-stream', revision: 0, globalPosition: 0 }),
      ]);
      await writer.sync();
      await writer.close();

      // Rebuild
      await index.rebuildFromSegment(reader, '/data/segment.log', 1n);

      expect(index.hasStream('old-stream')).toBe(false);
      expect(index.hasStream('new-stream')).toBe(true);
      expect(index.findByPosition(999)).toBeUndefined();
    });
  });
});
