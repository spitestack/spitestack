import { describe, test, expect, beforeEach } from 'bun:test';
import { StreamMap } from '../../../src/storage/stream-map';

describe('StreamMap', () => {
  let map: StreamMap;

  beforeEach(() => {
    map = new StreamMap();
  });

  describe('getRevision', () => {
    test('should return -1 for non-existent stream', () => {
      expect(map.getRevision('unknown')).toBe(-1);
    });

    test('should return latest revision after update', () => {
      map.updateStream('stream-1', 0, 1n);
      expect(map.getRevision('stream-1')).toBe(0);

      map.updateStream('stream-1', 1, 1n);
      expect(map.getRevision('stream-1')).toBe(1);

      map.updateStream('stream-1', 2, 1n);
      expect(map.getRevision('stream-1')).toBe(2);
    });

    test('should not decrease revision', () => {
      map.updateStream('stream-1', 5, 1n);
      map.updateStream('stream-1', 3, 1n); // Lower revision
      expect(map.getRevision('stream-1')).toBe(5);
    });
  });

  describe('getSegments', () => {
    test('should return empty array for non-existent stream', () => {
      expect(map.getSegments('unknown')).toEqual([]);
    });

    test('should return single segment', () => {
      map.updateStream('stream-1', 0, 1n);
      expect(map.getSegments('stream-1')).toEqual([1n]);
    });

    test('should accumulate unique segments', () => {
      map.updateStream('stream-1', 0, 1n);
      map.updateStream('stream-1', 1, 1n); // Same segment
      map.updateStream('stream-1', 2, 2n); // New segment
      map.updateStream('stream-1', 3, 3n); // New segment

      expect(map.getSegments('stream-1')).toEqual([1n, 2n, 3n]);
    });

    test('should not duplicate segments', () => {
      map.updateStream('stream-1', 0, 1n);
      map.updateStream('stream-1', 1, 1n);
      map.updateStream('stream-1', 2, 1n);

      expect(map.getSegments('stream-1')).toEqual([1n]);
    });
  });

  describe('hasStream', () => {
    test('should return false for non-existent stream', () => {
      expect(map.hasStream('unknown')).toBe(false);
    });

    test('should return true for existing stream', () => {
      map.updateStream('stream-1', 0, 1n);
      expect(map.hasStream('stream-1')).toBe(true);
    });
  });

  describe('updateStream', () => {
    test('should create new stream on first update', () => {
      expect(map.hasStream('new-stream')).toBe(false);
      map.updateStream('new-stream', 0, 1n);
      expect(map.hasStream('new-stream')).toBe(true);
    });

    test('should handle multiple streams independently', () => {
      map.updateStream('stream-a', 0, 1n);
      map.updateStream('stream-b', 0, 1n);
      map.updateStream('stream-a', 1, 2n);
      map.updateStream('stream-b', 1, 3n);

      expect(map.getRevision('stream-a')).toBe(1);
      expect(map.getRevision('stream-b')).toBe(1);
      expect(map.getSegments('stream-a')).toEqual([1n, 2n]);
      expect(map.getSegments('stream-b')).toEqual([1n, 3n]);
    });
  });

  describe('clear', () => {
    test('should remove all streams', () => {
      map.updateStream('stream-1', 0, 1n);
      map.updateStream('stream-2', 0, 1n);
      map.updateStream('stream-3', 0, 1n);

      expect(map.getStreamCount()).toBe(3);

      map.clear();

      expect(map.getStreamCount()).toBe(0);
      expect(map.hasStream('stream-1')).toBe(false);
      expect(map.hasStream('stream-2')).toBe(false);
      expect(map.hasStream('stream-3')).toBe(false);
    });
  });

  describe('getStreamCount', () => {
    test('should return 0 for empty map', () => {
      expect(map.getStreamCount()).toBe(0);
    });

    test('should count unique streams', () => {
      map.updateStream('stream-1', 0, 1n);
      expect(map.getStreamCount()).toBe(1);

      map.updateStream('stream-2', 0, 1n);
      expect(map.getStreamCount()).toBe(2);

      map.updateStream('stream-1', 1, 1n); // Update existing
      expect(map.getStreamCount()).toBe(2);
    });
  });

  describe('getAllStreamIds', () => {
    test('should return empty array for empty map', () => {
      expect(map.getAllStreamIds()).toEqual([]);
    });

    test('should return sorted stream IDs', () => {
      map.updateStream('zebra', 0, 1n);
      map.updateStream('apple', 0, 1n);
      map.updateStream('mango', 0, 1n);

      expect(map.getAllStreamIds()).toEqual(['apple', 'mango', 'zebra']);
    });
  });

  describe('getStats', () => {
    test('should return correct statistics', () => {
      expect(map.getStats()).toEqual({ streamCount: 0, totalSegmentRefs: 0 });

      map.updateStream('stream-1', 0, 1n);
      map.updateStream('stream-1', 1, 2n);
      map.updateStream('stream-2', 0, 1n);
      map.updateStream('stream-3', 0, 3n);
      map.updateStream('stream-3', 1, 4n);
      map.updateStream('stream-3', 2, 5n);

      expect(map.getStats()).toEqual({
        streamCount: 3,
        totalSegmentRefs: 6, // stream-1: 2, stream-2: 1, stream-3: 3
      });
    });
  });

  describe('evictSegment', () => {
    beforeEach(() => {
      // Set up test data:
      // stream-1: segments [1, 2]
      // stream-2: segments [1]
      // stream-3: segments [2, 3]
      map.updateStream('stream-1', 0, 1n);
      map.updateStream('stream-1', 1, 2n);
      map.updateStream('stream-2', 0, 1n);
      map.updateStream('stream-3', 0, 2n);
      map.updateStream('stream-3', 1, 3n);
    });

    test('should remove segment from streams', () => {
      map.evictSegment(1n);

      expect(map.getSegments('stream-1')).toEqual([2n]);
      expect(map.getSegments('stream-3')).toEqual([2n, 3n]);
    });

    test('should remove streams that only had evicted segment', () => {
      expect(map.hasStream('stream-2')).toBe(true);

      map.evictSegment(1n);

      expect(map.hasStream('stream-2')).toBe(false);
    });

    test('should handle evicting non-existent segment', () => {
      // Should not throw
      map.evictSegment(999n);

      expect(map.getStreamCount()).toBe(3);
    });

    test('should update stream count after eviction', () => {
      expect(map.getStreamCount()).toBe(3);

      map.evictSegment(1n);

      expect(map.getStreamCount()).toBe(2); // stream-2 removed
    });
  });

  describe('serialization', () => {
    test('should serialize to JSON', () => {
      map.updateStream('stream-1', 5, 1n);
      map.updateStream('stream-1', 10, 2n);
      map.updateStream('stream-2', 3, 3n);

      const json = map.toJSON();

      expect(json.streams).toHaveLength(2);

      const stream1 = json.streams.find((s) => s.streamId === 'stream-1');
      expect(stream1).toEqual({
        streamId: 'stream-1',
        latestRevision: 10,
        segments: ['1', '2'],
      });

      const stream2 = json.streams.find((s) => s.streamId === 'stream-2');
      expect(stream2).toEqual({
        streamId: 'stream-2',
        latestRevision: 3,
        segments: ['3'],
      });
    });

    test('should deserialize from JSON', () => {
      const json = {
        streams: [
          { streamId: 'stream-a', latestRevision: 100, segments: ['10', '20'] },
          { streamId: 'stream-b', latestRevision: 50, segments: ['30'] },
        ],
      };

      const restored = StreamMap.fromJSON(json);

      expect(restored.getRevision('stream-a')).toBe(100);
      expect(restored.getSegments('stream-a')).toEqual([10n, 20n]);
      expect(restored.getRevision('stream-b')).toBe(50);
      expect(restored.getSegments('stream-b')).toEqual([30n]);
    });

    test('should round-trip through JSON', () => {
      map.updateStream('stream-1', 5, 1n);
      map.updateStream('stream-1', 10, 2n);
      map.updateStream('stream-2', 3, 3n);

      const json = map.toJSON();
      const restored = StreamMap.fromJSON(json);

      expect(restored.getRevision('stream-1')).toBe(10);
      expect(restored.getSegments('stream-1')).toEqual([1n, 2n]);
      expect(restored.getRevision('stream-2')).toBe(3);
      expect(restored.getSegments('stream-2')).toEqual([3n]);
      expect(restored.getStreamCount()).toBe(2);
    });
  });

  describe('edge cases', () => {
    test('should handle empty stream ID', () => {
      map.updateStream('', 0, 1n);
      expect(map.hasStream('')).toBe(true);
      expect(map.getRevision('')).toBe(0);
    });

    test('should handle very large segment IDs', () => {
      const largeId = BigInt('9223372036854775807'); // Max int64
      map.updateStream('stream-1', 0, largeId);
      expect(map.getSegments('stream-1')).toEqual([largeId]);
    });

    test('should handle very large revisions', () => {
      const largeRevision = Number.MAX_SAFE_INTEGER;
      map.updateStream('stream-1', largeRevision, 1n);
      expect(map.getRevision('stream-1')).toBe(largeRevision);
    });

    test('should handle many streams', () => {
      for (let i = 0; i < 10000; i++) {
        map.updateStream(`stream-${i}`, 0, 1n);
      }
      expect(map.getStreamCount()).toBe(10000);
    });

    test('should handle stream spanning many segments', () => {
      for (let i = 0; i < 1000; i++) {
        map.updateStream('stream-1', i, BigInt(i));
      }
      expect(map.getSegments('stream-1')).toHaveLength(1000);
      expect(map.getRevision('stream-1')).toBe(999);
    });
  });
});
