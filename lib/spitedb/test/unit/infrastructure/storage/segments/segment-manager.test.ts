import { describe, test, expect, beforeEach } from 'bun:test';
import { SegmentManager } from '../../../../../src/infrastructure/storage/segments/segment-manager';
import { SimulatedFileSystem } from '../../../../../src/testing/simulated-filesystem';
import { MsgpackSerializer } from '../../../../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../../../../src/infrastructure/serialization/zstd-compressor';
import { SEGMENT_HEADER_SIZE } from '../../../../../src/infrastructure/storage/segments/segment-header';
import type { StoredEvent } from '../../../../../src/domain/events/stored-event';

describe('SegmentManager', () => {
  let fs: SimulatedFileSystem;
  let serializer: MsgpackSerializer;
  let compressor: ZstdCompressor;

  beforeEach(() => {
    fs = new SimulatedFileSystem();
    serializer = new MsgpackSerializer();
    compressor = new ZstdCompressor();
  });

  function createManager(config?: { maxSegmentSize?: number; indexCacheSize?: number }) {
    return new SegmentManager(fs, serializer, compressor, {
      dataDir: '/data',
      maxSegmentSize: config?.maxSegmentSize,
      indexCacheSize: config?.indexCacheSize,
    });
  }

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

  describe('initialize', () => {
    test('should create data directory if it does not exist', async () => {
      const manager = createManager();
      await manager.initialize();

      expect(await fs.exists('/data')).toBe(true);

      await manager.close();
    });

    test('should scan existing segments', async () => {
      // Create segments manually first
      const manager1 = createManager();
      await manager1.initialize();
      await manager1.writeBatch([createEvent({ globalPosition: 0 })]);
      await manager1.sync();
      await manager1.close();

      // Create new manager and verify it finds the segment
      const manager2 = createManager();
      await manager2.initialize();

      const segments = manager2.getSegments();
      expect(segments.length).toBeGreaterThanOrEqual(1);

      await manager2.close();
    });

    test('should throw if already initialized', async () => {
      const manager = createManager();
      await manager.initialize();

      await expect(manager.initialize()).rejects.toThrow('already initialized');

      await manager.close();
    });

    test('should track global position from existing segments', async () => {
      // Write some events
      const manager1 = createManager();
      await manager1.initialize();
      await manager1.writeBatch([
        createEvent({ globalPosition: 0 }),
        createEvent({ globalPosition: 1 }),
        createEvent({ globalPosition: 2 }),
      ]);
      await manager1.sync();
      await manager1.close();

      // New manager should pick up where we left off
      const manager2 = createManager();
      await manager2.initialize();

      expect(manager2.getGlobalPosition()).toBe(3);

      await manager2.close();
    });
  });

  describe('getWriter', () => {
    test('should create segment on first call', async () => {
      const manager = createManager();
      await manager.initialize();

      const writer = await manager.getWriter();

      expect(writer).toBeDefined();
      expect(writer.isOpen).toBe(true);

      await manager.close();
    });

    test('should return same writer on subsequent calls', async () => {
      const manager = createManager();
      await manager.initialize();

      const writer1 = await manager.getWriter();
      const writer2 = await manager.getWriter();

      expect(writer1).toBe(writer2);

      await manager.close();
    });

    test('should throw if not initialized', async () => {
      const manager = createManager();

      await expect(manager.getWriter()).rejects.toThrow('not initialized');
    });
  });

  describe('writeBatch', () => {
    test('should write events to segment', async () => {
      const manager = createManager();
      await manager.initialize();

      const result = await manager.writeBatch([
        createEvent({ data: { id: 1 } }),
        createEvent({ data: { id: 2 } }),
      ]);
      await manager.sync();

      expect(result.eventCount).toBe(2);
      expect(result.offset).toBe(SEGMENT_HEADER_SIZE);

      await manager.close();
    });

    test('should update segment size', async () => {
      const manager = createManager();
      await manager.initialize();

      await manager.writeBatch([createEvent()]);
      await manager.sync();

      const segments = manager.getSegments();
      expect(segments[0]!.size).toBeGreaterThan(SEGMENT_HEADER_SIZE);

      await manager.close();
    });

    test('should track global position from events', async () => {
      const manager = createManager();
      await manager.initialize();

      await manager.writeBatch([
        createEvent({ globalPosition: 100 }),
        createEvent({ globalPosition: 101 }),
      ]);

      expect(manager.getGlobalPosition()).toBe(102);

      await manager.close();
    });
  });

  describe('rotateSegment', () => {
    test('should create new segment', async () => {
      const manager = createManager();
      await manager.initialize();

      await manager.getWriter(); // Create first segment
      const firstSegmentId = manager.getActiveSegmentId();

      await manager.rotateSegment();
      const secondSegmentId = manager.getActiveSegmentId();

      expect(secondSegmentId).not.toBe(firstSegmentId);
      expect(manager.getSegments()).toHaveLength(2);

      await manager.close();
    });

    test('should close previous writer', async () => {
      const manager = createManager();
      await manager.initialize();

      const writer1 = await manager.getWriter();
      await manager.rotateSegment();
      const writer2 = await manager.getWriter();

      expect(writer2).not.toBe(writer1);

      await manager.close();
    });

    test('should auto-rotate when segment exceeds max size', async () => {
      // Use small max size to trigger rotation
      const manager = createManager({ maxSegmentSize: 100 });
      await manager.initialize();

      const firstSegmentId = (await manager.getWriter(), manager.getActiveSegmentId());

      // Write enough to exceed limit
      for (let i = 0; i < 10; i++) {
        await manager.writeBatch([createEvent({ data: { index: i } })]);
        await manager.sync();
      }

      // Should have rotated
      expect(manager.getActiveSegmentId()).not.toBe(firstSegmentId);

      await manager.close();
    });
  });

  describe('getIndex', () => {
    test('should build index for segment', async () => {
      const manager = createManager();
      await manager.initialize();

      await manager.writeBatch([
        createEvent({ streamId: 'stream-1', revision: 0, globalPosition: 0 }),
        createEvent({ streamId: 'stream-1', revision: 1, globalPosition: 1 }),
      ]);
      await manager.sync();

      const segmentId = manager.getActiveSegmentId();
      const index = await manager.getIndex(segmentId);

      expect(index.hasStream('stream-1')).toBe(true);
      expect(index.getStreamEventCount('stream-1')).toBe(2);

      await manager.close();
    });

    test('should cache index', async () => {
      const manager = createManager();
      await manager.initialize();

      await manager.writeBatch([createEvent()]);
      await manager.sync();

      const segmentId = manager.getActiveSegmentId();
      const index1 = await manager.getIndex(segmentId);
      const index2 = await manager.getIndex(segmentId);

      expect(index1).toBe(index2);

      await manager.close();
    });

    test('should throw for non-existent segment', async () => {
      const manager = createManager();
      await manager.initialize();

      await expect(manager.getIndex(999n)).rejects.toThrow('not found');

      await manager.close();
    });
  });

  describe('recover', () => {
    test('should validate all segments', async () => {
      const manager = createManager();
      await manager.initialize();

      await manager.writeBatch([createEvent()]);
      await manager.sync();

      const result = await manager.recover();

      expect(result.segmentCount).toBe(1);
      expect(result.recoveredSegments).toBe(0);
      expect(result.errors).toHaveLength(0);

      await manager.close();
    });

    test('should truncate corrupted segment', async () => {
      const manager1 = createManager();
      await manager1.initialize();

      await manager1.writeBatch([createEvent({ data: { batch: 1 } })]);
      await manager1.sync();
      await manager1.writeBatch([createEvent({ data: { batch: 2 } })]);
      await manager1.sync();

      const segments = manager1.getSegments();
      const originalSize = segments[0]!.size;
      await manager1.close();

      // Corrupt the end of the file
      const path = segments[0]!.path;
      const content = fs.getFileContent(path)!;
      const corrupted = new Uint8Array(content.length);
      corrupted.set(content);
      corrupted[content.length - 5] = 0xff;
      fs.setFileContent(path, corrupted);

      // Recover with new manager
      const manager2 = createManager();
      await manager2.initialize();

      const result = await manager2.recover();

      expect(result.recoveredSegments).toBe(1);
      expect(result.errors.length).toBeGreaterThan(0);

      // Segment should be truncated
      const recoveredSegments = manager2.getSegments();
      expect(recoveredSegments[0]!.size).toBeLessThan(originalSize);

      await manager2.close();
    });
  });

  describe('allocateGlobalPosition', () => {
    test('should return incrementing positions', async () => {
      const manager = createManager();
      await manager.initialize();

      expect(manager.allocateGlobalPosition()).toBe(0);
      expect(manager.allocateGlobalPosition()).toBe(1);
      expect(manager.allocateGlobalPosition()).toBe(2);

      await manager.close();
    });
  });

  describe('close', () => {
    test('should close active writer', async () => {
      const manager = createManager();
      await manager.initialize();

      const writer = await manager.getWriter();
      await manager.close();

      expect(writer.isOpen).toBe(false);
    });

    test('should allow reinitialization after close', async () => {
      const manager = createManager();
      await manager.initialize();
      await manager.close();

      // Should not throw
      await manager.initialize();

      expect(manager.isInitialized()).toBe(true);

      await manager.close();
    });
  });

  describe('segment file naming', () => {
    test('should use zero-padded segment IDs', async () => {
      const manager = createManager();
      await manager.initialize();

      await manager.getWriter();

      const segments = manager.getSegments();
      expect(segments[0]!.path).toMatch(/segment-0+\.log$/);

      await manager.close();
    });
  });

  describe('background rotation', () => {
    test('should write .idx files in background during rotation', async () => {
      // Use very small segment size (just header size + a bit)
      // Rotation is checked BEFORE write, so we need the first batch to exceed the limit
      const manager = createManager({ maxSegmentSize: SEGMENT_HEADER_SIZE + 50 });
      await manager.initialize();

      // Write first batch - will exceed the tiny segment size
      const events = [
        createEvent({
          streamId: 'stream-1',
          revision: 0,
          globalPosition: 0,
          data: { value: 1 },
        }),
      ];
      await manager.writeBatch(events);
      await manager.sync();

      // Write second batch - should trigger rotation since first segment exceeded limit
      const events2 = [
        createEvent({
          streamId: 'stream-1',
          revision: 1,
          globalPosition: 1,
          data: { value: 2 },
        }),
      ];
      await manager.writeBatch(events2);
      await manager.sync();

      // Should have rotated to a new segment
      const segments = manager.getSegments();
      expect(segments.length).toBeGreaterThanOrEqual(2);

      // Close waits for background .idx writes to complete
      await manager.close();

      // After close, all .idx files should exist
      for (const segment of segments) {
        const idxPath = segment.path.replace('.log', '.idx');
        expect(fs.getFileContent(idxPath)).toBeDefined();
      }
    });

    test('should allow reads immediately after rotation (rebuilds if needed)', async () => {
      const manager = createManager({ maxSegmentSize: SEGMENT_HEADER_SIZE + 50 });
      await manager.initialize();

      // Write to first segment (will exceed tiny limit)
      await manager.writeBatch([
        createEvent({
          streamId: 'test-stream',
          revision: 0,
          globalPosition: 0,
          data: { value: 1 },
        }),
      ]);
      await manager.sync();

      // Trigger rotation by writing more
      await manager.writeBatch([
        createEvent({
          streamId: 'test-stream',
          revision: 1,
          globalPosition: 1,
          data: { value: 2 },
        }),
      ]);
      await manager.sync();

      // StreamMap should still have correct info (updated synchronously)
      expect(manager.getStreamRevision('test-stream')).toBe(1);
      expect(manager.getStreamSegments('test-stream').length).toBeGreaterThanOrEqual(1);

      await manager.close();
    });

    test('should sync .log before rotation (data durability)', async () => {
      const manager = createManager({ maxSegmentSize: SEGMENT_HEADER_SIZE + 50 });
      await manager.initialize();

      // Write first batch (will exceed tiny limit)
      await manager.writeBatch([
        createEvent({
          streamId: 'safe-stream',
          revision: 0,
          globalPosition: 0,
          data: { safe: true },
        }),
      ]);
      await manager.sync();

      // Write more to trigger rotation
      await manager.writeBatch([
        createEvent({
          streamId: 'safe-stream',
          revision: 1,
          globalPosition: 1,
          data: { also_safe: true },
        }),
      ]);
      // Don't sync - rotation should have already synced the first segment

      // Crash before .idx writes complete
      fs.crash();

      // Reopen and verify first segment data is intact
      const manager2 = createManager({ maxSegmentSize: SEGMENT_HEADER_SIZE + 50 });
      await manager2.initialize();

      // First segment's data should be readable (was synced before rotation)
      const segments = manager2.getSegments();
      expect(segments.length).toBeGreaterThanOrEqual(1);

      await manager2.close();
    });

    test('close should wait for pending background closes', async () => {
      const manager = createManager({ maxSegmentSize: SEGMENT_HEADER_SIZE + 50 });
      await manager.initialize();

      // Write enough to trigger multiple rotations
      for (let i = 0; i < 5; i++) {
        await manager.writeBatch([
          createEvent({
            streamId: `stream-${i}`,
            revision: 0,
            globalPosition: i,
            data: { index: i },
          }),
        ]);
        await manager.sync();
      }

      // Close should wait for all background .idx writes
      await manager.close();

      // All .idx files should exist after close
      const files = await fs.readdir('/data');
      const idxFiles = files.filter((f) => f.endsWith('.idx'));
      const logFiles = files.filter((f) => f.endsWith('.log'));

      // All closed segments should have .idx files
      // (active segment at close time also gets .idx)
      expect(idxFiles.length).toBe(logFiles.length);
    });
  });
});
