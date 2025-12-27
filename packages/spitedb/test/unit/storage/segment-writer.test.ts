import { describe, test, expect, beforeEach } from 'bun:test';
import { SegmentWriter } from '../../../src/storage/segment-writer';
import { SimulatedFileSystem } from '../../../src/testing/simulated-filesystem';
import { MsgpackSerializer } from '../../../src/runtime/msgpack-serializer';
import { ZstdCompressor } from '../../../src/runtime/zstd-compressor';
import {
  decodeSegmentHeader,
  SEGMENT_HEADER_SIZE,
  SEGMENT_MAGIC,
} from '../../../src/storage/segment-header';
import {
  decodeBatchHeader,
  extractBatchPayload,
  BATCH_MAGIC,
} from '../../../src/storage/batch-record';
import type { StoredEvent } from '../../../src/storage/stored-event';

describe('SegmentWriter', () => {
  let fs: SimulatedFileSystem;
  let serializer: MsgpackSerializer;
  let compressor: ZstdCompressor;

  beforeEach(() => {
    fs = new SimulatedFileSystem();
    serializer = new MsgpackSerializer();
    compressor = new ZstdCompressor();
  });

  function createEvent(overrides: Partial<StoredEvent> = {}): StoredEvent {
    return {
      streamId: 'test-stream',
      type: 'TestEvent',
      data: { message: 'hello' },
      revision: 0,
      globalPosition: 0n,
      timestamp: Date.now(),
      tenantId: 'default',
      ...overrides,
    };
  }

  describe('open', () => {
    test('should create segment file with valid header', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 100n);

      const content = fs.getFileContent('/data/segment.log');
      expect(content).toBeDefined();
      expect(content!.length).toBeGreaterThanOrEqual(SEGMENT_HEADER_SIZE);

      const header = decodeSegmentHeader(content!);
      expect(header.magic).toBe(SEGMENT_MAGIC);
      expect(header.segmentId).toBe(1n);
      expect(header.basePosition).toBe(100n);

      await writer.close();
    });

    test('should set offset to header size after open', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      expect(writer.offset).toBe(SEGMENT_HEADER_SIZE);

      await writer.close();
    });

    test('should throw if already open', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      await expect(writer.open('/data/other.log', 2n, 0n)).rejects.toThrow(
        'already has an open file'
      );

      await writer.close();
    });

    test('should mark writer as open', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      expect(writer.isOpen).toBe(false);

      await writer.open('/data/segment.log', 1n, 0n);
      expect(writer.isOpen).toBe(true);

      await writer.close();
      expect(writer.isOpen).toBe(false);
    });
  });

  describe('appendBatch', () => {
    test('should write batch after header', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      const event = createEvent();
      const result = await writer.appendBatch([event]);
      await writer.sync();

      expect(result.offset).toBe(SEGMENT_HEADER_SIZE);
      expect(result.eventCount).toBe(1);
      expect(result.length).toBeGreaterThan(0);

      await writer.close();
    });

    test('should update offset after write', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      const initialOffset = writer.offset;
      const result = await writer.appendBatch([createEvent()]);
      await writer.sync();

      expect(writer.offset).toBe(initialOffset + result.length);

      await writer.close();
    });

    test('should increment batch IDs', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      const result1 = await writer.appendBatch([createEvent()]);
      const result2 = await writer.appendBatch([createEvent()]);
      const result3 = await writer.appendBatch([createEvent()]);

      expect(result1.batchId).toBe(0n);
      expect(result2.batchId).toBe(1n);
      expect(result3.batchId).toBe(2n);

      await writer.close();
    });

    test('should write valid batch record', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      const events = [createEvent({ data: { value: 42 } })];
      const result = await writer.appendBatch(events);
      await writer.sync();

      const content = fs.getFileContent('/data/segment.log')!;
      const batchData = content.subarray(result.offset);
      const batchHeader = decodeBatchHeader(batchData);

      expect(batchHeader.magic).toBe(BATCH_MAGIC);
      expect(batchHeader.batchId).toBe(result.batchId);
      expect(batchHeader.eventCount).toBe(1);

      // Verify we can decompress and decode the payload
      const payload = extractBatchPayload(batchData, batchHeader);
      const decompressed = compressor.decompress(payload);
      const deserialized = serializer.decode<StoredEvent[]>(decompressed);

      expect(deserialized).toHaveLength(1);
      expect(deserialized[0]!.data).toEqual({ value: 42 });

      await writer.close();
    });

    test('should handle multiple events in batch', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      const events = [
        createEvent({ revision: 0, data: { id: 1 } }),
        createEvent({ revision: 1, data: { id: 2 } }),
        createEvent({ revision: 2, data: { id: 3 } }),
      ];
      const result = await writer.appendBatch(events);
      await writer.sync();

      const content = fs.getFileContent('/data/segment.log')!;
      const batchData = content.subarray(result.offset);
      const batchHeader = decodeBatchHeader(batchData);

      expect(batchHeader.eventCount).toBe(3);

      const payload = extractBatchPayload(batchData, batchHeader);
      const decompressed = compressor.decompress(payload);
      const deserialized = serializer.decode<StoredEvent[]>(decompressed);

      expect(deserialized).toHaveLength(3);
      expect(deserialized[0]!.data).toEqual({ id: 1 });
      expect(deserialized[1]!.data).toEqual({ id: 2 });
      expect(deserialized[2]!.data).toEqual({ id: 3 });

      await writer.close();
    });

    test('should throw if not open', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);

      await expect(writer.appendBatch([createEvent()])).rejects.toThrow('not open');
    });

    test('should throw on empty batch', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      await expect(writer.appendBatch([])).rejects.toThrow('empty batch');

      await writer.close();
    });
  });

  describe('sync', () => {
    test('should throw if not open', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);

      await expect(writer.sync()).rejects.toThrow('not open');
    });

    test('should persist data to file', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      await writer.appendBatch([createEvent({ data: { persisted: true } })]);

      // Before sync, data might not be persisted (in simulated fs, it won't be)
      // After sync, it should be
      await writer.sync();

      const content = fs.getFileContent('/data/segment.log')!;
      expect(content.length).toBeGreaterThan(SEGMENT_HEADER_SIZE);

      await writer.close();
    });
  });

  describe('close', () => {
    test('should be idempotent', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      await writer.close();
      await writer.close(); // Should not throw
      await writer.close(); // Should not throw
    });

    test('should clear file path', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      expect(writer.filePath).toBe('/data/segment.log');

      await writer.close();

      expect(writer.filePath).toBe('');
    });
  });

  describe('resume', () => {
    test('should continue from given offset', async () => {
      const writer1 = new SegmentWriter(fs, serializer, compressor);
      await writer1.open('/data/segment.log', 1n, 0n);
      await writer1.appendBatch([createEvent()]);
      await writer1.sync();
      const offsetAfterFirstBatch = writer1.offset;
      const lastBatchId = writer1.nextBatchId - 1n;
      await writer1.close();

      // Resume with a new writer
      const writer2 = new SegmentWriter(fs, serializer, compressor);
      await writer2.resume('/data/segment.log', offsetAfterFirstBatch, lastBatchId);

      expect(writer2.offset).toBe(offsetAfterFirstBatch);
      expect(writer2.nextBatchId).toBe(lastBatchId + 1n);

      // Write another batch
      const result = await writer2.appendBatch([createEvent()]);
      await writer2.sync();

      expect(result.offset).toBe(offsetAfterFirstBatch);
      expect(result.batchId).toBe(lastBatchId + 1n);

      await writer2.close();
    });

    test('should throw if already open', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      await expect(writer.resume('/data/other.log', 100, 0n)).rejects.toThrow(
        'already has an open file'
      );

      await writer.close();
    });
  });

  describe('crash recovery', () => {
    test('should not persist unsynced writes on crash', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      // Write header is synced
      const headerContent = fs.getFileContent('/data/segment.log')!;
      expect(headerContent.length).toBe(SEGMENT_HEADER_SIZE);

      // Write a batch but don't sync
      await writer.appendBatch([createEvent({ data: { lost: true } })]);

      // Simulate crash
      fs.crash();

      // Only header should remain
      const afterCrash = fs.getFileContent('/data/segment.log')!;
      expect(afterCrash.length).toBe(SEGMENT_HEADER_SIZE);
    });

    test('should persist synced writes on crash', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      // Write and sync a batch
      const result = await writer.appendBatch([createEvent({ data: { safe: true } })]);
      await writer.sync();
      const expectedSize = writer.offset;

      // Write another batch without syncing
      await writer.appendBatch([createEvent({ data: { lost: true } })]);

      // Simulate crash
      fs.crash();

      // First batch should remain
      const afterCrash = fs.getFileContent('/data/segment.log')!;
      expect(afterCrash.length).toBe(expectedSize);

      // Verify the persisted batch is readable
      const batchData = afterCrash.subarray(result.offset);
      const batchHeader = decodeBatchHeader(batchData);
      const payload = extractBatchPayload(batchData, batchHeader);
      const decompressed = compressor.decompress(payload);
      const events = serializer.decode<StoredEvent[]>(decompressed);

      expect(events[0]!.data).toEqual({ safe: true });
    });
  });

  describe('index file generation', () => {
    test('should create .idx file on close', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);
      await writer.appendBatch([
        createEvent({ streamId: 'stream-a', revision: 0, globalPosition: 0n }),
        createEvent({ streamId: 'stream-a', revision: 1, globalPosition: 1n }),
      ]);
      await writer.sync();
      await writer.close();

      expect(fs.getFileContent('/data/segment.idx')).toBeDefined();
    });

    test('should not create .idx file for empty segment', async () => {
      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);
      await writer.close();

      expect(fs.getFileContent('/data/segment.idx')).toBeUndefined();
    });

    test('should include all events in .idx file', async () => {
      const { SegmentIndexFile } = await import('../../../src/storage/segment-index-file');

      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 42n, 0n);
      await writer.appendBatch([
        createEvent({ streamId: 'stream-a', revision: 0, globalPosition: 100n }),
        createEvent({ streamId: 'stream-b', revision: 0, globalPosition: 101n }),
      ]);
      await writer.appendBatch([
        createEvent({ streamId: 'stream-a', revision: 1, globalPosition: 102n }),
      ]);
      await writer.sync();
      await writer.close();

      const index = new SegmentIndexFile();
      await index.load(fs, '/data/segment.idx');

      expect(index.getSegmentId()).toBe(42n);
      expect(index.getEntryCount()).toBe(3);
      expect(index.getAllStreamIds().size).toBe(2);

      const streamAEntries = index.findByStream('stream-a');
      expect(streamAEntries).toHaveLength(2);
      expect(streamAEntries[0]!.globalPosition).toBe(100n);
      expect(streamAEntries[1]!.globalPosition).toBe(102n);
    });

    test('should track correct batch offsets', async () => {
      const { SegmentIndexFile } = await import('../../../src/storage/segment-index-file');

      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open('/data/segment.log', 1n, 0n);

      const result1 = await writer.appendBatch([
        createEvent({ streamId: 'stream-a', revision: 0, globalPosition: 0n }),
      ]);
      const result2 = await writer.appendBatch([
        createEvent({ streamId: 'stream-a', revision: 1, globalPosition: 1n }),
      ]);
      await writer.sync();
      await writer.close();

      const index = new SegmentIndexFile();
      await index.load(fs, '/data/segment.idx');

      const entries = index.findByStream('stream-a');
      expect(entries[0]!.batchOffset).toBe(result1.offset);
      expect(entries[1]!.batchOffset).toBe(result2.offset);
    });
  });
});
