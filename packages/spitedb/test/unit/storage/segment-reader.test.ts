import { describe, test, expect, beforeEach } from 'bun:test';
import { SegmentWriter } from '../../../src/storage/segment-writer';
import { SegmentReader } from '../../../src/storage/segment-reader';
import { SimulatedFileSystem } from '../../../src/testing/simulated-filesystem';
import { MsgpackSerializer } from '../../../src/runtime/msgpack-serializer';
import { ZstdCompressor } from '../../../src/runtime/zstd-compressor';
import { SEGMENT_HEADER_SIZE, SEGMENT_MAGIC } from '../../../src/storage/segment-header';
import type { StoredEvent } from '../../../src/storage/stored-event';

describe('SegmentReader', () => {
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

  async function writeTestSegment(
    events: StoredEvent[][],
    segmentId = 1n,
    basePosition = 0n
  ): Promise<{ offsets: number[] }> {
    await writer.open('/data/segment.log', segmentId, basePosition);
    const offsets: number[] = [];

    for (const batch of events) {
      const result = await writer.appendBatch(batch);
      offsets.push(result.offset);
    }

    await writer.sync();
    await writer.close();

    return { offsets };
  }

  describe('readHeader', () => {
    test('should read valid segment header', async () => {
      await writeTestSegment([[createEvent()]]);

      const header = await reader.readHeader('/data/segment.log');

      expect(header.magic).toBe(SEGMENT_MAGIC);
      expect(header.segmentId).toBe(1n);
      expect(header.basePosition).toBe(0n);
    });

    test('should read header with custom segment ID and base position', async () => {
      await writer.open('/data/segment.log', 42n, 1000n);
      await writer.appendBatch([createEvent()]);
      await writer.sync();
      await writer.close();

      const header = await reader.readHeader('/data/segment.log');

      expect(header.segmentId).toBe(42n);
      expect(header.basePosition).toBe(1000n);
    });

    test('should throw on invalid header', async () => {
      fs.setFileContent('/data/bad.log', new Uint8Array(32)); // All zeros

      await expect(reader.readHeader('/data/bad.log')).rejects.toThrow('Invalid magic');
    });
  });

  describe('readBatch', () => {
    test('should read single event batch', async () => {
      const event = createEvent({ data: { value: 42 } });
      const { offsets } = await writeTestSegment([[event]]);

      const events = await reader.readBatch('/data/segment.log', offsets[0]!);

      expect(events).toHaveLength(1);
      expect(events[0]!.data).toEqual({ value: 42 });
    });

    test('should read multi-event batch', async () => {
      const events = [
        createEvent({ revision: 0, data: { id: 1 } }),
        createEvent({ revision: 1, data: { id: 2 } }),
        createEvent({ revision: 2, data: { id: 3 } }),
      ];
      const { offsets } = await writeTestSegment([events]);

      const readEvents = await reader.readBatch('/data/segment.log', offsets[0]!);

      expect(readEvents).toHaveLength(3);
      expect(readEvents[0]!.data).toEqual({ id: 1 });
      expect(readEvents[1]!.data).toEqual({ id: 2 });
      expect(readEvents[2]!.data).toEqual({ id: 3 });
    });

    test('should read specific batch by offset', async () => {
      const batch1 = [createEvent({ data: { batch: 1 } })];
      const batch2 = [createEvent({ data: { batch: 2 } })];
      const batch3 = [createEvent({ data: { batch: 3 } })];
      const { offsets } = await writeTestSegment([batch1, batch2, batch3]);

      const events1 = await reader.readBatch('/data/segment.log', offsets[0]!);
      const events2 = await reader.readBatch('/data/segment.log', offsets[1]!);
      const events3 = await reader.readBatch('/data/segment.log', offsets[2]!);

      expect(events1[0]!.data).toEqual({ batch: 1 });
      expect(events2[0]!.data).toEqual({ batch: 2 });
      expect(events3[0]!.data).toEqual({ batch: 3 });
    });

    test('should throw on corrupted batch', async () => {
      const { offsets } = await writeTestSegment([[createEvent()]]);

      // Corrupt the batch data
      const content = fs.getFileContent('/data/segment.log')!;
      content[offsets[0]! + 20] = 0xff; // Corrupt somewhere in the batch
      fs.setFileContent('/data/segment.log', content);

      await expect(reader.readBatch('/data/segment.log', offsets[0]!)).rejects.toThrow();
    });
  });

  describe('readBatchWithMetadata', () => {
    test('should return batch ID and next offset', async () => {
      const { offsets } = await writeTestSegment([
        [createEvent({ data: { batch: 1 } })],
        [createEvent({ data: { batch: 2 } })],
      ]);

      const result1 = await reader.readBatchWithMetadata('/data/segment.log', offsets[0]!);
      const result2 = await reader.readBatchWithMetadata('/data/segment.log', offsets[1]!);

      expect(result1.batchId).toBe(0n);
      expect(result1.nextOffset).toBe(offsets[1]!);
      expect(result1.events[0]!.data).toEqual({ batch: 1 });

      expect(result2.batchId).toBe(1n);
      expect(result2.events[0]!.data).toEqual({ batch: 2 });
    });
  });

  describe('readAllBatches', () => {
    test('should iterate through all batches', async () => {
      await writeTestSegment([
        [createEvent({ data: { batch: 1 } })],
        [createEvent({ data: { batch: 2 } })],
        [createEvent({ data: { batch: 3 } })],
      ]);

      const batches: StoredEvent[][] = [];
      for await (const batch of reader.readAllBatches('/data/segment.log')) {
        batches.push(batch);
      }

      expect(batches).toHaveLength(3);
      expect(batches[0]![0]!.data).toEqual({ batch: 1 });
      expect(batches[1]![0]!.data).toEqual({ batch: 2 });
      expect(batches[2]![0]!.data).toEqual({ batch: 3 });
    });

    test('should handle empty segment (header only)', async () => {
      await writer.open('/data/segment.log', 1n, 0n);
      await writer.sync();
      await writer.close();

      const batches: StoredEvent[][] = [];
      for await (const batch of reader.readAllBatches('/data/segment.log')) {
        batches.push(batch);
      }

      expect(batches).toHaveLength(0);
    });

    test('should stop at incomplete batch', async () => {
      await writeTestSegment([
        [createEvent({ data: { batch: 1 } })],
        [createEvent({ data: { batch: 2 } })],
      ]);

      // Truncate file to cut off part of the second batch
      const content = fs.getFileContent('/data/segment.log')!;
      const truncated = content.slice(0, content.length - 10);
      fs.setFileContent('/data/segment.log', truncated);

      const batches: StoredEvent[][] = [];
      for await (const batch of reader.readAllBatches('/data/segment.log')) {
        batches.push(batch);
      }

      expect(batches).toHaveLength(1);
      expect(batches[0]![0]!.data).toEqual({ batch: 1 });
    });
  });

  describe('validateSegment', () => {
    test('should validate a good segment', async () => {
      await writeTestSegment([
        [createEvent()],
        [createEvent()],
        [createEvent()],
      ]);

      const result = await reader.validateSegment('/data/segment.log');

      expect(result.valid).toBe(true);
      expect(result.batchCount).toBe(3);
      expect(result.eventCount).toBe(3);
      expect(result.errors).toHaveLength(0);
    });

    test('should return error for non-existent file', async () => {
      const result = await reader.validateSegment('/data/nonexistent.log');

      expect(result.valid).toBe(false);
      expect(result.errors).toContain('File does not exist');
    });

    test('should return error for file too small', async () => {
      fs.setFileContent('/data/tiny.log', new Uint8Array(10));

      const result = await reader.validateSegment('/data/tiny.log');

      expect(result.valid).toBe(false);
      expect(result.errors[0]).toContain('File too small');
    });

    test('should return error for invalid header', async () => {
      fs.setFileContent('/data/bad.log', new Uint8Array(SEGMENT_HEADER_SIZE));

      const result = await reader.validateSegment('/data/bad.log');

      expect(result.valid).toBe(false);
      expect(result.errors).toContain('Invalid segment header');
    });

    test('should detect truncated batch', async () => {
      await writeTestSegment([
        [createEvent({ data: { batch: 1 } })],
        [createEvent({ data: { batch: 2 } })],
      ]);

      // Truncate in the middle of second batch
      const content = fs.getFileContent('/data/segment.log')!;
      const truncated = content.slice(0, content.length - 10);
      fs.setFileContent('/data/segment.log', truncated);

      const result = await reader.validateSegment('/data/segment.log');

      expect(result.valid).toBe(false);
      expect(result.batchCount).toBe(1);
      expect(result.eventCount).toBe(1);
      expect(result.errors[0]).toContain('Incomplete batch');
    });

    test('should detect corrupted batch', async () => {
      await writeTestSegment([
        [createEvent({ data: { batch: 1 } })],
        [createEvent({ data: { batch: 2 } })],
      ]);

      // Corrupt the second batch
      const content = fs.getFileContent('/data/segment.log')!;
      // Find second batch and corrupt it
      const secondBatchStart = content.length - 50;
      content[secondBatchStart + 20] = 0xff;
      fs.setFileContent('/data/segment.log', content);

      const result = await reader.validateSegment('/data/segment.log');

      expect(result.valid).toBe(false);
      expect(result.batchCount).toBe(1);
    });

    test('should report lastValidOffset for recovery', async () => {
      const { offsets } = await writeTestSegment([
        [createEvent({ data: { batch: 1 } })],
        [createEvent({ data: { batch: 2 } })],
      ]);

      // Truncate in the middle of second batch
      const content = fs.getFileContent('/data/segment.log')!;
      const truncated = content.slice(0, content.length - 10);
      fs.setFileContent('/data/segment.log', truncated);

      const result = await reader.validateSegment('/data/segment.log');

      // lastValidOffset should point to end of first batch
      expect(result.lastValidOffset).toBe(offsets[1]!);
    });
  });

  describe('getFileSize', () => {
    test('should return correct file size', async () => {
      await writeTestSegment([[createEvent()]]);

      const content = fs.getFileContent('/data/segment.log')!;
      const size = await reader.getFileSize('/data/segment.log');

      expect(size).toBe(content.length);
    });
  });

  describe('integration', () => {
    test('should round-trip complex events', async () => {
      const originalEvents = [
        createEvent({
          streamId: 'user-123',
          type: 'UserCreated',
          data: { name: 'Alice', email: 'alice@example.com' },
          metadata: { correlationId: 'abc-123' },
          revision: 0,
          globalPosition: 100n,
          tenantId: 'tenant-a',
        }),
        createEvent({
          streamId: 'user-123',
          type: 'UserUpdated',
          data: { name: 'Alice Smith' },
          revision: 1,
          globalPosition: 101n,
          tenantId: 'tenant-a',
        }),
      ];

      await writeTestSegment([originalEvents]);

      const readEvents = await reader.readBatch('/data/segment.log', SEGMENT_HEADER_SIZE);

      expect(readEvents).toHaveLength(2);
      expect(readEvents[0]!.streamId).toBe('user-123');
      expect(readEvents[0]!.type).toBe('UserCreated');
      expect(readEvents[0]!.data).toEqual({ name: 'Alice', email: 'alice@example.com' });
      expect(readEvents[0]!.metadata).toEqual({ correlationId: 'abc-123' });
      expect(readEvents[0]!.revision).toBe(0);
      expect(readEvents[0]!.globalPosition).toBe(100n);
      expect(readEvents[0]!.tenantId).toBe('tenant-a');

      expect(readEvents[1]!.type).toBe('UserUpdated');
      expect(readEvents[1]!.data).toEqual({ name: 'Alice Smith' });
      expect(readEvents[1]!.revision).toBe(1);
      expect(readEvents[1]!.globalPosition).toBe(101n);
    });
  });
});
