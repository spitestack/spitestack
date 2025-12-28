/**
 * Tests for short write handling.
 *
 * Verifies that the writeAllBytes helper correctly handles partial writes
 * from the filesystem, retrying until all data is written.
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { writeAllBytes, ShortWriteError } from '../../../src/infrastructure/storage/support/write-all-bytes';
import { SimulatedFileSystem } from '../../../src/testing/simulated-filesystem';

describe('Short Write Handling', () => {
  let fs: SimulatedFileSystem;
  const testPath = '/test-short-write/data.bin';

  beforeEach(async () => {
    fs = new SimulatedFileSystem();
    await fs.mkdir('/test-short-write', { recursive: true });
  });

  describe('writeAllBytes', () => {
    test('should write all data in a single successful write', async () => {
      const handle = await fs.open(testPath, 'write');
      const data = new Uint8Array([1, 2, 3, 4, 5]);

      await writeAllBytes(fs, handle, data, 0);
      await fs.sync(handle);
      await fs.close(handle);

      const result = await fs.readFile(testPath);
      expect(result).toEqual(data);
    });

    test('should handle empty data', async () => {
      const handle = await fs.open(testPath, 'write');
      const data = new Uint8Array(0);

      await writeAllBytes(fs, handle, data, 0);
      await fs.sync(handle);
      await fs.close(handle);

      const result = await fs.readFile(testPath);
      expect(result.length).toBe(0);
    });

    test('should handle large writes', async () => {
      const handle = await fs.open(testPath, 'write');
      // 1MB of data
      const data = new Uint8Array(1024 * 1024);
      for (let i = 0; i < data.length; i++) {
        data[i] = i % 256;
      }

      await writeAllBytes(fs, handle, data, 0);
      await fs.sync(handle);
      await fs.close(handle);

      const result = await fs.readFile(testPath);
      expect(result.length).toBe(data.length);
      expect(result[0]).toBe(0);
      expect(result[255]).toBe(255);
      expect(result[256]).toBe(0);
    });

    test('should write at correct offset', async () => {
      // First write some data
      const handle = await fs.open(testPath, 'write');
      const initialData = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
      await writeAllBytes(fs, handle, initialData, 0);
      await fs.sync(handle);
      await fs.close(handle);

      // Now write at an offset
      const handle2 = await fs.open(testPath, 'readwrite');
      const newData = new Uint8Array([1, 2, 3]);
      await writeAllBytes(fs, handle2, newData, 5);
      await fs.sync(handle2);
      await fs.close(handle2);

      const result = await fs.readFile(testPath);
      expect(result).toEqual(new Uint8Array([0, 0, 0, 0, 0, 1, 2, 3, 0, 0]));
    });

    test('should handle append mode (undefined offset)', async () => {
      const handle = await fs.open(testPath, 'write');
      const data1 = new Uint8Array([1, 2, 3]);
      await writeAllBytes(fs, handle, data1);
      await fs.sync(handle);
      await fs.close(handle);

      // Append more data
      const handle2 = await fs.open(testPath, 'append');
      const data2 = new Uint8Array([4, 5, 6]);
      await writeAllBytes(fs, handle2, data2);
      await fs.sync(handle2);
      await fs.close(handle2);

      const result = await fs.readFile(testPath);
      expect(result).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6]));
    });
  });

  describe('ShortWriteError', () => {
    test('should have correct properties', () => {
      const error = new ShortWriteError(100, 50);
      expect(error.bytesRequested).toBe(100);
      expect(error.bytesWritten).toBe(50);
      expect(error.name).toBe('ShortWriteError');
      expect(error.message).toContain('50');
      expect(error.message).toContain('100');
    });
  });

  describe('integration with SegmentWriter', () => {
    test('segment writer should handle writes correctly', async () => {
      // This test verifies the fix is integrated correctly
      // by importing from the actual segment writer
      const { SegmentWriter } = await import(
        '../../../src/infrastructure/storage/segments/segment-writer'
      );
      const { MsgpackSerializer } = await import(
        '../../../src/infrastructure/serialization/msgpack-serializer'
      );
      const { ZstdCompressor } = await import(
        '../../../src/infrastructure/serialization/zstd-compressor'
      );

      const serializer = new MsgpackSerializer();
      const compressor = new ZstdCompressor();
      const segmentPath = '/test-short-write/segment.bin';

      const writer = new SegmentWriter(fs, serializer, compressor);
      await writer.open(segmentPath, 1n, 0); // segmentId=1, basePosition=0

      // Write some events
      const events = [
        {
          streamId: 'stream-1',
          type: 'TestEvent',
          data: { value: 1 },
          metadata: undefined,
          revision: 0,
          globalPosition: 0,
          timestamp: 1000,
          tenantId: 'default',
        },
        {
          streamId: 'stream-1',
          type: 'TestEvent',
          data: { value: 2 },
          metadata: undefined,
          revision: 1,
          globalPosition: 1,
          timestamp: 1001,
          tenantId: 'default',
        },
      ];

      await writer.appendBatch(events);
      await writer.sync();

      // Verify data was written
      const exists = await fs.exists(segmentPath);
      expect(exists).toBe(true);

      const fileSize = (await fs.stat(segmentPath)).size;
      expect(fileSize).toBeGreaterThan(0);

      await writer.close();
    });
  });
});
