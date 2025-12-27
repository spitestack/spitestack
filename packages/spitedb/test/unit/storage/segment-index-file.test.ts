import { describe, test, expect, beforeEach } from 'bun:test';
import {
  SegmentIndexFile,
  IndexCorruptedError,
  INDEX_MAGIC,
  INDEX_VERSION,
  INDEX_HEADER_SIZE,
  INDEX_ENTRY_SIZE,
  type IndexEntry,
} from '../../../src/storage/segment-index-file';
import { SimulatedFileSystem } from '../../../src/testing/simulated-filesystem';

describe('SegmentIndexFile', () => {
  let fs: SimulatedFileSystem;

  beforeEach(() => {
    fs = new SimulatedFileSystem();
  });

  function createEntry(overrides: Partial<IndexEntry> = {}): IndexEntry {
    return {
      streamId: 'test-stream',
      revision: 0,
      globalPosition: 0n,
      batchOffset: 100,
      ...overrides,
    };
  }

  describe('write and load', () => {
    test('should write and load empty index', async () => {
      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, []);

      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      expect(index.isLoaded()).toBe(true);
      expect(index.getEntryCount()).toBe(0);
      expect(index.getSegmentId()).toBe(1n);
    });

    test('should write and load single entry', async () => {
      const entry = createEntry({
        streamId: 'user-123',
        revision: 5,
        globalPosition: 42n,
        batchOffset: 200,
      });

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, [entry]);

      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      expect(index.getEntryCount()).toBe(1);
      const entries = index.getAllEntries();
      expect(entries[0]).toEqual(entry);
    });

    test('should write and load multiple entries', async () => {
      const entries = [
        createEntry({ streamId: 'stream-a', revision: 0, globalPosition: 0n, batchOffset: 100 }),
        createEntry({ streamId: 'stream-a', revision: 1, globalPosition: 1n, batchOffset: 100 }),
        createEntry({ streamId: 'stream-b', revision: 0, globalPosition: 2n, batchOffset: 200 }),
        createEntry({ streamId: 'stream-c', revision: 0, globalPosition: 3n, batchOffset: 300 }),
      ];

      await SegmentIndexFile.write(fs, '/data/test.idx', 42n, entries);

      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      expect(index.getSegmentId()).toBe(42n);
      expect(index.getEntryCount()).toBe(4);
    });

    test('should sort entries by streamId then revision', async () => {
      // Write in random order
      const entries = [
        createEntry({ streamId: 'stream-b', revision: 1 }),
        createEntry({ streamId: 'stream-a', revision: 0 }),
        createEntry({ streamId: 'stream-b', revision: 0 }),
        createEntry({ streamId: 'stream-a', revision: 1 }),
      ];

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);

      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      const loaded = index.getAllEntries();
      expect(loaded[0]!.streamId).toBe('stream-a');
      expect(loaded[0]!.revision).toBe(0);
      expect(loaded[1]!.streamId).toBe('stream-a');
      expect(loaded[1]!.revision).toBe(1);
      expect(loaded[2]!.streamId).toBe('stream-b');
      expect(loaded[2]!.revision).toBe(0);
      expect(loaded[3]!.streamId).toBe('stream-b');
      expect(loaded[3]!.revision).toBe(1);
    });

    test('should deduplicate streamIds in string table', async () => {
      // Many entries with same streamId
      const entries = Array.from({ length: 100 }, (_, i) =>
        createEntry({ streamId: 'same-stream', revision: i, globalPosition: BigInt(i) })
      );

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);

      const content = fs.getFileContent('/data/test.idx')!;
      // String table should only contain "same-stream" once
      // Header (32) + 100 entries * 24 = 2432 bytes for entries
      // String table: "same-stream" (11 bytes) + null (1 byte) = 12 bytes
      expect(content.length).toBe(INDEX_HEADER_SIZE + 100 * INDEX_ENTRY_SIZE + 12);
    });

    test('should handle unicode streamIds', async () => {
      const entries = [
        createEntry({ streamId: 'user-日本語', revision: 0 }),
        createEntry({ streamId: 'emoji-🎉', revision: 0 }),
      ];

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);

      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      const ids = index.getAllStreamIds();
      expect(ids.has('user-日本語')).toBe(true);
      expect(ids.has('emoji-🎉')).toBe(true);
    });
  });

  describe('findByStream', () => {
    let index: SegmentIndexFile;

    beforeEach(async () => {
      const entries = [
        createEntry({ streamId: 'stream-a', revision: 0, globalPosition: 0n, batchOffset: 100 }),
        createEntry({ streamId: 'stream-a', revision: 1, globalPosition: 1n, batchOffset: 100 }),
        createEntry({ streamId: 'stream-a', revision: 2, globalPosition: 2n, batchOffset: 200 }),
        createEntry({ streamId: 'stream-b', revision: 0, globalPosition: 3n, batchOffset: 200 }),
        createEntry({ streamId: 'stream-b', revision: 1, globalPosition: 4n, batchOffset: 300 }),
        createEntry({ streamId: 'stream-c', revision: 0, globalPosition: 5n, batchOffset: 400 }),
      ];

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);
      index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');
    });

    test('should find all entries for a stream', () => {
      const entries = index.findByStream('stream-a');
      expect(entries).toHaveLength(3);
      expect(entries.map((e) => e.revision)).toEqual([0, 1, 2]);
    });

    test('should return empty array for non-existent stream', () => {
      const entries = index.findByStream('unknown');
      expect(entries).toHaveLength(0);
    });

    test('should filter by fromRevision', () => {
      const entries = index.findByStream('stream-a', 1);
      expect(entries).toHaveLength(2);
      expect(entries.map((e) => e.revision)).toEqual([1, 2]);
    });

    test('should filter by toRevision', () => {
      const entries = index.findByStream('stream-a', undefined, 1);
      expect(entries).toHaveLength(2);
      expect(entries.map((e) => e.revision)).toEqual([0, 1]);
    });

    test('should filter by revision range', () => {
      const entries = index.findByStream('stream-a', 1, 1);
      expect(entries).toHaveLength(1);
      expect(entries[0]!.revision).toBe(1);
    });

    test('should use binary search efficiently', () => {
      // With 3 streams in sorted order, binary search should find quickly
      const entriesB = index.findByStream('stream-b');
      expect(entriesB).toHaveLength(2);

      const entriesC = index.findByStream('stream-c');
      expect(entriesC).toHaveLength(1);
    });
  });

  describe('findByGlobalPosition', () => {
    test('should find entry by global position', async () => {
      const entries = [
        createEntry({ streamId: 'stream-a', globalPosition: 100n }),
        createEntry({ streamId: 'stream-b', globalPosition: 200n }),
        createEntry({ streamId: 'stream-c', globalPosition: 300n }),
      ];

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);
      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      const entry = index.findByGlobalPosition(200n);
      expect(entry).toBeDefined();
      expect(entry!.streamId).toBe('stream-b');
    });

    test('should return undefined for non-existent position', async () => {
      const entries = [createEntry({ globalPosition: 100n })];

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);
      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      const entry = index.findByGlobalPosition(999n);
      expect(entry).toBeUndefined();
    });
  });

  describe('getAllStreamIds', () => {
    test('should return all unique stream IDs', async () => {
      const entries = [
        createEntry({ streamId: 'stream-a', revision: 0 }),
        createEntry({ streamId: 'stream-a', revision: 1 }),
        createEntry({ streamId: 'stream-b', revision: 0 }),
        createEntry({ streamId: 'stream-c', revision: 0 }),
      ];

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);
      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      const ids = index.getAllStreamIds();
      expect(ids.size).toBe(3);
      expect(ids.has('stream-a')).toBe(true);
      expect(ids.has('stream-b')).toBe(true);
      expect(ids.has('stream-c')).toBe(true);
    });
  });

  describe('getStreamMaxRevision', () => {
    test('should return max revision for stream', async () => {
      const entries = [
        createEntry({ streamId: 'stream-a', revision: 0 }),
        createEntry({ streamId: 'stream-a', revision: 5 }),
        createEntry({ streamId: 'stream-a', revision: 3 }),
      ];

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);
      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      expect(index.getStreamMaxRevision('stream-a')).toBe(5);
    });

    test('should return -1 for non-existent stream', async () => {
      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, []);
      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      expect(index.getStreamMaxRevision('unknown')).toBe(-1);
    });
  });

  describe('checksum validation', () => {
    test('should validate correct checksum', async () => {
      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, [createEntry()]);

      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      expect(index.validateChecksum()).toBe(true);
    });

    test('should detect corrupted data', async () => {
      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, [createEntry()]);

      // Corrupt a byte in the entry area
      const content = fs.getFileContent('/data/test.idx')!;
      content[INDEX_HEADER_SIZE + 10] = 0xff;
      fs.setFileContent('/data/test.idx', content);

      const index = new SegmentIndexFile();
      await expect(index.load(fs, '/data/test.idx')).rejects.toThrow(IndexCorruptedError);
    });

    test('should detect corrupted header', async () => {
      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, [createEntry()]);

      // Corrupt the version field
      const content = fs.getFileContent('/data/test.idx')!;
      content[4] = 0xff;
      fs.setFileContent('/data/test.idx', content);

      const index = new SegmentIndexFile();
      await expect(index.load(fs, '/data/test.idx')).rejects.toThrow(IndexCorruptedError);
    });
  });

  describe('header validation', () => {
    test('should throw on invalid magic', async () => {
      const buffer = new Uint8Array(INDEX_HEADER_SIZE);
      const view = new DataView(buffer.buffer);
      view.setUint32(0, 0xdeadbeef, true); // Invalid magic
      fs.setFileContent('/data/bad.idx', buffer);

      const index = new SegmentIndexFile();
      await expect(index.load(fs, '/data/bad.idx')).rejects.toThrow('Invalid magic');
    });

    test('should throw on unsupported version', async () => {
      const buffer = new Uint8Array(INDEX_HEADER_SIZE);
      const view = new DataView(buffer.buffer);
      view.setUint32(0, INDEX_MAGIC, true);
      view.setUint32(4, 999, true); // Future version
      fs.setFileContent('/data/bad.idx', buffer);

      const index = new SegmentIndexFile();
      await expect(index.load(fs, '/data/bad.idx')).rejects.toThrow('Unsupported version');
    });

    test('should throw on file too small', async () => {
      fs.setFileContent('/data/tiny.idx', new Uint8Array(10));

      const index = new SegmentIndexFile();
      await expect(index.load(fs, '/data/tiny.idx')).rejects.toThrow('too small');
    });
  });

  describe('atomic write', () => {
    test('should write via temp file', async () => {
      // After write, temp file should not exist
      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, [createEntry()]);

      expect(await fs.exists('/data/test.idx')).toBe(true);
      expect(await fs.exists('/data/test.idx.tmp')).toBe(false);
    });

    test('should create valid file even if crashed mid-write', async () => {
      // First write succeeds
      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, [
        createEntry({ streamId: 'original', revision: 0 }),
      ]);

      // Simulate crash during second write (temp file left behind)
      fs.setFileContent('/data/test.idx.tmp', new Uint8Array(100));

      // Third write should overwrite temp and succeed
      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, [
        createEntry({ streamId: 'updated', revision: 0 }),
      ]);

      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');
      expect(index.getAllStreamIds().has('updated')).toBe(true);
    });
  });

  describe('loadIntoMemory', () => {
    test('should work as fallback loading method', async () => {
      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, [
        createEntry({ streamId: 'stream-1', revision: 5 }),
      ]);

      const index = new SegmentIndexFile();
      await index.loadIntoMemory(fs, '/data/test.idx');

      expect(index.isLoaded()).toBe(true);
      expect(index.getStreamMaxRevision('stream-1')).toBe(5);
    });
  });

  describe('edge cases', () => {
    test('should handle empty streamId', async () => {
      const entries = [createEntry({ streamId: '', revision: 0 })];

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);
      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      expect(index.getAllStreamIds().has('')).toBe(true);
    });

    test('should handle very long streamId', async () => {
      const longId = 'a'.repeat(1000);
      const entries = [createEntry({ streamId: longId, revision: 0 })];

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);
      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      expect(index.getAllStreamIds().has(longId)).toBe(true);
    });

    test('should handle large revision numbers', async () => {
      const entries = [createEntry({ revision: 2147483647 })]; // Max int32

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);
      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      expect(index.getAllEntries()[0]!.revision).toBe(2147483647);
    });

    test('should handle large global positions', async () => {
      const largePos = BigInt('9223372036854775807'); // Max int64
      const entries = [createEntry({ globalPosition: largePos })];

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);
      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      expect(index.getAllEntries()[0]!.globalPosition).toBe(largePos);
    });

    test('should handle many entries efficiently', async () => {
      const entries = Array.from({ length: 10000 }, (_, i) =>
        createEntry({
          streamId: `stream-${i % 100}`, // 100 unique streams
          revision: Math.floor(i / 100),
          globalPosition: BigInt(i),
          batchOffset: 100 + i * 50,
        })
      );

      await SegmentIndexFile.write(fs, '/data/test.idx', 1n, entries);
      const index = new SegmentIndexFile();
      await index.load(fs, '/data/test.idx');

      expect(index.getEntryCount()).toBe(10000);
      expect(index.getAllStreamIds().size).toBe(100);

      // Binary search should still be fast
      const stream50Entries = index.findByStream('stream-50');
      expect(stream50Entries).toHaveLength(100);
    });
  });
});
