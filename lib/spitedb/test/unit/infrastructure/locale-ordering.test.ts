/**
 * Tests for locale-independent string ordering.
 *
 * Verifies that segment index ordering is deterministic across platforms
 * and locales. Using localeCompare with ambient locale can cause
 * inconsistent index ordering if written under one locale and read
 * under another.
 *
 * The fix uses byte-wise (code point) comparison for deterministic ordering.
 */

import { describe, test, expect } from 'bun:test';
import { SegmentIndexFile } from '../../../src/infrastructure/storage/segments/segment-index-file';
import { SimulatedFileSystem } from '../../../src/testing/simulated-filesystem';
import type { IndexEntry } from '../../../src/infrastructure/storage/segments/segment-index-file';

describe('Locale-Independent Ordering', () => {
  const fs = new SimulatedFileSystem();
  const indexPath = '/test-locale/index.idx';

  /**
   * Helper to create and write an index file, then read it back.
   */
  async function roundTrip(entries: IndexEntry[]): Promise<IndexEntry[]> {
    // Clean up any existing file
    if (await fs.exists(indexPath)) {
      await fs.unlink(indexPath);
    }

    // Ensure directory exists
    if (!(await fs.exists('/test-locale'))) {
      await fs.mkdir('/test-locale', { recursive: true });
    }

    // Write index
    await SegmentIndexFile.write(fs, indexPath, 1n, entries);

    // Read index
    const indexFile = new SegmentIndexFile();
    await indexFile.load(fs, indexPath);

    return indexFile.getAllEntries();
  }

  describe('compareStreamIds byte-wise ordering', () => {
    test('should sort ASCII strings consistently', async () => {
      const entries: IndexEntry[] = [
        { streamId: 'zebra', revision: 0, globalPosition: 0, batchOffset: 100 },
        { streamId: 'apple', revision: 0, globalPosition: 1, batchOffset: 200 },
        { streamId: 'APPLE', revision: 0, globalPosition: 2, batchOffset: 300 },
        { streamId: 'banana', revision: 0, globalPosition: 3, batchOffset: 400 },
      ];

      const result = await roundTrip(entries);

      // Byte-wise: uppercase comes before lowercase
      // 'A' = 65, 'B' = 66, ..., 'a' = 97, 'b' = 98, ...
      expect(result.map((e) => e.streamId)).toEqual([
        'APPLE',  // A = 65
        'apple',  // a = 97
        'banana', // b = 98
        'zebra',  // z = 122
      ]);
    });

    test('should handle empty string', async () => {
      const entries: IndexEntry[] = [
        { streamId: '', revision: 0, globalPosition: 0, batchOffset: 100 },
        { streamId: 'a', revision: 0, globalPosition: 1, batchOffset: 200 },
        { streamId: '', revision: 1, globalPosition: 2, batchOffset: 300 },
      ];

      const result = await roundTrip(entries);

      // Empty string sorts first
      expect(result[0]!.streamId).toBe('');
      expect(result[1]!.streamId).toBe('');
      expect(result[2]!.streamId).toBe('a');
    });

    test('should handle numbers as strings correctly', async () => {
      const entries: IndexEntry[] = [
        { streamId: '10', revision: 0, globalPosition: 0, batchOffset: 100 },
        { streamId: '2', revision: 0, globalPosition: 1, batchOffset: 200 },
        { streamId: '1', revision: 0, globalPosition: 2, batchOffset: 300 },
      ];

      const result = await roundTrip(entries);

      // Lexicographic ordering, not numeric
      // '1' < '10' < '2' (by first character, then length)
      expect(result.map((e) => e.streamId)).toEqual(['1', '10', '2']);
    });

    test('should handle Unicode characters deterministically', async () => {
      const entries: IndexEntry[] = [
        { streamId: 'café', revision: 0, globalPosition: 0, batchOffset: 100 },
        { streamId: 'cafe', revision: 0, globalPosition: 1, batchOffset: 200 },
        { streamId: '日本', revision: 0, globalPosition: 2, batchOffset: 300 },
        { streamId: '中国', revision: 0, globalPosition: 3, batchOffset: 400 },
        { streamId: 'αβγ', revision: 0, globalPosition: 4, batchOffset: 500 },
      ];

      const result = await roundTrip(entries);

      // Order is deterministic based on UTF-8 code points
      // ASCII < Greek < CJK
      expect(result[0]!.streamId).toBe('cafe');  // ASCII
      expect(result[1]!.streamId).toBe('café');  // é = U+00E9 (Latin Extended)
      expect(result[2]!.streamId).toBe('αβγ');   // Greek starts at U+0370
      // CJK characters have high code points
    });

    test('should handle stream IDs with special characters', async () => {
      const entries: IndexEntry[] = [
        { streamId: 'user-123', revision: 0, globalPosition: 0, batchOffset: 100 },
        { streamId: 'user_123', revision: 0, globalPosition: 1, batchOffset: 200 },
        { streamId: 'user.123', revision: 0, globalPosition: 2, batchOffset: 300 },
        { streamId: 'user:123', revision: 0, globalPosition: 3, batchOffset: 400 },
      ];

      const result = await roundTrip(entries);

      // Verify order is deterministic (based on ASCII values)
      // - = 45, . = 46, : = 58, _ = 95
      expect(result.map((e) => e.streamId)).toEqual([
        'user-123',  // - = 45
        'user.123',  // . = 46
        'user:123',  // : = 58
        'user_123',  // _ = 95
      ]);
    });

    test('should handle strings with different lengths', async () => {
      const entries: IndexEntry[] = [
        { streamId: 'abc', revision: 0, globalPosition: 0, batchOffset: 100 },
        { streamId: 'ab', revision: 0, globalPosition: 1, batchOffset: 200 },
        { streamId: 'abcd', revision: 0, globalPosition: 2, batchOffset: 300 },
        { streamId: 'a', revision: 0, globalPosition: 3, batchOffset: 400 },
      ];

      const result = await roundTrip(entries);

      // Prefix sorts before longer strings
      expect(result.map((e) => e.streamId)).toEqual([
        'a',
        'ab',
        'abc',
        'abcd',
      ]);
    });
  });

  describe('findByStream with byte-wise comparison', () => {
    test('should find entries using binary search', async () => {
      const entries: IndexEntry[] = [
        { streamId: 'stream-a', revision: 0, globalPosition: 0, batchOffset: 100 },
        { streamId: 'stream-a', revision: 1, globalPosition: 1, batchOffset: 200 },
        { streamId: 'stream-b', revision: 0, globalPosition: 2, batchOffset: 300 },
        { streamId: 'stream-c', revision: 0, globalPosition: 3, batchOffset: 400 },
      ];

      // Ensure directory exists
      if (!(await fs.exists('/test-locale'))) {
        await fs.mkdir('/test-locale', { recursive: true });
      }

      await SegmentIndexFile.write(fs, indexPath, 1n, entries);

      const indexFile = new SegmentIndexFile();
      await indexFile.load(fs, indexPath);

      // Find specific stream
      const streamAEntries = indexFile.findByStream('stream-a');
      expect(streamAEntries).toHaveLength(2);
      expect(streamAEntries[0]!.revision).toBe(0);
      expect(streamAEntries[1]!.revision).toBe(1);

      // Find non-existent stream
      const noEntries = indexFile.findByStream('stream-x');
      expect(noEntries).toHaveLength(0);
    });

    test('should find entries with Unicode stream IDs', async () => {
      const entries: IndexEntry[] = [
        { streamId: '用户-1', revision: 0, globalPosition: 0, batchOffset: 100 },
        { streamId: '用户-1', revision: 1, globalPosition: 1, batchOffset: 200 },
        { streamId: '用户-2', revision: 0, globalPosition: 2, batchOffset: 300 },
      ];

      // Ensure directory exists
      if (!(await fs.exists('/test-locale'))) {
        await fs.mkdir('/test-locale', { recursive: true });
      }

      await SegmentIndexFile.write(fs, indexPath, 1n, entries);

      const indexFile = new SegmentIndexFile();
      await indexFile.load(fs, indexPath);

      const result = indexFile.findByStream('用户-1');
      expect(result).toHaveLength(2);
    });
  });

  describe('cross-platform consistency', () => {
    test('should produce same ordering regardless of locale', async () => {
      // These strings might sort differently under different locales
      const problematicStrings = [
        'ä',    // German umlaut
        'a',    // Plain ASCII
        'à',    // French grave accent
        'á',    // Spanish acute accent
        'ã',    // Portuguese tilde
      ];

      const entries: IndexEntry[] = problematicStrings.map((s, i) => ({
        streamId: s,
        revision: 0,
        globalPosition: i,
        batchOffset: (i + 1) * 100,
      }));

      const result = await roundTrip(entries);

      // Byte-wise ordering should be consistent
      // a = 97, à = 224, á = 225, â = 226, ã = 227, ä = 228
      expect(result.map((e) => e.streamId)).toEqual([
        'a',   // 97
        'à',   // 224
        'á',   // 225
        'ã',   // 227
        'ä',   // 228
      ]);
    });

    test('should maintain ordering through multiple write/read cycles', async () => {
      const entries: IndexEntry[] = [
        { streamId: 'zebra', revision: 0, globalPosition: 0, batchOffset: 100 },
        { streamId: 'ZEBRA', revision: 0, globalPosition: 1, batchOffset: 200 },
        { streamId: 'apple', revision: 0, globalPosition: 2, batchOffset: 300 },
      ];

      // Multiple round trips
      let result = await roundTrip(entries);
      const firstOrder = result.map((e) => e.streamId);

      for (let i = 0; i < 3; i++) {
        result = await roundTrip(result);
        expect(result.map((e) => e.streamId)).toEqual(firstOrder);
      }
    });
  });
});
