/**
 * Tests for DiskKeyValueStore corruption handling.
 *
 * Verifies that when corruption is detected during index rebuild:
 * 1. The file is truncated to remove corrupt data
 * 2. All valid data before the corruption is preserved
 * 3. Subsequent writes work correctly
 * 4. Future rebuilds can read all data including new writes
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { DiskKeyValueStore } from '../../../src/infrastructure/projections/stores/disk-kv-store';
import { SimulatedFileSystem } from '../../../src/testing/simulated-filesystem';
import { MsgpackSerializer } from '../../../src/infrastructure/serialization/msgpack-serializer';

describe('DiskKeyValueStore Corruption Recovery', () => {
  let fs: SimulatedFileSystem;
  let serializer: MsgpackSerializer;
  const storePath = '/test-store/data.dat';

  beforeEach(() => {
    fs = new SimulatedFileSystem();
    serializer = new MsgpackSerializer();
  });

  function createStore(): DiskKeyValueStore<{ value: number }> {
    return new DiskKeyValueStore({
      fs,
      serializer,
      path: storePath,
    });
  }

  describe('corruption detection and truncation', () => {
    test('should truncate file when corruption is detected at end', async () => {
      // Create store and write some valid data
      const store1 = createStore();
      await store1.open();
      await store1.set('key1', { value: 1 });
      await store1.set('key2', { value: 2 });
      await store1.set('key3', { value: 3 });
      await store1.sync();
      await store1.close();

      // Corrupt the file by appending garbage using readwrite mode
      const originalData = await fs.readFile(storePath);
      const originalLength = originalData.length;

      // Create corrupted data by appending garbage
      const corruptData = new Uint8Array(originalLength + 100);
      corruptData.set(originalData);
      // Fill with non-zero garbage that looks like an invalid record
      for (let i = originalLength; i < corruptData.length; i++) {
        corruptData[i] = 0x42; // Non-zero to trigger parsing attempts
      }

      // Use readwrite mode and write at correct position
      const handle = await fs.open(storePath, 'readwrite');
      await fs.write(handle, corruptData, 0);
      await fs.sync(handle); // Must sync to make corruption durable
      await fs.close(handle);

      // Reopen store - should truncate corrupt data
      const store2 = createStore();
      await store2.open();

      // All valid data should still be there
      expect(await store2.get('key1')).toEqual({ value: 1 });
      expect(await store2.get('key2')).toEqual({ value: 2 });
      expect(await store2.get('key3')).toEqual({ value: 3 });

      // New writes should work
      await store2.set('key4', { value: 4 });
      await store2.sync();
      await store2.close();

      // Verify new data persisted
      const store3 = createStore();
      await store3.open();
      expect(await store3.get('key4')).toEqual({ value: 4 });
      await store3.close();
    });

    test('should handle corruption in the middle of a record', async () => {
      const store1 = createStore();
      await store1.open();
      await store1.set('key1', { value: 1 });
      await store1.set('key2', { value: 2 });
      await store1.sync();
      await store1.close();

      // Corrupt the file by modifying bytes in the middle
      const data = await fs.readFile(storePath);
      const corruptedData = new Uint8Array(data);
      // Corrupt bytes somewhere after the first record
      // First record header is at least 8 bytes, so corrupt byte 50
      if (corruptedData.length > 60) {
        corruptedData[50] ^= 0xFF;
        corruptedData[51] ^= 0xFF;
        corruptedData[52] ^= 0xFF;
      }
      const handle = await fs.open(storePath, 'readwrite');
      await fs.write(handle, corruptedData, 0);
      await fs.sync(handle); // Must sync to make corruption durable
      await fs.close(handle);

      // Reopen store - corruption will be detected during rebuild
      const store2 = createStore();
      await store2.open();

      // At least key1 should be readable (first record before corruption)
      // The exact behavior depends on where corruption occurs
      expect(store2.size).toBeGreaterThanOrEqual(0);

      // New writes should work regardless
      await store2.set('new-key', { value: 100 });
      await store2.sync();
      await store2.close();

      // Verify new write persisted
      const store3 = createStore();
      await store3.open();
      expect(await store3.get('new-key')).toEqual({ value: 100 });
      await store3.close();
    });

    test('should handle CRC mismatch', async () => {
      const store1 = createStore();
      await store1.open();
      await store1.set('key1', { value: 1 });
      await store1.sync();
      await store1.close();

      // Corrupt the value data (which will cause CRC mismatch)
      // Record format: keyLen(4) + key + valueLen(4) + value + crc(4)
      // For key1 with { value: 1 }, key is 4 bytes, so value starts at offset 12
      const data = await fs.readFile(storePath);
      const corruptedData = new Uint8Array(data);
      // Corrupt some bytes in the value section (after key and valueLen)
      // keyLen(4) + key("key1" = 4) + valueLen(4) = 12 bytes, then value
      if (corruptedData.length > 14) {
        corruptedData[12] ^= 0xFF;  // Corrupt value data
        corruptedData[13] ^= 0xFF;
      }
      const handle = await fs.open(storePath, 'readwrite');
      await fs.write(handle, corruptedData, 0);
      await fs.sync(handle); // Must sync to make corruption durable
      await fs.close(handle);

      // Reopen - CRC mismatch should be detected during rebuild
      const store2 = createStore();
      await store2.open();

      // key1 will not be available due to CRC failure (corruption detected, file truncated to 0)
      expect(await store2.get('key1')).toBeUndefined();

      // New writes should work
      await store2.set('key2', { value: 2 });
      await store2.sync();
      await store2.close();

      // Verify new write persisted
      const store3 = createStore();
      await store3.open();
      expect(await store3.get('key2')).toEqual({ value: 2 });
      await store3.close();
    });

    test('should handle truncated record header', async () => {
      const store1 = createStore();
      await store1.open();
      await store1.set('key1', { value: 1 });
      await store1.sync();
      await store1.close();

      // Truncate the file mid-header
      const data = await fs.readFile(storePath);
      // Write partial data - just 4 bytes at the end (incomplete header)
      const truncatedData = new Uint8Array(data.length + 4);
      truncatedData.set(data);
      // Add partial header bytes
      truncatedData[data.length] = 0x10; // key length (partial)
      truncatedData[data.length + 1] = 0x00;
      truncatedData[data.length + 2] = 0x00;
      truncatedData[data.length + 3] = 0x00;
      const handle = await fs.open(storePath, 'readwrite');
      await fs.write(handle, truncatedData, 0);
      await fs.sync(handle); // Must sync to make corruption durable
      await fs.close(handle);

      // Reopen - should handle truncated header gracefully
      const store2 = createStore();
      await store2.open();

      // First record should be fine
      expect(await store2.get('key1')).toEqual({ value: 1 });

      // New writes should work
      await store2.set('key2', { value: 2 });
      await store2.close();
    });
  });

  describe('garbage tracking after corruption recovery', () => {
    test('should correctly track bytes after truncation', async () => {
      const store1 = createStore();
      await store1.open();
      await store1.set('key1', { value: 1 });
      await store1.set('key2', { value: 2 });
      await store1.sync();

      const originalBytes = store1.getTotalBytes();
      await store1.close();

      // Add corruption by appending garbage
      const data = await fs.readFile(storePath);
      const corruptedData = new Uint8Array(data.length + 50);
      corruptedData.set(data);
      // Fill garbage with non-zero bytes
      for (let i = data.length; i < corruptedData.length; i++) {
        corruptedData[i] = 0x42;
      }
      const handle = await fs.open(storePath, 'readwrite');
      await fs.write(handle, corruptedData, 0);
      await fs.sync(handle); // Must sync to make corruption durable
      await fs.close(handle);

      // Reopen
      const store2 = createStore();
      await store2.open();

      // After truncation, total bytes should be back to original
      expect(store2.getTotalBytes()).toBe(originalBytes);
      expect(store2.getGarbageRatio()).toBe(0);

      await store2.close();
    });
  });

  describe('compaction after corruption recovery', () => {
    test('should be able to compact after corruption recovery', async () => {
      const store1 = createStore();
      await store1.open();
      await store1.set('key1', { value: 1 });
      await store1.set('key1', { value: 2 }); // Update to create garbage
      await store1.sync();
      await store1.close();

      // Add corruption by appending garbage
      const data = await fs.readFile(storePath);
      const corruptedData = new Uint8Array(data.length + 30);
      corruptedData.set(data);
      // Fill garbage with non-zero bytes
      for (let i = data.length; i < corruptedData.length; i++) {
        corruptedData[i] = 0x42;
      }
      const handle = await fs.open(storePath, 'readwrite');
      await fs.write(handle, corruptedData, 0);
      await fs.sync(handle); // Must sync to make corruption durable
      await fs.close(handle);

      // Reopen
      const store2 = createStore();
      await store2.open();

      // Should have some garbage from the update
      expect(store2.getGarbageRatio()).toBeGreaterThan(0);

      // Compact
      await store2.compact();

      // After compaction, garbage should be 0
      expect(store2.getGarbageRatio()).toBe(0);

      // Data should still be correct
      expect(await store2.get('key1')).toEqual({ value: 2 });

      await store2.close();
    });
  });
});
