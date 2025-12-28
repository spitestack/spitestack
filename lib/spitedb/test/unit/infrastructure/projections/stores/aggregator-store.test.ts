/**
 * Unit tests for AggregatorStore.
 *
 * Tests the in-memory projection store with:
 * - State management (get/set)
 * - Checkpointing (persist/load)
 * - Corruption detection
 * - DST scenarios
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { AggregatorStore } from '../../../../../src/infrastructure/projections/stores/aggregator-store';
import {
  CheckpointWriteError,
  CheckpointCorruptionError,
  CheckpointVersionError,
} from '../../../../../src/errors';
import { createTestEnvironment, type TestEnvironment } from '../../../../setup/test-helpers';
import { FaultScheduler } from '../../../../setup/fault-scheduler';
import { SeededRandom } from '../../../../setup/seeded-random';

describe('AggregatorStore', () => {
  let env: TestEnvironment;
  let store: AggregatorStore<number>;
  const dataDir = '/test-aggregator';
  const projectionName = 'TestAggregator';

  beforeEach(async () => {
    env = createTestEnvironment(12345);
    store = new AggregatorStore(
      {
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
      },
      0
    );
    await store.initialize();
  });

  describe('initialize', () => {
    test('should create data directory if not exists', async () => {
      const newStore = new AggregatorStore(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir: '/new-dir',
          projectionName: 'New',
        },
        0
      );

      await newStore.initialize();

      expect(await env.fs.exists('/new-dir')).toBe(true);
    });

    test('should not fail if directory exists', async () => {
      // Already initialized in beforeEach
      await expect(store.initialize()).resolves.toBeUndefined();
    });
  });

  describe('get/set', () => {
    test('should return initial state', () => {
      expect(store.get()).toBe(0);
    });

    test('should update state with set', () => {
      store.set(42);
      expect(store.get()).toBe(42);
    });

    test('should handle multiple updates', () => {
      store.set(10);
      store.set(store.get() + 5);
      store.set(store.get() * 2);
      expect(store.get()).toBe(30);
    });

    test('should handle complex state objects', () => {
      const complexStore = new AggregatorStore<{ count: number; sum: number }>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'ComplexStore',
        },
        { count: 0, sum: 0 }
      );

      complexStore.set({ count: 10, sum: 100 });
      expect(complexStore.get()).toEqual({ count: 10, sum: 100 });
    });

    test('should isolate initial state from mutations', () => {
      const objStore = new AggregatorStore<{ value: number }>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'ObjStore',
        },
        { value: 1 }
      );

      // Get and mutate the state
      const state = objStore.get();
      state.value = 999;

      // Reset should restore original initial state
      objStore.reset();
      expect(objStore.get()).toEqual({ value: 1 });
    });
  });

  describe('persist', () => {
    test('should persist state to checkpoint file', async () => {
      store.set(12345);
      await store.persist(100);

      expect(await env.fs.exists('/test-aggregator/TestAggregator.ckpt')).toBe(true);
    });

    test('should persist correct position', async () => {
      store.set(42);
      await store.persist(999);

      const loaded = await store.load();
      expect(loaded).toBe(999);
    });

    test('should use atomic write pattern', async () => {
      const operations: string[] = [];
      const originalOpen = env.fs.open.bind(env.fs);
      const originalRename = env.fs.rename.bind(env.fs);

      env.fs.open = async (path, mode) => {
        operations.push(`open:${path}`);
        return originalOpen(path, mode);
      };

      env.fs.rename = async (from, to) => {
        operations.push(`rename:${from}:${to}`);
        return originalRename(from, to);
      };

      await store.persist(1);

      expect(operations).toContainEqual('open:/test-aggregator/TestAggregator.ckpt.tmp');
      expect(operations).toContainEqual(
        'rename:/test-aggregator/TestAggregator.ckpt.tmp:/test-aggregator/TestAggregator.ckpt'
      );
    });

    test('should throw CheckpointWriteError on sync failure', async () => {
      env.fs.injectFault({ syncFails: true });
      await expect(store.persist(1)).rejects.toThrow(CheckpointWriteError);
    });

    test('should clean up temp file on failure', async () => {
      env.fs.injectFault({ syncFails: true });

      try {
        await store.persist(1);
      } catch {
        // Expected
      }

      expect(await env.fs.exists('/test-aggregator/TestAggregator.ckpt.tmp')).toBe(false);
    });

    test('should overwrite existing checkpoint', async () => {
      store.set(100);
      await store.persist(10);

      store.set(200);
      await store.persist(20);

      // Create new store and load
      const newStore = new AggregatorStore(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName,
        },
        0
      );
      const position = await newStore.load();

      expect(position).toBe(20);
      expect(newStore.get()).toBe(200);
    });
  });

  describe('load', () => {
    test('should load persisted state', async () => {
      store.set(12345);
      await store.persist(500);

      const newStore = new AggregatorStore(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName,
        },
        0
      );
      await newStore.initialize();
      const position = await newStore.load();

      expect(position).toBe(500);
      expect(newStore.get()).toBe(12345);
    });

    test('should return null if no checkpoint exists', async () => {
      const result = await store.load();
      expect(result).toBeNull();
    });

    test('should detect corrupted magic number', async () => {
      store.set(42);
      await store.persist(1);

      // Corrupt magic
      const content = await env.fs.readFile('/test-aggregator/TestAggregator.ckpt');
      const corrupted = new Uint8Array(content);
      corrupted[0] = 0x00;
      corrupted[1] = 0x00;
      env.fs.setFileContent('/test-aggregator/TestAggregator.ckpt', corrupted);

      await expect(store.load()).rejects.toThrow(CheckpointCorruptionError);
    });

    test('should detect corrupted checksum', async () => {
      // Use a larger state to ensure we have bytes to corrupt
      const largeStore = new AggregatorStore<{ data: number[] }>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'CorruptTest',
        },
        { data: [] }
      );
      await largeStore.initialize();
      largeStore.set({ data: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] });
      await largeStore.persist(1);

      // Corrupt state data (after 32-byte header)
      const content = await env.fs.readFile('/test-aggregator/CorruptTest.ckpt');
      const corrupted = new Uint8Array(content);
      // Corrupt bytes in the state portion (after header)
      const stateStart = 32;
      if (corrupted.length > stateStart + 5) {
        corrupted[stateStart + 2] = (corrupted[stateStart + 2] ?? 0) ^ 0xff;
        corrupted[stateStart + 3] = (corrupted[stateStart + 3] ?? 0) ^ 0xff;
      }
      env.fs.setFileContent('/test-aggregator/CorruptTest.ckpt', corrupted);

      await expect(largeStore.load()).rejects.toThrow(CheckpointCorruptionError);
    });

    test('should detect truncated file', async () => {
      store.set(42);
      await store.persist(1);

      // Truncate file
      const content = await env.fs.readFile('/test-aggregator/TestAggregator.ckpt');
      env.fs.setFileContent('/test-aggregator/TestAggregator.ckpt', content.slice(0, 20));

      await expect(store.load()).rejects.toThrow(CheckpointCorruptionError);
    });

    test('should detect file too small for header', async () => {
      env.fs.setFileContent('/test-aggregator/TestAggregator.ckpt', new Uint8Array(10));

      await expect(store.load()).rejects.toThrow(CheckpointCorruptionError);
    });
  });

  describe('reset', () => {
    test('should reset to initial state', () => {
      store.set(99999);
      store.reset();
      expect(store.get()).toBe(0);
    });

    test('should reset complex state to initial', async () => {
      const objStore = new AggregatorStore<{ a: number; b: string }>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'ObjStore',
        },
        { a: 1, b: 'initial' }
      );

      objStore.set({ a: 100, b: 'modified' });
      objStore.reset();

      expect(objStore.get()).toEqual({ a: 1, b: 'initial' });
    });
  });

  describe('getMemoryUsage', () => {
    test('should return approximate memory usage', () => {
      store.set(12345);
      const usage = store.getMemoryUsage();
      expect(usage).toBeGreaterThan(0);
    });

    test('should increase with larger state', async () => {
      const smallStore = new AggregatorStore<number>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'Small',
        },
        0
      );

      const largeStore = new AggregatorStore<{ items: number[] }>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'Large',
        },
        { items: [] }
      );

      smallStore.set(42);
      largeStore.set({ items: Array.from({ length: 1000 }, (_, i) => i) });

      expect(largeStore.getMemoryUsage()).toBeGreaterThan(smallStore.getMemoryUsage());
    });
  });

  describe('close', () => {
    test('should close without error', async () => {
      await expect(store.close()).resolves.toBeUndefined();
    });
  });

  describe('DST scenarios', () => {
    test('should maintain consistency after crash and recovery', async () => {
      store.set(12345);
      await store.persist(100);

      // Simulate crash
      env.fs.crash();

      // Create new store and recover
      const recoveredStore = new AggregatorStore(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName,
        },
        0
      );
      await recoveredStore.initialize();
      const position = await recoveredStore.load();

      expect(position).toBe(100);
      expect(recoveredStore.get()).toBe(12345);
    });

    test('should handle crash during persist (temp file exists)', async () => {
      store.set(100);
      await store.persist(10);

      // Simulate crash during second persist by leaving temp file
      const originalContent = await env.fs.readFile('/test-aggregator/TestAggregator.ckpt');
      env.fs.setFileContent('/test-aggregator/TestAggregator.ckpt.tmp', new Uint8Array(20));

      env.fs.crash();

      // Recovery should use the valid checkpoint
      const recoveredStore = new AggregatorStore(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName,
        },
        0
      );
      await recoveredStore.initialize();
      const position = await recoveredStore.load();

      expect(position).toBe(10);
      expect(recoveredStore.get()).toBe(100);
    });

    test('should handle write failures and allow retry', async () => {
      const faultScheduler = new FaultScheduler(env.random, env.fs);

      store.set(100);

      // First persist should succeed
      await store.persist(10);

      // Inject fault for second persist
      faultScheduler.injectFault('sync');

      store.set(200);
      await expect(store.persist(20)).rejects.toThrow();

      // Clear fault and retry
      faultScheduler.clearFaults();

      await store.persist(20);

      // Verify recovery
      const newStore = new AggregatorStore(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName,
        },
        0
      );
      const position = await newStore.load();

      expect(position).toBe(20);
      expect(newStore.get()).toBe(200);
    });

    test('should handle random persist/load cycles with failures (fuzz)', async () => {
      const random = new SeededRandom(42);
      const faultScheduler = new FaultScheduler(random, env.fs);
      let expectedValue = 0;
      let expectedPosition = 0;

      for (let i = 0; i < 50; i++) {
        // Maybe inject fault
        if (random.bool(0.15)) {
          faultScheduler.injectFault('sync');
        }

        const newValue = random.int(0, 10000);
        const newPosition = i;

        try {
          store.set(newValue);
          await store.persist(newPosition);
          // Update expected only if persist succeeded
          expectedValue = newValue;
          expectedPosition = newPosition;
        } catch {
          // Expected on fault - state should remain at last successful persist
          store.set(expectedValue); // Restore state
        }

        faultScheduler.clearFaults();
      }

      // Verify final state matches expected
      const finalStore = new AggregatorStore(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName,
        },
        0
      );
      const position = await finalStore.load();

      expect(position).toBe(expectedPosition);
      expect(finalStore.get()).toBe(expectedValue);
    });
  });

  describe('edge cases', () => {
    test('should handle null state', async () => {
      const nullStore = new AggregatorStore<null>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'NullStore',
        },
        null
      );
      await nullStore.initialize();

      await nullStore.persist(1);

      const loadedStore = new AggregatorStore<null>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'NullStore',
        },
        null
      );
      await loadedStore.load();

      expect(loadedStore.get()).toBeNull();
    });

    test('should handle position = 0', async () => {
      store.set(42);
      await store.persist(0);

      const newStore = new AggregatorStore(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName,
        },
        0
      );
      const position = await newStore.load();

      expect(position).toBe(0);
    });

    test('should handle very large position', async () => {
      const largePosition = Number.MAX_SAFE_INTEGER;
      store.set(42);
      await store.persist(largePosition);

      const newStore = new AggregatorStore(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName,
        },
        0
      );
      const position = await newStore.load();

      expect(position).toBe(largePosition);
    });

    test('should sanitize projection name for filesystem', async () => {
      const specialStore = new AggregatorStore(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'Test/Special:Chars*?',
        },
        0
      );
      await specialStore.initialize();

      specialStore.set(42);
      await specialStore.persist(1);

      expect(await env.fs.exists('/test-aggregator/Test_Special_Chars__.ckpt')).toBe(true);
    });

    test('should handle unicode in state', async () => {
      const unicodeStore = new AggregatorStore<{ msg: string }>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'Unicode',
        },
        { msg: '' }
      );
      await unicodeStore.initialize();

      unicodeStore.set({ msg: '你好世界 🎉' });
      await unicodeStore.persist(1);

      const loadedStore = new AggregatorStore<{ msg: string }>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'Unicode',
        },
        { msg: '' }
      );
      await loadedStore.load();

      expect(loadedStore.get()).toEqual({ msg: '你好世界 🎉' });
    });

    test('should handle large state', async () => {
      const largeStore = new AggregatorStore<{ data: number[] }>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'LargeState',
        },
        { data: [] }
      );
      await largeStore.initialize();

      // Create large state
      const largeData = Array.from({ length: 10000 }, (_, i) => i);
      largeStore.set({ data: largeData });
      await largeStore.persist(1);

      const loadedStore = new AggregatorStore<{ data: number[] }>(
        {
          fs: env.fs,
          serializer: env.serializer,
          clock: env.clock,
          dataDir,
          projectionName: 'LargeState',
        },
        { data: [] }
      );
      await loadedStore.load();

      expect(loadedStore.get().data).toHaveLength(10000);
      expect(loadedStore.get().data[9999]).toBe(9999);
    });
  });
});
