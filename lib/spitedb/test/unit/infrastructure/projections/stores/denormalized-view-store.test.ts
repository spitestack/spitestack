/**
 * Unit tests for DenormalizedViewStore.
 *
 * Tests the denormalized view projection store with:
 * - Key-value operations (get/set/delete)
 * - Equality and range index queries
 * - Checkpointing with CRC32 validation
 * - Corruption detection
 * - DST scenarios
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import {
  DenormalizedViewStore,
  type DenormalizedViewStoreConfig,
} from '../../../../../src/infrastructure/projections/stores/denormalized-view-store';
import {
  CheckpointWriteError,
  CheckpointCorruptionError,
  CheckpointVersionError,
} from '../../../../../src/errors';
import { createTestEnvironment, type TestEnvironment } from '../../../../setup/test-helpers';
import { FaultScheduler } from '../../../../setup/fault-scheduler';
import { SeededRandom } from '../../../../setup/seeded-random';

interface TestRow {
  id: string;
  name: string;
  status: string;
  amount: number;
  createdAt: number;
  [key: string]: unknown;
}

describe('DenormalizedViewStore', () => {
  let env: TestEnvironment;
  let store: DenormalizedViewStore<TestRow>;
  const dataDir = '/test-views';
  const projectionName = 'TestView';

  beforeEach(async () => {
    env = createTestEnvironment(12345);
    store = new DenormalizedViewStore<TestRow>({
      fs: env.fs,
      serializer: env.serializer,
      clock: env.clock,
      dataDir,
      projectionName,
      indexFields: ['status', 'name'],
      rangeFields: ['amount'],
    });
    await store.initialize();
  });

  describe('initialize', () => {
    test('should create data directory if not exists', async () => {
      const newStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir: '/new-view-dir',
        projectionName: 'NewView',
      });

      await newStore.initialize();

      expect(await env.fs.exists('/new-view-dir')).toBe(true);
    });

    test('should not fail if directory exists', async () => {
      // Already initialized in beforeEach
      await expect(store.initialize()).resolves.toBeUndefined();
    });
  });

  describe('setByKey/getByKey', () => {
    test('should store and retrieve a row', () => {
      const row: TestRow = {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      };

      store.setByKey('user-1', row);

      expect(store.getByKey('user-1')).toEqual(row);
    });

    test('should return undefined for non-existent key', () => {
      expect(store.getByKey('non-existent')).toBeUndefined();
    });

    test('should overwrite existing row', () => {
      const row1: TestRow = {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      };
      const row2: TestRow = {
        id: 'user-1',
        name: 'Alice Updated',
        status: 'inactive',
        amount: 200,
        createdAt: 2000,
      };

      store.setByKey('user-1', row1);
      store.setByKey('user-1', row2);

      expect(store.getByKey('user-1')).toEqual(row2);
    });

    test('should handle multiple rows', () => {
      for (let i = 0; i < 100; i++) {
        store.setByKey(`user-${i}`, {
          id: `user-${i}`,
          name: `User ${i}`,
          status: i % 2 === 0 ? 'active' : 'inactive',
          amount: i * 10,
          createdAt: i * 1000,
        });
      }

      expect(store.size).toBe(100);
      expect(store.getByKey('user-50')?.name).toBe('User 50');
    });
  });

  describe('deleteByKey', () => {
    test('should delete existing row', () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      const deleted = store.deleteByKey('user-1');

      expect(deleted).toBe(true);
      expect(store.getByKey('user-1')).toBeUndefined();
    });

    test('should return false for non-existent key', () => {
      const deleted = store.deleteByKey('non-existent');
      expect(deleted).toBe(false);
    });

    test('should update indexes on delete', () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      store.deleteByKey('user-1');

      // Query should not find deleted row
      const results = store.query({ status: 'active' });
      expect(results).toHaveLength(0);
    });
  });

  describe('get/set (full state)', () => {
    test('should return all rows via get()', () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      store.setByKey('user-2', {
        id: 'user-2',
        name: 'Bob',
        status: 'inactive',
        amount: 200,
        createdAt: 2000,
      });

      const state = store.get();

      expect(state.size).toBe(2);
      expect(state.get('user-1')?.name).toBe('Alice');
      expect(state.get('user-2')?.name).toBe('Bob');
    });

    test('should replace all rows via set()', () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      const newState = new Map<string, TestRow>([
        [
          'user-new',
          {
            id: 'user-new',
            name: 'New User',
            status: 'pending',
            amount: 300,
            createdAt: 3000,
          },
        ],
      ]);

      store.set(newState);

      expect(store.size).toBe(1);
      expect(store.getByKey('user-1')).toBeUndefined();
      expect(store.getByKey('user-new')?.name).toBe('New User');
    });

    test('should rebuild indexes on set()', () => {
      const state = new Map<string, TestRow>([
        [
          'user-1',
          {
            id: 'user-1',
            name: 'Alice',
            status: 'active',
            amount: 100,
            createdAt: 1000,
          },
        ],
        [
          'user-2',
          {
            id: 'user-2',
            name: 'Bob',
            status: 'active',
            amount: 200,
            createdAt: 2000,
          },
        ],
      ]);

      store.set(state);

      const results = store.query({ status: 'active' });
      expect(results).toHaveLength(2);
    });
  });

  describe('query (equality index)', () => {
    beforeEach(() => {
      // Setup test data
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      store.setByKey('user-2', {
        id: 'user-2',
        name: 'Bob',
        status: 'active',
        amount: 200,
        createdAt: 2000,
      });
      store.setByKey('user-3', {
        id: 'user-3',
        name: 'Charlie',
        status: 'inactive',
        amount: 300,
        createdAt: 3000,
      });
      store.setByKey('user-4', {
        id: 'user-4',
        name: 'Alice',
        status: 'pending',
        amount: 400,
        createdAt: 4000,
      });
    });

    test('should return all rows with empty filter', () => {
      const results = store.query({});
      expect(results).toHaveLength(4);
    });

    test('should filter by single indexed field', () => {
      const results = store.query({ status: 'active' });
      expect(results).toHaveLength(2);
      expect(results.map((r) => r.id).sort()).toEqual(['user-1', 'user-2']);
    });

    test('should filter by multiple indexed fields', () => {
      const results = store.query({ name: 'Alice', status: 'active' });
      expect(results).toHaveLength(1);
      expect(results[0]?.id).toBe('user-1');
    });

    test('should return empty array for no matches', () => {
      const results = store.query({ status: 'deleted' });
      expect(results).toHaveLength(0);
    });

    test('should filter by non-indexed field (fallback scan)', () => {
      const results = store.query({ createdAt: 2000 } as Record<string, unknown>);
      expect(results).toHaveLength(1);
      expect(results[0]?.id).toBe('user-2');
    });

    test('should handle mixed indexed and non-indexed fields', () => {
      const results = store.query({ status: 'active', createdAt: 1000 } as Record<string, unknown>);
      expect(results).toHaveLength(1);
      expect(results[0]?.id).toBe('user-1');
    });
  });

  describe('queryRange', () => {
    beforeEach(() => {
      // Use keys that sort naturally for range queries
      store.setByKey('2024-01-01', {
        id: '2024-01-01',
        name: 'Event A',
        status: 'complete',
        amount: 100,
        createdAt: 1704067200,
      });
      store.setByKey('2024-01-15', {
        id: '2024-01-15',
        name: 'Event B',
        status: 'complete',
        amount: 200,
        createdAt: 1705276800,
      });
      store.setByKey('2024-02-01', {
        id: '2024-02-01',
        name: 'Event C',
        status: 'pending',
        amount: 300,
        createdAt: 1706745600,
      });
    });

    test('should query by key range', () => {
      const results = store.queryRange({ start: '2024-01-01', end: '2024-01-31' });
      expect(results.size).toBe(2);
      expect(results.has('2024-01-01')).toBe(true);
      expect(results.has('2024-01-15')).toBe(true);
    });

    test('should handle open-ended start', () => {
      const results = store.queryRange({ end: '2024-01-20' });
      expect(results.size).toBe(2);
    });

    test('should handle open-ended end', () => {
      const results = store.queryRange({ start: '2024-01-15' });
      expect(results.size).toBe(2);
    });

    test('should return all with empty range', () => {
      const results = store.queryRange({});
      expect(results.size).toBe(3);
    });
  });

  describe('size and clear', () => {
    test('should return 0 for empty store', () => {
      expect(store.size).toBe(0);
    });

    test('should return correct size', () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      store.setByKey('user-2', {
        id: 'user-2',
        name: 'Bob',
        status: 'active',
        amount: 200,
        createdAt: 2000,
      });

      expect(store.size).toBe(2);
    });

    test('should clear all rows', () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      store.setByKey('user-2', {
        id: 'user-2',
        name: 'Bob',
        status: 'active',
        amount: 200,
        createdAt: 2000,
      });

      store.clear();

      expect(store.size).toBe(0);
      expect(store.getByKey('user-1')).toBeUndefined();
    });

    test('should clear indexes on clear', () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      store.clear();

      const results = store.query({ status: 'active' });
      expect(results).toHaveLength(0);
    });
  });

  describe('persist', () => {
    test('should persist state to checkpoint file', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      await store.persist(100);

      expect(await env.fs.exists('/test-views/TestView.ckpt')).toBe(true);
    });

    test('should persist correct position', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      await store.persist(999);

      const position = await store.load();
      expect(position).toBe(999);
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

      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      await store.persist(1);

      expect(operations).toContainEqual('open:/test-views/TestView.ckpt.tmp');
      expect(operations).toContainEqual('rename:/test-views/TestView.ckpt.tmp:/test-views/TestView.ckpt');
    });

    test('should throw CheckpointWriteError on sync failure', async () => {
      env.fs.injectFault({ syncFails: true });

      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      await expect(store.persist(1)).rejects.toThrow(CheckpointWriteError);
    });

    test('should clean up temp file on failure', async () => {
      env.fs.injectFault({ syncFails: true });

      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      try {
        await store.persist(1);
      } catch {
        // Expected
      }

      expect(await env.fs.exists('/test-views/TestView.ckpt.tmp')).toBe(false);
    });

    test('should overwrite existing checkpoint', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      await store.persist(10);

      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice Updated',
        status: 'inactive',
        amount: 200,
        createdAt: 2000,
      });
      await store.persist(20);

      const newStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
      });
      const position = await newStore.load();

      expect(position).toBe(20);
      expect(newStore.getByKey('user-1')?.name).toBe('Alice Updated');
    });
  });

  describe('load', () => {
    test('should load persisted state', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      store.setByKey('user-2', {
        id: 'user-2',
        name: 'Bob',
        status: 'inactive',
        amount: 200,
        createdAt: 2000,
      });
      await store.persist(500);

      const newStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
        indexFields: ['status'],
      });
      await newStore.initialize();
      const position = await newStore.load();

      expect(position).toBe(500);
      expect(newStore.size).toBe(2);
      expect(newStore.getByKey('user-1')?.name).toBe('Alice');
      expect(newStore.getByKey('user-2')?.name).toBe('Bob');
    });

    test('should return null if no checkpoint exists', async () => {
      const result = await store.load();
      expect(result).toBeNull();
    });

    test('should rebuild indexes on load', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      store.setByKey('user-2', {
        id: 'user-2',
        name: 'Bob',
        status: 'active',
        amount: 200,
        createdAt: 2000,
      });
      await store.persist(1);

      const newStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
        indexFields: ['status'],
      });
      await newStore.load();

      const results = newStore.query({ status: 'active' });
      expect(results).toHaveLength(2);
    });

    test('should detect corrupted magic number', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      await store.persist(1);

      // Corrupt magic
      const content = await env.fs.readFile('/test-views/TestView.ckpt');
      const corrupted = new Uint8Array(content);
      corrupted[0] = 0x00;
      corrupted[1] = 0x00;
      env.fs.setFileContent('/test-views/TestView.ckpt', corrupted);

      await expect(store.load()).rejects.toThrow(CheckpointCorruptionError);
    });

    test('should detect corrupted checksum', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      await store.persist(1);

      // Corrupt state data (after 32-byte header)
      const content = await env.fs.readFile('/test-views/TestView.ckpt');
      const corrupted = new Uint8Array(content);
      const stateStart = 32;
      if (corrupted.length > stateStart + 5) {
        corrupted[stateStart + 2] = (corrupted[stateStart + 2] ?? 0) ^ 0xff;
        corrupted[stateStart + 3] = (corrupted[stateStart + 3] ?? 0) ^ 0xff;
      }
      env.fs.setFileContent('/test-views/TestView.ckpt', corrupted);

      await expect(store.load()).rejects.toThrow(CheckpointCorruptionError);
    });

    test('should detect truncated file', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      await store.persist(1);

      // Truncate file
      const content = await env.fs.readFile('/test-views/TestView.ckpt');
      env.fs.setFileContent('/test-views/TestView.ckpt', content.slice(0, 20));

      await expect(store.load()).rejects.toThrow(CheckpointCorruptionError);
    });

    test('should detect file too small for header', async () => {
      env.fs.setFileContent('/test-views/TestView.ckpt', new Uint8Array(10));

      await expect(store.load()).rejects.toThrow(CheckpointCorruptionError);
    });

    test('should detect unsupported version', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      await store.persist(1);

      // Change version to unsupported
      const content = await env.fs.readFile('/test-views/TestView.ckpt');
      const modified = new Uint8Array(content);
      const view = new DataView(modified.buffer);
      view.setUint32(4, 999, true); // Unsupported version
      env.fs.setFileContent('/test-views/TestView.ckpt', modified);

      await expect(store.load()).rejects.toThrow(CheckpointVersionError);
    });
  });

  describe('getMemoryUsage', () => {
    test('should return approximate memory usage', () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      const usage = store.getMemoryUsage();
      expect(usage).toBeGreaterThan(0);
    });

    test('should increase with more rows', () => {
      const usage1 = store.getMemoryUsage();

      for (let i = 0; i < 100; i++) {
        store.setByKey(`user-${i}`, {
          id: `user-${i}`,
          name: `User ${i}`,
          status: 'active',
          amount: i * 10,
          createdAt: i * 1000,
        });
      }

      const usage2 = store.getMemoryUsage();
      expect(usage2).toBeGreaterThan(usage1);
    });
  });

  describe('isMemoryThresholdExceeded', () => {
    test('should return false for small state', () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      expect(store.isMemoryThresholdExceeded()).toBe(false);
    });

    test('should return true when threshold exceeded', async () => {
      // Create store with low threshold
      const smallStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName: 'SmallThreshold',
        memoryThreshold: 100, // Very low threshold
      });

      // Add enough data to exceed threshold
      for (let i = 0; i < 100; i++) {
        smallStore.setByKey(`user-${i}`, {
          id: `user-${i}`,
          name: `User ${i} with a longer name to increase size`,
          status: 'active',
          amount: i * 10,
          createdAt: i * 1000,
        });
      }

      expect(smallStore.isMemoryThresholdExceeded()).toBe(true);
    });
  });

  describe('close', () => {
    test('should close without error', async () => {
      await expect(store.close()).resolves.toBeUndefined();
    });
  });

  describe('DST scenarios', () => {
    test('should maintain consistency after crash and recovery', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      store.setByKey('user-2', {
        id: 'user-2',
        name: 'Bob',
        status: 'inactive',
        amount: 200,
        createdAt: 2000,
      });
      await store.persist(100);

      // Simulate crash
      env.fs.crash();

      // Recover
      const recoveredStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
        indexFields: ['status'],
      });
      await recoveredStore.initialize();
      const position = await recoveredStore.load();

      expect(position).toBe(100);
      expect(recoveredStore.size).toBe(2);
      expect(recoveredStore.getByKey('user-1')?.name).toBe('Alice');
    });

    test('should handle crash during persist (temp file exists)', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      await store.persist(10);

      // Simulate crash during second persist by leaving temp file
      env.fs.setFileContent('/test-views/TestView.ckpt.tmp', new Uint8Array(20));

      env.fs.crash();

      // Recovery should use the valid checkpoint
      const recoveredStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
      });
      await recoveredStore.initialize();
      const position = await recoveredStore.load();

      expect(position).toBe(10);
      expect(recoveredStore.getByKey('user-1')?.name).toBe('Alice');
    });

    test('should handle write failures and allow retry', async () => {
      const faultScheduler = new FaultScheduler(env.random, env.fs);

      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      // First persist should succeed
      await store.persist(10);

      // Inject fault for second persist
      faultScheduler.injectFault('sync');

      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice Updated',
        status: 'active',
        amount: 200,
        createdAt: 2000,
      });

      await expect(store.persist(20)).rejects.toThrow();

      // Clear fault and retry
      faultScheduler.clearFaults();

      await store.persist(20);

      // Verify
      const newStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
      });
      const position = await newStore.load();

      expect(position).toBe(20);
      expect(newStore.getByKey('user-1')?.name).toBe('Alice Updated');
    });

    test('should handle random persist/load cycles with failures (fuzz)', async () => {
      const random = new SeededRandom(42);
      const faultScheduler = new FaultScheduler(random, env.fs);
      let expectedSize = 0;
      let expectedPosition = 0;

      for (let i = 0; i < 50; i++) {
        // Maybe inject fault
        if (random.bool(0.15)) {
          faultScheduler.injectFault('sync');
        }

        const key = `user-${random.int(0, 20)}`;
        const newRow: TestRow = {
          id: key,
          name: `User ${i}`,
          status: random.choice(['active', 'inactive', 'pending']),
          amount: random.int(0, 1000),
          createdAt: i * 1000,
        };

        const newPosition = i;

        try {
          store.setByKey(key, newRow);
          await store.persist(newPosition);
          expectedSize = store.size;
          expectedPosition = newPosition;
        } catch {
          // Expected on fault - restore state from last checkpoint
          const pos = await store.load();
          if (pos !== null) {
            expectedPosition = pos;
            expectedSize = store.size;
          }
        }

        faultScheduler.clearFaults();
      }

      // Verify final state matches expected
      const finalStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
      });
      const position = await finalStore.load();

      expect(position).toBe(expectedPosition);
      expect(finalStore.size).toBe(expectedSize);
    });
  });

  describe('edge cases', () => {
    test('should handle empty state persist/load', async () => {
      await store.persist(0);

      const newStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
      });
      const position = await newStore.load();

      expect(position).toBe(0);
      expect(newStore.size).toBe(0);
    });

    test('should handle position = 0', async () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      await store.persist(0);

      const newStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
      });
      const position = await newStore.load();

      expect(position).toBe(0);
    });

    test('should handle very large position', async () => {
      const largePosition = Number.MAX_SAFE_INTEGER;

      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      await store.persist(largePosition);

      const newStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
      });
      const position = await newStore.load();

      expect(position).toBe(largePosition);
    });

    test('should sanitize projection name for filesystem', async () => {
      const specialStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName: 'Test/Special:Chars*?',
      });
      await specialStore.initialize();

      specialStore.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      await specialStore.persist(1);

      expect(await env.fs.exists('/test-views/Test_Special_Chars__.ckpt')).toBe(true);
    });

    test('should handle unicode in row data', async () => {
      const unicodeRow: TestRow = {
        id: 'user-1',
        name: '你好世界 🎉',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      };

      store.setByKey('user-1', unicodeRow);
      await store.persist(1);

      const newStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
      });
      await newStore.load();

      expect(newStore.getByKey('user-1')?.name).toBe('你好世界 🎉');
    });

    test('should handle many rows', async () => {
      for (let i = 0; i < 1000; i++) {
        store.setByKey(`user-${i}`, {
          id: `user-${i}`,
          name: `User ${i}`,
          status: i % 3 === 0 ? 'active' : i % 3 === 1 ? 'inactive' : 'pending',
          amount: i * 10,
          createdAt: i * 1000,
        });
      }

      await store.persist(1000);

      const newStore = new DenormalizedViewStore<TestRow>({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
        projectionName,
        indexFields: ['status'],
      });
      await newStore.load();

      expect(newStore.size).toBe(1000);
      expect(newStore.getByKey('user-500')?.name).toBe('User 500');

      // Verify indexes work
      const activeRows = newStore.query({ status: 'active' });
      expect(activeRows.length).toBe(334); // ceil(1000/3)
    });

    test('should handle special characters in keys', () => {
      store.setByKey('user:with:colons', {
        id: 'user:with:colons',
        name: 'Special',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      store.setByKey('user/with/slashes', {
        id: 'user/with/slashes',
        name: 'Also Special',
        status: 'active',
        amount: 200,
        createdAt: 2000,
      });

      expect(store.getByKey('user:with:colons')?.name).toBe('Special');
      expect(store.getByKey('user/with/slashes')?.name).toBe('Also Special');
    });
  });

  describe('index updates on mutation', () => {
    test('should update index when row value changes', () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });

      // Should find by active
      let results = store.query({ status: 'active' });
      expect(results).toHaveLength(1);

      // Update status
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'inactive',
        amount: 100,
        createdAt: 1000,
      });

      // Should not find by active anymore
      results = store.query({ status: 'active' });
      expect(results).toHaveLength(0);

      // Should find by inactive
      results = store.query({ status: 'inactive' });
      expect(results).toHaveLength(1);
    });

    test('should handle index with multiple values', () => {
      store.setByKey('user-1', {
        id: 'user-1',
        name: 'Alice',
        status: 'active',
        amount: 100,
        createdAt: 1000,
      });
      store.setByKey('user-2', {
        id: 'user-2',
        name: 'Bob',
        status: 'active',
        amount: 200,
        createdAt: 2000,
      });
      store.setByKey('user-3', {
        id: 'user-3',
        name: 'Charlie',
        status: 'active',
        amount: 300,
        createdAt: 3000,
      });

      let results = store.query({ status: 'active' });
      expect(results).toHaveLength(3);

      // Change one to inactive
      store.setByKey('user-2', {
        id: 'user-2',
        name: 'Bob',
        status: 'inactive',
        amount: 200,
        createdAt: 2000,
      });

      results = store.query({ status: 'active' });
      expect(results).toHaveLength(2);

      results = store.query({ status: 'inactive' });
      expect(results).toHaveLength(1);
    });
  });
});
