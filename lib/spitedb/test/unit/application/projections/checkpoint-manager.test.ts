/**
 * Unit tests for CheckpointManager.
 *
 * Tests checkpoint persistence with:
 * - Atomic writes (temp file → fsync → rename)
 * - CRC32 checksum validation
 * - Corruption detection and recovery
 * - Jitter scheduling
 * - DST scenarios (crashes, partial writes)
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import {
  CheckpointManager,
  CHECKPOINT_MAGIC,
  CHECKPOINT_VERSION,
  CHECKPOINT_HEADER_SIZE,
  type Checkpoint,
} from '../../../../src/application/projections/checkpoint-manager';
import { SimulatedFileSystem } from '../../../../src/testing/simulated-filesystem';
import { SimulatedClock } from '../../../../src/testing/simulated-clock';
import { MsgpackSerializer } from '../../../../src/infrastructure/serialization/msgpack-serializer';
import {
  CheckpointWriteError,
  CheckpointCorruptionError,
  CheckpointVersionError,
} from '../../../../src/errors';
import { createTestEnvironment, type TestEnvironment } from '../../../setup/test-helpers';
import { FaultScheduler } from '../../../setup/fault-scheduler';
import { SeededRandom } from '../../../setup/seeded-random';

describe('CheckpointManager', () => {
  let env: TestEnvironment;
  let manager: CheckpointManager;
  const dataDir = '/test-checkpoints';

  beforeEach(async () => {
    env = createTestEnvironment(12345); // Fixed seed for reproducibility
    manager = new CheckpointManager({
      fs: env.fs,
      serializer: env.serializer,
      clock: env.clock,
      dataDir,
      jitterMs: 100,
    });
    await manager.initialize();
  });

  describe('initialize', () => {
    test('should create checkpoint directory if it does not exist', async () => {
      const newManager = new CheckpointManager({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir: '/new-checkpoints',
        jitterMs: 100,
      });

      await newManager.initialize();

      expect(await env.fs.exists('/new-checkpoints')).toBe(true);
    });

    test('should not fail if directory already exists', async () => {
      // Initialize already called in beforeEach
      await expect(manager.initialize()).resolves.toBeUndefined();
    });
  });

  describe('writeCheckpoint', () => {
    test('should write checkpoint to file', async () => {
      const checkpoint: Checkpoint = {
        projectionName: 'TestProjection',
        position: 100,
        state: { count: 42 },
        timestamp: 1000000,
      };

      await manager.writeCheckpoint(checkpoint);

      expect(await env.fs.exists('/test-checkpoints/TestProjection.ckpt')).toBe(true);
    });

    test('should write checkpoint with correct header format', async () => {
      const checkpoint: Checkpoint = {
        projectionName: 'TestProjection',
        position: 12345,
        state: { value: 'test' },
        timestamp: 9999999,
      };

      await manager.writeCheckpoint(checkpoint);

      const content = await env.fs.readFile('/test-checkpoints/TestProjection.ckpt');
      const view = new DataView(content.buffer, content.byteOffset, content.byteLength);

      expect(view.getUint32(0, true)).toBe(CHECKPOINT_MAGIC);
      expect(view.getUint32(4, true)).toBe(CHECKPOINT_VERSION);
      expect(view.getBigUint64(8, true)).toBe(12345n);
      expect(view.getBigUint64(16, true)).toBe(BigInt(9999999));
    });

    test('should use atomic write pattern (temp file → rename)', async () => {
      // Track filesystem operations
      const operations: string[] = [];
      const originalOpen = env.fs.open.bind(env.fs);
      const originalRename = env.fs.rename.bind(env.fs);

      env.fs.open = async (path: string, mode: 'read' | 'write') => {
        operations.push(`open:${path}:${mode}`);
        return originalOpen(path, mode);
      };

      env.fs.rename = async (from: string, to: string) => {
        operations.push(`rename:${from}:${to}`);
        return originalRename(from, to);
      };

      await manager.writeCheckpoint({
        projectionName: 'AtomicTest',
        position: 1,
        state: {},
        timestamp: 0,
      });

      // Should write to temp file then rename
      expect(operations).toContainEqual('open:/test-checkpoints/AtomicTest.ckpt.tmp:write');
      expect(operations).toContainEqual(
        'rename:/test-checkpoints/AtomicTest.ckpt.tmp:/test-checkpoints/AtomicTest.ckpt'
      );
    });

    test('should fsync before rename', async () => {
      const operations: string[] = [];
      const originalSync = env.fs.sync.bind(env.fs);
      const originalRename = env.fs.rename.bind(env.fs);

      let syncTime = 0;
      let renameTime = 0;

      env.fs.sync = async (handle) => {
        syncTime = performance.now();
        operations.push('sync');
        return originalSync(handle);
      };

      env.fs.rename = async (from, to) => {
        renameTime = performance.now();
        operations.push('rename');
        return originalRename(from, to);
      };

      await manager.writeCheckpoint({
        projectionName: 'SyncTest',
        position: 1,
        state: {},
        timestamp: 0,
      });

      expect(operations.indexOf('sync')).toBeLessThan(operations.indexOf('rename'));
      expect(syncTime).toBeLessThan(renameTime);
    });

    test('should sanitize projection names for filesystem', async () => {
      await manager.writeCheckpoint({
        projectionName: 'Test/Projection:Special*Chars?',
        position: 1,
        state: {},
        timestamp: 0,
      });

      // Special characters should be replaced with underscores
      expect(await env.fs.exists('/test-checkpoints/Test_Projection_Special_Chars_.ckpt')).toBe(true);
    });

    test('should handle complex state objects', async () => {
      const complexState = {
        users: new Map([['user1', { name: 'Alice', orders: [1, 2, 3] }]]),
        totals: { revenue: 50000, count: 100 },
        nested: { deep: { value: 42 } },
        array: [1, 2, 3, 4, 5],
      };

      // Note: Map is converted to object by msgpack
      await manager.writeCheckpoint({
        projectionName: 'ComplexState',
        position: 1000,
        state: complexState,
        timestamp: 0,
      });

      const loaded = await manager.loadCheckpoint('ComplexState');
      expect(loaded).not.toBeNull();
      expect(loaded!.state).toEqual({
        users: { user1: { name: 'Alice', orders: [1, 2, 3] } },
        totals: { revenue: 50000, count: 100 },
        nested: { deep: { value: 42 } },
        array: [1, 2, 3, 4, 5],
      });
    });

    test('should throw CheckpointWriteError on sync failure', async () => {
      env.fs.injectFault({ syncFails: true });

      await expect(
        manager.writeCheckpoint({
          projectionName: 'FailTest',
          position: 1,
          state: {},
          timestamp: 0,
        })
      ).rejects.toThrow(CheckpointWriteError);
    });

    test('should clean up temp file on failure', async () => {
      env.fs.injectFault({ syncFails: true });

      try {
        await manager.writeCheckpoint({
          projectionName: 'CleanupTest',
          position: 1,
          state: {},
          timestamp: 0,
        });
      } catch {
        // Expected
      }

      // Temp file should be cleaned up
      expect(await env.fs.exists('/test-checkpoints/CleanupTest.ckpt.tmp')).toBe(false);
    });
  });

  describe('loadCheckpoint', () => {
    test('should load checkpoint correctly', async () => {
      const original: Checkpoint = {
        projectionName: 'LoadTest',
        position: 500,
        state: { total: 12345 },
        timestamp: 1000000,
      };

      await manager.writeCheckpoint(original);
      const loaded = await manager.loadCheckpoint('LoadTest');

      expect(loaded).not.toBeNull();
      expect(loaded!.projectionName).toBe('LoadTest');
      expect(loaded!.position).toBe(500);
      expect(loaded!.state).toEqual({ total: 12345 });
      expect(loaded!.timestamp).toBe(1000000);
    });

    test('should return null for non-existent checkpoint', async () => {
      const result = await manager.loadCheckpoint('NonExistent');
      expect(result).toBeNull();
    });

    test('should detect corrupted magic number', async () => {
      await manager.writeCheckpoint({
        projectionName: 'CorruptMagic',
        position: 1,
        state: {},
        timestamp: 0,
      });

      // Corrupt the magic number
      const content = await env.fs.readFile('/test-checkpoints/CorruptMagic.ckpt');
      const corrupted = new Uint8Array(content);
      corrupted[0] = 0x00; // Corrupt first byte
      corrupted[1] = 0x00;
      env.fs.setFileContent('/test-checkpoints/CorruptMagic.ckpt', corrupted);

      await expect(manager.loadCheckpoint('CorruptMagic')).rejects.toThrow(
        CheckpointCorruptionError
      );
    });

    test('should detect corrupted checksum', async () => {
      await manager.writeCheckpoint({
        projectionName: 'CorruptChecksum',
        position: 1,
        state: { data: 'test' },
        timestamp: 0,
      });

      // Corrupt state data (after header)
      const content = await env.fs.readFile('/test-checkpoints/CorruptChecksum.ckpt');
      const corrupted = new Uint8Array(content);
      if (corrupted.length > CHECKPOINT_HEADER_SIZE + 5) {
        const idx1 = CHECKPOINT_HEADER_SIZE + 3;
        const idx2 = CHECKPOINT_HEADER_SIZE + 4;
        corrupted[idx1] = (corrupted[idx1] ?? 0) ^ 0xff;
        corrupted[idx2] = (corrupted[idx2] ?? 0) ^ 0xff;
      }
      env.fs.setFileContent('/test-checkpoints/CorruptChecksum.ckpt', corrupted);

      await expect(manager.loadCheckpoint('CorruptChecksum')).rejects.toThrow(
        CheckpointCorruptionError
      );
    });

    test('should detect truncated file', async () => {
      await manager.writeCheckpoint({
        projectionName: 'Truncated',
        position: 1,
        state: { large: 'data'.repeat(100) },
        timestamp: 0,
      });

      // Truncate the file
      const content = await env.fs.readFile('/test-checkpoints/Truncated.ckpt');
      const truncated = content.slice(0, CHECKPOINT_HEADER_SIZE + 5);
      env.fs.setFileContent('/test-checkpoints/Truncated.ckpt', truncated);

      await expect(manager.loadCheckpoint('Truncated')).rejects.toThrow(CheckpointCorruptionError);
    });

    test('should detect unsupported version', async () => {
      await manager.writeCheckpoint({
        projectionName: 'BadVersion',
        position: 1,
        state: {},
        timestamp: 0,
      });

      // Change version to unsupported
      const content = await env.fs.readFile('/test-checkpoints/BadVersion.ckpt');
      const modified = new Uint8Array(content);
      const view = new DataView(modified.buffer);
      view.setUint32(4, 999, true); // Unsupported version

      // Need to recalculate checksum
      env.fs.setFileContent('/test-checkpoints/BadVersion.ckpt', modified);

      await expect(manager.loadCheckpoint('BadVersion')).rejects.toThrow(CheckpointVersionError);
    });

    test('should detect file too small for header', async () => {
      // Write a file that's too small
      env.fs.setFileContent('/test-checkpoints/TooSmall.ckpt', new Uint8Array(10));

      await expect(manager.loadCheckpoint('TooSmall')).rejects.toThrow(CheckpointCorruptionError);
    });
  });

  describe('deleteCheckpoint', () => {
    test('should delete existing checkpoint', async () => {
      await manager.writeCheckpoint({
        projectionName: 'ToDelete',
        position: 1,
        state: {},
        timestamp: 0,
      });

      const result = await manager.deleteCheckpoint('ToDelete');

      expect(result).toBe(true);
      expect(await env.fs.exists('/test-checkpoints/ToDelete.ckpt')).toBe(false);
    });

    test('should return false for non-existent checkpoint', async () => {
      const result = await manager.deleteCheckpoint('NonExistent');
      expect(result).toBe(false);
    });
  });

  describe('listCheckpoints', () => {
    test('should list all checkpoints', async () => {
      await manager.writeCheckpoint({
        projectionName: 'Projection1',
        position: 1,
        state: {},
        timestamp: 0,
      });
      await manager.writeCheckpoint({
        projectionName: 'Projection2',
        position: 2,
        state: {},
        timestamp: 0,
      });
      await manager.writeCheckpoint({
        projectionName: 'Projection3',
        position: 3,
        state: {},
        timestamp: 0,
      });

      const list = await manager.listCheckpoints();

      expect(list).toHaveLength(3);
      expect(list).toContain('Projection1');
      expect(list).toContain('Projection2');
      expect(list).toContain('Projection3');
    });

    test('should return empty array when no checkpoints', async () => {
      const list = await manager.listCheckpoints();
      expect(list).toHaveLength(0);
    });

    test('should return empty array when directory does not exist', async () => {
      const newManager = new CheckpointManager({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir: '/nonexistent',
      });

      const list = await newManager.listCheckpoints();
      expect(list).toHaveLength(0);
    });
  });

  describe('scheduling', () => {
    test('should schedule checkpoint with base interval', () => {
      const baseInterval = 5000;
      const scheduled = manager.scheduleCheckpoint('TestProjection', baseInterval);

      // Should be roughly now + baseInterval (with some jitter)
      const now = env.clock.now();
      expect(scheduled).toBeGreaterThanOrEqual(now + baseInterval - 100);
      expect(scheduled).toBeLessThanOrEqual(now + baseInterval + 100);
    });

    test('should not trigger checkpoint before scheduled time', () => {
      manager.scheduleCheckpoint('TestProjection', 5000);

      // Should not checkpoint immediately
      expect(manager.shouldCheckpoint('TestProjection')).toBe(false);
    });

    test('should trigger checkpoint after scheduled time', () => {
      manager.scheduleCheckpoint('TestProjection', 1000);

      // Advance clock past scheduled time
      env.clock.tick(1200);

      expect(manager.shouldCheckpoint('TestProjection')).toBe(true);
    });

    test('should return false for unscheduled projection', () => {
      expect(manager.shouldCheckpoint('UnscheduledProjection')).toBe(false);
    });

    test('should apply jitter to prevent write storms', () => {
      const scheduledTimes: number[] = [];

      // Schedule many projections
      for (let i = 0; i < 100; i++) {
        const scheduled = manager.scheduleCheckpoint(`Projection${i}`, 5000);
        scheduledTimes.push(scheduled);
      }

      // Calculate variance - should not all be identical
      const uniqueTimes = new Set(scheduledTimes);
      expect(uniqueTimes.size).toBeGreaterThan(1);
    });
  });

  describe('DST scenarios', () => {
    test('should recover from crash during checkpoint write (temp file exists)', async () => {
      await manager.writeCheckpoint({
        projectionName: 'CrashRecovery',
        position: 100,
        state: { original: true },
        timestamp: 0,
      });

      // Simulate crash during write by leaving temp file
      const originalContent = await env.fs.readFile('/test-checkpoints/CrashRecovery.ckpt');
      const partialContent = new Uint8Array(CHECKPOINT_HEADER_SIZE + 10);
      env.fs.setFileContent('/test-checkpoints/CrashRecovery.ckpt.tmp', partialContent);

      // Simulate crash
      env.fs.crash();

      // Create new manager and load - should get the valid checkpoint
      const newManager = new CheckpointManager({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
      });
      await newManager.initialize();

      const loaded = await newManager.loadCheckpoint('CrashRecovery');
      expect(loaded).not.toBeNull();
      expect(loaded!.state).toEqual({ original: true });
    });

    test('should handle write failure and allow retry', async () => {
      const faultScheduler = new FaultScheduler(env.random, env.fs);

      // First write should succeed
      await manager.writeCheckpoint({
        projectionName: 'RetryTest',
        position: 1,
        state: { attempt: 1 },
        timestamp: 0,
      });

      // Inject fault for second write
      faultScheduler.injectFault('sync');

      await expect(
        manager.writeCheckpoint({
          projectionName: 'RetryTest',
          position: 2,
          state: { attempt: 2 },
          timestamp: 0,
        })
      ).rejects.toThrow();

      // Clear fault and retry
      faultScheduler.clearFaults();

      await manager.writeCheckpoint({
        projectionName: 'RetryTest',
        position: 2,
        state: { attempt: 2 },
        timestamp: 0,
      });

      const loaded = await manager.loadCheckpoint('RetryTest');
      expect(loaded!.position).toBe(2);
      expect(loaded!.state).toEqual({ attempt: 2 });
    });

    test('should handle multiple checkpoints with random failures (fuzz)', async () => {
      const random = new SeededRandom(42);
      const faultScheduler = new FaultScheduler(random, env.fs);
      const projections = ['A', 'B', 'C', 'D', 'E'];
      const successfulWrites = new Map<string, number>();

      // Write many checkpoints with random failures
      for (let i = 0; i < 50; i++) {
        const projection = random.choice(projections);
        const position = i;

        // 20% chance of fault
        if (random.bool(0.2)) {
          faultScheduler.injectFault('sync');
        }

        try {
          await manager.writeCheckpoint({
            projectionName: projection,
            position,
            state: { iteration: i },
            timestamp: i * 1000,
          });
          successfulWrites.set(projection, position);
        } catch {
          // Expected on fault
        }

        faultScheduler.clearFaults();
      }

      // Verify all successful writes are loadable
      for (const [projection, position] of successfulWrites) {
        const loaded = await manager.loadCheckpoint(projection);
        expect(loaded).not.toBeNull();
        expect(loaded!.position).toBe(position);
      }
    });

    test('should maintain consistency after simulated crash and recovery', async () => {
      // Write initial checkpoints
      for (let i = 0; i < 5; i++) {
        await manager.writeCheckpoint({
          projectionName: `Projection${i}`,
          position: i * 100,
          state: { id: i },
          timestamp: i * 1000,
        });
      }

      // Simulate crash
      env.fs.crash();

      // Create new manager
      const recoveredManager = new CheckpointManager({
        fs: env.fs,
        serializer: env.serializer,
        clock: env.clock,
        dataDir,
      });
      await recoveredManager.initialize();

      // Verify all checkpoints are recoverable
      const list = await recoveredManager.listCheckpoints();
      expect(list).toHaveLength(5);

      for (let i = 0; i < 5; i++) {
        const loaded = await recoveredManager.loadCheckpoint(`Projection${i}`);
        expect(loaded).not.toBeNull();
        expect(loaded!.position).toBe(i * 100);
        expect(loaded!.state).toEqual({ id: i });
      }
    });
  });

  describe('edge cases', () => {
    test('should handle empty state', async () => {
      await manager.writeCheckpoint({
        projectionName: 'EmptyState',
        position: 0,
        state: null,
        timestamp: 0,
      });

      const loaded = await manager.loadCheckpoint('EmptyState');
      expect(loaded!.state).toBeNull();
    });

    test('should handle position = 0', async () => {
      await manager.writeCheckpoint({
        projectionName: 'ZeroPosition',
        position: 0,
        state: { start: true },
        timestamp: 0,
      });

      const loaded = await manager.loadCheckpoint('ZeroPosition');
      expect(loaded!.position).toBe(0);
    });

    test('should handle very large position', async () => {
      const largePosition = Number.MAX_SAFE_INTEGER;

      await manager.writeCheckpoint({
        projectionName: 'LargePosition',
        position: largePosition,
        state: {},
        timestamp: 0,
      });

      const loaded = await manager.loadCheckpoint('LargePosition');
      expect(loaded!.position).toBe(largePosition);
    });

    test('should handle unicode in projection name', async () => {
      await manager.writeCheckpoint({
        projectionName: '总收入',
        position: 1,
        state: { revenue: 10000 },
        timestamp: 0,
      });

      // Sanitized name
      const loaded = await manager.loadCheckpoint('总收入');
      expect(loaded).not.toBeNull();
      expect(loaded!.state).toEqual({ revenue: 10000 });
    });

    test('should handle unicode in state', async () => {
      await manager.writeCheckpoint({
        projectionName: 'UnicodeState',
        position: 1,
        state: { greeting: 'こんにちは', emoji: '🎉' },
        timestamp: 0,
      });

      const loaded = await manager.loadCheckpoint('UnicodeState');
      expect(loaded!.state).toEqual({ greeting: 'こんにちは', emoji: '🎉' });
    });

    test('should overwrite existing checkpoint', async () => {
      await manager.writeCheckpoint({
        projectionName: 'Overwrite',
        position: 1,
        state: { version: 1 },
        timestamp: 1000,
      });

      await manager.writeCheckpoint({
        projectionName: 'Overwrite',
        position: 2,
        state: { version: 2 },
        timestamp: 2000,
      });

      const loaded = await manager.loadCheckpoint('Overwrite');
      expect(loaded!.position).toBe(2);
      expect(loaded!.state).toEqual({ version: 2 });
      expect(loaded!.timestamp).toBe(2000);
    });
  });
});
