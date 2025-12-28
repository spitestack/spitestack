/**
 * Real crash tests using child processes.
 *
 * These tests spawn actual child processes, write events, and kill them
 * with SIGKILL to verify data durability and crash recovery.
 *
 * Test scenarios:
 * 1. Kill during append (before flush) - unflushed data should be lost
 * 2. Kill after flush - all flushed data should survive
 * 3. Kill during segment rotation - should recover cleanly
 * 4. Multiple crashes and recoveries
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { spawn, type Subprocess } from 'bun';
import { tmpdir } from 'node:os';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import { EventStore } from '../../../src/application/event-store';
import { BunFileSystem } from '../../../src/infrastructure/filesystem/bun-filesystem';
import { BunClock } from '../../../src/infrastructure/time/bun-clock';
import { MsgpackSerializer } from '../../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../../src/infrastructure/serialization/zstd-compressor';

interface WorkerResponse {
  type: 'ready' | 'flushed' | 'written' | 'error';
  position?: number;
  error?: string;
}

interface WorkerMessage {
  type: 'write' | 'flush' | 'exit';
  count?: number;
  streamId?: string;
}

/**
 * Helper class to manage worker processes.
 */
class WorkerProcess {
  private proc: Subprocess<'pipe', 'pipe', 'inherit'>;
  private buffer = '';
  private responses: WorkerResponse[] = [];
  private waiters: Array<(response: WorkerResponse) => void> = [];

  constructor(dataDir: string) {
    const workerPath = path.join(__dirname, 'crash-test-worker.ts');
    this.proc = spawn(['bun', 'run', workerPath, dataDir], {
      stdin: 'pipe',
      stdout: 'pipe',
      stderr: 'inherit',
    });

    this.startReading();
  }

  private async startReading(): Promise<void> {
    const reader = this.proc.stdout.getReader();
    const decoder = new TextDecoder();

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        this.buffer += decoder.decode(value, { stream: true });
        const lines = this.buffer.split('\n');
        this.buffer = lines.pop() || '';

        for (const line of lines) {
          if (!line.trim()) continue;
          const response: WorkerResponse = JSON.parse(line);

          const waiter = this.waiters.shift();
          if (waiter) {
            waiter(response);
          } else {
            this.responses.push(response);
          }
        }
      }
    } catch {
      // Process was killed
    }
  }

  async waitForResponse(): Promise<WorkerResponse> {
    const existing = this.responses.shift();
    if (existing) return existing;

    return new Promise((resolve) => {
      this.waiters.push(resolve);
    });
  }

  async send(msg: WorkerMessage): Promise<void> {
    const data = JSON.stringify(msg) + '\n';
    this.proc.stdin.write(data);
    this.proc.stdin.flush();
  }

  kill(): void {
    this.proc.kill('SIGKILL');
  }

  async waitForExit(): Promise<number | null> {
    const exitCode = await this.proc.exited;
    // Small delay to ensure OS releases all resources (file locks, handles)
    await Bun.sleep(50);
    return exitCode;
  }

  get pid(): number {
    return this.proc.pid;
  }
}

/**
 * Check if flock is supported on this system.
 */
async function checkFlockSupport(): Promise<boolean> {
  const fs = new BunFileSystem();
  const root = await mkdtemp(path.join(tmpdir(), 'spitedb-flock-check-'));
  const lockPath = path.join(root, '.lock');
  try {
    const handle = await fs.open(lockPath, 'write');
    await fs.flock(handle, 'exclusive');
    await fs.close(handle);
    return true;
  } catch {
    return false;
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

const canUseFlock = await checkFlockSupport();
const flockTest = canUseFlock ? test : test.skip;

describe('Real Crash Tests', () => {
  let tempRoot: string;
  let dataDir: string;

  beforeEach(async () => {
    tempRoot = await mkdtemp(path.join(tmpdir(), 'spitedb-crash-'));
    dataDir = path.join(tempRoot, 'events');
  });

  afterEach(async () => {
    await rm(tempRoot, { recursive: true, force: true });
  });

  /**
   * Helper to open a store for verification.
   */
  function createVerificationStore(): EventStore {
    return new EventStore({
      fs: new BunFileSystem(),
      serializer: new MsgpackSerializer(),
      compressor: new ZstdCompressor(),
      clock: new BunClock(),
      autoFlushCount: 0,
    });
  }

  flockTest('should lose unflushed events after SIGKILL', async () => {
    // Start worker
    const worker = new WorkerProcess(dataDir);
    const ready = await worker.waitForResponse();
    expect(ready.type).toBe('ready');

    // Write and flush some events
    await worker.send({ type: 'write', count: 5 });
    await worker.waitForResponse();
    await worker.send({ type: 'flush' });
    const flushed = await worker.waitForResponse();
    expect(flushed.type).toBe('flushed');
    const flushedPosition = flushed.position!;

    // Write more events WITHOUT flushing
    await worker.send({ type: 'write', count: 5 });
    await worker.waitForResponse();

    // Kill the process abruptly
    worker.kill();
    await worker.waitForExit();

    // Verify: only flushed events should survive
    const store = createVerificationStore();
    await store.open(dataDir);

    const events = await store.readGlobal(0);
    expect(events.length).toBe(5); // Only the flushed events
    expect(store.getGlobalPosition()).toBe(flushedPosition);

    await store.close();
  });

  flockTest('should preserve all flushed events after SIGKILL', async () => {
    // Start worker
    const worker = new WorkerProcess(dataDir);
    const ready = await worker.waitForResponse();
    expect(ready.type).toBe('ready');

    // Write and flush multiple batches
    for (let batch = 0; batch < 3; batch++) {
      await worker.send({ type: 'write', count: 10 });
      await worker.waitForResponse();
      await worker.send({ type: 'flush' });
      await worker.waitForResponse();
    }

    // Kill the process
    worker.kill();
    await worker.waitForExit();

    // Verify: all 30 events should survive
    const store = createVerificationStore();
    await store.open(dataDir);

    const events = await store.readGlobal(0);
    expect(events.length).toBe(30);

    await store.close();
  });

  flockTest('should recover multiple streams after crash', async () => {
    // Start worker
    const worker = new WorkerProcess(dataDir);
    const ready = await worker.waitForResponse();
    expect(ready.type).toBe('ready');

    // Write to multiple streams
    const streams = ['user-1', 'user-2', 'order-1'];
    for (const streamId of streams) {
      await worker.send({ type: 'write', count: 5, streamId });
      await worker.waitForResponse();
    }
    await worker.send({ type: 'flush' });
    await worker.waitForResponse();

    // Kill the process
    worker.kill();
    await worker.waitForExit();

    // Verify: all streams should be recoverable
    const store = createVerificationStore();
    await store.open(dataDir);

    for (const streamId of streams) {
      const events = await store.readStream(streamId);
      expect(events.length).toBe(5);
      expect(events.every((e) => e.streamId === streamId)).toBe(true);
    }

    await store.close();
  });

  flockTest('should handle multiple crash/recover cycles', async () => {
    let totalExpectedEvents = 0;

    // Cycle 1: write and flush
    {
      const worker = new WorkerProcess(dataDir);
      await worker.waitForResponse();

      await worker.send({ type: 'write', count: 10 });
      await worker.waitForResponse();
      await worker.send({ type: 'flush' });
      await worker.waitForResponse();
      totalExpectedEvents += 10;

      worker.kill();
      await worker.waitForExit();
    }

    // Cycle 2: recover, write more, flush, crash
    {
      const worker = new WorkerProcess(dataDir);
      const ready = await worker.waitForResponse();
      expect(ready.type).toBe('ready');
      expect(ready.position).toBe(totalExpectedEvents);

      await worker.send({ type: 'write', count: 5 });
      await worker.waitForResponse();
      await worker.send({ type: 'flush' });
      await worker.waitForResponse();
      totalExpectedEvents += 5;

      worker.kill();
      await worker.waitForExit();
    }

    // Cycle 3: recover, write without flush, crash
    {
      const worker = new WorkerProcess(dataDir);
      const ready = await worker.waitForResponse();
      expect(ready.type).toBe('ready');
      expect(ready.position).toBe(totalExpectedEvents);

      await worker.send({ type: 'write', count: 5 }); // NOT flushed
      await worker.waitForResponse();

      worker.kill();
      await worker.waitForExit();
    }

    // Final verification
    const store = createVerificationStore();
    await store.open(dataDir);

    const events = await store.readGlobal(0);
    expect(events.length).toBe(totalExpectedEvents); // 15, not 20

    await store.close();
  });

  flockTest('should handle segment rotation during crash cycle', async () => {
    // Start worker with small segment size
    const worker = new WorkerProcess(dataDir);
    await worker.waitForResponse();

    // Write enough events to trigger segment rotation (64KB segments)
    // Each event is ~300 bytes, so ~200 events per segment
    for (let i = 0; i < 5; i++) {
      await worker.send({ type: 'write', count: 50 });
      await worker.waitForResponse();
      await worker.send({ type: 'flush' });
      await worker.waitForResponse();
    }

    worker.kill();
    await worker.waitForExit();

    // Verify recovery across multiple segments
    const store = createVerificationStore();
    await store.open(dataDir);

    const events = await store.readGlobal(0);
    expect(events.length).toBe(250);

    // Verify global ordering
    for (let i = 1; i < events.length; i++) {
      expect(events[i]!.globalPosition).toBe(events[i - 1]!.globalPosition + 1);
    }

    await store.close();
  });

  flockTest('should not corrupt data when killed mid-write', async () => {
    // This test writes continuously without waiting for responses,
    // then kills mid-operation

    const worker = new WorkerProcess(dataDir);
    await worker.waitForResponse(); // Wait for ready

    // Start writing without waiting
    for (let i = 0; i < 100; i++) {
      await worker.send({ type: 'write', count: 1 });
    }
    await worker.send({ type: 'flush' });

    // Small delay to let some writes happen
    await Bun.sleep(10);

    // Kill mid-operation
    worker.kill();
    await worker.waitForExit();

    // The store should recover without corruption
    const store = createVerificationStore();
    await store.open(dataDir);

    // Should have some events (exact count depends on timing)
    const events = await store.readGlobal(0);

    // Verify no corruption - all events should be readable and ordered
    for (let i = 1; i < events.length; i++) {
      expect(events[i]!.globalPosition).toBe(events[i - 1]!.globalPosition + 1);
    }

    // Verify we can still write after recovery
    await store.append('test-stream', [{ type: 'AfterRecovery', data: {} }]);
    await store.flush();

    const afterRecovery = await store.readGlobal(0);
    expect(afterRecovery.length).toBeGreaterThan(events.length);

    await store.close();
  });
});
