import { describe, test, expect } from 'bun:test';
import { tmpdir } from 'node:os';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import { EventStore } from '../../../src/application/event-store';
import { BunFileSystem } from '../../../src/infrastructure/filesystem/bun-filesystem';
import { BunClock } from '../../../src/infrastructure/time/bun-clock';
import { MsgpackSerializer } from '../../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../../src/infrastructure/serialization/zstd-compressor';

async function checkFlockSupport(): Promise<boolean> {
  const fs = new BunFileSystem();
  const root = await mkdtemp(path.join(tmpdir(), 'spitedb-flock-'));
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

describe('Concurrent Appends (Real FS)', () => {
  flockTest('handles concurrent appends with flock enabled', async () => {
    const clock = new BunClock();
    const fs = new BunFileSystem();
    const store = new EventStore({
      fs,
      serializer: new MsgpackSerializer(),
      compressor: new ZstdCompressor(),
      clock,
      maxSegmentSize: 64 * 1024,
      autoFlushCount: 50,
      indexCacheSize: 4,
    });

    const tempRoot = await mkdtemp(path.join(tmpdir(), 'spitedb-concurrency-'));
    try {
      await store.open(path.join(tempRoot, 'events'));

      const workerCount = 6;
      const iterations = 120;
      const eventsPerAppend = 4;
      const payload = 'x'.repeat(512);

      const workers = Array.from({ length: workerCount }, (_, workerId) => {
        return (async () => {
          for (let i = 0; i < iterations; i += 1) {
            const streamId = `stream-${workerId}-${i % 10}`;
            const events = Array.from({ length: eventsPerAppend }, (_, index) => ({
              type: 'StressEvent',
              data: {
                payload,
                workerId,
                iteration: i,
                index,
              },
            }));
            await store.append(streamId, events);
          }
        })();
      });

      await Promise.all(workers);
      await store.flush();

      const expectedEvents =(workerCount * iterations * eventsPerAppend);
      expect(store.getGlobalPosition()).toBe(expectedEvents);
    } finally {
      await store.close();
      await rm(tempRoot, { recursive: true, force: true });
    }
  }, 20000);

  flockTest('handles concurrent rotations under append pressure', async () => {
    const clock = new BunClock();
    const fs = new BunFileSystem();
    const store = new EventStore({
      fs,
      serializer: new MsgpackSerializer(),
      compressor: new ZstdCompressor(),
      clock,
      maxSegmentSize: 16 * 1024,
      autoFlushCount: 10,
      indexCacheSize: 4,
    });

    const tempRoot = await mkdtemp(path.join(tmpdir(), 'spitedb-concurrency-'));

    try {
      await store.open(path.join(tempRoot, 'events'));

      const workerCount = 8;
      const iterations = 80;
      const eventsPerAppend = 3;
      const payload = 'x'.repeat(2048);

      const workers = Array.from({ length: workerCount }, (_, workerId) => {
        return (async () => {
          for (let i = 0; i < iterations; i += 1) {
            const streamId = `stream-${workerId}-${i % 10}`;
            const events = Array.from({ length: eventsPerAppend }, (_, index) => ({
              type: 'StressEvent',
              data: {
                payload,
                workerId,
                iteration: i,
                index,
              },
            }));
            await store.append(streamId, events);
            if (i % 4 === 0) {
              await store.flush();
            }
          }
        })();
      });

      await Promise.all(workers);
      await store.flush();

      const expectedEvents =(workerCount * iterations * eventsPerAppend);
      expect(store.getGlobalPosition()).toBe(expectedEvents);
    } finally {
      await store.close();
      await rm(tempRoot, { recursive: true, force: true });
    }
  }, 20000);
});
