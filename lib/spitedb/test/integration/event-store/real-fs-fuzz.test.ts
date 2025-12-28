import { describe, test, expect } from 'bun:test';
import { tmpdir } from 'node:os';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import { EventStore } from '../../../src/application/event-store';
import { BunFileSystem } from '../../../src/infrastructure/filesystem/bun-filesystem';
import { BunClock } from '../../../src/infrastructure/time/bun-clock';
import { MsgpackSerializer } from '../../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../../src/infrastructure/serialization/zstd-compressor';
import { SeededRandom, getSeedFromEnv } from '../../setup/seeded-random';

const DEFAULT_ITERATIONS = 10;

function getIterations(): number {
  const override = process.env['REAL_FS_FUZZ_ITERATIONS'];
  if (override) {
    return parseInt(override, 10);
  }
  return DEFAULT_ITERATIONS;
}

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

describe('Real FS Fuzz', () => {
  const iterations = getIterations();

  for (let i = 0; i < iterations; i++) {
    flockTest(`iteration ${i}`, async () => {
      const baseSeed = getSeedFromEnv();
      const iterationSeed = baseSeed + i + 10000;
      const random = new SeededRandom(iterationSeed);

      const clock = new BunClock();
      const fs = new BunFileSystem();
      const store = new EventStore({
        fs,
        serializer: new MsgpackSerializer(),
        compressor: new ZstdCompressor(),
        clock,
        maxSegmentSize: 64 * 1024,
        autoFlushCount: 20,
        indexCacheSize: 4,
      });

      const tempRoot = await mkdtemp(path.join(tmpdir(), 'spitedb-fuzz-'));
      try {
        await store.open(path.join(tempRoot, 'events'));

        const numOperations = random.int(80, 160);
        const streams = Array.from({ length: 6 }, (_, idx) => `stream-${idx}`);
        const payload = 'x'.repeat(2048);

        for (let op = 0; op < numOperations; op++) {
          const roll = random.int(0, 99);

          if (roll < 60) {
            const streamId = random.choice(streams);
            const eventCount = random.int(1, 4);
            const events = Array.from({ length: eventCount }, (_, index) => ({
              type: 'RealFsEvent',
              data: {
                payload,
                streamId,
                op,
                index,
              },
            }));
            await store.append(streamId, events);
          } else if (roll < 75) {
            await store.flush();
          } else if (roll < 90) {
            const streamId = random.choice(streams);
            await store.readStream(streamId);
          } else {
            await store.readGlobal(0);
          }
        }

        await store.flush();

        const allEvents = await store.readGlobal(0);
        if (allEvents.length > 0) {
          const last = allEvents[allEvents.length - 1]!;
          expect(store.getGlobalPosition()).toBe(last.globalPosition + 1);
        }
      } catch (error) {
        throw new Error(
          `Real FS fuzz failed (seed=${iterationSeed}): ${error instanceof Error ? error.message : String(error)}`
        );
      } finally {
        await store.close();
        await rm(tempRoot, { recursive: true, force: true });
      }
    });
  }
});
