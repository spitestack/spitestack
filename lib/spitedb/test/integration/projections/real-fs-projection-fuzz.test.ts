import { describe, test, expect } from 'bun:test';
import { tmpdir } from 'node:os';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import { SpiteDB } from '../../../src/index';
import { createMockViewRegistration, MockViewProjection } from '../../setup/mock-projection';
import { SeededRandom, getSeedFromEnv } from '../../setup/seeded-random';
import { BunFileSystem } from '../../../src/infrastructure/filesystem/bun-filesystem';

const DEFAULT_ITERATIONS = 3;

function getIterations(): number {
  const override = process.env['REAL_FS_PROJECTION_FUZZ_ITERATIONS'];
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

describe('Real FS Projection Fuzz', () => {
  const iterations = getIterations();

  for (let i = 0; i < iterations; i++) {
    flockTest(`iteration ${i}`, async () => {
      const baseSeed = getSeedFromEnv();
      const iterationSeed = baseSeed + i + 20000;
      const random = new SeededRandom(iterationSeed);

      const tempRoot = await mkdtemp(path.join(tmpdir(), 'spitedb-projection-'));
      const dataDir = path.join(tempRoot, 'db');
      const expected = new Map<string, Record<string, unknown>>();
      const userIds = Array.from({ length: 80 }, (_, idx) => `user-${idx}`);
      const groups = ['north', 'south', 'east', 'west'];
      const payload = 'x'.repeat(4096);

      try {
        const db = await SpiteDB.open(dataDir, {
          autoFlushCount: 20,
          projectionPollingIntervalMs: 10,
          projectionCheckpointIntervalMs: 50,
          projectionBatchSize: 50,
        });

        db.registerProjection(
          createMockViewRegistration('users', ['UserCreated', 'UserUpdated', 'UserDeleted'], ['id', 'group']),
          { memoryThresholdBytes: 32 * 1024 }
        );
        await db.startProjections();

        const operations = random.int(120, 200);
        for (let op = 0; op < operations; op++) {
          const id = random.choice(userIds);
          const action = random.int(0, 2);
          let type = 'UserUpdated';

          if (action === 0) {
            type = 'UserCreated';
          } else if (action === 2) {
            type = 'UserDeleted';
          }

          const record = {
            id,
            group: random.choice(groups),
            updatedAt: Date.now(),
            payload,
          };

          await db.append('users', [{ type, data: record }]);

          if (type === 'UserDeleted') {
            expected.delete(id);
          } else {
            expected.set(id, record);
          }

          if (op % 25 === 0) {
            await db.flush();
          }
        }

        await db.flush();
        await db.waitForProjections(20000);
        await db.forceProjectionCheckpoint();
        await db.stopProjections();
        await db.close();

        const dbReloaded = await SpiteDB.open(dataDir, {
          projectionPollingIntervalMs: 10,
          projectionCheckpointIntervalMs: 50,
          projectionBatchSize: 50,
        });
        dbReloaded.registerProjection(
          createMockViewRegistration('users', ['UserCreated', 'UserUpdated', 'UserDeleted'], ['id', 'group']),
          { memoryThresholdBytes: 32 * 1024 }
        );
        await dbReloaded.startProjections();
        await dbReloaded.waitForProjections(20000);

        const projection = dbReloaded.getProjection('users') as MockViewProjection;
        expect(projection.getCount()).toBe(expected.size);

        for (const [id, record] of expected) {
          expect(projection.getById(id)).toEqual(record);
        }

        await dbReloaded.close();
      } catch (error) {
        throw new Error(
          `Real FS projection fuzz failed (seed=${iterationSeed}): ${error instanceof Error ? error.message : String(error)}`
        );
      } finally {
        await rm(tempRoot, { recursive: true, force: true });
      }
    }, 30000);
  }
});
