import { describe, it, expect, beforeEach } from 'bun:test';
import { SimulatedFileSystem } from '../../../src/testing/simulated-filesystem';
import { SimulatedClock } from '../../../src/testing/simulated-clock';
import { EventStore } from '../../../src/application/event-store';
import { MsgpackSerializer } from '../../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../../src/infrastructure/serialization/zstd-compressor';

describe('File Locking', () => {
  let fs: SimulatedFileSystem;
  let clock: SimulatedClock;

  beforeEach(() => {
    clock = new SimulatedClock();
    fs = new SimulatedFileSystem(clock);
  });

  function createStore() {
    return new EventStore({
      fs,
      serializer: new MsgpackSerializer(),
      compressor: new ZstdCompressor(),
      clock,
    });
  }

  describe('exclusive locking', () => {
    it('should acquire lock on open', async () => {
      const store = createStore();
      await store.open('/data/events');

      // Lock file should exist
      expect(await fs.exists('/data/events/.lock')).toBe(true);

      await store.close();
    });

    it('should prevent second store from opening same directory', async () => {
      const store1 = createStore();
      await store1.open('/data/events');

      const store2 = createStore();
      await expect(store2.open('/data/events')).rejects.toThrow(
        /another process may have it open/
      );

      await store1.close();
    });

    it('should allow second store after first closes', async () => {
      const store1 = createStore();
      await store1.open('/data/events');
      await store1.close();

      const store2 = createStore();
      await store2.open('/data/events');
      await store2.close();
    });

    it('should release lock on close', async () => {
      const store1 = createStore();
      await store1.open('/data/events');
      await store1.close();

      // Second store should be able to open
      const store2 = createStore();
      await store2.open('/data/events');
      await store2.close();
    });

    it('should allow different directories to be opened simultaneously', async () => {
      const store1 = createStore();
      const store2 = createStore();

      await store1.open('/data/events1');
      await store2.open('/data/events2');

      // Both should work
      await store1.append('stream-1', [{ type: 'Event1', data: {} }]);
      await store2.append('stream-2', [{ type: 'Event2', data: {} }]);

      await store1.close();
      await store2.close();
    });
  });

  describe('crash recovery', () => {
    it('should release lock after simulated crash', async () => {
      const store1 = createStore();
      await store1.open('/data/events');

      // Simulate crash (releases locks automatically)
      fs.crash();

      // New store should be able to open
      const store2 = createStore();
      await store2.open('/data/events');
      await store2.close();
    });
  });

  describe('SimulatedFileSystem flock behavior', () => {
    it('should allow multiple shared locks', async () => {
      fs.setFileContent('/test.lock', new Uint8Array(0));

      const handle1 = await fs.open('/test.lock', 'read');
      const handle2 = await fs.open('/test.lock', 'read');

      await fs.flock(handle1, 'shared');
      await fs.flock(handle2, 'shared');

      // Both should succeed
      await fs.close(handle1);
      await fs.close(handle2);
    });

    it('should block exclusive lock when shared locks exist', async () => {
      fs.setFileContent('/test.lock', new Uint8Array(0));

      const handle1 = await fs.open('/test.lock', 'read');
      const handle2 = await fs.open('/test.lock', 'write');

      await fs.flock(handle1, 'shared');

      await expect(fs.flock(handle2, 'exclusive')).rejects.toThrow(
        /shared lock/
      );

      await fs.close(handle1);
      await fs.close(handle2);
    });

    it('should block shared lock when exclusive lock exists', async () => {
      fs.setFileContent('/test.lock', new Uint8Array(0));

      const handle1 = await fs.open('/test.lock', 'write');
      const handle2 = await fs.open('/test.lock', 'read');

      await fs.flock(handle1, 'exclusive');

      await expect(fs.flock(handle2, 'shared')).rejects.toThrow(
        /exclusive lock/
      );

      await fs.close(handle1);
      await fs.close(handle2);
    });

    it('should release lock on close', async () => {
      fs.setFileContent('/test.lock', new Uint8Array(0));

      const handle1 = await fs.open('/test.lock', 'write');
      await fs.flock(handle1, 'exclusive');
      await fs.close(handle1);

      // Should be able to acquire lock now
      const handle2 = await fs.open('/test.lock', 'write');
      await fs.flock(handle2, 'exclusive');
      await fs.close(handle2);
    });

    it('should allow upgrading lock', async () => {
      fs.setFileContent('/test.lock', new Uint8Array(0));

      const handle = await fs.open('/test.lock', 'readwrite');
      await fs.flock(handle, 'shared');
      await fs.flock(handle, 'exclusive'); // Upgrade

      await fs.close(handle);
    });
  });
});
