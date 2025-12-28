import { describe, test, expect } from 'bun:test';
import { EventStore } from '../../../src/application/event-store';
import { SimulatedFileSystem } from '../../../src/testing/simulated-filesystem';
import { BunClock } from '../../../src/infrastructure/time/bun-clock';
import { MsgpackSerializer } from '../../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../../src/infrastructure/serialization/zstd-compressor';

describe('Concurrent Appends', () => {
  test('serializes flush and rotation under concurrent append load', async () => {
    const clock = new BunClock();
    const fs = new SimulatedFileSystem(clock);
    fs.injectFault({ syncDelayMs: 2 });

    const store = new EventStore({
      fs,
      serializer: new MsgpackSerializer(),
      compressor: new ZstdCompressor(),
      clock,
      maxSegmentSize: 32 * 1024,
      autoFlushCount: 25,
      indexCacheSize: 2,
    });

    const warnMessages: string[] = [];
    const originalWarn = console.warn;
    console.warn = (...args: unknown[]) => {
      warnMessages.push(args.map((arg) => String(arg)).join(' '));
      originalWarn(...args);
    };

    try {
      await store.open('/data/events');

      const workerCount = 8;
      const iterations = 200;
      const eventsPerAppend = 5;
      const payload = 'x'.repeat(512);

      const workers = Array.from({ length: workerCount }, (_, workerId) => {
        return (async () => {
          for (let i = 0; i < iterations; i += 1) {
            const streamId = `stream-${workerId}-${i % 20}`;
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

      const relevantWarnings = warnMessages.filter((message) => {
        return (
          message.includes('Failed to save manifest') ||
          message.includes('Background index write failed') ||
          message.includes('SegmentWriter is not open')
        );
      });
      expect(relevantWarnings).toHaveLength(0);
    } finally {
      console.warn = originalWarn;
      await store.close();
    }
  }, 20000);
});
