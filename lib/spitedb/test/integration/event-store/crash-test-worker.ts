/**
 * Worker script for real crash tests.
 *
 * This script is spawned as a child process and writes events until killed.
 * The parent process can SIGKILL it at various points to test crash recovery.
 *
 * Communication protocol:
 * - Worker sends JSON messages to stdout: { type: 'ready' | 'flushed', position?: number }
 * - Parent sends JSON messages to stdin: { type: 'write' | 'flush' | 'exit' }
 */

import path from 'node:path';
import { EventStore } from '../../../src/application/event-store';
import { BunFileSystem } from '../../../src/infrastructure/filesystem/bun-filesystem';
import { BunClock } from '../../../src/infrastructure/time/bun-clock';
import { MsgpackSerializer } from '../../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../../src/infrastructure/serialization/zstd-compressor';

interface Message {
  type: 'write' | 'flush' | 'exit';
  count?: number;
  streamId?: string;
}

interface Response {
  type: 'ready' | 'flushed' | 'written' | 'error';
  position?: number;
  error?: string;
}

function send(response: Response): void {
  process.stdout.write(JSON.stringify(response) + '\n');
}

async function main(): Promise<void> {
  const dataDir = process.argv[2];
  if (!dataDir) {
    send({ type: 'error', error: 'No data directory provided' });
    process.exit(1);
  }

  const store = new EventStore({
    fs: new BunFileSystem(),
    serializer: new MsgpackSerializer(),
    compressor: new ZstdCompressor(),
    clock: new BunClock(),
    autoFlushCount: 0, // Manual flush only
    maxSegmentSize: 64 * 1024, // Small segments for faster rotation
  });

  try {
    await store.open(dataDir);
    send({ type: 'ready', position: store.getGlobalPosition() });

    // Read commands from stdin
    const decoder = new TextDecoder();
    const reader = Bun.stdin.stream().getReader();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (!line.trim()) continue;

        const msg: Message = JSON.parse(line);

        if (msg.type === 'write') {
          const count = msg.count ?? 1;
          const streamId = msg.streamId ?? 'test-stream';

          const events = Array.from({ length: count }, (_, i) => ({
            type: 'TestEvent',
            data: {
              index: i,
              payload: 'x'.repeat(256), // Some data to make it realistic
              timestamp: Date.now(),
            },
          }));

          await store.append(streamId, events);
          send({ type: 'written', position: store.getGlobalPosition() });
        } else if (msg.type === 'flush') {
          await store.flush();
          send({ type: 'flushed', position: store.getGlobalPosition() });
        } else if (msg.type === 'exit') {
          await store.close();
          process.exit(0);
        }
      }
    }
  } catch (error) {
    send({
      type: 'error',
      error: error instanceof Error ? error.message : String(error),
    });
    process.exit(1);
  }
}

main();
