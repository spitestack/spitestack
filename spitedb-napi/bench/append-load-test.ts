/**
 * SpiteDB Append Load Test
 *
 * Time-based load test for measuring append throughput.
 * Fires concurrent appends with realistic 1KB msgpack-encoded events.
 *
 * Usage: bun run bench/append-load-test.ts [duration_seconds]
 */

import { SpiteDbNapi } from '../index.js';
import { pack } from 'msgpackr';
import { randomUUID } from 'crypto';
import { unlinkSync, existsSync } from 'fs';

const DURATION_SECONDS = parseInt(process.argv[2] || '60', 10);
const CONCURRENCY = parseInt(process.argv[3] || '5000', 10); // Parallel append calls in flight
const EVENTS_PER_APPEND = parseInt(process.argv[4] || '1', 10); // Events per append call

// Realistic 1KB event structure (mimics real-world e-commerce)
interface RealisticEvent {
  eventType: string;
  aggregateId: string;
  timestamp: number;
  version: number;
  correlationId: string;
  causationId: string;
  metadata: {
    userId: string;
    sessionId: string;
    ipAddress: string;
    userAgent: string;
  };
  payload: {
    orderId: string;
    customerId: string;
    items: Array<{
      productId: string;
      quantity: number;
      price: number;
      name: string;
    }>;
    totalAmount: number;
    currency: string;
    shippingAddress: {
      street: string;
      city: string;
      state: string;
      zip: string;
      country: string;
    };
  };
}

// Generate realistic event data (~1KB msgpack encoded)
function generateEvent(): Buffer {
  const event: RealisticEvent = {
    eventType: 'OrderPlaced',
    aggregateId: randomUUID(),
    timestamp: Date.now(),
    version: Math.floor(Math.random() * 1000),
    correlationId: randomUUID(),
    causationId: randomUUID(),
    metadata: {
      userId: randomUUID(),
      sessionId: randomUUID(),
      ipAddress: `192.168.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`,
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    },
    payload: {
      orderId: randomUUID(),
      customerId: randomUUID(),
      items: Array.from({ length: 3 }, () => ({
        productId: randomUUID(),
        quantity: Math.floor(Math.random() * 10) + 1,
        price: Math.random() * 1000,
        name: 'Product Name Here With Some Realistic Length For Padding To Hit Target Size',
      })),
      totalAmount: Math.random() * 5000,
      currency: 'USD',
      shippingAddress: {
        street: '123 Main Street Apartment 4B',
        city: 'San Francisco',
        state: 'CA',
        zip: '94102',
        country: 'USA',
      },
    },
  };

  const packed = pack(event);
  // Pad to exactly 1KB if smaller
  if (packed.length < 1024) {
    const padded = Buffer.alloc(1024);
    packed.copy(padded);
    return padded;
  }
  return Buffer.from(packed);
}

// Cleanup previous benchmark files
function cleanup(dbPath: string) {
  const files = [dbPath, `${dbPath}-wal`, `${dbPath}-shm`];
  for (const file of files) {
    if (existsSync(file)) {
      try {
        unlinkSync(file);
      } catch {
        // Ignore errors
      }
    }
  }
}

// Calculate percentiles from sorted array
function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

async function runLoadTest() {
  const dbPath = './bench-test.db';

  console.log('=== SpiteDB Append Load Test ===');
  console.log(`Duration: ${DURATION_SECONDS}s`);
  console.log(`Concurrency: ${CONCURRENCY} workers`);
  console.log(`Events per append: ${EVENTS_PER_APPEND}`);
  console.log(`Event size: ~1KB (msgpack encoded)`);
  console.log('');

  // Clean up any previous benchmark file
  cleanup(dbPath);

  const db = await SpiteDbNapi.open(dbPath);

  let eventsWritten = 0;
  let bytesWritten = 0;
  let commandCounter = 0;
  let errors = 0;
  let running = true;

  // Track latencies for percentile calculation
  const latencies: number[] = [];

  const startTime = performance.now();
  const endTime = startTime + DURATION_SECONDS * 1000;

  // Progress reporting
  const progressInterval = setInterval(() => {
    const elapsed = (performance.now() - startTime) / 1000;
    const eventsPerSec = eventsWritten / elapsed;
    const mbPerSec = bytesWritten / elapsed / 1024 / 1024;
    process.stdout.write(
      `\rProgress: ${eventsWritten.toLocaleString()} events | ${eventsPerSec.toFixed(0)} evt/s | ${mbPerSec.toFixed(2)} MB/s | ${Math.floor(elapsed)}s elapsed`
    );
  }, 1000);

  // Worker function that continuously appends
  async function worker(workerId: number) {
    while (running && performance.now() < endTime) {
      const commandId = `cmd-${workerId}-${commandCounter++}`;
      const streamId = `stream-${Math.floor(Math.random() * 10000)}`;

      // Generate multiple events per append
      const events: Buffer[] = [];
      for (let i = 0; i < EVENTS_PER_APPEND; i++) {
        events.push(generateEvent());
      }

      try {
        const opStart = performance.now();
        await db.append(streamId, commandId, -1, events); // -1 = any revision
        const opEnd = performance.now();
        latencies.push(opEnd - opStart);
        eventsWritten += EVENTS_PER_APPEND;
        bytesWritten += events.reduce((sum, e) => sum + e.length, 0);
      } catch (e) {
        errors++;
        if (errors < 10) {
          console.error('\nAppend failed:', e);
        }
      }
    }
  }

  // Launch concurrent workers
  const workers = Array.from({ length: CONCURRENCY }, (_, i) => worker(i));

  // Wait for all workers
  await Promise.all(workers);
  running = false;
  clearInterval(progressInterval);

  const elapsed = performance.now() - startTime;
  const elapsedSec = elapsed / 1000;

  console.log('\n');
  console.log('=== Results ===');
  console.log(`Duration:      ${elapsedSec.toFixed(2)}s`);
  console.log(`Total events:  ${eventsWritten.toLocaleString()}`);
  console.log(`Total bytes:   ${(bytesWritten / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Events/sec:    ${(eventsWritten / elapsedSec).toFixed(0)}`);
  console.log(`MB/sec:        ${(bytesWritten / elapsedSec / 1024 / 1024).toFixed(2)}`);
  if (errors > 0) {
    console.log(`Errors:        ${errors}`);
  }

  // Calculate and display latency percentiles
  if (latencies.length > 0) {
    latencies.sort((a, b) => a - b);
    const p50 = percentile(latencies, 50);
    const p95 = percentile(latencies, 95);
    const p99 = percentile(latencies, 99);
    const min = latencies[0];
    const max = latencies[latencies.length - 1];
    const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;

    console.log('');
    console.log('=== Latency (ms) ===');
    console.log(`Min:           ${min.toFixed(2)}`);
    console.log(`Avg:           ${avg.toFixed(2)}`);
    console.log(`p50:           ${p50.toFixed(2)}`);
    console.log(`p95:           ${p95.toFixed(2)}`);
    console.log(`p99:           ${p99.toFixed(2)}`);
    console.log(`Max:           ${max.toFixed(2)}`);
    console.log(`Samples:       ${latencies.length.toLocaleString()}`);
  }

  // Cleanup
  cleanup(dbPath);
}

runLoadTest().catch(console.error);
