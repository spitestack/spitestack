/**
 * SpiteDB Append Load Test
 *
 * Time-based load test for measuring append throughput with admission control.
 * Fires concurrent appends with realistic 1KB msgpack-encoded events.
 *
 * Features:
 * - Ramp-up phase to gradually increase load
 * - Separate tracking of timeout errors (expected under admission control)
 * - Latency target validation (p99)
 * - Goodput vs attempted throughput metrics
 *
 * Usage: bun run bench/append-load-test.ts [duration_seconds] [concurrency] [events_per_append] [p99_target_ms]
 *
 * Examples:
 *   bun run bench/append-load-test.ts 60 2000 1 60    # 60s, 2000 workers, p99 target 60ms
 *   bun run bench/append-load-test.ts 30 1000 1 50    # Conservative test
 */

import { SpiteDbNapi } from '../index.js';
import { pack } from 'msgpackr';
import { randomUUID } from 'crypto';
import { unlinkSync, existsSync } from 'fs';

// Configuration
const DURATION_SECONDS = parseInt(process.argv[2] || '60', 10);
const CONCURRENCY = parseInt(process.argv[3] || '2000', 10); // Below max_inflight (3000)
const EVENTS_PER_APPEND = parseInt(process.argv[4] || '1', 10);
const P99_TARGET_MS = parseInt(process.argv[5] || '60', 10);
const RAMP_UP_SECONDS = 5; // Gradually increase load over 5 seconds

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

// Test result interface for programmatic use
export interface LoadTestResult {
  durationSec: number;
  eventsWritten: number;
  bytesWritten: number;
  eventsPerSec: number;
  mbPerSec: number;
  timeoutErrors: number;
  otherErrors: number;
  timeoutRate: number;
  latency: {
    min: number;
    avg: number;
    p50: number;
    p95: number;
    p99: number;
    max: number;
  };
  p99TargetMet: boolean;
}

// Run load test with given parameters (exported for use by find-sustainable-throughput)
export async function runLoadTest(
  durationSeconds: number = DURATION_SECONDS,
  concurrency: number = CONCURRENCY,
  eventsPerAppend: number = EVENTS_PER_APPEND,
  p99TargetMs: number = P99_TARGET_MS,
  verbose: boolean = true
): Promise<LoadTestResult> {
  const dbPath = './bench-test.db';

  if (verbose) {
    console.log('=== SpiteDB Append Load Test ===');
    console.log(`Duration: ${durationSeconds}s (+ ${RAMP_UP_SECONDS}s ramp-up)`);
    console.log(`Concurrency: ${concurrency} workers`);
    console.log(`Events per append: ${eventsPerAppend}`);
    console.log(`Event size: ~1KB (msgpack encoded)`);
    console.log(`p99 target: ${p99TargetMs}ms`);
    console.log('');
  }

  // Clean up any previous benchmark file
  cleanup(dbPath);

  const db = await SpiteDbNapi.open(dbPath);

  let eventsWritten = 0;
  let bytesWritten = 0;
  let commandCounter = 0;
  let timeoutErrors = 0;
  let otherErrors = 0;
  let running = true;

  // Track latencies for percentile calculation
  const latencies: number[] = [];

  const startTime = performance.now();
  const endTime = startTime + (durationSeconds + RAMP_UP_SECONDS) * 1000;

  // Progress reporting
  let progressInterval: ReturnType<typeof setInterval> | null = null;
  if (verbose) {
    progressInterval = setInterval(() => {
      const elapsed = (performance.now() - startTime) / 1000;
      const eventsPerSec = eventsWritten / Math.max(1, elapsed - RAMP_UP_SECONDS);
      const mbPerSec = bytesWritten / Math.max(1, elapsed - RAMP_UP_SECONDS) / 1024 / 1024;
      const timeoutRate = timeoutErrors / Math.max(1, eventsWritten + timeoutErrors) * 100;
      process.stdout.write(
        `\rProgress: ${eventsWritten.toLocaleString()} events | ${eventsPerSec.toFixed(0)} evt/s | ${mbPerSec.toFixed(2)} MB/s | timeouts: ${timeoutRate.toFixed(1)}% | ${Math.floor(elapsed)}s elapsed`
      );
    }, 1000);
  }

  // Worker function that continuously appends
  async function worker(workerId: number) {
    // Stagger worker start times during ramp-up phase
    const rampDelay = (workerId / concurrency) * RAMP_UP_SECONDS * 1000;
    await new Promise(resolve => setTimeout(resolve, rampDelay));

    while (running && performance.now() < endTime) {
      const commandId = `cmd-${workerId}-${commandCounter++}`;
      const streamId = `stream-${Math.floor(Math.random() * 10000)}`;

      // Generate multiple events per append
      const events: Buffer[] = [];
      for (let i = 0; i < eventsPerAppend; i++) {
        events.push(generateEvent());
      }

      try {
        const opStart = performance.now();
        await db.append(streamId, commandId, -1, events); // -1 = any revision
        const opEnd = performance.now();
        latencies.push(opEnd - opStart);
        eventsWritten += eventsPerAppend;
        bytesWritten += events.reduce((sum, e) => sum + e.length, 0);
      } catch (e) {
        const errorMsg = String(e);
        if (errorMsg.includes('timeout')) {
          timeoutErrors++;
        } else {
          otherErrors++;
          if (otherErrors < 10 && verbose) {
            console.error('\nAppend failed:', e);
          }
        }
      }
    }
  }

  // Launch concurrent workers
  const workers = Array.from({ length: concurrency }, (_, i) => worker(i));

  // Wait for all workers
  await Promise.all(workers);
  running = false;
  if (progressInterval) {
    clearInterval(progressInterval);
  }

  const elapsed = performance.now() - startTime;
  const elapsedSec = elapsed / 1000;
  // Subtract ramp-up time for accurate throughput calculation
  const effectiveElapsedSec = Math.max(1, elapsedSec - RAMP_UP_SECONDS);

  // Calculate latency stats
  let latencyStats = { min: 0, avg: 0, p50: 0, p95: 0, p99: 0, max: 0 };
  if (latencies.length > 0) {
    latencies.sort((a, b) => a - b);
    latencyStats = {
      min: latencies[0],
      avg: latencies.reduce((a, b) => a + b, 0) / latencies.length,
      p50: percentile(latencies, 50),
      p95: percentile(latencies, 95),
      p99: percentile(latencies, 99),
      max: latencies[latencies.length - 1],
    };
  }

  const totalAttempted = eventsWritten + timeoutErrors * eventsPerAppend;
  const timeoutRate = timeoutErrors / Math.max(1, timeoutErrors + Math.floor(eventsWritten / eventsPerAppend));
  const p99TargetMet = latencyStats.p99 <= p99TargetMs;

  if (verbose) {
    console.log('\n');
    console.log('=== Throughput ===');
    console.log(`Duration:      ${elapsedSec.toFixed(2)}s (${RAMP_UP_SECONDS}s ramp-up + ${effectiveElapsedSec.toFixed(2)}s test)`);
    console.log(`Attempted:     ${totalAttempted.toLocaleString()} events`);
    console.log(`Successful:    ${eventsWritten.toLocaleString()} events (goodput)`);
    console.log(`Total bytes:   ${(bytesWritten / 1024 / 1024).toFixed(2)} MB`);
    console.log(`Events/sec:    ${(eventsWritten / effectiveElapsedSec).toFixed(0)}`);
    console.log(`MB/sec:        ${(bytesWritten / effectiveElapsedSec / 1024 / 1024).toFixed(2)}`);

    console.log('');
    console.log('=== Errors ===');
    console.log(`Timeouts:      ${timeoutErrors} (${(timeoutRate * 100).toFixed(1)}% of requests)`);
    console.log(`Other errors:  ${otherErrors}`);

    if (latencies.length > 0) {
      console.log('');
      console.log('=== Latency (ms) ===');
      console.log(`Min:           ${latencyStats.min.toFixed(2)}`);
      console.log(`Avg:           ${latencyStats.avg.toFixed(2)}`);
      console.log(`p50:           ${latencyStats.p50.toFixed(2)}`);
      console.log(`p95:           ${latencyStats.p95.toFixed(2)}`);
      console.log(`p99:           ${latencyStats.p99.toFixed(2)} ${p99TargetMet ? '✓' : '✗'} (target: ${p99TargetMs}ms)`);
      console.log(`Max:           ${latencyStats.max.toFixed(2)}`);
      console.log(`Samples:       ${latencies.length.toLocaleString()}`);
    }

    console.log('');
    console.log('=== Summary ===');
    console.log(`p99 Target:    ${p99TargetMs}ms ${p99TargetMet ? '✓ PASS' : '✗ FAIL'}`);
    if (timeoutRate > 0.05) {
      console.log(`⚠️  High timeout rate (${(timeoutRate * 100).toFixed(1)}%) - consider reducing concurrency`);
    }
  }

  // Cleanup
  cleanup(dbPath);

  return {
    durationSec: effectiveElapsedSec,
    eventsWritten,
    bytesWritten,
    eventsPerSec: eventsWritten / effectiveElapsedSec,
    mbPerSec: bytesWritten / effectiveElapsedSec / 1024 / 1024,
    timeoutErrors,
    otherErrors,
    timeoutRate,
    latency: latencyStats,
    p99TargetMet,
  };
}

// Run if executed directly
if (import.meta.main) {
  runLoadTest().catch(console.error);
}
