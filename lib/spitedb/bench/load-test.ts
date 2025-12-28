/**
 * SpiteDB Load Test
 *
 * Measures append throughput (events/sec) while projections run in parallel.
 * Uses a realistic mix of order + user events, multiple tenants, and
 * projections that model common event-sourcing workloads.
 *
 * Usage:
 *   bun run bench/load-test.ts [durationSec] [concurrency] [eventsPerAppend] [streamPool]
 *
 * Examples:
 *   bun run bench/load-test.ts 60 200 5 20000
 *   bun run bench/load-test.ts 30 100 10 5000 --keep
 */

import {
  SpiteDB,
  MsgpackSerializer,
  type InputEvent,
  type StoredEvent,
  type ReadBatchProfiler,
  type ReadBatchProfileSample,
  FastEventSerializer,
  NoopCompressor,
  type Serializer,
  type Compressor,
} from '../src/index';
import type {
  AccessPattern,
  Projection,
  ProjectionMetadata,
  ProjectionRegistration,
} from '../src/ports/projections';
import type { ProjectionStore } from '../src/ports/projections/projection-store';
import { mkdir, rm } from 'node:fs/promises';
import path from 'node:path';

interface LoadTestConfig {
  durationSeconds: number;
  concurrency: number;
  eventsPerAppend: number;
  streamPool: number;
  rampUpSeconds: number;
  payloadBytes: number;
  dataDir?: string;
  keepData: boolean;
  userEventRatio: number;
  userPool: number;
  orderPool: number;
  projectionWaitMs: number;
  debug: boolean;
  projectionBatchSize?: number;
  projectionPollingIntervalMs?: number;
  profile: boolean;
  autoFlushCount?: number;
  fastSerializer: boolean;
  noCompression: boolean;
  backpressureMaxLag?: number;
  backpressureMaxWaitMs?: number;
  backpressureMode?: 'block' | 'fail';
  disableBackpressure: boolean;
}

const DEFAULTS: LoadTestConfig = {
  durationSeconds: 60,
  concurrency: 200,
  eventsPerAppend: 5,
  streamPool: 20000,
  rampUpSeconds: 5,
  payloadBytes: 768,
  keepData: false,
  userEventRatio: 0.25,
  userPool: 8000,
  orderPool: 20000,
  projectionWaitMs: 60000,
  debug: false,
  profile: false,
  fastSerializer: false,
  noCompression: false,
  disableBackpressure: false,
};

class XorShift32 {
  private state: number;

  constructor(seed: number) {
    this.state = seed || 1;
  }

  next(): number {
    let x = this.state;
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    this.state = x >>> 0;
    return this.state;
  }

  float(): number {
    return this.next() / 0xffffffff;
  }

  int(maxExclusive: number): number {
    return Math.floor(this.float() * maxExclusive);
  }
}

class LatencySampler {
  private readonly maxSamples: number;
  private readonly rand: XorShift32;
  private samples: number[] = [];
  private count = 0;
  private sum = 0;
  private min = Number.POSITIVE_INFINITY;
  private max = 0;

  constructor(maxSamples: number, rand: XorShift32) {
    this.maxSamples = maxSamples;
    this.rand = rand;
  }

  add(valueMs: number): void {
    this.count += 1;
    this.sum += valueMs;
    if (valueMs < this.min) this.min = valueMs;
    if (valueMs > this.max) this.max = valueMs;

    if (this.samples.length < this.maxSamples) {
      this.samples.push(valueMs);
      return;
    }

    const j = this.rand.int(this.count);
    if (j < this.maxSamples) {
      this.samples[j] = valueMs;
    }
  }

  summary() {
    if (this.count === 0) {
      return { min: 0, avg: 0, p50: 0, p95: 0, p99: 0, max: 0, samples: 0 };
    }

    const sorted = [...this.samples].sort((a, b) => a - b);
    const percentile = (p: number) => {
      const idx = Math.max(0, Math.ceil((p / 100) * sorted.length) - 1);
      return sorted[idx] ?? 0;
    };

    return {
      min: this.min,
      avg: this.sum / this.count,
      p50: percentile(50),
      p95: percentile(95),
      p99: percentile(99),
      max: this.max,
      samples: this.samples.length,
    };
  }
}

function parseArgs(argv: string[]): LoadTestConfig {
  const config: LoadTestConfig = { ...DEFAULTS };
  let userPoolOverride = false;
  let orderPoolOverride = false;
  let streamPoolOverride = false;
  const positional = argv.filter((arg) => !arg.startsWith('--'));

  if (positional[0]) config.durationSeconds = Number(positional[0]);
  if (positional[1]) config.concurrency = Number(positional[1]);
  if (positional[2]) config.eventsPerAppend = Number(positional[2]);
  if (positional[3]) {
    config.streamPool = Number(positional[3]);
    streamPoolOverride = true;
  }

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i] ?? '';
    if (!arg.startsWith('--')) continue;

    const [key, rawValue] = arg.includes('=')
      ? arg.split('=')
      : [arg, argv[i + 1]];
    const value = rawValue ?? '';

    switch (key) {
      case '--keep':
        config.keepData = true;
        break;
      case '--data-dir':
        config.dataDir = value;
        break;
      case '--payload-bytes':
        config.payloadBytes = Number(value);
        break;
      case '--ramp-up':
        config.rampUpSeconds = Number(value);
        break;
      case '--stream-pool':
        config.streamPool = Number(value);
        streamPoolOverride = true;
        break;
      case '--user-ratio':
        config.userEventRatio = Number(value);
        break;
      case '--user-pool':
        config.userPool = Number(value);
        userPoolOverride = true;
        break;
      case '--order-pool':
        config.orderPool = Number(value);
        orderPoolOverride = true;
        break;
      case '--projection-wait-ms':
        config.projectionWaitMs = Number(value);
        break;
      case '--projection-batch-size':
        config.projectionBatchSize = Number(value);
        break;
      case '--projection-poll-ms':
        config.projectionPollingIntervalMs = Number(value);
        break;
      case '--auto-flush-count':
        config.autoFlushCount = Number(value);
        break;
      case '--fast-serializer':
        config.fastSerializer = true;
        break;
      case '--no-compress':
        config.noCompression = true;
        break;
      case '--profile':
        config.profile = true;
        break;
      case '--debug':
        config.debug = true;
        break;
      case '--backpressure-max-lag':
        config.backpressureMaxLag = Number(value);
        break;
      case '--backpressure-max-wait-ms':
        config.backpressureMaxWaitMs = Number(value);
        break;
      case '--backpressure-mode':
        config.backpressureMode = value as 'block' | 'fail';
        break;
      case '--no-backpressure':
        config.disableBackpressure = true;
        break;
      default:
        break;
    }
  }

  if (!userPoolOverride && !orderPoolOverride && streamPoolOverride && Number.isFinite(config.streamPool)) {
    const userPool = Math.max(1, Math.round(config.streamPool * config.userEventRatio));
    const orderPool = Math.max(1, config.streamPool - userPool);
    config.userPool = userPool;
    config.orderPool = orderPool;
  }

  return config;
}

function buildPadding(bytes: number): string {
  if (bytes <= 0) return '';
  return 'x'.repeat(bytes);
}

function randomId(rand: XorShift32, prefix: string, length: number = 8): string {
  const alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let out = prefix;
  for (let i = 0; i < length; i += 1) {
    out += alphabet[rand.int(alphabet.length)];
  }
  return out;
}

function avg(values: number[]): number {
  if (values.length === 0) return 0;
  const total = values.reduce((sum, value) => sum + value, 0);
  return total / values.length;
}

interface UserProfile {
  id: string;
  email: string;
  name: string;
  plan: string;
  updatedAt: number;
  lastOrderId?: string;
  padding?: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object';
}

function isUserProfile(value: unknown): value is UserProfile {
  if (!isRecord(value)) return false;
  return (
    typeof value['id'] === 'string' &&
    typeof value['email'] === 'string' &&
    typeof value['name'] === 'string' &&
    typeof value['plan'] === 'string' &&
    typeof value['updatedAt'] === 'number'
  );
}

function getTotalAmount(event: StoredEvent): number | null {
  if (!isRecord(event.data)) {
    return null;
  }
  const amount = event.data['totalAmount'];
  return typeof amount === 'number' ? amount : null;
}

class TotalRevenueProjection implements Projection<number> {
  private total = 0;

  build(event: StoredEvent): void {
    const amount = getTotalAmount(event);
    if (amount === null) {
      return;
    }
    if (event.type === 'OrderPlaced') {
      this.total += amount;
    }
    if (event.type === 'OrderRefunded') {
      this.total -= amount;
    }
  }

  getState(): number {
    return this.total;
  }

  setState(state: number): void {
    this.total = state;
  }

  reset(): void {
    this.total = 0;
  }

  getTotal(): number {
    return this.total;
  }
}

class UserDirectoryProjection implements Projection<Map<string, UserProfile>> {
  private users = new Map<string, UserProfile>();

  build(event: StoredEvent): void {
    if (!isUserProfile(event.data)) {
      return;
    }
    if (event.type === 'UserDeleted') {
      this.users.delete(event.data.id);
      return;
    }
    if (event.type === 'UserCreated' || event.type === 'UserUpdated') {
      this.users.set(event.data.id, event.data);
    }
  }

  applyToStore(
    event: StoredEvent,
    store: ProjectionStore<Map<string, UserProfile>>
  ): void {
    if (!isUserProfile(event.data)) {
      return;
    }
    if (event.type === 'UserDeleted') {
      store.deleteByKey?.(event.data.id);
      return;
    }
    if (event.type === 'UserCreated' || event.type === 'UserUpdated') {
      store.setByKey?.(event.data.id, event.data);
    }
  }

  getState(): Map<string, UserProfile> {
    return new Map(this.users);
  }

  setState(state: Map<string, UserProfile>): void {
    this.users = new Map(state);
  }

  reset(): void {
    this.users.clear();
  }

  getById(id: string): UserProfile | undefined {
    return this.users.get(id);
  }

  getByEmail(email: string): UserProfile | undefined {
    for (const user of this.users.values()) {
      if (user.email === email) {
        return user;
      }
    }
    return undefined;
  }

  getCount(): number {
    return this.users.size;
  }
}

function createProjectionRegistration<TState, TInstance extends Projection<TState>>(
  metadata: ProjectionMetadata,
  factory: () => TInstance
): ProjectionRegistration<TState> {
  let instance: TInstance | null = null;
  return {
    metadata,
    factory: () => {
      instance = factory();
      return instance;
    },
    getInstance: () => {
      if (!instance) {
        throw new Error(`Projection '${metadata.name}' has not been initialized yet.`);
      }
      return instance;
    },
  };
}

function createTotalRevenueRegistration(): ProjectionRegistration<number> {
  const metadata: ProjectionMetadata = {
    name: 'TotalRevenue',
    kind: 'aggregator',
    subscribedEvents: ['OrderPlaced', 'OrderRefunded'],
    accessPatterns: [{ methodName: 'getTotal', indexFields: [] }],
  };
  return createProjectionRegistration(metadata, () => new TotalRevenueProjection());
}

function createUserDirectoryRegistration(): ProjectionRegistration<Map<string, UserProfile>> {
  const accessPatterns: AccessPattern[] = [
    { methodName: 'getById', indexFields: ['id'] },
    { methodName: 'getByEmail', indexFields: ['email'] },
  ];
  const metadata: ProjectionMetadata = {
    name: 'UserDirectory',
    kind: 'denormalized_view',
    subscribedEvents: ['UserCreated', 'UserUpdated', 'UserDeleted'],
    accessPatterns,
  };
  return createProjectionRegistration(metadata, () => new UserDirectoryProjection());
}

function chooseWeighted<T>(rand: XorShift32, options: Array<{ value: T; weight: number }>): T {
  const total = options.reduce((sum, opt) => sum + opt.weight, 0);
  const target = rand.float() * total;
  let cumulative = 0;
  for (const option of options) {
    cumulative += option.weight;
    if (target <= cumulative) return option.value;
  }
  return options[options.length - 1]!.value;
}

function createUserEvent(
  rand: XorShift32,
  userId: string,
  padding: string
): InputEvent {
  const now = Date.now();
  const type = chooseWeighted(rand, [
    { value: 'UserCreated', weight: 35 },
    { value: 'UserUpdated', weight: 55 },
    { value: 'UserDeleted', weight: 10 },
  ]);

  return {
    type,
    data: {
      id: userId,
      email: `${userId}@spitestack.dev`,
      name: `User ${userId}`,
      plan: ['free', 'pro', 'enterprise'][rand.int(3)],
      updatedAt: now,
      lastOrderId: randomId(rand, 'order_', 10),
      padding,
    },
    metadata: {
      source: 'load-test',
      correlationId: randomId(rand, 'corr_', 12),
      causationId: randomId(rand, 'cause_', 12),
    },
  };
}

function createOrderEvent(
  rand: XorShift32,
  orderId: string,
  userId: string,
  padding: string
): InputEvent {
  const now = Date.now();
  const type = chooseWeighted(rand, [
    { value: 'OrderPlaced', weight: 45 },
    { value: 'OrderPaid', weight: 25 },
    { value: 'OrderShipped', weight: 20 },
    { value: 'OrderRefunded', weight: 10 },
  ]);
  const itemCount = 1 + rand.int(4);
  const items = Array.from({ length: itemCount }, (_, i) => ({
    sku: `sku-${rand.int(9000) + 1000}`,
    name: `Product ${i + 1}`,
    quantity: 1 + rand.int(3),
    price: Number((rand.float() * 200 + 20).toFixed(2)),
  }));
  const totalAmount = Number(items.reduce((sum, item) => sum + item.price * item.quantity, 0).toFixed(2));

  return {
    type,
    data: {
      orderId,
      userId,
      items,
      status: type.replace('Order', ''),
      totalAmount,
      currency: 'USD',
      placedAt: now,
      padding,
    },
    metadata: {
      source: 'load-test',
      correlationId: randomId(rand, 'corr_', 12),
      causationId: randomId(rand, 'cause_', 12),
    },
  };
}

async function run(): Promise<void> {
  const config = parseArgs(process.argv.slice(2));
  const seed = Number(process.env['SEED'] ?? Date.now());
  const rand = new XorShift32(seed);
  const serializer = new MsgpackSerializer();

  const dataDir =
    config.dataDir ??
    path.join(process.cwd(), '.bench', `spitedb-load-${Date.now()}`);

  await rm(dataDir, { recursive: true, force: true });
  await mkdir(dataDir, { recursive: true });

  const padding = buildPadding(config.payloadBytes);
  const eventSizes = new Map<string, number>();
  const headerSamples: number[] = [];
  const payloadSamples: number[] = [];
  const decompressSamples: number[] = [];
  const decodeSamples: number[] = [];
  const totalReadSamples: number[] = [];
  const profileSampleMax = 200;

  const readProfiler: ReadBatchProfiler | undefined = config.profile
    ? {
        record(sample: ReadBatchProfileSample) {
          headerSamples.push(sample.headerReadMs);
          payloadSamples.push(sample.payloadReadMs);
          decompressSamples.push(sample.decompressMs);
          decodeSamples.push(sample.decodeMs);
          totalReadSamples.push(sample.totalMs);
          if (headerSamples.length > profileSampleMax) headerSamples.shift();
          if (payloadSamples.length > profileSampleMax) payloadSamples.shift();
          if (decompressSamples.length > profileSampleMax) decompressSamples.shift();
          if (decodeSamples.length > profileSampleMax) decodeSamples.shift();
          if (totalReadSamples.length > profileSampleMax) totalReadSamples.shift();
        },
      }
    : undefined;

  const openOptions: {
    autoFlushCount?: number;
    projectionBatchSize?: number;
    projectionPollingIntervalMs?: number;
    readProfiler?: ReadBatchProfiler;
    eventSerializer?: Serializer;
    compressor?: Compressor;
    projectionBackpressure?: {
      maxLag?: number;
      maxWaitMs?: number;
      mode?: 'block' | 'fail';
    } | false;
  } = {};
  if (config.autoFlushCount !== undefined) {
    openOptions.autoFlushCount = config.autoFlushCount;
  }
  if (config.projectionBatchSize !== undefined) {
    openOptions.projectionBatchSize = config.projectionBatchSize;
  }
  if (config.projectionPollingIntervalMs !== undefined) {
    openOptions.projectionPollingIntervalMs = config.projectionPollingIntervalMs;
  }
  if (readProfiler) {
    openOptions.readProfiler = readProfiler;
  }
  if (config.fastSerializer) {
    openOptions.eventSerializer = new FastEventSerializer();
  }
  if (config.noCompression) {
    openOptions.compressor = new NoopCompressor();
  }
  if (config.disableBackpressure) {
    openOptions.projectionBackpressure = false;
  } else if (config.backpressureMaxLag !== undefined || config.backpressureMaxWaitMs !== undefined || config.backpressureMode !== undefined) {
    const backpressureOptions: { maxLag?: number; maxWaitMs?: number; mode?: 'block' | 'fail' } = {};
    if (config.backpressureMaxLag !== undefined) {
      backpressureOptions.maxLag = config.backpressureMaxLag;
    }
    if (config.backpressureMaxWaitMs !== undefined) {
      backpressureOptions.maxWaitMs = config.backpressureMaxWaitMs;
    }
    if (config.backpressureMode !== undefined) {
      backpressureOptions.mode = config.backpressureMode;
    }
    openOptions.projectionBackpressure = backpressureOptions;
  }

  const db = await SpiteDB.open(dataDir, openOptions);
  db.registerProjection(createTotalRevenueRegistration());
  db.registerProjection(createUserDirectoryRegistration());
  await db.startProjections();

  const userIds = Array.from({ length: config.userPool }, () => randomId(rand, 'user_', 10));
  const orderIds = Array.from({ length: config.orderPool }, () => randomId(rand, 'order_', 10));
  const totalStreams = config.userPool + config.orderPool;
  const tenants = ['northwind', 'acme', 'neotokyo', 'spite'];

  let eventsWritten = 0;
  let bytesWritten = 0;
  let appendOps = 0;
  let errors = 0;
  let running = true;
  let phase: 'appending' | 'catchup' = 'appending';
  let catchUpStart = 0;

  const latencySampler = new LatencySampler(100000, rand);
  const startTime = performance.now();
  const endTime = startTime + (config.durationSeconds + config.rampUpSeconds) * 1000;

  const progressInterval = setInterval(() => {
    const status = db.getProjectionStatus();
    const globalPos = status.globalPosition;
    const lags = status.projections.map((projection) => {
      const current = projection.currentPosition < 0 ? 0 : projection.currentPosition;
      const lag = globalPos > current ? globalPos - current : 0;
      return lag;
    });
    const maxLag = lags.length ? Math.max(...lags) : 0;
    if (phase === 'appending') {
      const elapsed = (performance.now() - startTime) / 1000;
      const effectiveElapsed = Math.max(1, elapsed - config.rampUpSeconds);
      const eventsPerSec = eventsWritten / effectiveElapsed;
      const opsPerSec = appendOps / effectiveElapsed;
      const profileSuffix =
        config.profile && totalReadSamples.length > 0
          ? ` | h ${avg(headerSamples).toFixed(2)}ms p ${avg(payloadSamples).toFixed(2)}ms d ${avg(decompressSamples).toFixed(2)}ms m ${avg(decodeSamples).toFixed(2)}ms`
          : '';
      process.stdout.write(
        `\rProgress: ${eventsWritten.toLocaleString()} events | ${eventsPerSec.toFixed(0)} evt/s | ${opsPerSec.toFixed(0)} appends/s | max lag: ${maxLag} | ${Math.floor(elapsed)}s${profileSuffix}`
      );
      return;
    }

    const catchUpElapsed = catchUpStart > 0 ? (performance.now() - catchUpStart) / 1000 : 0;
    const profileSuffix =
      config.profile && totalReadSamples.length > 0
        ? ` | h ${avg(headerSamples).toFixed(2)}ms p ${avg(payloadSamples).toFixed(2)}ms d ${avg(decompressSamples).toFixed(2)}ms m ${avg(decodeSamples).toFixed(2)}ms`
        : '';
    process.stdout.write(
      `\rCatch-up: max lag: ${maxLag} | ${Math.floor(catchUpElapsed)}s${profileSuffix}`
    );
  }, 1000);

  const debugInterval = config.debug
    ? setInterval(() => {
        const status = db.getProjectionStatus();
        const globalPos = status.globalPosition;
        const now = new Date().toISOString();
        console.log(`\n[debug ${now}] globalPosition=${globalPos.toString()}`);
        for (const projection of status.projections) {
          console.log(
            `[debug ${now}] ${projection.name} pos=${projection.currentPosition.toString()} processed=${projection.eventsProcessed} errors=${projection.errorsCount} lastCheckpoint=${projection.lastCheckpointPosition.toString()}`
          );
        }
      }, 5000)
    : null;

  const estimateEventSize = (event: InputEvent): number => {
    const cached = eventSizes.get(event.type);
    if (cached !== undefined) {
      return cached;
    }
    const size = serializer.encode(event).byteLength;
    eventSizes.set(event.type, size);
    return size;
  };

  async function worker(workerId: number): Promise<void> {
    const rampDelay = (workerId / config.concurrency) * config.rampUpSeconds * 1000;
    if (rampDelay > 0) {
      await new Promise((resolve) => setTimeout(resolve, rampDelay));
    }

    while (running && performance.now() < endTime) {
      const useUserStream = rand.float() < config.userEventRatio;
      const tenantId = tenants[rand.int(tenants.length)];
      const streamUserId = userIds[rand.int(userIds.length)]!;
      const streamOrderId = orderIds[rand.int(orderIds.length)]!;
      const streamId = useUserStream
        ? `user-${streamUserId}`
        : `order-${streamOrderId}`;
      const events: InputEvent[] = [];
      let estimatedBytes = 0;

      for (let i = 0; i < config.eventsPerAppend; i += 1) {
        const event = useUserStream
          ? createUserEvent(rand, streamUserId, padding)
          : createOrderEvent(rand, streamOrderId, streamUserId, padding);
        estimatedBytes += estimateEventSize(event);
        events.push(event);
      }

      try {
        const opStart = performance.now();
        const appendOptions = tenantId ? { tenantId } : undefined;
        await db.append(streamId, events, appendOptions);
        const opEnd = performance.now();
        latencySampler.add(opEnd - opStart);
        eventsWritten += events.length;
        appendOps += 1;
        bytesWritten += estimatedBytes;
      } catch (error) {
        errors += 1;
        if (errors < 5) {
          console.error('\nAppend failed:', error);
        }
      }
    }
  }

  const workers = Array.from({ length: config.concurrency }, (_, i) => worker(i));
  await Promise.all(workers);
  running = false;

  const elapsedMs = performance.now() - startTime;
  const elapsedSec = elapsedMs / 1000;
  const effectiveElapsedSec = Math.max(1, elapsedSec - config.rampUpSeconds);
  const latency = latencySampler.summary();

  phase = 'catchup';
  catchUpStart = performance.now();
  console.log('\n\nAppend phase complete. Waiting for projections to catch up...');

  let catchUpError: unknown = null;
  let catchUpMs = 0;

  try {
    await db.flush();
    await db.waitForProjections(config.projectionWaitMs);
  } catch (error) {
    catchUpError = error;
  } finally {
    catchUpMs = performance.now() - catchUpStart;
    clearInterval(progressInterval);
    if (debugInterval) {
      clearInterval(debugInterval);
    }
  }

  const revenue = db.getProjection<TotalRevenueProjection>('TotalRevenue');
  const users = db.getProjection<UserDirectoryProjection>('UserDirectory');
  const status = db.getProjectionStatus();

  console.log('\n\n=== SpiteDB Load Test Results ===');
  console.log(`Data dir: ${dataDir}`);
  console.log(`Seed: ${seed}`);
  console.log(`Duration: ${config.durationSeconds}s (+${config.rampUpSeconds}s ramp-up)`);
  console.log(`Concurrency: ${config.concurrency}`);
  console.log(`Events per append: ${config.eventsPerAppend}`);
  console.log(`Streams: ${totalStreams} (users: ${config.userPool}, orders: ${config.orderPool})`);
  console.log(`Events written: ${eventsWritten.toLocaleString()}`);
  console.log(`Append ops: ${appendOps.toLocaleString()}`);
  console.log(`Errors: ${errors}`);
  console.log(`Events/sec: ${(eventsWritten / effectiveElapsedSec).toFixed(0)}`);
  console.log(`MB/sec (approx): ${(bytesWritten / effectiveElapsedSec / 1024 / 1024).toFixed(2)}`);
  console.log(
    `Append latency ms: avg ${latency.avg.toFixed(2)} | p50 ${latency.p50.toFixed(2)} | p95 ${latency.p95.toFixed(2)} | p99 ${latency.p99.toFixed(2)} | max ${latency.max.toFixed(2)} | samples ${latency.samples}`
  );
  console.log(`Projection catch-up: ${catchUpMs.toFixed(0)}ms`);
  console.log(`Projection status: ${status.projections.length} running`);
  console.log(`TotalRevenue: ${revenue?.getTotal().toFixed(2) ?? 'n/a'}`);
  console.log(`UserDirectory: ${users?.getCount() ?? 'n/a'} users`);

  if (catchUpError) {
    console.error('Projection catch-up failed:', catchUpError);
    process.exitCode = 1;
  }

  await db.close();

  if (!config.keepData) {
    await rm(dataDir, { recursive: true, force: true });
  } else {
    console.log('Retained data directory for inspection.');
  }
}

run().catch((error) => {
  console.error('Load test failed:', error);
  process.exitCode = 1;
});
