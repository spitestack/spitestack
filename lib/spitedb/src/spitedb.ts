/**
 * SpiteDB - The unified public API for the spite event sourcing database.
 *
 * SpiteDB is designed for joy. It combines event storage with built-in
 * projections, providing a delightful developer experience for building
 * event-sourced applications.
 *
 * @example
 * ```ts
 * // Open a database (creates directory if needed)
 * const db = await SpiteDB.open('./data/myapp');
 *
 * // Register projections
 * db.registerProjection(TotalRevenueRegistration);
 * db.registerProjection(UserProfilesRegistration);
 *
 * // Start projections (begins processing events)
 * await db.startProjections();
 *
 * // Append events
 * await db.append('user-123', [
 *   { type: 'UserCreated', data: { name: 'Alice' } }
 * ]);
 *
 * // Wait for projections to catch up
 * await db.waitForProjections();
 *
 * // Query a projection
 * const profiles = db.getProjection<UserProfiles>('UserProfiles');
 * const user = profiles?.getById('user-123');
 *
 * // Clean shutdown
 * await db.close();
 * ```
 *
 * @module spitedb
 */

import {
  EventStore,
  type EventStoreConfig,
  type InputEvent,
  type AppendOptions,
  type AppendResult,
  type StreamAppend,
  type BatchAppendResult,
  type ReadStreamOptions,
  type ReadGlobalOptions,
} from './application/event-store';
import type { StoredEvent } from './domain/events/stored-event';
import {
  ProjectionCoordinator,
  type ProjectionCoordinatorConfig,
  type ProjectionCoordinatorStatus,
} from './application/projections';
import type {
  ProjectionRegistration,
  ProjectionRuntimeOptions,
} from './ports/projections';
import { BunFileSystem } from './infrastructure/filesystem/bun-filesystem';
import { BunClock } from './infrastructure/time/bun-clock';
import { MsgpackSerializer } from './infrastructure/serialization/msgpack-serializer';
import { BinaryEventBatchSerializer } from './infrastructure/serialization/binary-event-batch-serializer';
import { NoopCompressor } from './infrastructure/serialization/noop-compressor';
import type { Serializer } from './ports/serialization/serializer';
import type { Compressor } from './ports/serialization/compressor';
import {
  SpiteDBNotOpenError,
  ProjectionsNotStartedError,
  ProjectionBackpressureError,
  ProjectionBackpressureTimeoutError,
} from './errors';

/**
 * Configuration options for SpiteDB.
 *
 * All options have sensible defaults. Only override when you need to.
 */
export interface SpiteDBOptions {
  /**
   * Maximum segment size in bytes.
   * Default: 128MB
   * Larger segments = fewer files, but slower recovery.
   */
  maxSegmentSize?: number;

  /**
   * Number of segment indexes to cache.
   * Default: 10
   * Higher = more memory, faster random reads.
   */
  indexCacheSize?: number;

  /**
   * Auto-flush after this many events.
   * Default: 1000
   * Set to 0 to disable auto-flush (manual flush only).
   */
  autoFlushCount?: number;

  /**
   * Polling interval for projections in milliseconds.
   * Default: 100
   * Lower = more responsive, higher CPU.
   */
  projectionPollingIntervalMs?: number;

  /**
   * Checkpoint interval for projections in milliseconds.
   * Default: 5000
   * Lower = less data loss on crash, more disk IO.
   */
  projectionCheckpointIntervalMs?: number;

  /**
   * Batch size when reading events for projections.
   * Default: 100
   * Higher = better throughput, more memory.
   */
  projectionBatchSize?: number;

  /**
   * Use a shared global reader for projections.
   * Default: true
   * Disable only for debugging or specialized workloads.
   */
  projectionSharedReader?: boolean;

  /**
   * Serializer used for event storage.
   * Default: BinaryEventBatchSerializer
   */
  eventSerializer?: Serializer;

  /**
   * Serializer used for projection state.
   * Default: MsgpackSerializer
   */
  projectionSerializer?: Serializer;

  /**
   * Compressor used for event storage.
   * Default: NoopCompressor
   */
  compressor?: Compressor;

  /**
   * Optional profiler for batch reads.
   * Use only for diagnostics (bench/profiling).
   */
  readProfiler?: import('./infrastructure/storage/segments/segment-reader').ReadBatchProfiler;

  /**
   * Projection backpressure settings.
   * Default: enabled with maxLag 200_000, maxWaitMs 5000, pollIntervalMs 25, mode 'block'
   */
  projectionBackpressure?: ProjectionBackpressureOptions | false;
}

export interface ProjectionBackpressureOptions {
  /**
   * Maximum allowed lag (global position - slowest projection position).
   */
  maxLag?: number;
  /**
   * Max time to wait in blocking mode before failing.
   */
  maxWaitMs?: number;
  /**
   * Poll interval while waiting for lag to drop.
   */
  pollIntervalMs?: number;
  /**
   * Backpressure behavior.
   * - block: wait until lag drops, then append
   * - fail: throw immediately when lag exceeds max
   */
  mode?: 'block' | 'fail';
}

/**
 * SpiteDB - The unified public API for event sourcing.
 *
 * Use the static `open()` method to create an instance.
 */
export class SpiteDB {
  private readonly eventStore: EventStore;
  private readonly coordinator: ProjectionCoordinator;
  private readonly dataDir: string;
  private projectionsStarted = false;
  private readonly backpressure: Required<ProjectionBackpressureOptions> | undefined;

  private constructor(
    eventStore: EventStore,
    coordinator: ProjectionCoordinator,
    dataDir: string,
    backpressure?: Required<ProjectionBackpressureOptions>
  ) {
    this.eventStore = eventStore;
    this.coordinator = coordinator;
    this.dataDir = dataDir;
    this.backpressure = backpressure;
  }

  // ============================================================
  // Lifecycle
  // ============================================================

  /**
   * Open a SpiteDB instance.
   *
   * Creates the data directory if it doesn't exist.
   * Uses sensible defaults for all runtime components.
   *
   * @param path - Directory to store database files
   * @param options - Optional configuration overrides
   * @returns Ready-to-use SpiteDB instance
   *
   * @example
   * ```ts
   * const db = await SpiteDB.open('./data/myapp');
   * ```
   */
  static async open(path: string, options: SpiteDBOptions = {}): Promise<SpiteDB> {
    const fs = new BunFileSystem();
    const clock = new BunClock();
    const eventSerializer = options.eventSerializer ?? new BinaryEventBatchSerializer();
    const projectionSerializer = options.projectionSerializer ?? new MsgpackSerializer();
    const compressor = options.compressor ?? new NoopCompressor();

    // Build event store config (only include defined options)
    const eventStoreConfig: EventStoreConfig = {
      fs,
      clock,
      serializer: eventSerializer,
      compressor,
    };
    if (options.maxSegmentSize !== undefined) {
      eventStoreConfig.maxSegmentSize = options.maxSegmentSize;
    }
    if (options.indexCacheSize !== undefined) {
      eventStoreConfig.indexCacheSize = options.indexCacheSize;
    }
    if (options.autoFlushCount !== undefined) {
      eventStoreConfig.autoFlushCount = options.autoFlushCount;
    }
    if (options.readProfiler !== undefined) {
      eventStoreConfig.readProfiler = options.readProfiler;
    }

    // Create event store
    const eventStore = new EventStore(eventStoreConfig);

    // Open event store (events subdirectory)
    const eventsDir = `${path}/events`;
    await eventStore.open(eventsDir);

    // Build projection coordinator config (only include defined options)
    const coordinatorConfig: ProjectionCoordinatorConfig = {
      eventStore,
      fs,
      serializer: projectionSerializer,
      clock,
      dataDir: `${path}/projections`,
    };
    if (options.projectionPollingIntervalMs !== undefined) {
      coordinatorConfig.pollingIntervalMs = options.projectionPollingIntervalMs;
    }
    if (options.projectionCheckpointIntervalMs !== undefined) {
      coordinatorConfig.defaultCheckpointIntervalMs = options.projectionCheckpointIntervalMs;
    }
    if (options.projectionBatchSize !== undefined) {
      coordinatorConfig.defaultBatchSize = options.projectionBatchSize;
    }
    if (options.projectionSharedReader !== undefined) {
      coordinatorConfig.sharedReader = options.projectionSharedReader;
    }

    // Create projection coordinator
    const coordinator = new ProjectionCoordinator(coordinatorConfig);
    const backpressure = resolveBackpressure(options.projectionBackpressure);

    return new SpiteDB(eventStore, coordinator, path, backpressure);
  }

  /**
   * Close the database.
   *
   * Stops projections, flushes events, and releases all resources.
   * Safe to call multiple times.
   *
   * @example
   * ```ts
   * await db.close();
   * ```
   */
  async close(): Promise<void> {
    if (!this.eventStore.isOpen()) {
      return; // Already closed
    }

    // Stop projections first (they depend on event store)
    await this.stopProjections();

    // Then close event store
    await this.eventStore.close();
  }

  /**
   * Check if the database is open.
   */
  isOpen(): boolean {
    return this.eventStore.isOpen();
  }

  /**
   * Get the data directory path.
   */
  getDataDir(): string {
    return this.dataDir;
  }

  // ============================================================
  // Events
  // ============================================================

  /**
   * Append events to a stream.
   *
   * @param streamId - Stream to append to
   * @param events - Events to append
   * @param options - Append options (expectedRevision, tenantId)
   * @returns Append result with new revision and global position
   * @throws {ConcurrencyError} if expectedRevision doesn't match
   *
   * @example
   * ```ts
   * const result = await db.append('user-123', [
   *   { type: 'UserCreated', data: { name: 'Alice', email: 'alice@example.com' } }
   * ]);
   * console.log(`New revision: ${result.streamRevision}`);
   * ```
   */
  async append(
    streamId: string,
    events: InputEvent[],
    options?: AppendOptions
  ): Promise<AppendResult> {
    this.ensureOpen();
    await this.applyProjectionBackpressure();
    return this.eventStore.append(streamId, events, options);
  }


  /**
   * Append events to multiple streams atomically.
   *
   * This is the preferred method for sagas, process managers, and any scenario
   * requiring cross-aggregate consistency. All operations succeed or fail together.
   *
   * @example
   * ```ts
   * // Atomic append across order and inventory
   * await db.appendBatch([
   *   {
   *     streamId: 'order-123',
   *     events: [{ type: 'OrderCreated', data: { ... } }],
   *     expectedRevision: -1,  // Stream must not exist
   *   },
   *   {
   *     streamId: 'inventory-abc',
   *     events: [{ type: 'StockReserved', data: { ... } }],
   *     expectedRevision: 5,   // Must be at revision 5
   *   },
   * ]);
   * ```
   *
   * @param operations - Array of stream append operations
   * @returns Batch append result with per-stream revisions
   * @throws {ConcurrencyError} if any expectedRevision doesn't match (fail-fast)
   */
  async appendBatch(operations: StreamAppend[]): Promise<BatchAppendResult> {
    this.ensureOpen();
    await this.applyProjectionBackpressure();
    return this.eventStore.appendBatch(operations);
  }

  /**
   * Read events from a stream.
   *
   * @param streamId - Stream to read
   * @param options - Read options (fromRevision, toRevision, maxCount, direction)
   * @returns Array of events
   *
   * @example
   * ```ts
   * // Read all events
   * const events = await db.readStream('user-123');
   *
   * // Read from revision 5 onwards
   * const recent = await db.readStream('user-123', { fromRevision: 5 });
   *
   * // Read last 10 events
   * const last10 = await db.readStream('user-123', { direction: 'backward', maxCount: 10 });
   * ```
   */
  async readStream(
    streamId: string,
    options?: ReadStreamOptions
  ): Promise<StoredEvent[]> {
    this.ensureOpen();
    return this.eventStore.readStream(streamId, options);
  }

  /**
   * Read events from the global log.
   *
   * @param fromPosition - Start position (inclusive, default: 0)
   * @param options - Read options (maxCount)
   * @returns Array of events in global order
   *
   * @example
   * ```ts
   * // Read from beginning
   * const events = await db.readGlobal();
   *
   * // Read from position 1000
   * const recent = await db.readGlobal(1000, { maxCount: 100 });
   * ```
   */
  async readGlobal(
    fromPosition = 0,
    options?: ReadGlobalOptions
  ): Promise<StoredEvent[]> {
    this.ensureOpen();
    return this.eventStore.readGlobal(fromPosition, options);
  }

  /**
   * Stream events from the global log.
   *
   * Use this for processing large event sets without loading all into memory.
   *
   * @param fromPosition - Start position (inclusive, default: 0)
   * @yields Events in global order
   *
   * @example
   * ```ts
   * for await (const event of db.streamGlobal()) {
   *   console.log(event.type, event.streamId);
   * }
   * ```
   */
  async *streamGlobal(fromPosition = 0): AsyncGenerator<StoredEvent> {
    this.ensureOpen();
    yield* this.eventStore.streamGlobal(fromPosition);
  }

  /**
   * Get the current revision for a stream.
   *
   * @param streamId - Stream to query
   * @returns Current revision, or -1 if stream doesn't exist
   *
   * @example
   * ```ts
   * const revision = db.getStreamRevision('user-123');
   * if (revision === -1) {
   *   console.log('Stream does not exist');
   * }
   * ```
   */
  getStreamRevision(streamId: string): number {
    this.ensureOpen();
    return this.eventStore.getStreamRevision(streamId);
  }

  /**
   * Check if a stream exists.
   *
   * @param streamId - Stream to check
   * @returns true if stream has at least one event
   */
  hasStream(streamId: string): boolean {
    this.ensureOpen();
    return this.eventStore.hasStream(streamId);
  }

  /**
   * Get all stream IDs in the store.
   *
   * Note: This scans all indexes and may be slow for large stores.
   *
   * @returns Array of stream IDs (sorted)
   */
  async getStreamIds(): Promise<string[]> {
    this.ensureOpen();
    return this.eventStore.getStreamIds();
  }

  /**
   * Get the current global position.
   *
   * This is the position that will be assigned to the next event.
   */
  getGlobalPosition(): number {
    this.ensureOpen();
    return this.eventStore.getGlobalPosition();
  }

  /**
   * Flush pending events to disk.
   *
   * After flush returns, all previously appended events are durable.
   * This is called automatically based on autoFlushCount, but you can
   * call it manually for explicit durability guarantees.
   *
   * @example
   * ```ts
   * await db.append('stream', events);
   * await db.flush(); // Events are now durable
   * ```
   */
  async flush(): Promise<void> {
    this.ensureOpen();
    await this.eventStore.flush();
  }

  // ============================================================
  // Projections
  // ============================================================

  /**
   * Register a projection.
   *
   * Call this before startProjections(). Projections are typically
   * generated by the compiler from user projection classes.
   *
   * @param registration - Projection registration (from compiler)
   * @param options - Optional runtime options
   *
   * @example
   * ```ts
   * // Generated by compiler
   * db.registerProjection(UserProfilesRegistration);
   * db.registerProjection(TotalRevenueRegistration);
   * ```
   */
  registerProjection<TState>(
    registration: ProjectionRegistration<TState>,
    options?: ProjectionRuntimeOptions
  ): void {
    this.ensureOpen();
    this.coordinator.getRegistry().register(registration, options);
  }

  /**
   * Start projection processing.
   *
   * Call this after registering all projections.
   * Projections will begin catching up to the current event position.
   *
   * @example
   * ```ts
   * db.registerProjection(MyProjection);
   * await db.startProjections();
   * ```
   */
  async startProjections(): Promise<void> {
    this.ensureOpen();
    if (this.projectionsStarted) {
      return; // Idempotent
    }
    await this.coordinator.start();
    this.projectionsStarted = true;
  }

  /**
   * Stop projection processing.
   *
   * Gracefully stops all projections and persists checkpoints.
   * Called automatically by close().
   */
  async stopProjections(): Promise<void> {
    if (!this.projectionsStarted) {
      return;
    }
    await this.coordinator.stop();
    this.projectionsStarted = false;
  }

  /**
   * Get a projection by name.
   *
   * Returns undefined if the projection is not found or not ready.
   * Use requireProjection() if you expect it to exist.
   *
   * @param name - Projection name
   * @returns Projection instance or undefined
   *
   * @example
   * ```ts
   * const profiles = db.getProjection<UserProfiles>('UserProfiles');
   * if (profiles) {
   *   const user = profiles.getById('user-123');
   * }
   * ```
   */
  getProjection<T = unknown>(name: string): T | undefined {
    this.ensureOpen();
    return this.coordinator.getProjection<T>(name);
  }

  /**
   * Get a projection by name, throwing if not found.
   *
   * Use this when you know the projection should exist.
   *
   * @param name - Projection name
   * @returns Projection instance
   * @throws {ProjectionsNotStartedError} if projections not started
   * @throws {ProjectionNotFoundError} if projection not found
   *
   * @example
   * ```ts
   * const profiles = db.requireProjection<UserProfiles>('UserProfiles');
   * const user = profiles.getById('user-123');
   * ```
   */
  requireProjection<T = unknown>(name: string): T {
    this.ensureOpen();
    if (!this.projectionsStarted) {
      throw new ProjectionsNotStartedError();
    }
    return this.coordinator.requireProjection<T>(name);
  }

  /**
   * Wait for all projections to catch up to current position.
   *
   * Useful after appending events to ensure projections are ready to query.
   *
   * @param timeoutMs - Maximum wait time (default: 30000)
   * @throws {ProjectionsNotStartedError} if projections not started
   * @throws {ProjectionCatchUpTimeoutError} if timeout exceeded
   *
   * @example
   * ```ts
   * await db.append('user-123', [{ type: 'UserUpdated', data: { name: 'Bob' } }]);
   * await db.waitForProjections();
   * // Now safe to query projections
   * ```
   */
  async waitForProjections(timeoutMs: number = 30000): Promise<void> {
    this.ensureOpen();
    if (!this.projectionsStarted) {
      throw new ProjectionsNotStartedError();
    }
    await this.coordinator.waitForCatchUp(timeoutMs);
  }

  /**
   * Force an immediate checkpoint of all projections.
   *
   * Normally checkpoints happen automatically. Use this for explicit
   * checkpoint control (e.g., before maintenance).
   *
   * @throws {ProjectionsNotStartedError} if projections not started
   */
  async forceProjectionCheckpoint(): Promise<void> {
    this.ensureOpen();
    if (!this.projectionsStarted) {
      throw new ProjectionsNotStartedError();
    }
    await this.coordinator.forceCheckpoint();
  }

  /**
   * Get the status of all projections.
   *
   * Returns information about each projection's state, position, and health.
   */
  getProjectionStatus(): ProjectionCoordinatorStatus {
    this.ensureOpen();
    return this.coordinator.getStatus();
  }

  /**
   * Check if projections are currently running.
   */
  projectionsRunning(): boolean {
    return this.projectionsStarted;
  }

  // ============================================================
  // Private helpers
  // ============================================================

  private ensureOpen(): void {
    if (!this.eventStore.isOpen()) {
      throw new SpiteDBNotOpenError();
    }
  }

  private async applyProjectionBackpressure(): Promise<void> {
    if (!this.backpressure || !this.projectionsStarted) {
      return;
    }

    const {
      maxLag,
      maxWaitMs,
      pollIntervalMs,
      mode,
    } = this.backpressure;

    const start = Date.now();

    while (true) {
      const status = this.coordinator.getStatus();
      if (status.projections.length === 0) {
        return;
      }

      let slowest = status.projections[0]!;
      for (const projection of status.projections) {
        if (projection.currentPosition < slowest.currentPosition) {
          slowest = projection;
        }
      }

      const current = slowest.currentPosition < 0 ? 0 : slowest.currentPosition;
      const lag = status.globalPosition > current ? status.globalPosition - current : 0;

      if (lag <= maxLag) {
        return;
      }

      if (mode === 'fail') {
        throw new ProjectionBackpressureError(slowest.name, lag, maxLag);
      }

      const waitedMs = Date.now() - start;
      if (waitedMs >= maxWaitMs) {
        throw new ProjectionBackpressureTimeoutError(
          slowest.name,
          lag,
          maxLag,
          waitedMs,
          maxWaitMs
        );
      }

      await new Promise<void>((resolve) => {
        setTimeout(resolve, pollIntervalMs);
      });
    }
  }
}

function resolveBackpressure(
  options: ProjectionBackpressureOptions | false | undefined
): Required<ProjectionBackpressureOptions> | undefined {
  if (options === false) {
    return undefined;
  }

  return {
    maxLag: options?.maxLag ?? 200_000,
    maxWaitMs: options?.maxWaitMs ?? 5000,
    pollIntervalMs: options?.pollIntervalMs ?? 25,
    mode: options?.mode ?? 'block',
  };
}
