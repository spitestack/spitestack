/**
 * EventStore - the main facade for storing and reading events.
 *
 * Provides a high-level API for:
 * - Appending events to streams with optimistic concurrency
 * - Reading events by stream or global position
 * - Automatic batching and flushing
 *
 * @example
 * ```ts
 * const store = new EventStore({
 *   fs: new BunFileSystem(),
 *   serializer: new MsgpackSerializer(),
 *   compressor: new ZstdCompressor(),
 *   clock: new BunClock(),
 * });
 *
 * await store.open('/data/events');
 *
 * // Append events
 * const result = await store.append('user-123', [
 *   { type: 'UserCreated', data: { name: 'Alice' } },
 *   { type: 'EmailVerified', data: {} },
 * ]);
 *
 * // Read stream
 * const events = await store.readStream('user-123');
 *
 * // Read global (all streams)
 * for await (const event of store.streamGlobal(0)) {
 *   console.log(event.type);
 * }
 *
 * await store.close();
 * ```
 */

import type { FileSystem, FileHandle } from '../../ports/storage/filesystem';
import type { Serializer } from '../../ports/serialization/serializer';
import type { Compressor } from '../../ports/serialization/compressor';
import type { Clock } from '../../ports/time/clock';
import { SegmentManager } from '../../infrastructure/storage/segments/segment-manager';
import { SEGMENT_HEADER_SIZE } from '../../infrastructure/storage/segments/segment-header';
import type { StoredEvent } from '../../domain/events/stored-event';
import { ConcurrencyError, StoreFatalError } from '../../domain/errors';
import { increaseFileDescriptorLimit } from '../../infrastructure/resource-limits';

/**
 * Input event for appending (without position info).
 */
export interface InputEvent {
  /** Event type name */
  type: string;
  /** Event data payload */
  data: unknown;
  /** Optional event metadata */
  metadata?: unknown;
}

/**
 * Options for appending events.
 */
export interface AppendOptions {
  /**
   * Expected stream revision (for optimistic concurrency).
   * - undefined: No check (append unconditionally)
   * - -1: Stream must not exist
   * - >= 0: Stream must be at this revision
   */
  expectedRevision?: number;
  /** Tenant ID for multi-tenancy (default: 'default') */
  tenantId?: string;
}

/**
 * Result of appending events.
 */
export interface AppendResult {
  /** New stream revision after append */
  streamRevision: number;
  /** Global position of first event in batch */
  globalPosition: number;
  /** Number of events appended */
  eventCount: number;
}

/**
 * Single stream operation for batch append.
 */
export interface StreamAppend {
  /** Stream to append to */
  streamId: string;
  /** Events to append */
  events: InputEvent[];
  /** Expected stream revision (for optimistic concurrency) */
  expectedRevision?: number;
  /** Tenant ID for multi-tenancy (default: 'default') */
  tenantId?: string;
}

/**
 * Result of batch append across multiple streams.
 */
export interface BatchAppendResult {
  /** Per-stream results */
  streams: Map<string, { streamRevision: number; eventCount: number }>;
  /** Global position of first event in batch */
  globalPosition: number;
  /** Total events appended across all streams */
  totalEvents: number;
}

/**
 * Options for reading streams.
 */
export interface ReadStreamOptions {
  /** Start reading from this revision (inclusive, default: 0) */
  fromRevision?: number;
  /** Stop reading at this revision (inclusive) */
  toRevision?: number;
  /** Maximum number of events to return */
  maxCount?: number;
  /** Read direction (default: 'forward') */
  direction?: 'forward' | 'backward';
}

/**
 * Options for reading global log.
 */
export interface ReadGlobalOptions {
  /** Maximum number of events to return */
  maxCount?: number;
}

/**
 * EventStore configuration.
 */
export interface EventStoreConfig {
  /** Filesystem implementation */
  fs: FileSystem;
  /** Serializer implementation */
  serializer: Serializer;
  /** Compressor implementation */
  compressor: Compressor;
  /** Clock implementation */
  clock: Clock;
  /** Maximum segment size in bytes (default: 128MB) */
  maxSegmentSize?: number;
  /** Number of segment indexes to cache (default: 10) */
  indexCacheSize?: number;
  /** Auto-flush after this many events (default: 1000, 0 = disabled) */
  autoFlushCount?: number;
  /** Optional profiler for batch reads */
  readProfiler?: import('../../infrastructure/storage/segments/segment-reader').ReadBatchProfiler;
}

const DEFAULT_AUTO_FLUSH_COUNT = 1000;
const DEFAULT_MAX_CACHED_EVENTS = 100000;

/**
 * Main EventStore class.
 */
export class EventStore {
  private readonly config: Required<
    Omit<EventStoreConfig, 'maxSegmentSize' | 'indexCacheSize' | 'readProfiler'>
  > & {
    maxSegmentSize?: number;
    indexCacheSize?: number;
    readProfiler?: import('../../infrastructure/storage/segments/segment-reader').ReadBatchProfiler;
  };
  private segmentManager: SegmentManager | null = null;
  private pendingEvents: StoredEvent[] = [];
  private streamRevisions = new Map<string, number>();
  private dataDir: string = '';
  private lockHandle: FileHandle | null = null;
  private lastFlushedGlobalPosition = -1;
  private writeQueue: Promise<void> = Promise.resolve();
  private failed = false;
  private failedError: Error | null = null;

  /**
   * Cache of recently flushed events for fast projection reads.
   * Only contains events that are durable on disk.
   */
  private recentlyFlushedEvents = new Map<number, StoredEvent>();
  private readonly maxCachedEvents = DEFAULT_MAX_CACHED_EVENTS;

  constructor(config: EventStoreConfig) {
    this.config = {
      ...config,
      autoFlushCount: config.autoFlushCount ?? DEFAULT_AUTO_FLUSH_COUNT,
    };
  }

  /**
   * Open the event store.
   *
   * Initializes the segment manager and scans existing segments.
   * Automatically increases file descriptor limits and acquires
   * an exclusive lock to prevent multi-process corruption.
   *
   * @param dataDir - Directory to store segment files
   * @throws {Error} if another process has the store open
   */
  async open(dataDir: string): Promise<void> {
    if (this.segmentManager) {
      throw new Error('EventStore already open');
    }

    // Auto-configure file descriptor limits (convention over configuration)
    const fdResult = increaseFileDescriptorLimit();
    if (fdResult.warning) {
      console.warn(`[spitedb] ${fdResult.warning}`);
    }

    this.dataDir = dataDir;

    // Ensure data directory exists
    if (!(await this.config.fs.exists(dataDir))) {
      await this.config.fs.mkdir(dataDir, { recursive: true });
    }

    // Acquire exclusive lock to prevent multiple writers
    const lockPath = `${dataDir}/.lock`;
    const disableFlock = process.env['SPITEDB_DISABLE_FLOCK'] === '1'
      || process.env['SPITEDB_DISABLE_FLOCK'] === 'true';
    if (disableFlock) {
      console.warn('[spitedb] File locking disabled (SPITEDB_DISABLE_FLOCK=1).');
    } else {
      try {
        this.lockHandle = await this.config.fs.open(lockPath, 'write');
        await this.config.fs.flock(this.lockHandle, 'exclusive');
      } catch (error) {
        // Clean up if we opened the file but couldn't lock
        if (this.lockHandle) {
          try {
            await this.config.fs.close(this.lockHandle);
          } catch {
            // Ignore close errors
          }
          this.lockHandle = null;
        }
        throw new Error(
          `Failed to open EventStore: another process may have it open. ` +
            `Lock file: ${lockPath}. Original error: ${error instanceof Error ? error.message : error}`
        );
      }
    }

    try {
      const segmentConfig: import('../../infrastructure/storage/segments/segment-manager').SegmentManagerConfig = {
        dataDir,
        maxSegmentSize: this.config.maxSegmentSize,
        indexCacheSize: this.config.indexCacheSize,
      };
      if (this.config.readProfiler) {
        segmentConfig.readProfiler = this.config.readProfiler;
      }
      this.segmentManager = new SegmentManager(
        this.config.fs,
        this.config.serializer,
        this.config.compressor,
        segmentConfig
      );

      await this.segmentManager.initialize();

      // Rebuild stream revisions from existing data
      await this.rebuildStreamRevisions();

      // Track last durable position for safe reads without implicit flushes.
      const currentGlobal = this.segmentManager.getGlobalPosition();
      this.lastFlushedGlobalPosition = currentGlobal > 0 ? currentGlobal - 1 : -1;
    } catch (error) {
      // Release lock on initialization failure to allow retry
      if (this.lockHandle) {
        try {
          await this.config.fs.close(this.lockHandle);
        } catch {
          // Ignore close errors during cleanup
        }
        this.lockHandle = null;
      }
      this.segmentManager = null;
      throw error;
    }
  }

  /**
   * Close the event store.
   *
   * Flushes pending events (unless in failed state), closes the segment manager,
   * and releases the lock.
   */
  async close(): Promise<void> {
    if (!this.segmentManager) {
      return;
    }

    // Flush any pending events (skip if in failed state - data is already lost)
    if (!this.failed) {
      await this.flush();
    }

    // Reset failed state for potential reopening
    this.failed = false;
    this.failedError = null;

    await this.segmentManager.close();
    this.segmentManager = null;
    this.streamRevisions.clear();
    this.pendingEvents = [];
    this.recentlyFlushedEvents.clear();
    this.lastFlushedGlobalPosition = -1;

    // Release the lock (closing the handle automatically releases flock)
    if (this.lockHandle) {
      await this.config.fs.close(this.lockHandle);
      this.lockHandle = null;
    }
  }

  /**
   * Append events to a stream.
   *
   * @param streamId - Stream to append to
   * @param events - Events to append
   * @param options - Append options
   * @returns Append result
   * @throws {ConcurrencyError} if expectedRevision doesn't match
   */
  async append(
    streamId: string,
    events: InputEvent[],
    options: AppendOptions = {}
  ): Promise<AppendResult> {
    return this.enqueueWrite(async () => {
      this.ensureOpen();

      if (events.length === 0) {
        throw new Error('Cannot append empty event list');
      }

      const tenantId = options.tenantId ?? 'default';
      const currentRevision = this.getStreamRevision(streamId);

      // Check expected revision
      if (options.expectedRevision !== undefined) {
        if (options.expectedRevision === -1) {
          // Stream must not exist
          if (currentRevision >= 0) {
            throw new ConcurrencyError(
              streamId,
              options.expectedRevision,
              currentRevision
            );
          }
        } else if (options.expectedRevision !== currentRevision) {
          throw new ConcurrencyError(
            streamId,
            options.expectedRevision,
            currentRevision
          );
        }
      }

      const timestamp = this.config.clock.now();
      const firstGlobalPosition = this.segmentManager!.getNextGlobalPosition();

      // Create stored events
      let revision = currentRevision + 1;
      const storedEvents: StoredEvent[] = [];

      for (const event of events) {
        storedEvents.push({
          streamId,
          type: event.type,
          data: event.data,
          metadata: event.metadata,
          revision,
          globalPosition: this.segmentManager!.allocateGlobalPosition(),
          timestamp,
          tenantId,
        });
        revision++;
      }

      // Add to pending batch
      this.pendingEvents.push(...storedEvents);

      // Update stream revision
      this.streamRevisions.set(streamId, revision - 1);

      // Auto-flush if needed
      if (
        this.config.autoFlushCount > 0 &&
        this.pendingEvents.length >= this.config.autoFlushCount
      ) {
        await this.flushInternal();
      }

      return {
        streamRevision: revision - 1,
        globalPosition: firstGlobalPosition,
        eventCount: events.length,
      };
    });
  }


  /**
   * Append events to multiple streams atomically.
   *
   * This is the preferred method for sagas, process managers, and any scenario
   * requiring cross-aggregate consistency. All operations succeed or fail together.
   *
   * @param operations - Array of stream append operations
   * @returns Batch append result with per-stream revisions
   * @throws {ConcurrencyError} if any expectedRevision doesn't match (fail-fast)
   */
  async appendBatch(operations: StreamAppend[]): Promise<BatchAppendResult> {
    return this.enqueueWrite(async () => {
      this.ensureOpen();

      if (operations.length === 0) {
        return {
          streams: new Map(),
          globalPosition: this.segmentManager!.getNextGlobalPosition(),
          totalEvents: 0,
        };
      }

      // Phase 1: Validate ALL expected revisions (fail-fast)
      // This ensures we don't allocate positions or modify state if any check fails
      for (const op of operations) {
        if (op.events.length === 0) {
          throw new Error(`Cannot append empty event list for stream ${op.streamId}`);
        }

        if (op.expectedRevision !== undefined) {
          const current = this.getStreamRevision(op.streamId);
          if (op.expectedRevision === -1) {
            // Stream must not exist
            if (current >= 0) {
              throw new ConcurrencyError(op.streamId, -1, current);
            }
          } else if (op.expectedRevision !== current) {
            throw new ConcurrencyError(op.streamId, op.expectedRevision, current);
          }
        }
      }

      // Phase 2: Create all stored events atomically
      const timestamp = this.config.clock.now();
      const firstGlobalPosition = this.segmentManager!.getNextGlobalPosition();
      const storedEvents: StoredEvent[] = [];
      const results = new Map<string, { streamRevision: number; eventCount: number }>();

      for (const op of operations) {
        const tenantId = op.tenantId ?? 'default';
        let revision = this.getStreamRevision(op.streamId) + 1;

        for (const event of op.events) {
          storedEvents.push({
            streamId: op.streamId,
            type: event.type,
            data: event.data,
            metadata: event.metadata,
            revision,
            globalPosition: this.segmentManager!.allocateGlobalPosition(),
            timestamp,
            tenantId,
          });
          revision++;
        }

        // Update stream revision tracking
        this.streamRevisions.set(op.streamId, revision - 1);
        results.set(op.streamId, {
          streamRevision: revision - 1,
          eventCount: op.events.length,
        });
      }

      // Phase 3: Add all events to pending batch
      this.pendingEvents.push(...storedEvents);

      // Auto-flush if needed
      if (
        this.config.autoFlushCount > 0 &&
        this.pendingEvents.length >= this.config.autoFlushCount
      ) {
        await this.flushInternal();
      }

      return {
        streams: results,
        globalPosition: firstGlobalPosition,
        totalEvents: storedEvents.length,
      };
    });
  }

  /**
   * Flush pending events to disk.
   *
   * After flush returns, all previously appended events are durable.
   */
  async flush(): Promise<void> {
    await this.enqueueWrite(async () => {
      this.ensureOpen();
      await this.flushInternal();
    });
  }

  /**
   * Serialize write operations to avoid concurrent flush/append races.
   */
  private enqueueWrite<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.writeQueue.then(fn);
    this.writeQueue = run.then(() => undefined, () => undefined);
    return run;
  }

  /**
   * Flush implementation without queueing.
   *
   * This method is idempotent - if a previous flush attempt wrote the batch
   * but failed during sync, retrying will detect the already-written batch
   * and only perform the sync.
   *
   * CRITICAL: If sync fails after write, the store enters a "failed" state
   * to prevent data corruption from retry with new events. The store must
   * be closed and reopened to recover.
   */
  private async flushInternal(): Promise<void> {
    if (this.pendingEvents.length === 0) {
      return;
    }

    // writeBatch is idempotent - detects and skips duplicate writes
    const result = await this.segmentManager!.writeBatch(this.pendingEvents);

    // Always sync - even for retries, we need to ensure durability
    try {
      await this.segmentManager!.sync();
    } catch (error) {
      // CRITICAL: Mark store as failed to prevent data corruption.
      // If sync fails after write, we cannot safely retry because:
      // 1. The data may or may not be durable
      // 2. New events may have been appended to pendingEvents
      // 3. Retrying would write duplicate events
      this.failed = true;
      this.failedError = error instanceof Error ? error : new Error(String(error));
      throw new StoreFatalError(
        'Sync failed after write. Store is now in failed state and must be closed and reopened. ' +
          'Events may need to be re-appended after recovery.',
        this.failedError
      );
    }

    // Cache the flushed events for fast projection reads
    // This is idempotent - re-caching the same events is safe
    if (!result.alreadyWritten) {
      for (const event of this.pendingEvents) {
        this.recentlyFlushedEvents.set(event.globalPosition, event);
      }
      this.trimRecentCache();
    }

    this.lastFlushedGlobalPosition = this.pendingEvents[this.pendingEvents.length - 1]!
      .globalPosition;
    this.pendingEvents = [];
  }

  /**
   * Trim the recent events cache to stay under the maximum size.
   * Removes oldest events (lowest global positions) first.
   *
   * Optimization: Since events are flushed in globalPosition order and
   * JavaScript Maps maintain insertion order, we can delete the first N
   * entries without sorting. This is O(k) where k = entries to remove,
   * instead of O(n log n) for sorting all keys.
   */
  private trimRecentCache(): void {
    if (this.recentlyFlushedEvents.size <= this.maxCachedEvents) {
      return;
    }

    const toRemove = this.recentlyFlushedEvents.size - this.maxCachedEvents;
    let removed = 0;

    // Map.keys() iterates in insertion order, which equals position order
    // since we always add events with increasing globalPositions
    for (const key of this.recentlyFlushedEvents.keys()) {
      if (removed >= toRemove) break;
      this.recentlyFlushedEvents.delete(key);
      removed++;
    }
  }

  /**
   * Read events from the global log using the cache when possible.
   *
   * This is optimized for projection catch-up reads. It tries to serve
   * events from the in-memory cache first, falling back to disk on cache miss.
   *
   * @param fromPosition - Start position (inclusive)
   * @param maxCount - Maximum events to return
   * @returns Events if all were in cache, null on cache miss (caller should use readGlobal)
   */
  readGlobalCached(fromPosition: number, maxCount: number): StoredEvent[] | null {
    // Only serve durable events
    if (fromPosition > this.lastFlushedGlobalPosition) {
      return null;
    }

    const events: StoredEvent[] = [];
    for (let i = 0; i < maxCount; i++) {
      const pos = fromPosition + i;

      // Don't read beyond durable position
      if (pos > this.lastFlushedGlobalPosition) {
        break;
      }

      const cached = this.recentlyFlushedEvents.get(pos);
      if (!cached) {
        // Cache miss - caller should fall back to disk read
        return null;
      }

      events.push(cached);
    }

    return events;
  }

  /**
   * Read events from a stream.
   *
   * @param streamId - Stream to read
   * @param options - Read options
   * @returns Array of events
   */
  async readStream(
    streamId: string,
    options: ReadStreamOptions = {}
  ): Promise<StoredEvent[]> {
    this.ensureOpen();

    const events: StoredEvent[] = [];
    const maxCount = options.maxCount ?? Infinity;

    for await (const event of this.streamEvents(streamId, options)) {
      events.push(event);
      if (events.length >= maxCount) {
        break;
      }
    }

    return events;
  }

  /**
   * Stream events from a stream.
   *
   * Uses the StreamMap for O(1) routing to relevant segments,
   * then SegmentIndexFile for O(log n) lookups within segments.
   *
   * Optimized: Filters are pushed down to the index query to avoid
   * materializing events outside the requested revision range.
   *
   * @param streamId - Stream to read
   * @param options - Read options
   * @yields Events from the stream
   */
  async *streamEvents(
    streamId: string,
    options: ReadStreamOptions = {}
  ): AsyncGenerator<StoredEvent> {
    this.ensureOpen();

    const fromRevision = options.fromRevision ?? 0;
    const toRevision = options.toRevision;
    const direction = options.direction ?? 'forward';
    const maxCount = options.maxCount ?? Infinity;

    // Pass revision filters to durable read for efficient index-based filtering
    const durableEvents = await this.readDurableStreamEvents(
      streamId,
      fromRevision,
      toRevision
    );

    // Filter pending events by revision range
    const pendingEvents = this.pendingEvents.filter((event) => {
      if (event.streamId !== streamId) return false;
      if (event.revision < fromRevision) return false;
      if (toRevision !== undefined && event.revision > toRevision) return false;
      return true;
    });

    // Combine and sort - both arrays are already filtered
    const combined = [...durableEvents, ...pendingEvents];
    combined.sort((a, b) => a.revision - b.revision);

    if (direction === 'backward') {
      combined.reverse();
    }

    let yielded = 0;
    for (const event of combined) {
      if (yielded >= maxCount) {
        break;
      }
      yield event;
      yielded++;
    }
  }

  /**
   * Read events from the global log.
   *
   * @param fromPosition - Start position (inclusive, default: 0)
   * @param options - Read options
   * @returns Array of events
   */
  async readGlobal(
    fromPosition = 0,
    options: ReadGlobalOptions = {}
  ): Promise<StoredEvent[]> {
    this.ensureOpen();

    const events: StoredEvent[] = [];
    const maxCount = options.maxCount ?? Infinity;

    for await (const event of this.streamGlobal(fromPosition)) {
      events.push(event);
      if (events.length >= maxCount) {
        break;
      }
    }

    return events;
  }

  /**
   * Read only durable (flushed) events from the global log.
   *
   * This method is safe for projection checkpointing - it never returns
   * pending events that could be lost on crash.
   *
   * @param fromPosition - Start position (inclusive, default: 0)
   * @param options - Read options
   * @returns Array of durable events
   */
  async readGlobalDurable(
    fromPosition = 0,
    options: ReadGlobalOptions = {}
  ): Promise<StoredEvent[]> {
    this.ensureOpen();

    const events: StoredEvent[] = [];
    const maxCount = options.maxCount ?? Infinity;

    for await (const event of this.streamGlobalDurable(fromPosition)) {
      events.push(event);
      if (events.length >= maxCount) {
        break;
      }
    }

    return events;
  }

  /**
   * Stream only durable (flushed) events from the global log.
   *
   * This generator is safe for projection checkpointing - it never yields
   * pending events that could be lost on crash.
   *
   * @param fromPosition - Start position (inclusive, default: 0)
   * @yields Durable events in global order
   */
  async *streamGlobalDurable(fromPosition = 0): AsyncGenerator<StoredEvent> {
    this.ensureOpen();

    const maxDurablePosition = this.lastFlushedGlobalPosition;
    if (maxDurablePosition < 0 || maxDurablePosition < fromPosition) {
      return; // No durable events in requested range
    }

    let lastPosition = fromPosition > 0 ? fromPosition - 1 : -1;

    const reader = this.segmentManager!.getReader();
    const segments = this.segmentManager!.getSegments();

    let startIndex = 0;
    if (segments.length > 0 && fromPosition > segments[0]!.basePosition) {
      for (let i = 0; i < segments.length; i += 1) {
        const segment = segments[i]!;
        const next = segments[i + 1];
        if (fromPosition >= segment.basePosition && (!next || fromPosition < next.basePosition)) {
          startIndex = i;
          break;
        }
      }
    }

    for (let i = startIndex; i < segments.length; i += 1) {
      const segment = segments[i]!;
      if (segment.basePosition > maxDurablePosition) {
        break;
      }

      let startOffset = SEGMENT_HEADER_SIZE;
      if (i === startIndex && fromPosition > segment.basePosition) {
        try {
          const indexFile = await this.segmentManager!.getSegmentIndexFile(segment.id);
          const batchOffset = indexFile.findBatchOffsetForGlobalPosition(fromPosition);
          if (batchOffset !== null) {
            startOffset = batchOffset;
          }
        } catch {
          // Fall back to scanning from segment start if index is unavailable.
        }
      }

      for await (const batch of reader.readAllBatches(segment.path, startOffset)) {
        for (const event of batch) {
          if (event.globalPosition < fromPosition) {
            continue;
          }
          if (event.globalPosition > maxDurablePosition) {
            return; // Stop at durable boundary
          }
          if (event.globalPosition <= lastPosition) {
            return;
          }
          yield event;
          lastPosition = event.globalPosition;
        }
      }
    }
  }

  /**
   * Stream events from the global log.
   *
   * @param fromPosition - Start position (inclusive, default: 0)
   * @yields Events in global order
   */
  async *streamGlobal(fromPosition = 0): AsyncGenerator<StoredEvent> {
    this.ensureOpen();

    const maxDurablePosition = this.lastFlushedGlobalPosition;
    let lastPosition = fromPosition > 0 ? fromPosition - 1 : -1;
    const pendingSnapshot = this.pendingEvents.slice();

    if (maxDurablePosition >= 0 && maxDurablePosition >= fromPosition) {
      const reader = this.segmentManager!.getReader();
      const segments = this.segmentManager!.getSegments();

      let startIndex = 0;
      if (segments.length > 0 && fromPosition > segments[0]!.basePosition) {
        for (let i = 0; i < segments.length; i += 1) {
          const segment = segments[i]!;
          const next = segments[i + 1];
          if (fromPosition >= segment.basePosition && (!next || fromPosition < next.basePosition)) {
            startIndex = i;
            break;
          }
        }
      }

      readDurable: for (let i = startIndex; i < segments.length; i += 1) {
        const segment = segments[i]!;
        if (segment.basePosition > maxDurablePosition) {
          break;
        }

        let startOffset = SEGMENT_HEADER_SIZE;
        if (i === startIndex && fromPosition > segment.basePosition) {
          try {
            const indexFile = await this.segmentManager!.getSegmentIndexFile(segment.id);
            const batchOffset = indexFile.findBatchOffsetForGlobalPosition(fromPosition);
            if (batchOffset !== null) {
              startOffset = batchOffset;
            }
          } catch {
            // Fall back to scanning from segment start if index is unavailable.
          }
        }

        for await (const batch of reader.readAllBatches(segment.path, startOffset)) {
          for (const event of batch) {
            if (event.globalPosition < fromPosition) {
              continue;
            }
            if (event.globalPosition > maxDurablePosition) {
              break readDurable;
            }
            if (event.globalPosition <= lastPosition) {
              return;
            }
            yield event;
            lastPosition = event.globalPosition;
          }
        }
      }
    }

    if (pendingSnapshot.length > 0) {
      for (const event of pendingSnapshot) {
        if (event.globalPosition <= maxDurablePosition) {
          continue;
        }
        if (event.globalPosition >= fromPosition) {
          if (event.globalPosition <= lastPosition) {
            return;
          }
          yield event;
          lastPosition = event.globalPosition;
        }
      }
    }
  }

  /**
   * Get the current revision for a stream.
   *
   * @param streamId - Stream to query
   * @returns Current revision, or -1 if stream doesn't exist
   */
  getStreamRevision(streamId: string): number {
    // Check in-memory cache first
    const cached = this.streamRevisions.get(streamId);
    if (cached !== undefined) {
      return cached;
    }

    // Check pending events
    for (let i = this.pendingEvents.length - 1; i >= 0; i--) {
      if (this.pendingEvents[i]!.streamId === streamId) {
        return this.pendingEvents[i]!.revision;
      }
    }

    return -1;
  }

  /**
   * Check if a stream exists.
   *
   * @param streamId - Stream to check
   * @returns true if stream has at least one event
   */
  hasStream(streamId: string): boolean {
    return this.getStreamRevision(streamId) >= 0;
  }

  /**
   * Get all stream IDs in the store.
   *
   * Note: This scans all indexes and may be slow for large stores.
   */
  async getStreamIds(): Promise<string[]> {
    this.ensureOpen();
    const streamIds = new Set<string>(this.segmentManager!.getAllStreamIds());

    for (const event of this.pendingEvents) {
      streamIds.add(event.streamId);
    }

    return Array.from(streamIds).sort();
  }

  /**
   * Get the current global position (includes pending events).
   */
  getGlobalPosition(): number {
    this.ensureOpen();
    return this.segmentManager!.getGlobalPosition();
  }

  /**
   * Get the last durable (flushed) global position.
   *
   * This returns the position of the last event that has been successfully
   * written to disk. Use this for projection checkpointing to ensure
   * projections never checkpoint beyond durable events.
   *
   * @returns The last flushed position, or -1 if no events have been flushed
   */
  getDurableGlobalPosition(): number {
    this.ensureOpen();
    return this.lastFlushedGlobalPosition;
  }

  /**
   * Check if the store is open.
   */
  isOpen(): boolean {
    return this.segmentManager !== null;
  }

  /**
   * Rebuild stream revisions from existing segments.
   */
  private async rebuildStreamRevisions(): Promise<void> {
    for (const segment of this.segmentManager!.getSegments()) {
      const index = await this.segmentManager!.getIndex(segment.id);

      for (const streamId of index.getStreamIds()) {
        const currentMax = this.streamRevisions.get(streamId) ?? -1;
        const segmentMax = index.getStreamRevision(streamId);
        if (segmentMax > currentMax) {
          this.streamRevisions.set(streamId, segmentMax);
        }
      }
    }
  }

  /**
   * Read durable events for a stream without mutating durability state.
   *
   * Optimized: Uses the segment index to filter by revision range,
   * avoiding loading events outside the requested range.
   *
   * @param streamId - Stream to read
   * @param fromRevision - Minimum revision (inclusive)
   * @param toRevision - Maximum revision (inclusive), undefined for no limit
   */
  private async readDurableStreamEvents(
    streamId: string,
    fromRevision = 0,
    toRevision?: number
  ): Promise<StoredEvent[]> {
    const maxDurablePosition = this.lastFlushedGlobalPosition;
    if (maxDurablePosition < 0) {
      return [];
    }

    const segmentIds = this.segmentManager!.getStreamSegments(streamId);
    if (segmentIds.length === 0) {
      return [];
    }

    const events: StoredEvent[] = [];
    const batchCache = new Map<string, StoredEvent[]>();
    const reader = this.segmentManager!.getReader();

    for (const segmentId of segmentIds) {
      const segment = this.segmentManager!.getSegment(segmentId);
      if (!segment) {
        continue;
      }

      if (segment.basePosition > maxDurablePosition) {
        continue;
      }

      if (segment.isActive) {
        // Active segment may not have index yet - scan all batches
        // but still apply revision filter
        for await (const batch of reader.readAllBatches(segment.path)) {
          for (const event of batch) {
            if (event.streamId !== streamId) {
              continue;
            }
            if (event.globalPosition > maxDurablePosition) {
              continue;
            }
            // Apply revision filter
            if (event.revision < fromRevision) {
              continue;
            }
            if (toRevision !== undefined && event.revision > toRevision) {
              continue;
            }
            events.push(event);
          }
        }
        continue;
      }

      const indexFile = await this.segmentManager!.getSegmentIndexFile(segmentId);
      // Pass revision filters to index query - this uses binary search
      // to efficiently find only entries in the requested range
      const entries = indexFile.findByStream(streamId, fromRevision, toRevision);

      for (const entry of entries) {
        if (entry.globalPosition > maxDurablePosition) {
          continue;
        }

        const cacheKey = `${segmentId}:${entry.batchOffset}`;
        let batch = batchCache.get(cacheKey);
        if (!batch) {
          try {
            batch = await reader.readBatch(segment.path, entry.batchOffset);
          } catch {
            continue;
          }
          batchCache.set(cacheKey, batch);
        }

        const event = batch.find(
          (e) => e.streamId === streamId && e.revision === entry.revision
        );
        if (event) {
          events.push(event);
        }
      }
    }

    return events;
  }

  /**
   * Ensure the store is open and not in a failed state.
   */
  private ensureOpen(): void {
    if (!this.segmentManager) {
      throw new Error('EventStore not open. Call open() first.');
    }
    if (this.failed) {
      throw new StoreFatalError(
        'EventStore is in failed state due to previous sync error. Close and reopen to recover.',
        this.failedError ?? undefined
      );
    }
  }

  /**
   * Check if the store is in a failed state.
   *
   * A store enters the failed state when a sync operation fails after data
   * has been written. In this state, the store will reject all operations
   * until it is closed and reopened.
   *
   * @returns true if the store is in a failed state
   */
  isFailed(): boolean {
    return this.failed;
  }

  /**
   * Get the error that caused the store to enter the failed state.
   *
   * @returns The original error, or null if the store is not in a failed state
   */
  getFailedError(): Error | null {
    return this.failedError;
  }
}
