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
 * for await (const event of store.streamGlobal(0n)) {
 *   console.log(event.type);
 * }
 *
 * await store.close();
 * ```
 */

import type { FileSystem } from './interfaces/filesystem';
import type { Serializer } from './interfaces/serializer';
import type { Compressor } from './interfaces/compressor';
import type { Clock } from './interfaces/clock';
import { SegmentManager } from './storage/segment-manager';
import type { StoredEvent, StreamEntry } from './storage';
import { ConcurrencyError } from './domain/errors';

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
  globalPosition: bigint;
  /** Number of events appended */
  eventCount: number;
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
}

const DEFAULT_AUTO_FLUSH_COUNT = 1000;

/**
 * Main EventStore class.
 */
export class EventStore {
  private readonly config: Required<
    Omit<EventStoreConfig, 'maxSegmentSize' | 'indexCacheSize'>
  > & {
    maxSegmentSize?: number;
    indexCacheSize?: number;
  };
  private segmentManager: SegmentManager | null = null;
  private pendingEvents: StoredEvent[] = [];
  private streamRevisions = new Map<string, number>();
  private dataDir: string = '';

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
   *
   * @param dataDir - Directory to store segment files
   */
  async open(dataDir: string): Promise<void> {
    if (this.segmentManager) {
      throw new Error('EventStore already open');
    }

    this.dataDir = dataDir;

    this.segmentManager = new SegmentManager(
      this.config.fs,
      this.config.serializer,
      this.config.compressor,
      {
        dataDir,
        maxSegmentSize: this.config.maxSegmentSize,
        indexCacheSize: this.config.indexCacheSize,
      }
    );

    await this.segmentManager.initialize();

    // Rebuild stream revisions from existing data
    await this.rebuildStreamRevisions();
  }

  /**
   * Close the event store.
   *
   * Flushes pending events and closes the segment manager.
   */
  async close(): Promise<void> {
    if (!this.segmentManager) {
      return;
    }

    // Flush any pending events
    await this.flush();

    await this.segmentManager.close();
    this.segmentManager = null;
    this.streamRevisions.clear();
    this.pendingEvents = [];
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
      await this.flush();
    }

    return {
      streamRevision: revision - 1,
      globalPosition: firstGlobalPosition,
      eventCount: events.length,
    };
  }

  /**
   * Flush pending events to disk.
   *
   * After flush returns, all previously appended events are durable.
   */
  async flush(): Promise<void> {
    this.ensureOpen();

    if (this.pendingEvents.length === 0) {
      return;
    }

    await this.segmentManager!.writeBatch(this.pendingEvents);
    await this.segmentManager!.sync();
    this.pendingEvents = [];
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
   * @param streamId - Stream to read
   * @param options - Read options
   * @yields Events from the stream
   */
  async *streamEvents(
    streamId: string,
    options: ReadStreamOptions = {}
  ): AsyncGenerator<StoredEvent> {
    this.ensureOpen();

    // Flush pending events first to include them in reads
    await this.flush();

    // O(1) lookup: get segments containing this stream from StreamMap
    const segmentIds = this.segmentManager!.getStreamSegments(streamId);

    if (segmentIds.length === 0) {
      return; // Stream doesn't exist
    }

    // Collect all matching entries across relevant segments only
    const allEntries: Array<{ segmentId: bigint; batchOffset: number; revision: number }> = [];

    for (const segmentId of segmentIds) {
      // O(log n) lookup within segment using binary search
      const indexFile = await this.segmentManager!.getSegmentIndexFile(segmentId);
      const entries = indexFile.findByStream(
        streamId,
        options.fromRevision,
        options.toRevision
      );

      for (const entry of entries) {
        allEntries.push({
          segmentId,
          batchOffset: entry.batchOffset,
          revision: entry.revision,
        });
      }
    }

    // Sort by revision
    allEntries.sort((a, b) => a.revision - b.revision);

    // Reverse if backward
    if (options.direction === 'backward') {
      allEntries.reverse();
    }

    // Read events from segment files, caching batches
    const batchCache = new Map<string, StoredEvent[]>();
    let yielded = 0;
    const maxCount = options.maxCount ?? Infinity;

    for (const entry of allEntries) {
      if (yielded >= maxCount) {
        break;
      }

      const cacheKey = `${entry.segmentId}:${entry.batchOffset}`;
      let batch = batchCache.get(cacheKey);

      if (!batch) {
        const segment = this.segmentManager!.getSegment(entry.segmentId);
        if (!segment) continue;

        batch = await this.segmentManager!
          .getReader()
          .readBatch(segment.path, entry.batchOffset);
        batchCache.set(cacheKey, batch);
      }

      // Find the specific event in the batch
      const event = batch.find(
        (e) => e.streamId === streamId && e.revision === entry.revision
      );

      if (event) {
        yield event;
        yielded++;
      }
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
    fromPosition: bigint = 0n,
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
   * Stream events from the global log.
   *
   * @param fromPosition - Start position (inclusive, default: 0)
   * @yields Events in global order
   */
  async *streamGlobal(fromPosition: bigint = 0n): AsyncGenerator<StoredEvent> {
    this.ensureOpen();

    // Flush pending events first
    await this.flush();

    // Read from all segments in order
    for (const segment of this.segmentManager!.getSegments()) {
      // Skip segments that are entirely before our start position
      // (We'd need basePosition + eventCount to know for sure, but this is an optimization)

      for await (const batch of this.segmentManager!.getReader().readAllBatches(segment.path)) {
        for (const event of batch) {
          if (event.globalPosition >= fromPosition) {
            yield event;
          }
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
    await this.flush();

    const streamIds = new Set<string>();

    for (const segment of this.segmentManager!.getSegments()) {
      const index = await this.segmentManager!.getIndex(segment.id);
      for (const id of index.getStreamIds()) {
        streamIds.add(id);
      }
    }

    return Array.from(streamIds).sort();
  }

  /**
   * Get the current global position.
   */
  getGlobalPosition(): bigint {
    this.ensureOpen();
    return this.segmentManager!.getGlobalPosition();
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
   * Ensure the store is open.
   */
  private ensureOpen(): void {
    if (!this.segmentManager) {
      throw new Error('EventStore not open. Call open() first.');
    }
  }
}
