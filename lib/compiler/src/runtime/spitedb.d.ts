/**
 * SpiteDB Runtime Type Declarations
 *
 * Auto-generated - do not edit manually.
 */

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
 * A stored event with all position and metadata.
 */
export interface StoredEvent {
  /** Stream this event belongs to */
  streamId: string;
  /** Event type name */
  type: string;
  /** Event data payload */
  data: unknown;
  /** Optional event metadata */
  metadata?: unknown;
  /** Revision within the stream (0-based) */
  revision: number;
  /** Global position across all streams */
  globalPosition: number;
  /** Timestamp when the event was stored */
  timestamp: number;
  /** Tenant ID */
  tenantId: string;
}

/**
 * Thrown when optimistic concurrency check fails.
 */
export declare class ConcurrencyError extends Error {
  readonly streamId: string;
  readonly expected: number;
  readonly actual: number;
  constructor(streamId: string, expected: number, actual: number);
}

/**
 * Configuration options for SpiteDB.
 */
export interface SpiteDBOptions {
  /** Maximum segment size in bytes (default: 128MB) */
  maxSegmentSize?: number;
  /** Number of segment indexes to cache (default: 10) */
  indexCacheSize?: number;
  /** Auto-flush after this many events (default: 1000, 0 = disabled) */
  autoFlushCount?: number;
}

/**
 * The main SpiteDB class.
 */
export declare class SpiteDB {
  /**
   * Open a SpiteDB instance at the given path.
   */
  static open(path: string, options?: SpiteDBOptions): Promise<SpiteDB>;

  /**
   * Append events to a stream.
   */
  append(
    streamId: string,
    events: InputEvent[],
    options?: AppendOptions
  ): Promise<AppendResult>;

  /**
   * Read events from a stream.
   */
  readStream(
    streamId: string,
    options?: ReadStreamOptions
  ): Promise<StoredEvent[]>;

  /**
   * Flush pending events to disk.
   */
  flush(): Promise<void>;

  /**
   * Close the database.
   */
  close(): Promise<void>;

  /**
   * Check if the database is open.
   */
  isOpen(): boolean;

  /**
   * Get the current stream revision.
   */
  getStreamRevision(streamId: string): number;
}
