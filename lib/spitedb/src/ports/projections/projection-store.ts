/**
 * Projection storage abstraction.
 *
 * Each projection kind has its own store implementation:
 * - AggregatorStore: Memory-only, simple value
 * - DenormalizedViewStore: Key-value with indexes and optional disk spillover
 */

import type { FileSystem } from '../storage/filesystem';
import type { Serializer } from '../serialization/serializer';
import type { Clock } from '../time/clock';

/**
 * Query filter for equality lookups.
 *
 * Maps field names to expected values.
 * All conditions are ANDed together.
 *
 * @example
 * ```ts
 * // Find users with status 'active' and plan 'pro'
 * store.query({ status: 'active', plan: 'pro' })
 * ```
 */
export interface QueryFilter {
  [field: string]: unknown;
}

/**
 * Range for sorted key queries (time-series style).
 *
 * Both bounds are inclusive. Omit a bound for unbounded range.
 *
 * @example
 * ```ts
 * // Get all buckets from January to March 2024
 * store.queryRange({ start: '2024-01', end: '2024-03' })
 *
 * // Get all buckets from January onwards
 * store.queryRange({ start: '2024-01' })
 * ```
 */
export interface TimeRange {
  /** Start key (inclusive) */
  start?: string;
  /** End key (inclusive) */
  end?: string;
}

/**
 * Configuration for projection stores.
 */
export interface ProjectionStoreConfig {
  /** Filesystem for persistence */
  fs: FileSystem;
  /** Serializer for state */
  serializer: Serializer;
  /** Clock for timestamps */
  clock: Clock;
  /** Data directory for checkpoints and spillover */
  dataDir: string;
  /** Projection name (used for file naming) */
  projectionName: string;
}

/**
 * Storage abstraction for projection state.
 *
 * Each projection kind has its own implementation optimized for its access patterns.
 *
 * @typeParam TState - The state type (e.g., number for aggregator, Map<string, Row> for denormalized view)
 */
export interface ProjectionStore<TState = unknown> {
  /**
   * Initialize the store.
   *
   * Creates data directories if needed.
   * Does not load checkpoint (that's done by the runner).
   */
  initialize(): Promise<void>;

  /**
   * Get current state.
   *
   * For aggregators: returns the value directly.
   * For denormalized views: returns the full map (or throws if too large).
   */
  get(): TState;

  /**
   * Set state (used during build and recovery).
   *
   * @param state - New state value
   */
  set(state: TState): void;

  /**
   * Query rows by equality filter.
   *
   * Only available for DenormalizedViewStore.
   *
   * @param filter - Field equality conditions
   * @returns Matching rows
   */
  query?(filter: QueryFilter): unknown[];

  /**
   * Query rows by key range (sorted order).
   *
   * Only available for DenormalizedViewStore with isRange access patterns.
   *
   * @param range - Key range (start/end bounds)
   * @returns Map of key -> value for matching rows
   */
  queryRange?(range: TimeRange): Map<string, unknown>;

  /**
   * Get a single row by primary key.
   *
   * Only available for DenormalizedViewStore.
   *
   * @param key - Primary key
   * @returns Row or undefined if not found
   */
  getByKey?(key: string): unknown | undefined;

  /**
   * Set a single row by primary key.
   *
   * Only available for DenormalizedViewStore.
   * Updates indexes automatically.
   *
   * @param key - Primary key
   * @param value - Row value
   */
  setByKey?(key: string, value: unknown): void;

  /**
   * Delete a row by primary key.
   *
   * Only available for DenormalizedViewStore.
   *
   * @param key - Primary key
   * @returns true if row existed and was deleted
   */
  deleteByKey?(key: string): boolean;

  /**
   * Persist state to disk.
   *
   * Called periodically by the checkpoint manager.
   * Should be atomic (write to temp file, then rename).
   *
   * @param position - Global position to checkpoint at
   */
  persist(position: number): Promise<void>;

  /**
   * Load state from disk.
   *
   * Called during recovery.
   *
   * @returns Checkpoint position if loaded, or null if no checkpoint exists
   */
  load(): Promise<number | null>;

  /**
   * Close and release resources.
   */
  close(): Promise<void>;

  /**
   * Get memory usage estimate in bytes.
   *
   * Used to decide when to spill to disk for DenormalizedViewStore.
   */
  getMemoryUsage(): number;

  /**
   * Force compaction (for append-only stores).
   *
   * Called when garbage ratio exceeds threshold.
   */
  compact?(): Promise<void>;
}

/**
 * Factory for creating projection stores.
 *
 * Used by the projection coordinator to create stores based on metadata.
 */
export interface ProjectionStoreFactory {
  /**
   * Create an aggregator store for simple value state.
   *
   * @param config - Store configuration
   * @param initialValue - Initial state value
   */
  createAggregatorStore<T>(config: ProjectionStoreConfig, initialValue: T): ProjectionStore<T>;

  /**
   * Create a denormalized view store for key-value state.
   *
   * @param config - Store configuration
   * @param indexFields - Fields to create equality indexes on
   * @param rangeFields - Fields to create sorted indexes on (for range queries)
   * @param memoryThreshold - Memory threshold before disk spillover (default: 50MB)
   */
  createDenormalizedViewStore<TRow>(
    config: ProjectionStoreConfig,
    indexFields: string[],
    rangeFields: string[],
    memoryThreshold?: number
  ): ProjectionStore<Map<string, TRow>>;
}
