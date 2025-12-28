/**
 * Projection coordinator that manages multiple projection runners.
 *
 * The coordinator is the main entry point for the projection system:
 * - Provides a registry for projection registration
 * - Creates runners for each registered projection
 * - Starts/stops all projections together
 * - Provides unified status and query access
 *
 * @example
 * ```ts
 * const coordinator = new ProjectionCoordinator({
 *   eventStore,
 *   fs,
 *   serializer,
 *   clock,
 *   dataDir: '/data/projections',
 * });
 *
 * // Register projections (typically done by compiler-generated code)
 * coordinator.getRegistry().register(TotalRevenueRegistration);
 * coordinator.getRegistry().register(UserProfilesRegistration);
 *
 * // Start all projections
 * await coordinator.start();
 *
 * // Query a projection
 * const revenue = coordinator.getProjection<TotalRevenue>('TotalRevenue');
 * console.log(revenue?.getTotal());
 *
 * // Later
 * await coordinator.stop();
 * ```
 */

import type { FileSystem, FileHandle } from '../../ports/storage/filesystem';
import type { Serializer } from '../../ports/serialization/serializer';
import type { Clock } from '../../ports/time/clock';
import type { EventStore } from '../event-store';
import type { StoredEvent } from '../../domain/events/stored-event';
import type {
  ProjectionRegistry,
  ResolvedRegistration,
  Projection,
  ProjectionStore,
} from '../../ports/projections';
import { DefaultProjectionRegistry } from '../../infrastructure/projections/default-registry';
import { ProjectionRunner, type ProjectionRunnerStatus } from './projection-runner';
import { AggregatorStore } from '../../infrastructure/projections/stores/aggregator-store';
import { DenormalizedViewStore } from '../../infrastructure/projections/stores/denormalized-view-store';
import {
  ProjectionNotFoundError,
  ProjectionDisabledError,
  ProjectionCatchUpTimeoutError,
  ProjectionCoordinatorError,
} from '../../errors';

/**
 * Status of all projections.
 */
export interface ProjectionCoordinatorStatus {
  /** Whether the coordinator is running */
  running: boolean;
  /** Number of registered projections */
  registeredCount: number;
  /** Number of enabled projections */
  enabledCount: number;
  /** Status of each projection */
  projections: ProjectionRunnerStatus[];
  /** Current global position from event store */
  globalPosition: number;
}

/**
 * Configuration for projection coordinator.
 */
export interface ProjectionCoordinatorConfig {
  /** Event store to read from */
  eventStore: EventStore;
  /** Filesystem implementation */
  fs: FileSystem;
  /** Serializer implementation */
  serializer: Serializer;
  /** Clock implementation */
  clock: Clock;
  /** Data directory for checkpoints */
  dataDir: string;
  /** Polling interval in ms (default: 100) */
  pollingIntervalMs?: number;
  /** Default checkpoint interval in ms (default: 5000) */
  defaultCheckpointIntervalMs?: number;
  /** Checkpoint jitter range in ms (default: 1000) */
  checkpointJitterMs?: number;
  /** Default batch size for reading events (default: 100) */
  defaultBatchSize?: number;
  /** Use a shared global reader for all projections (default: true) */
  sharedReader?: boolean;
}

/**
 * Manages multiple projection runners.
 *
 * Provides a unified interface for:
 * - Registering projections
 * - Starting/stopping all projections
 * - Querying projection state
 * - Monitoring projection status
 */
export class ProjectionCoordinator {
  private readonly config: Required<ProjectionCoordinatorConfig>;
  private readonly registry: ProjectionRegistry;
  private readonly runners = new Map<string, ProjectionRunner>();
  private readonly stores = new Map<string, ProjectionStore>();
  private readonly projectionInstances = new Map<string, unknown>();
  private running = false;
  private lockHandle: FileHandle | null = null;
  private sharedPollingTimer: ReturnType<Clock['setTimeout']> | null = null;
  private sharedCursor: number | null = null;
  private sharedPollingPaused = false;

  constructor(config: ProjectionCoordinatorConfig) {
    this.config = {
      ...config,
      pollingIntervalMs: config.pollingIntervalMs ?? 100,
      defaultCheckpointIntervalMs: config.defaultCheckpointIntervalMs ?? 5000,
      checkpointJitterMs: config.checkpointJitterMs ?? 1000,
      defaultBatchSize: config.defaultBatchSize ?? 100,
      sharedReader: config.sharedReader ?? true,
    };

    this.registry = new DefaultProjectionRegistry();
  }

  /**
   * Get the registry for projection registration.
   *
   * Use this to register projections before calling start().
   */
  getRegistry(): ProjectionRegistry {
    return this.registry;
  }

  /**
   * Start all registered projections.
   *
   * Creates runners and stores for each enabled projection,
   * then starts them all in parallel.
   */
  async start(): Promise<void> {
    if (this.running) {
      throw new ProjectionCoordinatorError('Coordinator already running');
    }

    if (!this.config.eventStore.isOpen()) {
      throw new ProjectionCoordinatorError('EventStore is not open');
    }

    // Create data directory if needed
    if (!(await this.config.fs.exists(this.config.dataDir))) {
      await this.config.fs.mkdir(this.config.dataDir, { recursive: true });
    }

    // Acquire exclusive lock to prevent multiple coordinators
    const lockPath = `${this.config.dataDir}/.lock`;
    try {
      this.lockHandle = await this.config.fs.open(lockPath, 'write');
      await this.config.fs.flock(this.lockHandle, 'exclusive');
    } catch (error) {
      if (this.lockHandle) {
        try {
          await this.config.fs.close(this.lockHandle);
        } catch {
          // Ignore cleanup errors
        }
        this.lockHandle = null;
      }
      throw new ProjectionCoordinatorError(
        `Failed to acquire lock on ${lockPath}: ${error instanceof Error ? error.message : String(error)}`
      );
    }

    try {
      // Create checkpoint directory
      const checkpointDir = `${this.config.dataDir}/checkpoints`;
      if (!(await this.config.fs.exists(checkpointDir))) {
        await this.config.fs.mkdir(checkpointDir, { recursive: true });
      }

      // Create runners for all enabled projections
      const startPromises: Promise<void>[] = [];

      for (const resolved of this.registry.getAll()) {
        if (!resolved.options.enabled) {
          continue;
        }

        const { registration, options } = resolved;
        const { metadata } = registration;

        // Create projection instance
        const projection = registration.factory();

        // Create store based on kind
        const store = this.createStore(metadata.name, metadata.kind, projection, resolved);
        await store.initialize();

        // Create runner
        const runner = new ProjectionRunner({
          eventStore: this.config.eventStore,
          projection,
          store,
          metadata,
          clock: this.config.clock,
          pollingIntervalMs: this.config.pollingIntervalMs,
          checkpointIntervalMs: options.checkpointIntervalMs,
          jitterMs: this.config.checkpointJitterMs,
          eventFilter: options.eventFilter,
          batchSize: this.config.defaultBatchSize,
          useSharedReader: this.config.sharedReader,
        });

        this.runners.set(metadata.name, runner);
        this.stores.set(metadata.name, store);

        // Store the projection instance for query access
        if (registration.getInstance) {
          this.projectionInstances.set(metadata.name, registration.getInstance());
        } else {
          this.projectionInstances.set(metadata.name, projection);
        }

        startPromises.push(runner.start());
      }

      // Start all runners in parallel
      await Promise.all(startPromises);
      this.running = true;

      if (this.config.sharedReader) {
        this.startSharedReader();
      }
    } catch (error) {
      await this.cleanupAfterStartFailure();
      throw error;
    }
  }

  /**
   * Stop all projections gracefully.
   *
   * Stops all runners and persists final checkpoints.
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    if (this.sharedPollingTimer) {
      this.sharedPollingTimer.cancel();
      this.sharedPollingTimer = null;
    }

    // Stop all runners in parallel
    const stopPromises = Array.from(this.runners.values()).map((runner) => runner.stop());
    await Promise.all(stopPromises);

    // Close all stores
    const closePromises = Array.from(this.stores.values()).map((store) => store.close());
    await Promise.all(closePromises);

    this.runners.clear();
    this.stores.clear();
    this.projectionInstances.clear();

    // Release lock (closing the handle releases the lock)
    if (this.lockHandle) {
      try {
        await this.config.fs.close(this.lockHandle);
      } catch {
        // Ignore errors during cleanup
      }
      this.lockHandle = null;
    }

    this.running = false;
  }

  /**
   * Cleanup resources when start() fails after acquiring a lock.
   */
  private async cleanupAfterStartFailure(): Promise<void> {
    if (this.sharedPollingTimer) {
      this.sharedPollingTimer.cancel();
      this.sharedPollingTimer = null;
    }

    const stopPromises = Array.from(this.runners.values()).map((runner) =>
      runner.stop().catch(() => undefined)
    );
    await Promise.all(stopPromises);

    const closePromises = Array.from(this.stores.values()).map((store) =>
      store.close().catch(() => undefined)
    );
    await Promise.all(closePromises);

    this.runners.clear();
    this.stores.clear();
    this.projectionInstances.clear();

    if (this.lockHandle) {
      try {
        await this.config.fs.close(this.lockHandle);
      } catch {
        // Ignore cleanup errors
      }
      this.lockHandle = null;
    }

    this.running = false;
  }

  private startSharedReader(): void {
    this.sharedCursor = null;
    this.sharedPollingPaused = false;
    this.scheduleSharedPoll();
  }

  private scheduleSharedPoll(immediate = false): void {
    if (!this.running) {
      return;
    }

    const delay = immediate ? 0 : this.config.pollingIntervalMs;

    this.sharedPollingTimer = this.config.clock.setTimeout(async () => {
      let hadEvents = false;
      try {
        if (!this.sharedPollingPaused) {
          hadEvents = await this.pollShared();
        }
      } catch (error) {
        console.error('[ProjectionCoordinator] Shared poll error:', error);
      } finally {
        // If we had events, poll immediately; otherwise wait the normal interval
        this.scheduleSharedPoll(hadEvents);
      }
    }, delay);
  }

  private async pollShared(): Promise<boolean> {
    if (!this.running) {
      return false;
    }

    const startTime = this.config.clock.now();
    const realStart = Date.now();
    const durationBudget = this.config.pollingIntervalMs;
    let processedAny = false;

    while (this.running) {
      const events = await this.readSharedBatch();
      if (events.length === 0) {
        break;
      }

      processedAny = true;
      this.dispatchBatch(events);

      const elapsed = this.config.clock.now() - startTime;
      const realElapsed = Date.now() - realStart;
      if (elapsed >= durationBudget || realElapsed >= durationBudget) {
        // We hit the time budget, may have more events
        await this.tickCheckpoints();
        return true;
      }
    }

    await this.tickCheckpoints();
    // No more events available (or didn't process any)
    return false;
  }

  private async tickCheckpoints(): Promise<void> {
    const checkpointPromises = Array.from(this.runners.values()).map((runner) =>
      runner.checkpointIfNeeded()
    );
    await Promise.all(checkpointPromises);
  }

  private dispatchBatch(events: StoredEvent[]): void {
    for (const runner of this.runners.values()) {
      try {
        runner.processBatch(events);
      } catch (error) {
        console.error(
          `[ProjectionCoordinator] Runner '${runner.getStatus().name}' error:`,
          error
        );
      }
    }
  }

  private async readSharedBatch(): Promise<StoredEvent[]> {
    if (this.sharedCursor === null) {
      this.sharedCursor = this.getSharedStartCursor();
    }

    // Try cache first for fast reads
    const cached = this.config.eventStore.readGlobalCached(
      this.sharedCursor,
      this.config.defaultBatchSize
    );

    if (cached !== null && cached.length > 0) {
      const last = cached[cached.length - 1]!;
      this.sharedCursor = last.globalPosition + 1;
      return cached;
    }

    // Fall back to disk read on cache miss
    const events = await this.config.eventStore.readGlobal(this.sharedCursor, {
      maxCount: this.config.defaultBatchSize,
    });

    if (events.length > 0) {
      const last = events[events.length - 1]!;
      this.sharedCursor = last.globalPosition + 1;
    }

    return events;
  }

  private getSharedStartCursor(): number {
    let minPosition: number | null = null;
    for (const runner of this.runners.values()) {
      const position = runner.getCurrentPosition();
      if (minPosition === null || position < minPosition) {
        minPosition = position;
      }
    }

    if (minPosition === null || minPosition < 0) {
      return 0;
    }

    return minPosition + 1;
  }

  /**
   * Get a projection instance by name for querying.
   *
   * @param name - Projection name
   * @returns Projection instance or undefined if not found
   *
   * @example
   * ```ts
   * const revenue = coordinator.getProjection<TotalRevenue>('TotalRevenue');
   * if (revenue) {
   *   console.log(revenue.getTotal());
   * }
   * ```
   */
  getProjection<T = unknown>(name: string): T | undefined {
    return this.projectionInstances.get(name) as T | undefined;
  }

  /**
   * Get a projection instance, throwing if not found.
   *
   * @param name - Projection name
   * @returns Projection instance
   * @throws {ProjectionNotFoundError} if projection doesn't exist
   * @throws {ProjectionDisabledError} if projection is disabled
   */
  requireProjection<T = unknown>(name: string): T {
    const resolved = this.registry.get(name);

    if (!resolved) {
      throw new ProjectionNotFoundError(name);
    }

    if (!resolved.options.enabled) {
      throw new ProjectionDisabledError(name);
    }

    const projection = this.projectionInstances.get(name);
    if (!projection) {
      throw new ProjectionNotFoundError(name);
    }

    return projection as T;
  }

  /**
   * Wait until all projections have caught up to current position.
   *
   * @param timeoutMs - Maximum time to wait (default: 30000)
   * @throws {ProjectionCatchUpTimeoutError} if any projection times out
   */
  async waitForCatchUp(timeoutMs: number = 30000): Promise<void> {
    const globalPosition = this.config.eventStore.getGlobalPosition();
    // EventStore returns the next position to allocate, so the last committed event is -1.
    const targetPosition = globalPosition > 0 ? globalPosition - 1 : -1;

    if (this.config.sharedReader) {
      const success = await this.drainSharedUntil(targetPosition, timeoutMs);
      if (!success) {
        const firstRunner = this.runners.values().next().value as ProjectionRunner | undefined;
        const current = firstRunner ? firstRunner.getCurrentPosition() : -1;
        const name = firstRunner ? firstRunner.getStatus().name : 'unknown';
        throw new ProjectionCatchUpTimeoutError(name, current, targetPosition, timeoutMs);
      }
      return;
    }

    const catchUpPromises = Array.from(this.runners.entries()).map(async ([name, runner]) => {
      const success = await runner.waitForCatchUp(targetPosition, timeoutMs);
      if (!success) {
        throw new ProjectionCatchUpTimeoutError(
          name,
          runner.getCurrentPosition(),
          targetPosition,
          timeoutMs
        );
      }
    });

    await Promise.all(catchUpPromises);
  }

  private async drainSharedUntil(targetPosition: number, timeoutMs: number): Promise<boolean> {
    const startTime = this.config.clock.now();
    const realStart = Date.now();

    this.sharedPollingPaused = true;
    try {
      while (this.getMinRunnerPosition() < targetPosition) {
        const events = await this.readSharedBatch();
        if (events.length > 0) {
          this.dispatchBatch(events);
          await this.tickCheckpoints();
        } else {
          await this.config.clock.sleep(10);
        }

        const elapsed = this.config.clock.now() - startTime;
        const realElapsed = Date.now() - realStart;
        if (elapsed > timeoutMs || realElapsed > timeoutMs) {
          return false;
        }
      }
    } finally {
      this.sharedPollingPaused = false;
    }

    return true;
  }

  private getMinRunnerPosition(): number {
    let minPosition: number | null = null;
    for (const runner of this.runners.values()) {
      const position = runner.getCurrentPosition();
      if (minPosition === null || position < minPosition) {
        minPosition = position;
      }
    }
    return minPosition ?? -1;
  }

  /**
   * Force checkpoint for all projections.
   */
  async forceCheckpoint(): Promise<void> {
    const checkpointPromises = Array.from(this.runners.values()).map((runner) =>
      runner.forceCheckpoint()
    );
    await Promise.all(checkpointPromises);
  }

  /**
   * Get status of all projections.
   */
  getStatus(): ProjectionCoordinatorStatus {
    const projections = Array.from(this.runners.values()).map((runner) => runner.getStatus());

    const enabledCount = this.registry.getAll().filter((r) => r.options.enabled).length;

    return {
      running: this.running,
      registeredCount: this.registry.count(),
      enabledCount,
      projections,
      globalPosition: this.config.eventStore.isOpen()
        ? this.config.eventStore.getGlobalPosition()
        : 0,
    };
  }

  /**
   * Get status of a specific projection.
   *
   * @param name - Projection name
   * @returns Runner status or undefined if not found
   */
  getProjectionStatus(name: string): ProjectionRunnerStatus | undefined {
    return this.runners.get(name)?.getStatus();
  }

  /**
   * Check if coordinator is running.
   */
  isRunning(): boolean {
    return this.running;
  }

  /**
   * Create a store for a projection based on its kind.
   */
  private createStore(
    name: string,
    kind: string,
    projection: Projection,
    resolved: ResolvedRegistration
  ): ProjectionStore {
    const storeConfig = {
      fs: this.config.fs,
      serializer: this.config.serializer,
      clock: this.config.clock,
      dataDir: `${this.config.dataDir}/checkpoints`,
      projectionName: name,
    };

    switch (kind) {
      case 'aggregator':
        // Get initial state from projection
        projection.reset();
        const initialState = projection.getState();
        return new AggregatorStore(storeConfig, initialState);

      case 'denormalized_view':
        // Extract index fields from access patterns
        const { accessPatterns } = resolved.registration.metadata;
        const indexFields: string[] = [];
        const rangeFields: string[] = [];

        for (const pattern of accessPatterns) {
          if (pattern.isRange) {
            rangeFields.push(...pattern.indexFields);
          } else {
            indexFields.push(...pattern.indexFields);
          }
        }

        return new DenormalizedViewStore({
          ...storeConfig,
          indexFields: [...new Set(indexFields)], // Dedupe
          rangeFields: [...new Set(rangeFields)],
          memoryThreshold: resolved.options.memoryThresholdBytes,
        });

      default:
        throw new ProjectionCoordinatorError(`Unknown projection kind: ${kind}`);
    }
  }
}
