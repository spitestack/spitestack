/**
 * Projection runner that polls the event log and processes events.
 *
 * Each projection has its own runner that:
 * - Polls the event store at a configurable interval
 * - Filters events by subscribed types
 * - Calls the projection's build() method
 * - Manages checkpointing with jitter
 *
 * @example
 * ```ts
 * const runner = new ProjectionRunner({
 *   eventStore,
 *   projection,
 *   store,
 *   metadata,
 *   clock,
 *   pollingIntervalMs: 100,
 *   checkpointIntervalMs: 5000,
 *   jitterMs: 1000,
 * });
 *
 * await runner.start();
 * // ... later
 * await runner.stop();
 * ```
 */

import type { Clock, Timer } from '../../ports/time/clock';
import type { EventStore } from '../event-store';
import type { StoredEvent } from '../../domain/events/stored-event';
import type { Projection, ProjectionMetadata } from '../../ports/projections/projection';
import type { ProjectionStore } from '../../ports/projections/projection-store';
import { ProjectionBuildError } from '../../errors';

/**
 * Runner status information.
 */
export interface ProjectionRunnerStatus {
  /** Projection name */
  name: string;
  /** Projection kind */
  kind: string;
  /** Whether the runner is active */
  running: boolean;
  /** Current position in the global log */
  currentPosition: number;
  /** Total events processed */
  eventsProcessed: number;
  /** Total errors encountered */
  errorsCount: number;
  /** Last checkpoint position */
  lastCheckpointPosition: number;
  /** Last checkpoint timestamp */
  lastCheckpointTime: number;
  /** Next scheduled checkpoint time */
  nextCheckpointTime: number;
}

/**
 * Configuration for projection runner.
 */
export interface ProjectionRunnerConfig {
  /** Event store to read from */
  eventStore: EventStore;
  /** Projection instance */
  projection: Projection;
  /** Storage for projection state */
  store: ProjectionStore;
  /** Projection metadata */
  metadata: ProjectionMetadata;
  /** Clock implementation */
  clock: Clock;
  /** Polling interval in ms (default: 100) */
  pollingIntervalMs?: number;
  /** Checkpoint interval in ms (default: 5000) */
  checkpointIntervalMs?: number;
  /** Jitter range for checkpoints in ms (default: 1000) */
  jitterMs?: number;
  /** Optional event filter (for tenant-specific projections) */
  eventFilter?: ((event: StoredEvent) => boolean) | undefined;
  /** Batch size for reading events (default: 100) */
  batchSize?: number;
  /** Disable internal polling when using a shared reader */
  useSharedReader?: boolean;
}

/**
 * Runs a single projection, polling for new events.
 *
 * Lifecycle:
 * 1. start() - Load checkpoint, begin polling
 * 2. processEvents() - Called on each poll cycle
 * 3. stop() - Stop polling, persist final checkpoint
 */
export class ProjectionRunner {
  private readonly eventStore: EventStore;
  private readonly projection: Projection;
  private readonly store: ProjectionStore;
  private readonly metadata: ProjectionMetadata;
  private readonly clock: Clock;
  private readonly pollingIntervalMs: number;
  private readonly checkpointIntervalMs: number;
  private readonly jitterMs: number;
  private readonly eventFilter: ((event: StoredEvent) => boolean) | undefined;
  private readonly batchSize: number;
  private readonly useIncrementalStore: boolean;
  private readonly useSharedReader: boolean;

  /** Set of event types this projection subscribes to */
  private readonly subscribedEventTypes: Set<string>;

  /** Current position in the global log (-1 means no events processed yet) */
  private currentPosition = -1;

  /** Polling timer */
  private pollingTimer: Timer | null = null;

  /** Whether the runner is active */
  private running = false;

  /** Total events processed */
  private eventsProcessed = 0;

  /** Total errors encountered */
  private errorsCount = 0;

  /** Last checkpoint position */
  private lastCheckpointPosition = -1;

  /** Last checkpoint timestamp */
  private lastCheckpointTime = 0;

  /** Next scheduled checkpoint time */
  private nextCheckpointTime = 0;
  /** Whether projection state changed since last checkpoint */
  private stateDirty = false;

  constructor(config: ProjectionRunnerConfig) {
    this.eventStore = config.eventStore;
    this.projection = config.projection;
    this.store = config.store;
    this.metadata = config.metadata;
    this.clock = config.clock;
    this.pollingIntervalMs = config.pollingIntervalMs ?? 100;
    this.checkpointIntervalMs = config.checkpointIntervalMs ?? 5000;
    this.jitterMs = config.jitterMs ?? 1000;
    this.eventFilter = config.eventFilter;
    this.batchSize = config.batchSize ?? 100;
    this.useSharedReader = config.useSharedReader ?? false;
    this.useIncrementalStore =
      typeof (this.projection as { applyToStore?: unknown }).applyToStore === 'function' &&
      typeof this.store.setByKey === 'function';

    // Build subscribed event types set for fast lookup
    this.subscribedEventTypes = new Set(config.metadata.subscribedEvents);
  }

  /**
   * Start the projection runner.
   *
   * Loads from checkpoint if available, then begins polling.
   */
  async start(): Promise<void> {
    if (this.running) {
      return;
    }

    // Load checkpoint
    const checkpointPosition = await this.store.load();
    if (checkpointPosition !== null) {
      this.currentPosition = checkpointPosition;
      this.lastCheckpointPosition = checkpointPosition;
      this.lastCheckpointTime = this.clock.now();
      this.projection.setState(this.store.get());
      this.stateDirty = false;
    }

    // Schedule first checkpoint
    this.scheduleNextCheckpoint();

    // Start polling
    this.running = true;
    if (!this.useSharedReader) {
      this.poll();
    }
  }

  /**
   * Stop the runner and persist final checkpoint.
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.running = false;

    // Cancel polling timer
    if (this.pollingTimer) {
      this.pollingTimer.cancel();
      this.pollingTimer = null;
    }

    // Persist final checkpoint
    await this.checkpoint();
  }

  /**
   * Get current status.
   */
  getStatus(): ProjectionRunnerStatus {
    return {
      name: this.metadata.name,
      kind: this.metadata.kind,
      running: this.running,
      currentPosition: this.currentPosition,
      eventsProcessed: this.eventsProcessed,
      errorsCount: this.errorsCount,
      lastCheckpointPosition: this.lastCheckpointPosition,
      lastCheckpointTime: this.lastCheckpointTime,
      nextCheckpointTime: this.nextCheckpointTime,
    };
  }

  /**
   * Get current position.
   */
  getCurrentPosition(): number {
    return this.currentPosition;
  }

  /**
   * Process a batch of events from a shared reader.
   */
  processBatch(events: StoredEvent[]): void {
    if (!this.running || events.length === 0) {
      return;
    }
    this.applyEvents(events);
  }

  /**
   * Trigger checkpoint logic if needed.
   */
  async checkpointIfNeeded(): Promise<void> {
    await this.maybeCheckpoint();
  }

  /**
   * Force a checkpoint now.
   */
  async forceCheckpoint(): Promise<void> {
    await this.checkpoint();
  }

  /**
   * Wait until caught up to the given position.
   *
   * @param targetPosition - Position to catch up to
   * @param timeoutMs - Maximum time to wait
   * @returns true if caught up, false if timed out
   */
  async waitForCatchUp(targetPosition: number, timeoutMs: number = 30000): Promise<boolean> {
    const startTime = this.clock.now();
    const realStartTime = Date.now();
    const isSimulatedClock = typeof (this.clock as { tick?: (ms: number) => void }).tick === 'function';

    while (this.currentPosition < targetPosition) {
      const elapsed = this.clock.now() - startTime;
      const realElapsed = Date.now() - realStartTime;
      if (elapsed > timeoutMs || realElapsed > timeoutMs) {
        return false;
      }

      // Process events immediately if we're behind
      if (this.running) {
        const catchUpBudget = Math.min(1000, timeoutMs);
        await this.processEvents(catchUpBudget);
      }

      const remaining = timeoutMs - (this.clock.now() - startTime);
      if (remaining <= 0) {
        return false;
      }

      if (isSimulatedClock) {
        await Promise.resolve();
      } else {
        // Small sleep to avoid tight loop, capped by remaining timeout.
        await this.clock.sleep(Math.min(10, remaining));
      }
    }

    return true;
  }

  /**
   * Poll loop - schedules itself recursively while running.
   */
  private poll(): void {
    if (!this.running) {
      return;
    }

    this.pollingTimer = this.clock.setTimeout(async () => {
      try {
        await this.processEvents();
        await this.maybeCheckpoint();
      } catch (error) {
        // Log error but continue polling
        console.error(`[Projection: ${this.metadata.name}] Poll error:`, error);
        this.errorsCount++;
      }

      // Schedule next poll
      this.poll();
    }, this.pollingIntervalMs);
  }

  /**
   * Process available events.
   */
  private async processEvents(maxDurationMs?: number): Promise<void> {
    const durationBudget = maxDurationMs ?? this.pollingIntervalMs;
    const startTime = this.clock.now();
    const realStartTime = Date.now();

    // CRITICAL: Use durable position to prevent checkpointing beyond flushed events.
    // This prevents state drift on crash - projections can only see events that
    // have been safely written to disk.
    while (this.currentPosition < this.eventStore.getDurableGlobalPosition()) {
      // Read only durable events - never process pending events that could be lost
      const events = await this.eventStore.readGlobalDurable(this.currentPosition, {
        maxCount: this.batchSize,
      });

      if (events.length === 0) {
        break;
      }

      this.applyEvents(events);

      if (!this.running) {
        break;
      }

      const elapsed = this.clock.now() - startTime;
      const realElapsed = Date.now() - realStartTime;
      if (elapsed >= durationBudget || realElapsed >= durationBudget) {
        break;
      }

      if (events.length < this.batchSize) {
        break;
      }
    }

    this.commitStateIfNeeded();
  }

  private applyEvents(events: StoredEvent[]): void {
    let updated = false;

    for (const event of events) {
      // Skip events we've already processed
      if (event.globalPosition <= this.currentPosition) {
        continue;
      }

      // Check if this projection subscribes to this event type
      // '*' is a wildcard that matches all event types
      const subscribesAll = this.subscribedEventTypes.has('*');
      if (!subscribesAll && !this.subscribedEventTypes.has(event.type)) {
        this.currentPosition = event.globalPosition;
        continue;
      }

      // Apply custom filter if provided
      if (this.eventFilter && !this.eventFilter(event)) {
        this.currentPosition = event.globalPosition;
        continue;
      }

      // Process the event
      try {
        this.projection.build(event);
        if (this.useIncrementalStore) {
          (this.projection as Projection).applyToStore?.(event, this.store);
        }
        this.eventsProcessed++;
        this.currentPosition = event.globalPosition;
        updated = true;
      } catch (error) {
        this.errorsCount++;
        throw new ProjectionBuildError(
          this.metadata.name,
          event.type,
          event.globalPosition,
          error instanceof Error ? error : new Error(String(error))
        );
      }
    }

    if (updated && !this.useIncrementalStore) {
      this.stateDirty = true;
    }
  }

  private commitStateIfNeeded(): void {
    if (!this.useIncrementalStore && this.stateDirty) {
      this.store.set(this.projection.getState());
      this.stateDirty = false;
    }
  }

  /**
   * Checkpoint if it's time.
   */
  private async maybeCheckpoint(): Promise<void> {
    const now = this.clock.now();

    if (now >= this.nextCheckpointTime) {
      await this.checkpoint();
    }
  }

  /**
   * Persist checkpoint.
   */
  private async checkpoint(): Promise<void> {
    if (this.currentPosition === this.lastCheckpointPosition) {
      // Nothing changed, skip checkpoint
      this.scheduleNextCheckpoint();
      return;
    }

    try {
      this.commitStateIfNeeded();
      await this.store.persist(this.currentPosition);
      this.lastCheckpointPosition = this.currentPosition;
      this.lastCheckpointTime = this.clock.now();
    } catch (error) {
      console.error(`[Projection: ${this.metadata.name}] Checkpoint error:`, error);
      this.errorsCount++;
    }

    this.scheduleNextCheckpoint();
  }

  /**
   * Schedule next checkpoint with jitter.
   */
  private scheduleNextCheckpoint(): void {
    const now = this.clock.now();
    const jitter = (Math.random() * 2 - 1) * this.jitterMs;
    this.nextCheckpointTime = now + this.checkpointIntervalMs + jitter;
  }
}
