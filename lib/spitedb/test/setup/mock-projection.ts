/**
 * Mock projection implementations for testing.
 *
 * Provides configurable mock projections that track their behavior
 * for assertions in tests.
 *
 * @example
 * ```ts
 * const projection = new MockAggregatorProjection(['OrderCreated']);
 *
 * // Process events
 * projection.build({ type: 'OrderCreated', ... });
 *
 * // Assert behavior
 * expect(projection.getProcessedEvents()).toHaveLength(1);
 * expect(projection.getState()).toBe(1);
 * ```
 */

import type { Projection, ProjectionKind, ProjectionMetadata, AccessPattern } from '../../src/ports/projections';
import type { ProjectionRegistration } from '../../src/ports/projections';
import type { StoredEvent } from '../../src/domain/events/stored-event';

/**
 * Mock aggregator projection that counts events.
 *
 * Tracks processed events for test assertions.
 */
export class MockAggregatorProjection implements Projection<number> {
  private count = 0;
  private processedEvents: StoredEvent[] = [];
  private buildError: Error | null = null;

  constructor(private readonly subscribedEvents: string[] = ['*']) {}

  /**
   * Process an event by incrementing count.
   */
  build(event: StoredEvent): void {
    if (this.buildError) {
      throw this.buildError;
    }

    // Only process subscribed events (or all if '*')
    if (
      this.subscribedEvents.includes('*') ||
      this.subscribedEvents.includes(event.type)
    ) {
      this.count++;
      this.processedEvents.push(event);
    }
  }

  /**
   * Get the current count.
   */
  getState(): number {
    return this.count;
  }

  /**
   * Set the count from checkpoint.
   */
  setState(state: number): void {
    this.count = state;
  }

  /**
   * Reset to zero.
   */
  reset(): void {
    this.count = 0;
    this.processedEvents = [];
  }

  // === Test helpers ===

  /**
   * Get all events that were processed.
   */
  getProcessedEvents(): StoredEvent[] {
    return [...this.processedEvents];
  }

  /**
   * Get count of processed events.
   */
  getProcessedCount(): number {
    return this.processedEvents.length;
  }

  /**
   * Inject an error to be thrown on next build.
   */
  injectBuildError(error: Error): void {
    this.buildError = error;
  }

  /**
   * Clear injected build error.
   */
  clearBuildError(): void {
    this.buildError = null;
  }
}

/**
 * Mock denormalized view projection with key-value storage.
 *
 * Extracts ID from event data and stores the event data by ID.
 */
export class MockViewProjection implements Projection<Map<string, unknown>> {
  private items = new Map<string, unknown>();
  private processedEvents: StoredEvent[] = [];
  private buildError: Error | null = null;
  private idField: string;

  constructor(
    private readonly subscribedEvents: string[] = ['*'],
    idField: string = 'id'
  ) {
    this.idField = idField;
  }

  /**
   * Process an event by storing its data.
   */
  build(event: StoredEvent): void {
    if (this.buildError) {
      throw this.buildError;
    }

    if (
      this.subscribedEvents.includes('*') ||
      this.subscribedEvents.includes(event.type)
    ) {
      const data = event.data as Record<string, unknown>;
      const id = data[this.idField] as string;

      if (id) {
        // Check for delete events
        if (event.type.endsWith('Deleted')) {
          this.items.delete(id);
        } else {
          this.items.set(id, data);
        }
      }

      this.processedEvents.push(event);
    }
  }

  /**
   * Get all items.
   */
  getState(): Map<string, unknown> {
    return new Map(this.items);
  }

  /**
   * Set all items from checkpoint.
   */
  setState(state: Map<string, unknown>): void {
    this.items = new Map(state);
  }

  /**
   * Clear all items.
   */
  reset(): void {
    this.items.clear();
    this.processedEvents = [];
  }

  // === Query methods (like a real projection would have) ===

  /**
   * Get item by ID.
   */
  getById(id: string): unknown | undefined {
    return this.items.get(id);
  }

  /**
   * Get all items.
   */
  getAll(): Map<string, unknown> {
    return new Map(this.items);
  }

  /**
   * Get item count.
   */
  getCount(): number {
    return this.items.size;
  }

  // === Test helpers ===

  /**
   * Get all events that were processed.
   */
  getProcessedEvents(): StoredEvent[] {
    return [...this.processedEvents];
  }

  /**
   * Inject an error to be thrown on next build.
   */
  injectBuildError(error: Error): void {
    this.buildError = error;
  }

  /**
   * Clear injected build error.
   */
  clearBuildError(): void {
    this.buildError = null;
  }
}

/**
 * Create a projection registration for testing.
 *
 * @param name - Projection name
 * @param kind - Projection kind
 * @param subscribedEvents - Events to subscribe to
 * @param accessPatterns - Optional access patterns
 * @returns ProjectionRegistration for use with coordinator
 */
export function createMockRegistration<TState>(
  name: string,
  kind: ProjectionKind,
  subscribedEvents: string[],
  projectionFactory: () => Projection<TState>,
  accessPatterns: AccessPattern[] = []
): ProjectionRegistration<TState> {
  const metadata: ProjectionMetadata = {
    name,
    kind,
    subscribedEvents,
    accessPatterns,
    checkpointIntervalMs: 100, // Fast checkpoints for tests
  };

  // Track the instance for getInstance
  let instance: Projection<TState> | null = null;

  return {
    metadata,
    factory: () => {
      instance = projectionFactory();
      return instance;
    },
    getInstance: () => instance,
  };
}

/**
 * Create a mock aggregator registration.
 */
export function createMockAggregatorRegistration(
  name: string,
  subscribedEvents: string[]
): ProjectionRegistration<number> {
  return createMockRegistration(
    name,
    'aggregator',
    subscribedEvents,
    () => new MockAggregatorProjection(subscribedEvents),
    [{ methodName: 'getState', indexFields: [] }]
  );
}

/**
 * Create a mock denormalized view registration.
 */
export function createMockViewRegistration(
  name: string,
  subscribedEvents: string[],
  indexFields: string[] = ['id']
): ProjectionRegistration<Map<string, unknown>> {
  return createMockRegistration(
    name,
    'denormalized_view',
    subscribedEvents,
    () => new MockViewProjection(subscribedEvents),
    [
      { methodName: 'getById', indexFields },
      { methodName: 'getAll', indexFields: [] },
    ]
  );
}

/**
 * A projection that records timing information.
 * Useful for testing checkpoint scheduling.
 */
export class TimingRecordingProjection implements Projection<{ count: number; timestamps: number[] }> {
  private count = 0;
  private timestamps: number[] = [];

  constructor(private readonly clock: { now(): number }) {}

  build(_event: StoredEvent): void {
    this.count++;
    this.timestamps.push(this.clock.now());
  }

  getState(): { count: number; timestamps: number[] } {
    return { count: this.count, timestamps: [...this.timestamps] };
  }

  setState(state: { count: number; timestamps: number[] }): void {
    this.count = state.count;
    this.timestamps = [...state.timestamps];
  }

  reset(): void {
    this.count = 0;
    this.timestamps = [];
  }

  getTimestamps(): number[] {
    return [...this.timestamps];
  }
}

/**
 * A projection that can be paused/resumed.
 * Useful for testing waitForCatchUp.
 */
export class PausableProjection implements Projection<number> {
  private count = 0;
  private paused = false;
  private pausePromiseResolve: (() => void) | null = null;

  build(_event: StoredEvent): void {
    if (this.paused && this.pausePromiseResolve) {
      // Block until resumed
      throw new Error('Projection is paused');
    }
    this.count++;
  }

  getState(): number {
    return this.count;
  }

  setState(state: number): void {
    this.count = state;
  }

  reset(): void {
    this.count = 0;
  }

  pause(): void {
    this.paused = true;
  }

  resume(): void {
    this.paused = false;
    if (this.pausePromiseResolve) {
      this.pausePromiseResolve();
      this.pausePromiseResolve = null;
    }
  }

  isPaused(): boolean {
    return this.paused;
  }
}
