/**
 * Default implementation of ProjectionRegistry.
 */

import type {
  ProjectionRegistry,
  ProjectionRegistration,
  ProjectionRuntimeOptions,
  ResolvedRegistration,
} from '../../ports/projections';

/**
 * Default implementation of ProjectionRegistry.
 */
export class DefaultProjectionRegistry implements ProjectionRegistry {
  private readonly registrations = new Map<string, ResolvedRegistration>();

  private readonly defaultOptions: Required<Omit<ProjectionRuntimeOptions, 'eventFilter'>> = {
    checkpointIntervalMs: 5000,
    memoryThresholdBytes: 50 * 1024 * 1024, // 50MB
    enabled: true,
  };

  register<TState>(
    registration: ProjectionRegistration<TState>,
    options: ProjectionRuntimeOptions = {}
  ): void {
    const { name } = registration.metadata;

    if (this.registrations.has(name)) {
      throw new Error(`Projection '${name}' is already registered`);
    }

    const resolved: ResolvedRegistration<TState> = {
      registration,
      options: {
        checkpointIntervalMs:
          options.checkpointIntervalMs ??
          registration.metadata.checkpointIntervalMs ??
          this.defaultOptions.checkpointIntervalMs,
        memoryThresholdBytes:
          options.memoryThresholdBytes ?? this.defaultOptions.memoryThresholdBytes,
        enabled: options.enabled ?? this.defaultOptions.enabled,
        eventFilter: options.eventFilter,
      },
    };

    this.registrations.set(name, resolved as ResolvedRegistration);
  }

  getAll(): ResolvedRegistration[] {
    return Array.from(this.registrations.values());
  }

  get(name: string): ResolvedRegistration | undefined {
    return this.registrations.get(name);
  }

  has(name: string): boolean {
    return this.registrations.has(name);
  }

  count(): number {
    return this.registrations.size;
  }

  clear(): void {
    this.registrations.clear();
  }
}
