/**
 * Projection Runner
 *
 * Manages projection workers. Each projection runs in its own Bun Worker thread
 * with `smol: true` for minimal memory footprint.
 *
 * ## Architecture
 *
 * ```
 * Main Thread
 *     │
 *     └── ProjectionRunner.load('./projections/user-stats.ts')
 *             │
 *             └── spawns Worker with module path
 *
 * Worker "user_stats" (smol: true)
 *     ├── imports projection module
 *     ├── validates module exports
 *     ├── loads NAPI bindings
 *     ├── connects to event store
 *     ├── owns projection DB (user_stats.db)
 *     ├── polls for events
 *     ├── runs apply() function
 *     └── writes to its own SQLite
 * ```
 *
 * ## Key Benefits
 *
 * - **True parallelism**: Each projection runs in its own thread
 * - **Isolation**: One slow/failing projection doesn't affect others
 * - **Memory efficient**: smol workers use ~2MB stack instead of 8MB
 * - **Auto-recovery**: Workers restart on crash with exponential backoff
 */
import type { ProjectionModule } from './define';
/**
 * Configuration for the projection runner.
 */
export interface RunnerConfig {
    /** Path to the event store database file */
    eventStorePath: string;
    /** Directory where projection databases will be stored */
    projectionsDir: string;
}
/**
 * Projection runner that manages worker threads for projections.
 *
 * Each projection runs in its own worker thread, providing true parallelism
 * and isolation. Workers are spawned with `smol: true` for memory efficiency.
 *
 * @example
 * ```typescript
 * import { ProjectionRunner } from 'spitedb';
 * import userStats from './projections/user-stats';
 * import orderTotals from './projections/order-totals';
 *
 * const runner = new ProjectionRunner({
 *   eventStorePath: './events.db',
 *   projectionsDir: './projections-data',
 * });
 *
 * await runner.load(userStats);
 * await runner.load(orderTotals);
 * await runner.startAll();
 *
 * // Later, to stop all projections:
 * runner.stopAll();
 * ```
 */
export declare class ProjectionRunner {
    private config;
    private workers;
    private modulePaths;
    /**
     * Creates a new projection runner.
     *
     * @param config - Runner configuration including paths
     */
    constructor(config: RunnerConfig);
    /**
     * Loads a projection module.
     *
     * The projection must be defined using `defineProjection()` and export default.
     * This method does not start the worker - call `startAll()` to start processing.
     *
     * @param module - The projection module path (from `defineProjection()`)
     *
     * @example
     * ```typescript
     * import userStats from './projections/user-stats';
     * await runner.load(userStats);
     * ```
     */
    load(module: ProjectionModule): Promise<void>;
    /**
     * Starts all loaded projections.
     *
     * Each projection runs in its own worker thread. Workers that crash will be
     * automatically restarted with exponential backoff.
     */
    startAll(): Promise<void>;
    /**
     * Spawns a worker for a projection module.
     * If existingState is provided, preserves restart tracking state.
     */
    private spawnWorker;
    /**
     * Handles a worker crash by restarting with exponential backoff.
     */
    private handleWorkerCrash;
    /**
     * Stops a specific projection worker.
     *
     * @param modulePath - The module path of the projection to stop
     */
    stop(modulePath: string): void;
    /**
     * Stops all projection workers.
     */
    stopAll(): void;
    /**
     * Returns whether a projection is running.
     *
     * @param modulePath - The module path to check
     */
    isRunning(modulePath: string): boolean;
    /**
     * Returns the list of loaded projection module paths.
     */
    getLoadedModules(): string[];
    /**
     * Returns the number of running workers.
     */
    getWorkerCount(): number;
}
//# sourceMappingURL=runner.d.ts.map