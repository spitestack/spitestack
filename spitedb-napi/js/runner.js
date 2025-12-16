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
/// <reference types="bun-types" />
import { Worker } from 'worker_threads';
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
export class ProjectionRunner {
    config;
    workers = new Map();
    modulePaths = new Set();
    /**
     * Creates a new projection runner.
     *
     * @param config - Runner configuration including paths
     */
    constructor(config) {
        this.config = config;
    }
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
    async load(module) {
        if (!module || typeof module !== 'string') {
            throw new Error(`Invalid projection module. Expected a module path from defineProjection().\n\n` +
                `Usage:\n` +
                `  // projections/my-projection.ts\n` +
                `  export default defineProjection(import.meta.path, { ... });\n\n` +
                `  // main.ts\n` +
                `  import myProjection from './projections/my-projection';\n` +
                `  await runner.load(myProjection);`);
        }
        this.modulePaths.add(module);
    }
    /**
     * Starts all loaded projections.
     *
     * Each projection runs in its own worker thread. Workers that crash will be
     * automatically restarted with exponential backoff.
     */
    async startAll() {
        const promises = [];
        for (const modulePath of this.modulePaths) {
            promises.push(this.spawnWorker(modulePath));
        }
        await Promise.all(promises);
    }
    /**
     * Spawns a worker for a projection module.
     */
    async spawnWorker(modulePath) {
        // Get the path to worker.ts relative to this file
        const workerPath = new URL('./worker.ts', import.meta.url).pathname;
        const worker = new Worker(workerPath, {
            // @ts-expect-error - Bun-specific option for smaller stack size
            smol: true,
            workerData: {
                modulePath,
                eventStorePath: this.config.eventStorePath,
                projectionsDir: this.config.projectionsDir,
            },
        });
        const state = {
            worker,
            modulePath,
            restartCount: 0,
            lastCrash: 0,
            terminating: false,
        };
        // Handle worker errors
        worker.on('error', (error) => {
            if (state.terminating)
                return;
            console.error(`[ProjectionRunner] Worker crashed for ${modulePath}:`, error.message);
            this.handleWorkerCrash(modulePath);
        });
        // Handle worker exit
        worker.on('exit', (code) => {
            if (state.terminating)
                return;
            if (code !== 0) {
                console.error(`[ProjectionRunner] Worker exited with code ${code} for ${modulePath}`);
                this.handleWorkerCrash(modulePath);
            }
        });
        this.workers.set(modulePath, state);
    }
    /**
     * Handles a worker crash by restarting with exponential backoff.
     */
    async handleWorkerCrash(modulePath) {
        const state = this.workers.get(modulePath);
        if (!state || state.terminating)
            return;
        const now = Date.now();
        const timeSinceLastCrash = now - state.lastCrash;
        // Reset restart count if it's been more than 60 seconds since last crash
        if (timeSinceLastCrash > 60000) {
            state.restartCount = 0;
        }
        state.restartCount++;
        state.lastCrash = now;
        // Exponential backoff: 100ms, 200ms, 400ms, 800ms, ... up to 30s
        const backoff = Math.min(100 * Math.pow(2, state.restartCount - 1), 30000);
        console.log(`[ProjectionRunner] Restarting ${modulePath} in ${backoff}ms (attempt ${state.restartCount})`);
        // Wait for backoff period
        await new Promise((resolve) => setTimeout(resolve, backoff));
        // Check if we've been stopped during the backoff
        if (state.terminating || !this.workers.has(modulePath)) {
            return;
        }
        // Respawn the worker
        await this.spawnWorker(modulePath);
    }
    /**
     * Stops a specific projection worker.
     *
     * @param modulePath - The module path of the projection to stop
     */
    stop(modulePath) {
        const state = this.workers.get(modulePath);
        if (state) {
            state.terminating = true;
            state.worker.terminate();
            this.workers.delete(modulePath);
        }
    }
    /**
     * Stops all projection workers.
     */
    stopAll() {
        for (const state of this.workers.values()) {
            state.terminating = true;
            state.worker.terminate();
        }
        this.workers.clear();
    }
    /**
     * Returns whether a projection is running.
     *
     * @param modulePath - The module path to check
     */
    isRunning(modulePath) {
        const state = this.workers.get(modulePath);
        return state !== undefined && !state.terminating;
    }
    /**
     * Returns the list of loaded projection module paths.
     */
    getLoadedModules() {
        return Array.from(this.modulePaths);
    }
    /**
     * Returns the number of running workers.
     */
    getWorkerCount() {
        return this.workers.size;
    }
}
