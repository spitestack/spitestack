/**
 * Projection Definition Helper
 *
 * Provides the `defineProjection()` function for creating projection modules
 * that run in worker threads.
 *
 * Each projection must be defined in its own file and export default using
 * `defineProjection()`. This ensures the apply function runs in the worker's
 * isolate rather than the main thread.
 *
 * @example
 * ```typescript
 * // projections/user-stats.ts
 * import { defineProjection } from 'spitedb';
 *
 * export default defineProjection(import.meta.path, {
 *   name: 'user_stats',
 *   schema: {
 *     user_id: { type: 'text', primaryKey: true },
 *     login_count: 'integer',
 *   },
 *   apply(event, table) {
 *     const data = JSON.parse(event.data.toString());
 *     if (data.type === 'UserLoggedIn') {
 *       const row = table[event.streamId] ?? { login_count: 0 };
 *       row.login_count++;
 *       table[event.streamId] = row;
 *     }
 *   },
 * });
 * ```
 */
import type { ErrorStrategy, ProjectionEvent, ProjectionTable, SchemaDefinition, SchemaToRow } from './types';
/**
 * Branded type for projection module paths.
 * Only `defineProjection()` can create valid `ProjectionModule` values.
 */
declare const __brand: unique symbol;
export type ProjectionModule = string & {
    [__brand]: 'ProjectionModule';
};
/**
 * Configuration for a projection definition.
 */
export interface ProjectionDefinition<TSchema extends SchemaDefinition = SchemaDefinition> {
    /**
     * Unique name for the projection.
     * This becomes the SQLite database filename: `{projectionsDir}/{name}.db`
     */
    name: string;
    /**
     * Schema definition for the projection table.
     * One column must be marked as primaryKey.
     */
    schema: TSchema;
    /**
     * Event handler function.
     * Called for each event in the batch.
     * Use the table proxy to read/write projection state.
     *
     * This function runs IN the worker thread, not the main thread.
     */
    apply: (event: ProjectionEvent, table: ProjectionTable<SchemaToRow<TSchema>>) => void | Promise<void>;
    /**
     * Error handler (optional).
     * Called when apply() throws an error.
     * Returns the strategy: 'skip' to skip the event, 'retry' to retry once, 'stop' to halt.
     * Default: 'stop'
     */
    onError?: (error: Error, event: ProjectionEvent) => ErrorStrategy;
    /**
     * Batch size for processing events.
     * Higher values reduce database round-trips but increase memory usage.
     * Default: 100
     */
    batchSize?: number;
    /**
     * Poll interval in milliseconds when no events are available.
     * Lower values reduce latency but increase CPU usage.
     * Default: 50
     */
    pollIntervalMs?: number;
}
/**
 * Retrieves the current projection definition.
 * Called by the worker after importing the projection module.
 * @internal
 */
export declare function __getProjectionDefinition(): ProjectionDefinition | null;
/**
 * Defines a projection that will run in a worker thread.
 *
 * Each projection must be in its own file and export default using this function.
 * Pass `import.meta.path` as the first argument to capture the module path.
 *
 * @param modulePath - The module path (use `import.meta.path`)
 * @param definition - The projection configuration
 * @returns A branded module path that can be passed to `ProjectionRunner.load()`
 *
 * @example
 * ```typescript
 * // projections/order-totals.ts
 * import { defineProjection } from 'spitedb';
 *
 * export default defineProjection(import.meta.path, {
 *   name: 'order_totals',
 *   schema: {
 *     order_id: { type: 'text', primaryKey: true },
 *     total: 'real',
 *     item_count: 'integer',
 *   },
 *   apply(event, table) {
 *     const data = JSON.parse(event.data.toString());
 *     if (data.type === 'ItemAdded') {
 *       const row = table[data.orderId] ?? { total: 0, item_count: 0 };
 *       row.total += data.price;
 *       row.item_count++;
 *       table[data.orderId] = row;
 *     }
 *   },
 * });
 * ```
 *
 * @example
 * ```typescript
 * // main.ts
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
 * ```
 */
export declare function defineProjection<TSchema extends SchemaDefinition>(modulePath: string, definition: ProjectionDefinition<TSchema>): ProjectionModule;
export {};
//# sourceMappingURL=define.d.ts.map