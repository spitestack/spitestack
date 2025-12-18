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
/**
 * Internal storage for the projection definition.
 * Used by the worker to retrieve the definition after importing the module.
 */
let __currentDefinition = null;
/**
 * Retrieves the current projection definition.
 * Called by the worker after importing the projection module.
 * @internal
 */
export function __getProjectionDefinition() {
    return __currentDefinition;
}
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
export function defineProjection(modulePath, definition) {
    // Store the definition for the worker to retrieve
    __currentDefinition = definition;
    // Return the module path as a branded string
    // The path is passed by the user as import.meta.path
    return modulePath;
}
