/**
 * SpiteDB Projection System
 *
 * Provides a delightful API for building projections (read models) from event streams.
 * Each projection runs in its own worker thread for true parallelism.
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
 *     total_spent: 'real',
 *   },
 *   apply(event, table) {
 *     const data = JSON.parse(event.data.toString());
 *
 *     if (data.type === 'UserCreated') {
 *       table[event.streamId] = { login_count: 0, total_spent: 0 };
 *     }
 *
 *     if (data.type === 'UserLoggedIn') {
 *       table[event.streamId].login_count++;
 *     }
 *
 *     if (data.type === 'Purchase') {
 *       table[event.streamId].total_spent += data.amount;
 *     }
 *
 *     if (data.type === 'UserDeleted') {
 *       delete table[event.streamId];
 *     }
 *   }
 * });
 * ```
 *
 * @example
 * ```typescript
 * // main.ts
 * import { ProjectionRunner } from 'spitedb';
 * import userStats from './projections/user-stats';
 *
 * const runner = new ProjectionRunner({
 *   eventStorePath: './events.db',
 *   projectionsDir: './projections-data',
 * });
 *
 * await runner.load(userStats);
 * await runner.startAll();
 * ```
 */

// Types
export type {
  ColumnDef,
  ColumnType,
  ErrorStrategy,
  Projection,
  ProjectionEvent,
  ProjectionOptions,
  ProjectionTable,
  RowData,
  SchemaDefinition,
  SchemaToRow,
} from './types';

// Define projection helper and branded type (for worker-based projections)
export { defineProjection, type ProjectionModule, type ProjectionDefinition } from './define';

// Proxy for magic table access
export { createProjectionProxy } from './proxy';

// Runner for managing projection workers
export { ProjectionRunner, type RunnerConfig } from './runner';

import type { Projection, ProjectionOptions, SchemaDefinition } from './types';

/**
 * Creates a projection definition (legacy API).
 *
 * For worker-based projections with true parallelism, use `defineProjection()` instead.
 *
 * @param name - Unique name for the projection
 * @param options - Projection configuration
 * @returns A projection definition
 *
 * @deprecated Use `defineProjection()` for worker-based projections
 */
export function projection<TSchema extends SchemaDefinition>(
  name: string,
  options: ProjectionOptions<TSchema>
): Projection<TSchema> {
  return {
    name,
    options,
  };
}
