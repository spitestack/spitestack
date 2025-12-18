/**
 * Magic Proxy for Projection Tables
 *
 * Provides a delightful DX for interacting with projection state:
 *
 * ```typescript
 * // Read a row
 * const user = table[userId];
 *
 * // Create/update a row
 * table[userId] = { loginCount: 0, totalSpent: 0 };
 *
 * // Increment a field (read + modify)
 * table[userId].loginCount++;
 *
 * // Delete a row
 * delete table[userId];
 * ```
 */
import type { ProjectionOp, RowData } from './types';
/** Native binding interface for projection operations */
export interface NativeBinding {
    /** Reads a row by key, returns JSON string or null */
    readProjectionRow(projectionName: string, key: string): string | null;
}
/**
 * Creates a magic proxy for projection table access.
 *
 * The proxy intercepts property access to provide seamless read/write operations:
 * - `table[key]` reads from the database (synchronously, cached)
 * - `table[key] = value` queues an upsert operation
 * - `table[key].field++` reads, modifies, and queues an upsert
 * - `delete table[key]` queues a delete operation
 *
 * Call `flush()` to get all queued operations and reset the queue.
 */
export declare function createProjectionProxy<TRow extends RowData>(projectionName: string, primaryKeyColumn: string, native: NativeBinding): {
    proxy: Record<string, TRow | undefined>;
    flush: () => ProjectionOp[];
};
//# sourceMappingURL=proxy.d.ts.map