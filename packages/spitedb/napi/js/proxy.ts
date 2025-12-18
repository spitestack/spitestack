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

import type { OpType, ProjectionOp, RowData } from './types';

/** Native binding interface for projection operations */
export interface NativeBinding {
  /** Reads a row by key, returns JSON string or null */
  readProjectionRow(projectionName: string, key: string): string | null;
}

/**
 * Operation queued by the proxy.
 */
interface QueuedOp {
  opType: OpType;
  key: string;
  value?: RowData;
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
export function createProjectionProxy<TRow extends RowData>(
  projectionName: string,
  primaryKeyColumn: string,
  native: NativeBinding
): { proxy: Record<string, TRow | undefined>; flush: () => ProjectionOp[] } {
  // Operation queue - keyed by primary key for deduplication
  const ops = new Map<string, QueuedOp>();

  // Cache for read values within this batch
  const readCache = new Map<string, TRow | null>();

  /**
   * Reads a row from the database.
   * Results are cached for the duration of the batch.
   */
  function readRow(key: string): TRow | null {
    // Check cache first
    if (readCache.has(key)) {
      return readCache.get(key) ?? null;
    }

    // Check if we have a pending upsert for this key
    const pendingOp = ops.get(key);
    if (pendingOp?.opType === 'upsert' && pendingOp.value) {
      const row = { ...pendingOp.value, [primaryKeyColumn]: key } as TRow;
      readCache.set(key, row);
      return row;
    }
    if (pendingOp?.opType === 'delete') {
      readCache.set(key, null);
      return null;
    }

    // Read from database
    const json = native.readProjectionRow(projectionName, key);
    if (json === null) {
      readCache.set(key, null);
      return null;
    }

    const row = JSON.parse(json) as TRow;
    readCache.set(key, row);
    return row;
  }

  /**
   * Queues an upsert operation.
   * Merges with any existing pending operation for the same key.
   */
  function queueUpsert(key: string, value: RowData): void {
    ops.set(key, { opType: 'upsert', key, value });
    // Update cache
    readCache.set(key, { ...value, [primaryKeyColumn]: key } as TRow);
  }

  /**
   * Queues a delete operation.
   */
  function queueDelete(key: string): void {
    ops.set(key, { opType: 'delete', key });
    // Update cache
    readCache.set(key, null);
  }

  /**
   * Creates a row proxy that intercepts property access/mutation.
   * This enables `table[key].field++` syntax.
   */
  function createRowProxy(key: string, initialRow: TRow): TRow {
    // Clone the row so mutations don't affect the original
    const row = { ...initialRow };

    return new Proxy(row, {
      get(target, prop) {
        if (typeof prop === 'symbol') {
          return Reflect.get(target, prop);
        }
        return target[prop as keyof TRow];
      },

      set(target, prop, value) {
        if (typeof prop === 'symbol') {
          return Reflect.set(target, prop, value);
        }

        // Update the row
        (target as Record<string, unknown>)[prop] = value;

        // Queue an upsert with the updated row
        const rowWithoutPk = { ...target };
        delete (rowWithoutPk as Record<string, unknown>)[primaryKeyColumn];
        queueUpsert(key, rowWithoutPk);

        return true;
      },
    });
  }

  // The main table proxy
  const proxy = new Proxy({} as Record<string, TRow | undefined>, {
    get(_target, prop) {
      if (typeof prop === 'symbol') {
        return undefined;
      }

      const key = prop as string;
      const row = readRow(key);

      if (row === null) {
        return undefined;
      }

      // Return a row proxy so mutations can be tracked
      return createRowProxy(key, row);
    },

    set(_target, prop, value) {
      if (typeof prop === 'symbol') {
        return false;
      }

      const key = prop as string;

      if (value === undefined) {
        // Treat `table[key] = undefined` as delete
        queueDelete(key);
      } else {
        // Queue upsert
        queueUpsert(key, value as RowData);
      }

      return true;
    },

    deleteProperty(_target, prop) {
      if (typeof prop === 'symbol') {
        return false;
      }

      const key = prop as string;
      queueDelete(key);
      return true;
    },

    has(_target, prop) {
      if (typeof prop === 'symbol') {
        return false;
      }

      const key = prop as string;
      return readRow(key) !== null;
    },
  });

  /**
   * Flushes all queued operations and resets the queue.
   * Returns operations in the format expected by the Rust layer.
   */
  function flush(): ProjectionOp[] {
    const operations: ProjectionOp[] = [];

    for (const op of ops.values()) {
      operations.push({
        opType: op.opType,
        key: op.key,
        value: op.value ? JSON.stringify(op.value) : undefined,
      });
    }

    // Clear the queue and cache for next batch
    ops.clear();
    readCache.clear();

    return operations;
  }

  return { proxy, flush };
}
