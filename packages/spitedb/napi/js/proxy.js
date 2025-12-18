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
export function createProjectionProxy(projectionName, primaryKeyColumn, native) {
    // Operation queue - keyed by primary key for deduplication
    const ops = new Map();
    // Cache for read values within this batch
    const readCache = new Map();
    /**
     * Reads a row from the database.
     * Results are cached for the duration of the batch.
     */
    function readRow(key) {
        // Check cache first
        if (readCache.has(key)) {
            return readCache.get(key) ?? null;
        }
        // Check if we have a pending upsert for this key
        const pendingOp = ops.get(key);
        if (pendingOp?.opType === 'upsert' && pendingOp.value) {
            const row = { ...pendingOp.value, [primaryKeyColumn]: key };
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
        const row = JSON.parse(json);
        readCache.set(key, row);
        return row;
    }
    /**
     * Queues an upsert operation.
     * Merges with any existing pending operation for the same key.
     */
    function queueUpsert(key, value) {
        ops.set(key, { opType: 'upsert', key, value });
        // Update cache
        readCache.set(key, { ...value, [primaryKeyColumn]: key });
    }
    /**
     * Queues a delete operation.
     */
    function queueDelete(key) {
        ops.set(key, { opType: 'delete', key });
        // Update cache
        readCache.set(key, null);
    }
    /**
     * Creates a row proxy that intercepts property access/mutation.
     * This enables `table[key].field++` syntax.
     */
    function createRowProxy(key, initialRow) {
        // Clone the row so mutations don't affect the original
        const row = { ...initialRow };
        return new Proxy(row, {
            get(target, prop) {
                if (typeof prop === 'symbol') {
                    return Reflect.get(target, prop);
                }
                return target[prop];
            },
            set(target, prop, value) {
                if (typeof prop === 'symbol') {
                    return Reflect.set(target, prop, value);
                }
                // Update the row
                target[prop] = value;
                // Queue an upsert with the updated row
                const rowWithoutPk = { ...target };
                delete rowWithoutPk[primaryKeyColumn];
                queueUpsert(key, rowWithoutPk);
                return true;
            },
        });
    }
    // The main table proxy
    const proxy = new Proxy({}, {
        get(_target, prop) {
            if (typeof prop === 'symbol') {
                return undefined;
            }
            const key = prop;
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
            const key = prop;
            if (value === undefined) {
                // Treat `table[key] = undefined` as delete
                queueDelete(key);
            }
            else {
                // Queue upsert
                queueUpsert(key, value);
            }
            return true;
        },
        deleteProperty(_target, prop) {
            if (typeof prop === 'symbol') {
                return false;
            }
            const key = prop;
            queueDelete(key);
            return true;
        },
        has(_target, prop) {
            if (typeof prop === 'symbol') {
                return false;
            }
            const key = prop;
            return readRow(key) !== null;
        },
    });
    /**
     * Flushes all queued operations and resets the queue.
     * Returns operations in the format expected by the Rust layer.
     */
    function flush() {
        const operations = [];
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
