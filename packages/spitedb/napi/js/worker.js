/**
 * Projection Worker
 *
 * Entry point for projection worker threads.
 * Each worker is fully autonomous - it owns its projection database,
 * polls for events, runs the apply function, and writes results.
 *
 * Workers are spawned by ProjectionRunner.startAll().
 */
/// <reference types="bun-types" />
import { workerData } from 'worker_threads';
import { __getProjectionDefinition } from './define';
import { createProjectionProxy } from './proxy';
/**
 * Converts a schema definition to NAPI column definitions.
 */
function schemaToColumnDefs(schema) {
    const columns = [];
    for (const [name, def] of Object.entries(schema)) {
        if (typeof def === 'string') {
            // Simple type definition
            columns.push({
                name,
                colType: def,
                primaryKey: false,
                nullable: true,
                defaultValue: undefined,
            });
        }
        else {
            // Full definition
            columns.push({
                name,
                colType: def.type,
                primaryKey: def.primaryKey ?? false,
                nullable: def.nullable ?? true,
                defaultValue: def.defaultValue !== undefined
                    ? JSON.stringify(def.defaultValue)
                    : undefined,
            });
        }
    }
    return columns;
}
/**
 * Finds the primary key column name from a schema.
 */
function findPrimaryKey(schema) {
    for (const [name, def] of Object.entries(schema)) {
        if (typeof def === 'object' && def.primaryKey) {
            return name;
        }
    }
    // Default to first column if no explicit primary key
    return Object.keys(schema)[0];
}
/**
 * Converts a NAPI event to a ProjectionEvent.
 */
function napiEventToProjectionEvent(event) {
    return {
        globalPos: BigInt(event.globalPos),
        streamId: event.streamId,
        streamRev: BigInt(event.streamRev),
        timestampMs: BigInt(event.timestampMs),
        data: event.data,
    };
}
/**
 * Main worker function.
 */
async function main() {
    const config = workerData;
    // Import the projection module
    // This will execute defineProjection() and populate __currentDefinition
    await import(config.modulePath);
    // Retrieve the projection definition
    const projection = __getProjectionDefinition();
    // Validate the projection
    if (!projection) {
        throw new Error(`Projection module must use defineProjection() and export default.\n` +
            `File: ${config.modulePath}\n\n` +
            `Example:\n` +
            `  import { defineProjection } from 'spitedb';\n\n` +
            `  export default defineProjection(import.meta.path, {\n` +
            `    name: 'my_projection',\n` +
            `    schema: { id: { type: 'text', primaryKey: true }, ... },\n` +
            `    apply(event, table) { ... }\n` +
            `  });`);
    }
    if (typeof projection.apply !== 'function') {
        throw new Error(`Projection "${projection.name}" missing apply function.\n` +
            `defineProjection() requires an apply(event, table) function.`);
    }
    if (!projection.schema) {
        throw new Error(`Projection "${projection.name}" missing schema.\n` +
            `defineProjection() requires a schema definition.`);
    }
    if (!projection.name) {
        throw new Error(`Projection missing name.\n` +
            `defineProjection() requires a name property.`);
    }
    // Load NAPI bindings
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { SpiteDBNapi } = require('../index.js');
    // Open the event store
    const db = await SpiteDBNapi.open(config.eventStorePath);
    // Initialize this projection's database
    await db.initProjections(config.projectionsDir);
    await db.registerProjection(projection.name, schemaToColumnDefs(projection.schema));
    const batchSize = projection.batchSize ?? 100;
    const pollInterval = projection.pollIntervalMs ?? 50;
    const primaryKey = findPrimaryKey(projection.schema);
    console.log(`[Projection ${projection.name}] Worker started`);
    // Main processing loop
    while (true) {
        try {
            const batch = await db.getProjectionEvents(projection.name, batchSize);
            if (!batch || batch.events.length === 0) {
                // No events available, wait and poll again
                await Bun.sleep(pollInterval);
                continue;
            }
            // Create proxy for this batch
            const { proxy, flush } = createProjectionProxy(projection.name, primaryKey, db);
            let lastGlobalPos = batch.batchId;
            // Process each event
            for (const eventNapi of batch.events) {
                const event = napiEventToProjectionEvent(eventNapi);
                try {
                    await projection.apply(event, proxy);
                    lastGlobalPos = eventNapi.globalPos;
                }
                catch (error) {
                    // Handle error based on strategy
                    const strategy = projection.onError?.(error, event) ?? 'stop';
                    switch (strategy) {
                        case 'skip':
                            // Skip this event and continue
                            lastGlobalPos = eventNapi.globalPos;
                            continue;
                        case 'retry':
                            // Retry once
                            try {
                                await projection.apply(event, proxy);
                                lastGlobalPos = eventNapi.globalPos;
                            }
                            catch {
                                // If retry fails, stop
                                throw error;
                            }
                            break;
                        case 'stop':
                        default:
                            throw error;
                    }
                }
            }
            // Flush operations and apply to database
            const operations = flush();
            await db.applyProjectionBatch({
                projectionName: projection.name,
                operations: operations.map((op) => ({
                    opType: op.opType,
                    key: op.key,
                    value: op.value,
                })),
                lastGlobalPos,
            });
        }
        catch (error) {
            console.error(`[Projection ${projection.name}] Error:`, error);
            // Let the worker crash - the runner will restart it with backoff
            throw error;
        }
    }
}
// Run the worker
main().catch((error) => {
    console.error('Worker fatal error:', error);
    process.exit(1);
});
