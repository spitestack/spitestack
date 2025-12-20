import { existsSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import type { AggregateAnalysis, TypeInfo } from "../types";

// ============================================================================
// Lock File Types
// ============================================================================

export interface EventSchemaProperty {
  type: string;
  optional?: boolean;
  elementType?: string;
  properties?: Record<string, EventSchemaProperty>;
}

export interface EventSchema {
  schemaVersion: number;
  properties: Record<string, EventSchemaProperty>;
}

export interface AggregateSchemaLock {
  events: Record<string, EventSchema>;
}

export interface SchemaLock {
  version: number;
  lockedAt: string;
  aggregates: Record<string, AggregateSchemaLock>;
}

// ============================================================================
// Lock File I/O
// ============================================================================

const SCHEMA_LOCK_FILENAME = "schema.lock";

/**
 * Get the path to the schema lock file
 */
export function getSchemaLockPath(outDir: string): string {
  return join(dirname(outDir), ".spitestack", SCHEMA_LOCK_FILENAME);
}

/**
 * Check if a schema lock file exists
 */
export function schemaLockExists(outDir: string): boolean {
  return existsSync(getSchemaLockPath(outDir));
}

/**
 * Read the schema lock file
 */
export function readSchemaLock(outDir: string): SchemaLock | null {
  const lockPath = getSchemaLockPath(outDir);
  if (!existsSync(lockPath)) {
    return null;
  }

  try {
    const content = readFileSync(lockPath, "utf-8");
    return JSON.parse(content) as SchemaLock;
  } catch (error) {
    throw new Error(
      `Failed to read schema lock file at ${lockPath}: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

/**
 * Write the schema lock file
 */
export function writeSchemaLock(outDir: string, lock: SchemaLock): void {
  const lockPath = getSchemaLockPath(outDir);
  const lockDir = dirname(lockPath);

  if (!existsSync(lockDir)) {
    mkdirSync(lockDir, { recursive: true });
  }

  writeFileSync(lockPath, JSON.stringify(lock, null, 2) + "\n", "utf-8");
}

// ============================================================================
// Lock File Generation
// ============================================================================

/**
 * Convert TypeInfo to a simplified schema property for the lock file
 */
function typeInfoToSchemaProperty(typeInfo: TypeInfo): EventSchemaProperty {
  const prop: EventSchemaProperty = {
    type: typeInfo.kind,
  };

  if (typeInfo.kind === "array" && typeInfo.elementType) {
    prop.elementType = typeInfo.elementType.kind;
  }

  if (typeInfo.kind === "object" && typeInfo.properties) {
    prop.properties = {};
    for (const [key, value] of Object.entries(typeInfo.properties)) {
      prop.properties[key] = typeInfoToSchemaProperty(value);
    }
  }

  if (typeInfo.kind === "union" && typeInfo.types) {
    prop.type = "union";
    // For union types, store as comma-separated kinds
    prop.type = `union(${typeInfo.types.map((t) => t.kind).join("|")})`;
  }

  if (typeInfo.kind === "literal" && typeInfo.literalValue !== undefined) {
    prop.type = `literal(${JSON.stringify(typeInfo.literalValue)})`;
  }

  return prop;
}

/**
 * Generate a schema lock from aggregate analysis results
 */
export function generateSchemaLock(aggregates: AggregateAnalysis[]): SchemaLock {
  const lock: SchemaLock = {
    version: 1,
    lockedAt: new Date().toISOString(),
    aggregates: {},
  };

  for (const aggregate of aggregates) {
    const aggregateLock: AggregateSchemaLock = {
      events: {},
    };

    for (const variant of aggregate.eventType.variants) {
      const eventSchema: EventSchema = {
        schemaVersion: 1,
        properties: {},
      };

      for (const [propName, propType] of Object.entries(variant.properties)) {
        // Skip the 'type' discriminant property
        if (propName === "type") continue;
        eventSchema.properties[propName] = typeInfoToSchemaProperty(propType);
      }

      aggregateLock.events[variant.type] = eventSchema;
    }

    lock.aggregates[aggregate.aggregateName] = aggregateLock;
  }

  return lock;
}

/**
 * Update an existing lock with new aggregates while preserving schema versions
 */
export function mergeSchemaLock(
  existing: SchemaLock,
  aggregates: AggregateAnalysis[]
): SchemaLock {
  const newLock = generateSchemaLock(aggregates);

  // Preserve schema versions from existing lock
  for (const [aggName, aggLock] of Object.entries(newLock.aggregates)) {
    const existingAgg = existing.aggregates[aggName];
    if (!existingAgg) continue;

    for (const [eventType, eventSchema] of Object.entries(aggLock.events)) {
      const existingEvent = existingAgg.events[eventType];
      if (existingEvent) {
        // Preserve the existing schema version
        eventSchema.schemaVersion = existingEvent.schemaVersion;
      }
    }
  }

  // Update the timestamp
  newLock.lockedAt = new Date().toISOString();

  return newLock;
}

/**
 * Increment the schema version for a specific event type
 */
export function incrementEventSchemaVersion(
  lock: SchemaLock,
  aggregateName: string,
  eventType: string
): SchemaLock {
  const aggregate = lock.aggregates[aggregateName];
  if (!aggregate) {
    throw new Error(`Aggregate ${aggregateName} not found in schema lock`);
  }

  const event = aggregate.events[eventType];
  if (!event) {
    throw new Error(`Event ${eventType} not found in aggregate ${aggregateName}`);
  }

  event.schemaVersion += 1;
  lock.lockedAt = new Date().toISOString();

  return lock;
}
