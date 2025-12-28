/**
 * Schema lock file handling.
 *
 * The lock file (events.lock.json) captures the schema of events at a point in time,
 * enabling safe schema evolution and upcast generation.
 */

import { readFile, writeFile } from "fs/promises";
import { existsSync } from "fs";
import { join } from "path";
import type { DomainIR, EventVariant, EventField } from "../ir/index.js";

/**
 * Schema lock file structure.
 */
export interface SchemaLockFile {
  /** Lock file format version. */
  version: number;

  /** Timestamp when the lock file was created/updated. */
  timestamp: string;

  /** Locked event schemas by aggregate. */
  aggregates: Record<string, LockedAggregate>;
}

/**
 * Locked aggregate schema.
 */
export interface LockedAggregate {
  /** Event variants for this aggregate. */
  events: LockedEvent[];
}

/**
 * Locked event schema.
 */
export interface LockedEvent {
  /** Event variant name. */
  name: string;

  /** Event fields. */
  fields: LockedField[];
}

/**
 * Locked field schema.
 */
export interface LockedField {
  /** Field name. */
  name: string;

  /** Field type as string representation. */
  type: string;
}

/**
 * Reads the schema lock file from a directory.
 */
export async function readLockFile(dir: string): Promise<SchemaLockFile | undefined> {
  const lockPath = join(dir, "events.lock.json");

  if (!existsSync(lockPath)) {
    return undefined;
  }

  const content = await readFile(lockPath, "utf-8");
  return JSON.parse(content) as SchemaLockFile;
}

/**
 * Writes the schema lock file to a directory.
 */
export async function writeLockFile(dir: string, lock: SchemaLockFile): Promise<void> {
  const lockPath = join(dir, "events.lock.json");
  const content = JSON.stringify(lock, null, 2);
  await writeFile(lockPath, content, "utf-8");
}

/**
 * Creates a lock file from the current domain IR.
 */
export function createLockFile(domain: DomainIR): SchemaLockFile {
  const aggregates: Record<string, LockedAggregate> = {};

  for (const aggregate of domain.aggregates) {
    aggregates[aggregate.name] = {
      events: aggregate.events.variants.map((v) => variantToLocked(v)),
    };
  }

  return {
    version: 1,
    timestamp: new Date().toISOString(),
    aggregates,
  };
}

/**
 * Converts an event variant to a locked event.
 */
function variantToLocked(variant: EventVariant): LockedEvent {
  return {
    name: variant.name,
    fields: variant.fields.map((f) => fieldToLocked(f)),
  };
}

/**
 * Converts a field to a locked field.
 */
function fieldToLocked(field: EventField): LockedField {
  return {
    name: field.name,
    type: domainTypeToString(field.type),
  };
}

/**
 * Converts a DomainType to a string representation for the lock file.
 */
function domainTypeToString(type: import("../ir/index.js").DomainType): string {
  switch (type.kind) {
    case "string":
      return "string";
    case "number":
      return "number";
    case "boolean":
      return "boolean";
    case "array":
      return `${domainTypeToString(type.element)}[]`;
    case "option":
      return `${domainTypeToString(type.inner)}?`;
    case "object":
      const fields = type.fields.map((f) => `${f.name}: ${domainTypeToString(f.type)}`);
      return `{ ${fields.join("; ")} }`;
    case "reference":
      return type.name;
  }
}
