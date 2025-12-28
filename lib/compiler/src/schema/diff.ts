/**
 * Schema diffing for detecting changes between versions.
 */

import { CompilerError } from "../diagnostic/index.js";
import type { DomainIR } from "../ir/index.js";
import type { SchemaLockFile, LockedEvent, LockedField } from "./lock.js";

/**
 * Schema change types.
 */
export type SchemaChange =
  | { kind: "eventAdded"; aggregate: string; event: string }
  | { kind: "eventRemoved"; aggregate: string; event: string }
  | { kind: "fieldAdded"; aggregate: string; event: string; field: string; isOptional: boolean }
  | { kind: "fieldRemoved"; aggregate: string; event: string; field: string }
  | { kind: "fieldTypeChanged"; aggregate: string; event: string; field: string; from: string; to: string };

/**
 * Diff result.
 */
export interface SchemaDiff {
  /** Changes that are non-breaking (e.g., adding optional fields). */
  nonBreaking: SchemaChange[];

  /** Changes that are breaking (e.g., removing fields, changing types). */
  breaking: SchemaChange[];
}

/**
 * Diffs the current schema against the locked schema.
 */
export function diffSchema(domain: DomainIR, lock: SchemaLockFile): SchemaDiff {
  const nonBreaking: SchemaChange[] = [];
  const breaking: SchemaChange[] = [];

  // Check each aggregate
  for (const aggregate of domain.aggregates) {
    const lockedAggregate = lock.aggregates[aggregate.name];

    if (!lockedAggregate) {
      // New aggregate - non-breaking
      for (const variant of aggregate.events.variants) {
        nonBreaking.push({
          kind: "eventAdded",
          aggregate: aggregate.name,
          event: variant.name,
        });
      }
      continue;
    }

    // Check each event variant
    const currentEvents = new Map(
      aggregate.events.variants.map((v) => [v.name, v])
    );
    const lockedEvents = new Map(
      lockedAggregate.events.map((e) => [e.name, e])
    );

    // Find removed events (breaking)
    for (const [name] of lockedEvents) {
      if (!currentEvents.has(name)) {
        breaking.push({
          kind: "eventRemoved",
          aggregate: aggregate.name,
          event: name,
        });
      }
    }

    // Find added events (non-breaking)
    for (const [name] of currentEvents) {
      if (!lockedEvents.has(name)) {
        nonBreaking.push({
          kind: "eventAdded",
          aggregate: aggregate.name,
          event: name,
        });
      }
    }

    // Check fields of existing events
    for (const [name, current] of currentEvents) {
      const locked = lockedEvents.get(name);
      if (!locked) continue;

      const fieldChanges = diffEventFields(aggregate.name, name, current.fields, locked);
      nonBreaking.push(...fieldChanges.nonBreaking);
      breaking.push(...fieldChanges.breaking);
    }
  }

  // Check for removed aggregates
  for (const aggName of Object.keys(lock.aggregates)) {
    if (!domain.aggregates.find((a) => a.name === aggName)) {
      const lockedAgg = lock.aggregates[aggName]!;
      for (const event of lockedAgg.events) {
        breaking.push({
          kind: "eventRemoved",
          aggregate: aggName,
          event: event.name,
        });
      }
    }
  }

  return { nonBreaking, breaking };
}

/**
 * Diffs event fields.
 */
function diffEventFields(
  aggregate: string,
  event: string,
  currentFields: import("../ir/index.js").EventField[],
  locked: LockedEvent
): SchemaDiff {
  const nonBreaking: SchemaChange[] = [];
  const breaking: SchemaChange[] = [];

  const currentMap = new Map(currentFields.map((f) => [f.name, f]));
  const lockedMap = new Map(locked.fields.map((f) => [f.name, f]));

  // Find removed fields (breaking)
  for (const [name] of lockedMap) {
    if (!currentMap.has(name)) {
      breaking.push({
        kind: "fieldRemoved",
        aggregate,
        event,
        field: name,
      });
    }
  }

  // Find added fields
  for (const [name, current] of currentMap) {
    if (!lockedMap.has(name)) {
      const isOptional = current.type.kind === "option";
      if (isOptional) {
        nonBreaking.push({
          kind: "fieldAdded",
          aggregate,
          event,
          field: name,
          isOptional: true,
        });
      } else {
        // Adding required field is breaking
        breaking.push({
          kind: "fieldAdded",
          aggregate,
          event,
          field: name,
          isOptional: false,
        });
      }
    }
  }

  // Check for type changes
  for (const [name, current] of currentMap) {
    const locked = lockedMap.get(name);
    if (!locked) continue;

    const currentType = domainTypeToString(current.type);
    if (currentType !== locked.type) {
      breaking.push({
        kind: "fieldTypeChanged",
        aggregate,
        event,
        field: name,
        from: locked.type,
        to: currentType,
      });
    }
  }

  return { nonBreaking, breaking };
}

/**
 * Validates that no breaking changes exist.
 */
export function validateNoBreakingChanges(diff: SchemaDiff): void {
  if (diff.breaking.length === 0) return;

  // Format the breaking changes for the error message
  const changes = diff.breaking
    .map((c) => {
      switch (c.kind) {
        case "eventRemoved":
          return `  - Event ${c.aggregate}.${c.event} was removed`;
        case "fieldRemoved":
          return `  - Field ${c.aggregate}.${c.event}.${c.field} was removed`;
        case "fieldAdded":
          return `  - Required field ${c.aggregate}.${c.event}.${c.field} was added`;
        case "fieldTypeChanged":
          return `  - Field ${c.aggregate}.${c.event}.${c.field} type changed from ${c.from} to ${c.to}`;
        default:
          return `  - Unknown change`;
      }
    })
    .join("\n");

  const firstChange = diff.breaking[0]!;
  if (firstChange.kind === "eventRemoved") {
    throw CompilerError.eventRemoved(firstChange.aggregate, firstChange.event);
  }

  throw CompilerError.breakingSchemaChange(
    firstChange.kind === "eventAdded" ? firstChange.aggregate : "unknown",
    firstChange.kind === "eventAdded" ? firstChange.event : "unknown",
    changes
  );
}

/**
 * Converts a DomainType to a string.
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
