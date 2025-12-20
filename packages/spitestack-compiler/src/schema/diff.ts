import type {
  SchemaLock,
  AggregateSchemaLock,
  EventSchema,
  EventSchemaProperty,
} from "./lock";

// ============================================================================
// Diff Types
// ============================================================================

export type ChangeType = "additive" | "breaking";

export interface PropertyChange {
  kind: "added" | "removed" | "modified";
  propertyName: string;
  changeType: ChangeType;
  oldType?: string;
  newType?: string;
  message: string;
}

export interface EventChange {
  kind: "added" | "removed" | "modified";
  eventType: string;
  changeType: ChangeType;
  propertyChanges: PropertyChange[];
  message: string;
}

export interface AggregateChange {
  kind: "added" | "removed" | "modified";
  aggregateName: string;
  changeType: ChangeType;
  eventChanges: EventChange[];
  message: string;
}

export interface SchemaDiff {
  hasChanges: boolean;
  hasBreakingChanges: boolean;
  aggregateChanges: AggregateChange[];
}

// ============================================================================
// Diff Logic
// ============================================================================

/**
 * Compare two schema properties
 */
function compareProperties(
  propName: string,
  oldProp: EventSchemaProperty | undefined,
  newProp: EventSchemaProperty | undefined
): PropertyChange | null {
  // Property added
  if (!oldProp && newProp) {
    // New required property is breaking, new optional property is additive
    const isBreaking = !newProp.optional;
    return {
      kind: "added",
      propertyName: propName,
      changeType: isBreaking ? "breaking" : "additive",
      newType: newProp.type,
      message: isBreaking
        ? `New required property '${propName}' added - requires upcaster to provide default value`
        : `New optional property '${propName}' added`,
    };
  }

  // Property removed
  if (oldProp && !newProp) {
    return {
      kind: "removed",
      propertyName: propName,
      changeType: "breaking",
      oldType: oldProp.type,
      message: `Property '${propName}' removed - requires upcaster to handle old events`,
    };
  }

  // Property modified
  if (oldProp && newProp) {
    if (oldProp.type !== newProp.type) {
      return {
        kind: "modified",
        propertyName: propName,
        changeType: "breaking",
        oldType: oldProp.type,
        newType: newProp.type,
        message: `Property '${propName}' type changed from '${oldProp.type}' to '${newProp.type}' - requires upcaster`,
      };
    }

    // Check nested properties for object types
    if (oldProp.properties && newProp.properties) {
      const allKeys = new Set([
        ...Object.keys(oldProp.properties),
        ...Object.keys(newProp.properties),
      ]);

      for (const key of allKeys) {
        const nestedChange = compareProperties(
          `${propName}.${key}`,
          oldProp.properties[key],
          newProp.properties[key]
        );
        if (nestedChange) {
          return nestedChange;
        }
      }
    }
  }

  return null;
}

/**
 * Compare two event schemas
 */
function compareEvents(
  eventType: string,
  oldEvent: EventSchema | undefined,
  newEvent: EventSchema | undefined
): EventChange | null {
  // Event added
  if (!oldEvent && newEvent) {
    return {
      kind: "added",
      eventType,
      changeType: "additive",
      propertyChanges: [],
      message: `New event type '${eventType}' added`,
    };
  }

  // Event removed
  if (oldEvent && !newEvent) {
    return {
      kind: "removed",
      eventType,
      changeType: "breaking",
      propertyChanges: [],
      message: `Event type '${eventType}' removed - this will break existing aggregate hydration`,
    };
  }

  // Event modified
  if (oldEvent && newEvent) {
    const propertyChanges: PropertyChange[] = [];
    const allKeys = new Set([
      ...Object.keys(oldEvent.properties),
      ...Object.keys(newEvent.properties),
    ]);

    for (const key of allKeys) {
      const change = compareProperties(
        key,
        oldEvent.properties[key],
        newEvent.properties[key]
      );
      if (change) {
        propertyChanges.push(change);
      }
    }

    if (propertyChanges.length > 0) {
      const hasBreaking = propertyChanges.some((c) => c.changeType === "breaking");
      return {
        kind: "modified",
        eventType,
        changeType: hasBreaking ? "breaking" : "additive",
        propertyChanges,
        message: hasBreaking
          ? `Event '${eventType}' has breaking changes`
          : `Event '${eventType}' has additive changes`,
      };
    }
  }

  return null;
}

/**
 * Compare two aggregate schema locks
 */
function compareAggregates(
  aggregateName: string,
  oldAgg: AggregateSchemaLock | undefined,
  newAgg: AggregateSchemaLock | undefined
): AggregateChange | null {
  // Aggregate added
  if (!oldAgg && newAgg) {
    return {
      kind: "added",
      aggregateName,
      changeType: "additive",
      eventChanges: [],
      message: `New aggregate '${aggregateName}' added`,
    };
  }

  // Aggregate removed
  if (oldAgg && !newAgg) {
    return {
      kind: "removed",
      aggregateName,
      changeType: "breaking",
      eventChanges: [],
      message: `Aggregate '${aggregateName}' removed - this will break existing streams`,
    };
  }

  // Aggregate modified
  if (oldAgg && newAgg) {
    const eventChanges: EventChange[] = [];
    const allEventTypes = new Set([
      ...Object.keys(oldAgg.events),
      ...Object.keys(newAgg.events),
    ]);

    for (const eventType of allEventTypes) {
      const change = compareEvents(
        eventType,
        oldAgg.events[eventType],
        newAgg.events[eventType]
      );
      if (change) {
        eventChanges.push(change);
      }
    }

    if (eventChanges.length > 0) {
      const hasBreaking = eventChanges.some((c) => c.changeType === "breaking");
      return {
        kind: "modified",
        aggregateName,
        changeType: hasBreaking ? "breaking" : "additive",
        eventChanges,
        message: hasBreaking
          ? `Aggregate '${aggregateName}' has breaking changes`
          : `Aggregate '${aggregateName}' has additive changes`,
      };
    }
  }

  return null;
}

/**
 * Compare two schema locks and return the differences
 */
export function compareSchemaLocks(
  oldLock: SchemaLock,
  newLock: SchemaLock
): SchemaDiff {
  const aggregateChanges: AggregateChange[] = [];
  const allAggregates = new Set([
    ...Object.keys(oldLock.aggregates),
    ...Object.keys(newLock.aggregates),
  ]);

  for (const aggregateName of allAggregates) {
    const change = compareAggregates(
      aggregateName,
      oldLock.aggregates[aggregateName],
      newLock.aggregates[aggregateName]
    );
    if (change) {
      aggregateChanges.push(change);
    }
  }

  return {
    hasChanges: aggregateChanges.length > 0,
    hasBreakingChanges: aggregateChanges.some((c) => c.changeType === "breaking"),
    aggregateChanges,
  };
}

/**
 * Get all breaking changes from a diff
 */
export function getBreakingChanges(diff: SchemaDiff): {
  aggregate: string;
  event: string;
  changes: PropertyChange[];
}[] {
  const result: { aggregate: string; event: string; changes: PropertyChange[] }[] = [];

  for (const aggChange of diff.aggregateChanges) {
    if (aggChange.changeType !== "breaking") continue;

    for (const eventChange of aggChange.eventChanges) {
      if (eventChange.changeType !== "breaking") continue;

      const breakingProps = eventChange.propertyChanges.filter(
        (p) => p.changeType === "breaking"
      );

      if (breakingProps.length > 0 || eventChange.kind === "removed") {
        result.push({
          aggregate: aggChange.aggregateName,
          event: eventChange.eventType,
          changes: breakingProps,
        });
      }
    }

    // Handle removed aggregates
    if (aggChange.kind === "removed") {
      result.push({
        aggregate: aggChange.aggregateName,
        event: "*",
        changes: [],
      });
    }
  }

  return result;
}

/**
 * Format diff for display
 */
export function formatSchemaDiff(diff: SchemaDiff): string {
  if (!diff.hasChanges) {
    return "No schema changes detected.";
  }

  const lines: string[] = [];

  if (diff.hasBreakingChanges) {
    lines.push("BREAKING CHANGES DETECTED:");
    lines.push("");
  }

  for (const aggChange of diff.aggregateChanges) {
    const icon = aggChange.changeType === "breaking" ? "!" : "+";
    lines.push(`[${icon}] ${aggChange.message}`);

    for (const eventChange of aggChange.eventChanges) {
      const eventIcon = eventChange.changeType === "breaking" ? "!" : "+";
      lines.push(`    [${eventIcon}] ${eventChange.message}`);

      for (const propChange of eventChange.propertyChanges) {
        const propIcon = propChange.changeType === "breaking" ? "!" : "+";
        lines.push(`        [${propIcon}] ${propChange.message}`);
      }
    }
  }

  return lines.join("\n");
}
