/**
 * Projection intermediate representation.
 *
 * Projections are read models that subscribe to events and build queryable state.
 * They are detected by type shape and exposed via HTTP GET endpoints.
 */

import type { AccessLevel } from "./access.js";
import type { DomainType, ParameterIR } from "./types.js";

/**
 * The projection type determines storage and query patterns.
 */
export type ProjectionKind =
  /**
   * SQLite-backed, row-based storage.
   * State shape: `{ [key: string]: ObjectType }`
   * Fast lookups by indexed columns.
   */
  | "denormalizedView"
  /**
   * Memory-resident with time-based checkpointing.
   * State shape: `{ field: T, ... }` (named fields)
   * Fast reads, no DB hit.
   */
  | "aggregator"
  /**
   * Time-bucketed data with range queries.
   * State shape: `{ [key: string]: number }` with time signals.
   */
  | "timeSeries";

/**
 * Convert projection kind to string representation.
 */
export function projectionKindToString(kind: ProjectionKind): string {
  switch (kind) {
    case "denormalizedView":
      return "denormalized_view";
    case "aggregator":
      return "aggregator";
    case "timeSeries":
      return "time_series";
  }
}

/**
 * IR representation of a projection.
 */
export interface ProjectionIR {
  /** Name of the projection (e.g., "UserProfiles", "OrderStats"). */
  name: string;

  /** Source file path. */
  sourcePath: string;

  /** The projection type (determines storage and query patterns). */
  kind: ProjectionKind;

  /** Events this projection subscribes to (from build() method param union type). */
  subscribedEvents: SubscribedEvent[];

  /** The state/row schema. */
  schema: ProjectionSchema;

  /** Query methods that become HTTP GET endpoints. */
  queries: QueryMethodIR[];

  /** Raw build method body for codegen pass-through. */
  rawBuildBody?: string;

  /** Access level for this projection's endpoints. */
  access: AccessLevel;

  /** Required roles to access this projection. */
  roles: string[];
}

/**
 * An event the projection subscribes to.
 */
export interface SubscribedEvent {
  /** Event type name (e.g., "UserCreated", "OrderCompleted"). */
  eventName: string;

  /**
   * Aggregate this event belongs to (derived from naming convention).
   * e.g., "UserCreated" likely belongs to "User" aggregate.
   */
  aggregate?: string;
}

/**
 * Schema for projection state/rows.
 */
export interface ProjectionSchema {
  /** Name of the state property in the source class. */
  statePropertyName: string;

  /** Primary key column(s). */
  primaryKeys: ColumnDef[];

  /** Non-key columns. */
  columns: ColumnDef[];

  /** Indexes derived from query method parameters. */
  indexes: IndexDef[];
}

/**
 * Column definition for SQLite schema.
 */
export interface ColumnDef {
  /** Column name (snake_case). */
  name: string;

  /** SQLite type. */
  sqlType: SqlType;

  /** Whether the column can be NULL. */
  nullable: boolean;

  /** Default value (SQL expression). */
  default?: string;
}

/**
 * SQLite column types.
 */
export type SqlType =
  /** TEXT - for strings, JSON serialization */
  | "text"
  /** INTEGER - for integers, booleans (0/1) */
  | "integer"
  /** REAL - for floating point numbers */
  | "real"
  /** BLOB - for binary data */
  | "blob";

/**
 * Convert a domain type to SQLite type.
 */
export function domainTypeToSqlType(dt: DomainType): SqlType {
  switch (dt.kind) {
    case "string":
      return "text";
    case "number":
      return "real";
    case "boolean":
      return "integer";
    case "array":
      return "text"; // JSON serialized
    case "option":
      return domainTypeToSqlType(dt.inner);
    case "object":
      return "text"; // JSON serialized
    case "reference":
      return "text"; // Assume string ID
  }
}

/**
 * Convert SQL type to SQL string.
 */
export function sqlTypeToSql(sqlType: SqlType): string {
  switch (sqlType) {
    case "text":
      return "TEXT";
    case "integer":
      return "INTEGER";
    case "real":
      return "REAL";
    case "blob":
      return "BLOB";
  }
}

/**
 * Index definition.
 */
export interface IndexDef {
  /** Index name. */
  name: string;

  /** Columns in the index. */
  columns: string[];

  /** Whether the index enforces uniqueness. */
  unique: boolean;
}

/**
 * IR representation of a query method.
 */
export interface QueryMethodIR {
  /** Method name (e.g., "getById", "getByEmail"). */
  name: string;

  /** Parameters to the query. */
  parameters: ParameterIR[];

  /** Return type of the query. */
  returnType?: DomainType;

  /** Columns that need indexes (derived from parameter names). */
  indexedColumns: string[];

  /** Whether this is a range query (has start/end parameters). */
  isRangeQuery: boolean;

  /** Raw method body for codegen pass-through. */
  rawBody?: string;
}

/**
 * Analyzed state shape for projection kind detection.
 */
export type StateShape =
  /** Index signature with object value: `{ [key: string]: ObjectType }` */
  | { kind: "indexedObject"; keyName: string; valueType: DomainType }
  /** Index signature with number value: `{ [key: string]: number }` */
  | { kind: "indexedNumber"; keyName: string }
  /** Named fields (no top-level index signature): `{ field: T, ... }` */
  | { kind: "namedFields"; fields: Array<{ name: string; type: DomainType }> };

/**
 * Time-series detection signals.
 */
export interface TimeSeriesSignals {
  /** Signal 1: Key is derived from timestamp field in build() method. */
  hasTimestampDerivation: boolean;

  /** Signal 2: Key name contains time-related words. */
  hasTimeRelatedKeyName: boolean;

  /** Signal 3: Has query methods with range parameters. */
  hasRangeQueryMethods: boolean;
}

/**
 * Creates default time series signals.
 */
export function defaultTimeSeriesSignals(): TimeSeriesSignals {
  return {
    hasTimestampDerivation: false,
    hasTimeRelatedKeyName: false,
    hasRangeQueryMethods: false,
  };
}

/**
 * Returns true if any time-series signal is present.
 */
export function hasAnyTimeSeriesSignal(signals: TimeSeriesSignals): boolean {
  return (
    signals.hasTimestampDerivation ||
    signals.hasTimeRelatedKeyName ||
    signals.hasRangeQueryMethods
  );
}

/**
 * Time-related keywords for detecting Time-Series projections.
 */
export const TIME_KEYWORDS = [
  "date",
  "day",
  "month",
  "year",
  "week",
  "hour",
  "minute",
  "time",
  "timestamp",
  "period",
] as const;

/**
 * Timestamp field names for detecting timestamp derivation.
 */
export const TIMESTAMP_FIELDS = [
  "timestamp",
  "date",
  "time",
  "createdAt",
  "updatedAt",
  "occurredAt",
  "eventTime",
  "eventDate",
] as const;

/**
 * String methods used to derive time keys from timestamps.
 */
export const TIME_STRING_METHODS = [
  "slice",
  "substring",
  "substr",
  "toISOString",
  "toDateString",
  "toLocaleDateString",
  "toTimeString",
] as const;

/**
 * Range parameter names for detecting Time-Series projections.
 */
export const RANGE_PARAMS = [
  "start",
  "end",
  "from",
  "to",
  "startdate",
  "enddate",
  "fromdate",
  "todate",
  "starttime",
  "endtime",
] as const;

/**
 * Check if a name contains time-related keywords.
 */
export function isTimeRelatedName(name: string): boolean {
  const lower = name.toLowerCase();
  return TIME_KEYWORDS.some((kw) => lower.includes(kw));
}

/**
 * Check if a parameter name suggests a range query.
 */
export function isRangeParam(name: string): boolean {
  const lower = name.toLowerCase();
  return RANGE_PARAMS.some((rp) => lower.includes(rp));
}
