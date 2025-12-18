/**
 * Projection System Types
 *
 * Type definitions for the SpiteDB projection system.
 */

/// <reference types="node" />

/**
 * Column type for projection schema.
 */
export type ColumnType = 'text' | 'integer' | 'real' | 'blob' | 'boolean';

/**
 * Column definition for a projection table.
 */
export interface ColumnDef {
  /** Column name */
  name: string;
  /** Column type */
  type: ColumnType;
  /** Whether this column is part of the primary key */
  primaryKey?: boolean;
  /** Whether this column allows NULL values (default: true) */
  nullable?: boolean;
  /** Default value for the column */
  defaultValue?: unknown;
}

/**
 * Schema definition using a simple object format.
 * Keys are column names, values are column definitions.
 */
export type SchemaDefinition = Record<
  string,
  ColumnType | { type: ColumnType; primaryKey?: boolean; nullable?: boolean; defaultValue?: unknown }
>;

/**
 * An event from the event store.
 */
export interface ProjectionEvent {
  /** Global position in the log */
  globalPos: bigint;
  /** Stream this event belongs to */
  streamId: string;
  /** Revision within the stream */
  streamRev: bigint;
  /** Timestamp when stored (Unix milliseconds) */
  timestampMs: bigint;
  /** Event payload as a Buffer */
  data: Buffer;
}

/**
 * Row type derived from schema - maps column names to their values.
 */
export type RowData = Record<string, unknown>;

/**
 * The magic proxy table interface.
 * Allows `table[key]` access for reading/writing projection rows.
 */
export interface ProjectionTable<TRow extends RowData = RowData> {
  [key: string]: TRow | undefined;
}

/**
 * Error handling strategy for projections.
 */
export type ErrorStrategy = 'skip' | 'retry' | 'stop';

/**
 * Options for defining a projection.
 */
export interface ProjectionOptions<TSchema extends SchemaDefinition> {
  /** Schema definition for the projection table */
  schema: TSchema;

  /**
   * Event handler function.
   * Called for each event in the batch.
   * Use the table proxy to read/write projection state.
   */
  apply: (event: ProjectionEvent, table: ProjectionTable<SchemaToRow<TSchema>>) => void | Promise<void>;

  /**
   * Error handler (optional).
   * Called when apply() throws an error.
   * Returns the strategy: 'skip' to skip the event, 'retry' to retry, 'stop' to halt.
   */
  onError?: (error: Error, event: ProjectionEvent) => ErrorStrategy;

  /**
   * Batch size for processing events (default: 100).
   */
  batchSize?: number;
}

/**
 * Extracts primary key column names from a schema.
 */
type PrimaryKeyColumns<TSchema extends SchemaDefinition> = {
  [K in keyof TSchema]: TSchema[K] extends { primaryKey: true } ? K : never;
}[keyof TSchema];

/**
 * Helper type to convert schema definition to row type.
 * Primary key columns are optional since they're provided as the table key.
 */
export type SchemaToRow<TSchema extends SchemaDefinition> = {
  [K in keyof TSchema as K extends PrimaryKeyColumns<TSchema> ? never : K]: TSchema[K] extends ColumnType
    ? ColumnTypeToTS<TSchema[K]>
    : TSchema[K] extends { type: ColumnType }
      ? ColumnTypeToTS<TSchema[K]['type']>
      : unknown;
} & {
  [K in PrimaryKeyColumns<TSchema>]?: TSchema[K] extends { type: ColumnType }
    ? ColumnTypeToTS<TSchema[K]['type']>
    : unknown;
};

/**
 * Maps column types to TypeScript types.
 */
export type ColumnTypeToTS<T extends ColumnType> = T extends 'text'
  ? string
  : T extends 'integer'
    ? number
    : T extends 'real'
      ? number
      : T extends 'boolean'
        ? boolean
        : T extends 'blob'
          ? Buffer
          : unknown;

/**
 * Operation type for projection updates.
 */
export type OpType = 'upsert' | 'delete';

/**
 * A single projection operation.
 */
export interface ProjectionOp {
  /** Operation type */
  opType: OpType;
  /** Primary key value */
  key: string;
  /** Row values for upsert (as JSON) */
  value?: string;
}

/**
 * Result of processing a batch.
 */
export interface BatchResult {
  /** Name of the projection */
  projectionName: string;
  /** Operations to apply */
  operations: ProjectionOp[];
  /** Last global position processed */
  lastGlobalPos: number;
}

/**
 * A registered projection.
 */
export interface Projection<TSchema extends SchemaDefinition = SchemaDefinition> {
  /** Projection name */
  name: string;
  /** Projection options */
  options: ProjectionOptions<TSchema>;
}
