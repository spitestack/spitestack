/**
 * Intermediate Representation (IR) module.
 *
 * This IR is produced by language frontends and consumed by code generators.
 * It represents the domain concepts (aggregates, events, orchestrators, projections)
 * in a way that's independent of the source language.
 */

// Base types
export {
  type DomainType,
  type ObjectType,
  type FieldDef,
  type ParameterIR,
  type InitialValue,
  stringType,
  numberType,
  booleanType,
  arrayType,
  optionType,
  objectType,
  referenceType,
  stringValue,
  numberValue,
  booleanValue,
  nullValue,
  emptyArrayValue,
  emptyObjectValue,
} from "./types.js";

// Access control
export {
  type AppMode,
  type AccessLevel,
  type MethodAccessConfig,
  type EntityAccessConfig,
  type AppConfig,
  parseAppMode,
  parseAccessLevel,
  defaultMethodAccessConfig,
  defaultEntityAccessConfig,
  resolveMethodAccess,
  defaultAppConfig,
  getEntityConfig,
} from "./access.js";

// Aggregate
export {
  type AggregateIR,
  type EventTypeIR,
  type EventVariant,
  type EventField,
  type CommandIR,
  type StatementIR,
  type ExpressionIR,
  type BinaryOp,
  type UnaryOp,
  stringToBinaryOp,
  binaryOpToString,
} from "./aggregate.js";

// Orchestrator
export {
  type OrchestratorIR,
  type OrchestratorDependency,
} from "./orchestrator.js";

// Projection
export {
  type ProjectionKind,
  type ProjectionIR,
  type SubscribedEvent,
  type ProjectionSchema,
  type ColumnDef,
  type SqlType,
  type IndexDef,
  type QueryMethodIR,
  type StateShape,
  type TimeSeriesSignals,
  projectionKindToString,
  domainTypeToSqlType,
  sqlTypeToSql,
  defaultTimeSeriesSignals,
  hasAnyTimeSeriesSignal,
  TIME_KEYWORDS,
  TIMESTAMP_FIELDS,
  TIME_STRING_METHODS,
  RANGE_PARAMS,
  isTimeRelatedName,
  isRangeParam,
} from "./projection.js";

// Domain
export { type DomainIR, createDomainIR } from "./domain.js";
