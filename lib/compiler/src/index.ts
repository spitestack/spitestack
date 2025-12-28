/**
 * SpiteStack Compiler
 *
 * A compiler that parses TypeScript domain code and generates event-sourcing scaffolding.
 */

// Main compiler
export { Compiler, createCompiler } from "./compiler.js";
export { type CompilerConfig, defaultConfig, mergeConfig } from "./config.js";

// Diagnostic
export { CompilerError, ErrorCode, type Span, createSpan, formatSpan } from "./diagnostic/index.js";

// IR types
export {
  // Base types
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
  // Access control
  type AppMode,
  type AccessLevel,
  type MethodAccessConfig,
  type EntityAccessConfig,
  type AppConfig,
  parseAppMode,
  parseAccessLevel,
  // Aggregate
  type AggregateIR,
  type EventTypeIR,
  type EventVariant,
  type EventField,
  type CommandIR,
  type StatementIR,
  type ExpressionIR,
  type BinaryOp,
  type UnaryOp,
  // Orchestrator
  type OrchestratorIR,
  type OrchestratorDependency,
  // Projection
  type ProjectionKind,
  type ProjectionIR,
  type SubscribedEvent,
  type ProjectionSchema,
  type ColumnDef,
  type SqlType,
  type IndexDef,
  type QueryMethodIR,
  // Domain
  type DomainIR,
  createDomainIR,
} from "./ir/index.js";

// Frontend
export { TypeScriptParser, createParser, parseAppConfig } from "./frontend/index.js";

// Validation
export { validateDomain, validateAggregatePurity, validateStructure } from "./validate/index.js";

// Code generation
export { generate, type GeneratedCode, toSnakeCase, toPascalCase, typeToTs } from "./codegen/index.js";

// Schema evolution
export {
  type SchemaLockFile,
  type SchemaDiff,
  type SchemaChange,
  readLockFile,
  writeLockFile,
  createLockFile,
  diffSchema,
  validateNoBreakingChanges,
} from "./schema/index.js";
