/**
 * Schema evolution module.
 *
 * Handles schema locking, diffing, and upcast generation for safe event schema evolution.
 */

export {
  type SchemaLockFile,
  type LockedAggregate,
  type LockedEvent,
  type LockedField,
  readLockFile,
  writeLockFile,
  createLockFile,
} from "./lock.js";

export {
  type SchemaChange,
  type SchemaDiff,
  diffSchema,
  validateNoBreakingChanges,
} from "./diff.js";
