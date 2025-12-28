/**
 * Orchestrator intermediate representation.
 */

import type { ParameterIR } from "./types.js";

/**
 * IR representation of an orchestrator.
 */
export interface OrchestratorIR {
  /** Name of the orchestrator. */
  name: string;

  /** Source file path. */
  sourcePath: string;

  /** Aggregates this orchestrator depends on. */
  dependencies: OrchestratorDependency[];

  /** The orchestrate method parameters. */
  parameters: ParameterIR[];

  /** Whether the orchestrator is async. */
  isAsync: boolean;
}

/**
 * A dependency of an orchestrator.
 */
export interface OrchestratorDependency {
  /** Name of the dependency parameter. */
  name: string;

  /** Type of the dependency (aggregate name or adapter interface). */
  type: string;

  /** Whether this is an optional dependency. */
  optional: boolean;
}
