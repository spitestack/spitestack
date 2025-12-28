/**
 * Complete domain IR containing all analyzed aggregates, orchestrators, and projections.
 */

import type { AppConfig } from "./access.js";
import type { AggregateIR } from "./aggregate.js";
import type { OrchestratorIR } from "./orchestrator.js";
import type { ProjectionIR } from "./projection.js";

/**
 * The complete domain IR containing all analyzed constructs.
 */
export interface DomainIR {
  /** All aggregates in the domain. */
  aggregates: AggregateIR[];

  /** All orchestrators in the domain. */
  orchestrators: OrchestratorIR[];

  /** All projections in the domain. */
  projections: ProjectionIR[];

  /** The source directory path. */
  sourceDir: string;

  /** App configuration for access control (parsed from index.ts). */
  appConfig?: AppConfig;
}

/**
 * Creates an empty domain IR.
 */
export function createDomainIR(sourceDir: string): DomainIR {
  return {
    aggregates: [],
    orchestrators: [],
    projections: [],
    sourceDir,
  };
}
