/**
 * Validation module for domain IR.
 */

import type { DomainIR } from "../ir/index.js";
import { validateAggregatePurity } from "./purity.js";
import { validateStructure } from "./structure.js";

export { validateAggregatePurity } from "./purity.js";
export { validateStructure } from "./structure.js";

/**
 * Validates the entire domain.
 */
export function validateDomain(domain: DomainIR): void {
  // Validate structure
  validateStructure(domain);

  // Validate purity of aggregates
  for (const aggregate of domain.aggregates) {
    validateAggregatePurity(aggregate);
  }
}
