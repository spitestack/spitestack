/**
 * Structure validation for domain types.
 *
 * Validates that aggregates have all required members and
 * that types are correctly structured.
 */

import { CompilerError } from "../diagnostic/index.js";
import type { AggregateIR, DomainIR } from "../ir/index.js";

/**
 * Validates the structure of the domain IR.
 */
export function validateStructure(domain: DomainIR): void {
  for (const aggregate of domain.aggregates) {
    validateAggregateStructure(aggregate);
  }
}

/**
 * Validates an aggregate has all required components.
 */
function validateAggregateStructure(aggregate: AggregateIR): void {
  // Check that state has at least one field
  if (aggregate.state.fields.length === 0) {
    throw CompilerError.invalidStateType(`${aggregate.name}State`);
  }

  // Check that events have at least one variant
  if (aggregate.events.variants.length === 0) {
    throw CompilerError.invalidEventType(aggregate.events.name);
  }

  // Check that each event variant has a valid name
  for (const variant of aggregate.events.variants) {
    if (!variant.name) {
      throw CompilerError.invalidEventType(`${aggregate.events.name} variant`);
    }
  }
}
