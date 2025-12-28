/**
 * Purity validation for domain logic.
 *
 * Domain logic (aggregates) must be pure - no side effects allowed.
 * This includes:
 * - No console.log, fetch, setTimeout, etc.
 * - No Date.now(), Math.random()
 * - No await in aggregates
 * - No external package imports
 */

import { CompilerError } from "../diagnostic/index.js";
import type { AggregateIR, ExpressionIR, StatementIR } from "../ir/index.js";

/**
 * Forbidden function/method calls.
 */
const FORBIDDEN_CALLS = [
  // Console
  "console.log",
  "console.error",
  "console.warn",
  "console.info",
  "console.debug",
  // Network
  "fetch",
  "XMLHttpRequest",
  // Timers
  "setTimeout",
  "setInterval",
  "setImmediate",
  // Non-deterministic
  "Date.now",
  "Math.random",
  // DOM
  "document",
  "window",
  // File system
  "fs.readFileSync",
  "fs.writeFileSync",
] as const;

/**
 * Validates that an aggregate is pure.
 */
export function validateAggregatePurity(aggregate: AggregateIR): void {
  for (const command of aggregate.commands) {
    for (const stmt of command.body) {
      validateStatement(stmt, aggregate.sourcePath);
    }
  }
}

/**
 * Validates a statement for purity.
 */
function validateStatement(stmt: StatementIR, file: string): void {
  switch (stmt.kind) {
    case "if":
      validateExpression(stmt.condition, file);
      for (const s of stmt.thenBranch) {
        validateStatement(s, file);
      }
      if (stmt.elseBranch) {
        for (const s of stmt.elseBranch) {
          validateStatement(s, file);
        }
      }
      break;

    case "throw":
      // Throws are allowed
      break;

    case "emit":
      for (const { value } of stmt.fields) {
        validateExpression(value, file);
      }
      break;

    case "let":
      validateExpression(stmt.value, file);
      break;

    case "expression":
      validateExpression(stmt.expression, file);
      break;

    case "return":
      if (stmt.value) {
        validateExpression(stmt.value, file);
      }
      break;
  }
}

/**
 * Validates an expression for purity.
 */
function validateExpression(expr: ExpressionIR, file: string): void {
  switch (expr.kind) {
    case "call":
      // Check for forbidden calls
      if (FORBIDDEN_CALLS.some((f) => expr.callee.includes(f))) {
        throw CompilerError.forbiddenCall(expr.callee, file, 0);
      }
      for (const arg of expr.arguments) {
        validateExpression(arg, file);
      }
      break;

    case "methodCall":
      // Check for forbidden method calls
      const objName = expressionToName(expr.object);
      const fullName = objName ? `${objName}.${expr.method}` : expr.method;
      if (FORBIDDEN_CALLS.some((f) => fullName === f || fullName.startsWith(f + "."))) {
        throw CompilerError.forbiddenCall(fullName, file, 0);
      }
      validateExpression(expr.object, file);
      for (const arg of expr.arguments) {
        validateExpression(arg, file);
      }
      break;

    case "propertyAccess":
      validateExpression(expr.object, file);
      break;

    case "binary":
      validateExpression(expr.left, file);
      validateExpression(expr.right, file);
      break;

    case "unary":
      validateExpression(expr.operand, file);
      break;

    case "object":
      for (const { value } of expr.fields) {
        validateExpression(value, file);
      }
      break;

    case "array":
      for (const elem of expr.elements) {
        validateExpression(elem, file);
      }
      break;

    // Literals and identifiers are always pure
    default:
      break;
  }
}

/**
 * Extracts a name from an expression if it's a simple identifier or property access.
 */
function expressionToName(expr: ExpressionIR): string | undefined {
  switch (expr.kind) {
    case "identifier":
      return expr.name;
    case "propertyAccess":
      const base = expressionToName(expr.object);
      return base ? `${base}.${expr.property}` : expr.property;
    default:
      return undefined;
  }
}
