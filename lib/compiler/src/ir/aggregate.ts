/**
 * Aggregate intermediate representation.
 */

import type { AccessLevel } from "./access.js";
import type { DomainType, InitialValue, ObjectType, ParameterIR } from "./types.js";

/**
 * IR representation of an aggregate.
 */
export interface AggregateIR {
  /** Name of the aggregate (e.g., "Todo", "Project"). */
  name: string;

  /** Source file path. */
  sourcePath: string;

  /** The state type. */
  state: ObjectType;

  /** Initial values for state fields. */
  initialState: Array<{ field: string; value: InitialValue }>;

  /** The event type (discriminated union). */
  events: EventTypeIR;

  /** Commands that can be executed on this aggregate. */
  commands: CommandIR[];

  /**
   * Raw apply method body (switch statement content) from source.
   * If present, used directly in codegen instead of auto-generating field mapping.
   */
  rawApplyBody?: string;
}

/**
 * IR representation of an event type (discriminated union).
 */
export interface EventTypeIR {
  /** Name of the event type (e.g., "TodoEvent"). */
  name: string;

  /** Event variants. */
  variants: EventVariant[];
}

/**
 * A single event variant.
 */
export interface EventVariant {
  /** The discriminant value (e.g., "Created", "Completed"). */
  name: string;

  /** Fields in this event variant. */
  fields: EventField[];
}

/**
 * A field in an event variant.
 */
export interface EventField {
  name: string;
  type: DomainType;
}

/**
 * IR representation of a command method.
 */
export interface CommandIR {
  /** Name of the command (e.g., "create", "complete"). */
  name: string;

  /** Parameters to the command. */
  parameters: ParameterIR[];

  /** The body statements (for translation). */
  body: StatementIR[];

  /** Access level for this command endpoint. */
  access: AccessLevel;

  /**
   * Required roles to access this command.
   * Only applicable for `internal` and `private` access levels.
   */
  roles: string[];
}

/**
 * IR representation of a statement.
 */
export type StatementIR =
  | { kind: "if"; condition: ExpressionIR; thenBranch: StatementIR[]; elseBranch?: StatementIR[] }
  | { kind: "throw"; message: string }
  | { kind: "emit"; eventType: string; fields: Array<{ name: string; value: ExpressionIR }> }
  | { kind: "let"; name: string; value: ExpressionIR }
  | { kind: "expression"; expression: ExpressionIR }
  | { kind: "return"; value?: ExpressionIR };

/**
 * IR representation of an expression.
 */
export type ExpressionIR =
  | { kind: "stringLiteral"; value: string }
  | { kind: "numberLiteral"; value: number }
  | { kind: "booleanLiteral"; value: boolean }
  | { kind: "identifier"; name: string }
  | { kind: "stateAccess"; field: string }
  | { kind: "propertyAccess"; object: ExpressionIR; property: string }
  | { kind: "methodCall"; object: ExpressionIR; method: string; arguments: ExpressionIR[] }
  | { kind: "call"; callee: string; arguments: ExpressionIR[] }
  | { kind: "new"; callee: string; arguments: ExpressionIR[] }
  | { kind: "binary"; left: ExpressionIR; operator: BinaryOp; right: ExpressionIR }
  | { kind: "unary"; operator: UnaryOp; operand: ExpressionIR }
  | { kind: "object"; fields: Array<{ name: string; value: ExpressionIR }> }
  | { kind: "array"; elements: ExpressionIR[] };

/**
 * Binary operators.
 */
export type BinaryOp =
  | "eq" // ===
  | "notEq" // !==
  | "lt" // <
  | "ltEq" // <=
  | "gt" // >
  | "gtEq" // >=
  | "and" // &&
  | "or" // ||
  | "add" // +
  | "sub" // -
  | "mul" // *
  | "div"; // /

/**
 * Unary operators.
 */
export type UnaryOp =
  | "not" // !
  | "neg"; // -

/**
 * Convert a string operator to BinaryOp.
 */
export function stringToBinaryOp(op: string): BinaryOp | undefined {
  switch (op) {
    case "===":
      return "eq";
    case "!==":
      return "notEq";
    case "<":
      return "lt";
    case "<=":
      return "ltEq";
    case ">":
      return "gt";
    case ">=":
      return "gtEq";
    case "&&":
      return "and";
    case "||":
      return "or";
    case "+":
      return "add";
    case "-":
      return "sub";
    case "*":
      return "mul";
    case "/":
      return "div";
    default:
      return undefined;
  }
}

/**
 * Convert BinaryOp to string operator.
 */
export function binaryOpToString(op: BinaryOp): string {
  switch (op) {
    case "eq":
      return "===";
    case "notEq":
      return "!==";
    case "lt":
      return "<";
    case "ltEq":
      return "<=";
    case "gt":
      return ">";
    case "gtEq":
      return ">=";
    case "and":
      return "&&";
    case "or":
      return "||";
    case "add":
      return "+";
    case "sub":
      return "-";
    case "mul":
      return "*";
    case "div":
      return "/";
  }
}
