/**
 * TypeScript-specific AST types.
 */

import type { Span } from "../../diagnostic/index.js";

/**
 * A parsed TypeScript file.
 */
export interface ParsedFile {
  path: string;
  imports: ImportDecl[];
  typeAliases: TypeAlias[];
  classes: ClassDecl[];
}

/**
 * An import declaration.
 */
export interface ImportDecl {
  specifiers: ImportSpecifier[];
  source: string;
  span: Span;
}

/**
 * An import specifier.
 */
export interface ImportSpecifier {
  name: string;
  alias?: string;
}

/**
 * A type alias declaration.
 */
export interface TypeAlias {
  name: string;
  typeNode: TypeNode;
  exported: boolean;
  span: Span;
}

/**
 * Type AST nodes.
 */
export type TypeNode =
  /** string, number, boolean, etc. */
  | { kind: "primitive"; name: string }
  /** T[] */
  | { kind: "array"; element: TypeNode }
  /** T | U */
  | { kind: "union"; variants: TypeNode[] }
  /** { type: "Foo", field: T } */
  | { kind: "objectLiteral"; properties: ObjectProperty[] }
  /** { [key: string]: T } - index signature */
  | { kind: "indexSignature"; keyName: string; keyType: TypeNode; valueType: TypeNode }
  /** Reference to another type */
  | { kind: "reference"; name: string }
  /** T | undefined or T? */
  | { kind: "optional"; inner: TypeNode };

/**
 * Creates a primitive type node.
 */
export function primitiveType(name: string): TypeNode {
  return { kind: "primitive", name };
}

/**
 * Creates an array type node.
 */
export function arrayTypeNode(element: TypeNode): TypeNode {
  return { kind: "array", element };
}

/**
 * Creates a union type node.
 */
export function unionType(variants: TypeNode[]): TypeNode {
  return { kind: "union", variants };
}

/**
 * Creates an object literal type node.
 */
export function objectLiteralType(properties: ObjectProperty[]): TypeNode {
  return { kind: "objectLiteral", properties };
}

/**
 * Creates an index signature type node.
 */
export function indexSignatureType(
  keyName: string,
  keyType: TypeNode,
  valueType: TypeNode
): TypeNode {
  return { kind: "indexSignature", keyName, keyType, valueType };
}

/**
 * Creates a reference type node.
 */
export function referenceTypeNode(name: string): TypeNode {
  return { kind: "reference", name };
}

/**
 * Creates an optional type node.
 */
export function optionalTypeNode(inner: TypeNode): TypeNode {
  return { kind: "optional", inner };
}

/**
 * A property in an object literal type.
 */
export interface ObjectProperty {
  name: string;
  typeNode: TypeNode;
  optional: boolean;
}

/**
 * A class declaration.
 */
export interface ClassDecl {
  name: string;
  properties: PropertyDecl[];
  methods: MethodDecl[];
  exported: boolean;
  span: Span;
}

/**
 * A class property declaration.
 */
export interface PropertyDecl {
  name: string;
  typeNode?: TypeNode;
  isStatic: boolean;
  isReadonly: boolean;
  initializer?: string;
  span: Span;
}

/**
 * A method declaration.
 */
export interface MethodDecl {
  name: string;
  parameters: Parameter[];
  returnType?: TypeNode;
  body: Statement[];
  /** Raw source text of the method body (for TS->TS pass-through like apply methods). */
  rawBody?: string;
  isAsync: boolean;
  visibility: Visibility;
  span: Span;
}

/**
 * Method visibility.
 */
export type Visibility = "public" | "protected" | "private";

/**
 * A parameter.
 */
export interface Parameter {
  name: string;
  typeNode?: TypeNode;
  optional: boolean;
  defaultValue?: string;
}

/**
 * A statement.
 */
export type Statement =
  | {
      kind: "if";
      condition: Expression;
      thenBranch: Statement[];
      elseBranch?: Statement[];
      span: Span;
    }
  | {
      kind: "switch";
      discriminant: Expression;
      cases: SwitchCase[];
      span: Span;
    }
  | {
      kind: "return";
      value?: Expression;
      span: Span;
    }
  | {
      kind: "throw";
      argument: Expression;
      span: Span;
    }
  | {
      kind: "expression";
      expression: Expression;
      span: Span;
    }
  | {
      kind: "variableDecl";
      varKind: VarKind;
      name: string;
      typeNode?: TypeNode;
      initializer?: Expression;
      span: Span;
    }
  | {
      kind: "block";
      statements: Statement[];
      span: Span;
    };

/**
 * Variable declaration kind.
 */
export type VarKind = "const" | "let" | "var";

/**
 * A switch case.
 */
export interface SwitchCase {
  /** The test expression, or undefined for default case. */
  test?: Expression;
  consequent: Statement[];
}

/**
 * An expression.
 */
export type Expression =
  | { kind: "identifier"; name: string; span: Span }
  | { kind: "stringLiteral"; value: string; span: Span }
  | { kind: "numberLiteral"; value: number; span: Span }
  | { kind: "booleanLiteral"; value: boolean; span: Span }
  | { kind: "nullLiteral"; span: Span }
  | { kind: "arrayLiteral"; elements: Expression[]; span: Span }
  | {
      kind: "objectLiteral";
      properties: Array<{ key: string; value: Expression }>;
      span: Span;
    }
  | { kind: "memberAccess"; object: Expression; property: string; span: Span }
  | { kind: "call"; callee: Expression; arguments: Expression[]; span: Span }
  | { kind: "new"; callee: Expression; arguments: Expression[]; span: Span }
  | {
      kind: "binary";
      left: Expression;
      operator: string;
      right: Expression;
      span: Span;
    }
  | {
      kind: "unary";
      operator: string;
      argument: Expression;
      prefix: boolean;
      span: Span;
    }
  | { kind: "this"; span: Span }
  | { kind: "await"; argument: Expression; span: Span }
  | { kind: "spread"; argument: Expression; span: Span };

/**
 * Creates an identifier expression.
 */
export function identifier(name: string, span: Span): Expression {
  return { kind: "identifier", name, span };
}

/**
 * Creates a string literal expression.
 */
export function stringLiteral(value: string, span: Span): Expression {
  return { kind: "stringLiteral", value, span };
}

/**
 * Creates a number literal expression.
 */
export function numberLiteral(value: number, span: Span): Expression {
  return { kind: "numberLiteral", value, span };
}

/**
 * Creates a boolean literal expression.
 */
export function booleanLiteral(value: boolean, span: Span): Expression {
  return { kind: "booleanLiteral", value, span };
}

/**
 * Creates a null literal expression.
 */
export function nullLiteral(span: Span): Expression {
  return { kind: "nullLiteral", span };
}

/**
 * Creates a this expression.
 */
export function thisExpr(span: Span): Expression {
  return { kind: "this", span };
}
