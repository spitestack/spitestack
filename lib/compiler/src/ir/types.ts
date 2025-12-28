/**
 * Language-agnostic intermediate representation base types.
 */

/**
 * Domain types that can be represented in the IR.
 */
export type DomainType =
  | { kind: "string" }
  | { kind: "number" }
  | { kind: "boolean" }
  | { kind: "array"; element: DomainType }
  | { kind: "option"; inner: DomainType }
  | { kind: "object"; fields: FieldDef[] }
  | { kind: "reference"; name: string };

/**
 * Creates a string type.
 */
export function stringType(): DomainType {
  return { kind: "string" };
}

/**
 * Creates a number type.
 */
export function numberType(): DomainType {
  return { kind: "number" };
}

/**
 * Creates a boolean type.
 */
export function booleanType(): DomainType {
  return { kind: "boolean" };
}

/**
 * Creates an array type.
 */
export function arrayType(element: DomainType): DomainType {
  return { kind: "array", element };
}

/**
 * Creates an optional type.
 */
export function optionType(inner: DomainType): DomainType {
  return { kind: "option", inner };
}

/**
 * Creates an object type.
 */
export function objectType(fields: FieldDef[]): DomainType {
  return { kind: "object", fields };
}

/**
 * Creates a reference type.
 */
export function referenceType(name: string): DomainType {
  return { kind: "reference", name };
}

/**
 * An object type with named fields.
 */
export interface ObjectType {
  fields: FieldDef[];
}

/**
 * A field definition.
 */
export interface FieldDef {
  name: string;
  type: DomainType;
  optional: boolean;
}

/**
 * A parameter to a command or function.
 */
export interface ParameterIR {
  name: string;
  type: DomainType;
}

/**
 * Initial value for state fields.
 */
export type InitialValue =
  | { kind: "string"; value: string }
  | { kind: "number"; value: number }
  | { kind: "boolean"; value: boolean }
  | { kind: "null" }
  | { kind: "emptyArray" }
  | { kind: "emptyObject" };

/**
 * Creates a string initial value.
 */
export function stringValue(value: string): InitialValue {
  return { kind: "string", value };
}

/**
 * Creates a number initial value.
 */
export function numberValue(value: number): InitialValue {
  return { kind: "number", value };
}

/**
 * Creates a boolean initial value.
 */
export function booleanValue(value: boolean): InitialValue {
  return { kind: "boolean", value };
}

/**
 * Creates a null initial value.
 */
export function nullValue(): InitialValue {
  return { kind: "null" };
}

/**
 * Creates an empty array initial value.
 */
export function emptyArrayValue(): InitialValue {
  return { kind: "emptyArray" };
}

/**
 * Creates an empty object initial value.
 */
export function emptyObjectValue(): InitialValue {
  return { kind: "emptyObject" };
}
