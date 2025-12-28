/**
 * TypeScript type utilities for code generation.
 */

import type { DomainType } from "../ir/index.js";

/**
 * Converts camelCase to snake_case.
 */
export function toSnakeCase(s: string): string {
  let result = "";
  for (let i = 0; i < s.length; i++) {
    const c = s[i]!;
    if (c >= "A" && c <= "Z") {
      if (i > 0) {
        result += "_";
      }
      result += c.toLowerCase();
    } else {
      result += c;
    }
  }
  return result;
}

/**
 * Converts snake_case to camelCase.
 */
export function toCamelCase(s: string): string {
  return s.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
}

/**
 * Converts snake_case to PascalCase.
 */
export function toPascalCase(s: string): string {
  const camel = toCamelCase(s);
  return camel.charAt(0).toUpperCase() + camel.slice(1);
}

/**
 * Converts a DomainType to a TypeScript type string.
 */
export function typeToTs(type: DomainType): string {
  switch (type.kind) {
    case "string":
      return "string";
    case "number":
      return "number";
    case "boolean":
      return "boolean";
    case "array":
      return `${typeToTs(type.element)}[]`;
    case "option":
      return `${typeToTs(type.inner)} | undefined`;
    case "object":
      const fields = type.fields
        .map((f) => `${f.name}${f.optional ? "?" : ""}: ${typeToTs(f.type)}`)
        .join("; ");
      return `{ ${fields} }`;
    case "reference":
      return type.name;
  }
}

/**
 * Escapes a string for use in TypeScript code.
 */
export function escapeString(s: string): string {
  return s
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'")
    .replace(/"/g, '\\"')
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r")
    .replace(/\t/g, "\\t");
}

/**
 * Indents a block of code.
 */
export function indent(code: string, spaces = 2): string {
  const prefix = " ".repeat(spaces);
  return code
    .split("\n")
    .map((line) => (line.trim() ? prefix + line : line))
    .join("\n");
}
