import type { AggregateAnalysis, CommandInfo, TypeInfo, GeneratedFile } from "../types";

/**
 * Generate the shared validation types (once per output)
 */
function generateValidationTypes(): string {
  return `/**
 * Validation error details
 */
export interface ValidationError {
  path: string;
  message: string;
  expected: string;
  received: string;
}

/**
 * Result of validation - either success with typed data, or failure with errors
 */
export type ValidationResult<T> =
  | { success: true; data: T }
  | { success: false; errors: ValidationError[] };

/**
 * UUIDv7 regex pattern
 * Format: xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx
 * Where y is 8, 9, a, or b
 */
const UUID_V7_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/**
 * Fast UUIDv7 validation
 */
export function isUUIDv7(value: string): boolean {
  return UUID_V7_REGEX.test(value);
}
`;
}

/**
 * Generate TypeScript type string from TypeInfo
 */
function typeInfoToTypeScript(type: TypeInfo): string {
  switch (type.kind) {
    case "string":
      return "string";
    case "number":
      return "number";
    case "boolean":
      return "boolean";
    case "null":
      return "null";
    case "undefined":
      return "undefined";
    case "literal":
      if (typeof type.literalValue === "string") {
        return `"${type.literalValue}"`;
      }
      return String(type.literalValue);
    case "array":
      if (type.elementType) {
        return `${typeInfoToTypeScript(type.elementType)}[]`;
      }
      return "unknown[]";
    case "union":
      if (type.types) {
        return type.types.map(typeInfoToTypeScript).join(" | ");
      }
      return "unknown";
    case "object":
      if (type.properties) {
        const props = Object.entries(type.properties)
          .map(([key, value]) => `${key}: ${typeInfoToTypeScript(value)}`)
          .join("; ");
        return `{ ${props} }`;
      }
      return "object";
    default:
      return "unknown";
  }
}

/**
 * Generate validation code for a single type
 * Returns code that validates `value` and pushes to `errors` array
 */
function generateTypeValidation(
  type: TypeInfo,
  valuePath: string,
  errorPath: string
): string {
  switch (type.kind) {
    case "string":
      return `if (typeof ${valuePath} !== "string") {
    errors.push({ path: "${errorPath}", message: "Expected string", expected: "string", received: typeof ${valuePath} });
  }`;

    case "number":
      return `if (typeof ${valuePath} !== "number") {
    errors.push({ path: "${errorPath}", message: "Expected number", expected: "number", received: typeof ${valuePath} });
  }`;

    case "boolean":
      return `if (typeof ${valuePath} !== "boolean") {
    errors.push({ path: "${errorPath}", message: "Expected boolean", expected: "boolean", received: typeof ${valuePath} });
  }`;

    case "null":
      return `if (${valuePath} !== null) {
    errors.push({ path: "${errorPath}", message: "Expected null", expected: "null", received: String(${valuePath}) });
  }`;

    case "literal": {
      const literalExpected = typeof type.literalValue === "string"
        ? `"${type.literalValue}"`
        : String(type.literalValue);
      return `if (${valuePath} !== ${literalExpected}) {
    errors.push({ path: "${errorPath}", message: "Expected ${literalExpected}", expected: "${type.literalValue}", received: String(${valuePath}) });
  }`;
    }

    case "array":
      if (!type.elementType) {
        return `if (!Array.isArray(${valuePath})) {
    errors.push({ path: "${errorPath}", message: "Expected array", expected: "array", received: typeof ${valuePath} });
  }`;
      }
      const elementValidation = generateTypeValidation(
        type.elementType,
        `${valuePath}[i]`,
        `${errorPath}[\${i}]`
      );
      return `if (!Array.isArray(${valuePath})) {
    errors.push({ path: "${errorPath}", message: "Expected array", expected: "array", received: typeof ${valuePath} });
  } else {
    for (let i = 0; i < ${valuePath}.length; i++) {
      ${elementValidation}
    }
  }`;

    case "union": {
      if (!type.types || type.types.length === 0) {
        return "";
      }
      // For unions, we check if the value matches any of the types
      // This is simplified - in practice you'd want more sophisticated union handling
      const typeChecks = type.types.map((t) => {
        switch (t.kind) {
          case "string":
            return `typeof ${valuePath} === "string"`;
          case "number":
            return `typeof ${valuePath} === "number"`;
          case "boolean":
            return `typeof ${valuePath} === "boolean"`;
          case "null":
            return `${valuePath} === null`;
          case "literal": {
            const lit = typeof t.literalValue === "string"
              ? `"${t.literalValue}"`
              : String(t.literalValue);
            return `${valuePath} === ${lit}`;
          }
          default:
            return "true";
        }
      });
      const unionExpected = type.types.map(typeInfoToTypeScript).join(" | ");
      return `if (!(${typeChecks.join(" || ")})) {
    errors.push({ path: "${errorPath}", message: "Expected ${unionExpected}", expected: "${unionExpected}", received: String(${valuePath}) });
  }`;
    }

    case "object":
      if (!type.properties) {
        return `if (typeof ${valuePath} !== "object" || ${valuePath} === null) {
    errors.push({ path: "${errorPath}", message: "Expected object", expected: "object", received: typeof ${valuePath} });
  }`;
      }
      const propValidations = Object.entries(type.properties)
        .map(([propName, propType]) => {
          const propPath = `${valuePath}["${propName}"]`;
          const propErrorPath = errorPath ? `${errorPath}.${propName}` : propName;
          return generateTypeValidation(propType, propPath, propErrorPath);
        })
        .join("\n  ");
      return `if (typeof ${valuePath} !== "object" || ${valuePath} === null) {
    errors.push({ path: "${errorPath}", message: "Expected object", expected: "object", received: typeof ${valuePath} });
  } else {
    ${propValidations}
  }`;

    default:
      return "";
  }
}

/**
 * Generate a validator function for a command
 */
function generateCommandValidator(
  command: CommandInfo,
  aggregate: AggregateAnalysis
): string {
  const funcName = `validate${capitalize(aggregate.aggregateName)}${capitalize(command.methodName)}`;

  // Build the input type with 'id' always required
  const inputProps: string[] = ["id: string"];
  for (const param of command.parameters) {
    const typeStr = typeInfoToTypeScript(param.type);
    inputProps.push(`${param.name}: ${typeStr}`);
  }
  const inputType = `{ ${inputProps.join("; ")} }`;

  // Build validation code
  const validations: string[] = [];

  // Always validate 'id' as UUIDv7
  validations.push(`// Validate id (required UUIDv7)
  if (typeof obj.id !== "string") {
    errors.push({ path: "id", message: "Expected string", expected: "string", received: typeof obj.id });
  } else if (!isUUIDv7(obj.id)) {
    errors.push({ path: "id", message: "Expected UUIDv7", expected: "UUIDv7", received: obj.id });
  }`);

  // Validate each parameter
  for (const param of command.parameters) {
    const validation = generateTypeValidation(
      param.type,
      `obj.${param.name}`,
      param.name
    );
    if (validation) {
      validations.push(`// Validate ${param.name}
  ${validation}`);
    }
  }

  return `/**
 * Validate input for ${aggregate.aggregateName}.${command.methodName}
 */
export function ${funcName}(input: unknown): ValidationResult<${inputType}> {
  const errors: ValidationError[] = [];

  if (typeof input !== "object" || input === null) {
    return { success: false, errors: [{ path: "", message: "Expected object", expected: "object", received: typeof input }] };
  }

  const obj = input as Record<string, unknown>;

  ${validations.join("\n\n  ")}

  if (errors.length > 0) {
    return { success: false, errors };
  }

  return { success: true, data: obj as ${inputType} };
}`;
}

/**
 * Generate validator file for an aggregate
 */
export function generateValidatorFile(aggregate: AggregateAnalysis): GeneratedFile {
  const fileName = `${aggregate.aggregateName}.validator.ts`;

  const validators = aggregate.commands
    .map((cmd) => generateCommandValidator(cmd, aggregate))
    .join("\n\n");

  const content = `/**
 * Auto-generated validators for ${aggregate.className}
 * DO NOT EDIT - regenerate with \`spitestack compile\`
 *
 * @generated from ${aggregate.relativePath}
 */

${generateValidationTypes()}

${validators}
`;

  return {
    path: `validators/${fileName}`,
    content,
  };
}

/**
 * Generate all validator files
 */
export function generateValidators(aggregates: AggregateAnalysis[]): GeneratedFile[] {
  return aggregates.map(generateValidatorFile);
}

/**
 * Helper to capitalize first letter
 */
function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
}
