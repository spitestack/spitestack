import type {
  AggregateAnalysis,
  Diagnostic,
  ValidationResult,
  TypeInfo,
} from "../types";
import { DiagnosticCode, DiagnosticMessages } from "../errors/codes";
import { getLineAndColumn, getEndLineAndColumn } from "./parse";

/**
 * Check if a type is serializable to JSON
 */
function isSerializableType(type: TypeInfo): boolean {
  switch (type.kind) {
    case "string":
    case "number":
    case "boolean":
    case "null":
    case "literal":
      return true;

    case "array":
      return type.elementType ? isSerializableType(type.elementType) : false;

    case "object":
      if (!type.properties) return false;
      return Object.values(type.properties).every(isSerializableType);

    case "union":
      if (!type.types) return false;
      return type.types.every(isSerializableType);

    case "undefined":
    case "unknown":
      return false;
  }
}

/**
 * Validate command parameters are serializable
 */
function validateCommandParams(aggregate: AggregateAnalysis): Diagnostic[] {
  const diagnostics: Diagnostic[] = [];

  for (const command of aggregate.commands) {
    for (const param of command.parameters) {
      if (!isSerializableType(param.type)) {
        const sourceFile = aggregate.node.getSourceFile();
        const { line, column } = getLineAndColumn(
          sourceFile,
          param.node.getStart()
        );
        const { endLine, endColumn } = getEndLineAndColumn(sourceFile, param.node);

        diagnostics.push({
          code: DiagnosticCode.COMMAND_PARAM_NOT_SERIALIZABLE,
          severity: "error",
          message: `Parameter '${param.name}' on command '${command.methodName}' has type that cannot be serialized to JSON`,
          location: {
            filePath: aggregate.filePath,
            line,
            column,
            endLine,
            endColumn,
          },
          suggestion: `Ensure parameter type only contains: string, number, boolean, null, arrays, or plain objects`,
        });
      }

      if (param.type.kind === "unknown") {
        const sourceFile = aggregate.node.getSourceFile();
        const { line, column } = getLineAndColumn(
          sourceFile,
          param.node.getStart()
        );

        diagnostics.push({
          code: DiagnosticCode.COMMAND_PARAM_UNKNOWN_TYPE,
          severity: "error",
          message: `Could not determine type of parameter '${param.name}' on command '${command.methodName}'`,
          location: {
            filePath: aggregate.filePath,
            line,
            column,
          },
          suggestion: `Add an explicit type annotation to the parameter`,
        });
      }
    }
  }

  return diagnostics;
}

/**
 * Validate event variants all have proper discriminants
 */
function validateEventVariants(aggregate: AggregateAnalysis): Diagnostic[] {
  const diagnostics: Diagnostic[] = [];

  if (aggregate.eventType.node && aggregate.eventType.variants.length === 0) {
    const sourceFile = aggregate.node.getSourceFile();
    const { line, column } = getLineAndColumn(
      sourceFile,
      aggregate.eventType.node.getStart()
    );

    diagnostics.push({
      code: DiagnosticCode.EVENT_NOT_UNION,
      severity: "error",
      message: DiagnosticMessages[DiagnosticCode.EVENT_NOT_UNION],
      location: {
        filePath: aggregate.filePath,
        line,
        column,
      },
    });
  }

  return diagnostics;
}

/**
 * Check for duplicate aggregate names
 */
function validateUniqueAggregateNames(
  aggregates: AggregateAnalysis[]
): Diagnostic[] {
  const diagnostics: Diagnostic[] = [];
  const seen = new Map<string, AggregateAnalysis>();

  for (const aggregate of aggregates) {
    const existing = seen.get(aggregate.aggregateName);

    if (existing) {
      const sourceFile = aggregate.node.getSourceFile();
      const { line, column } = getLineAndColumn(
        sourceFile,
        aggregate.node.getStart()
      );

      diagnostics.push({
        code: DiagnosticCode.DUPLICATE_AGGREGATE_NAME,
        severity: "error",
        message: `Duplicate aggregate name '${aggregate.aggregateName}' - already defined in ${existing.relativePath}`,
        location: {
          filePath: aggregate.filePath,
          line,
          column,
        },
        suggestion: `Choose a unique aggregate name`,
      });
    } else {
      seen.set(aggregate.aggregateName, aggregate);
    }
  }

  return diagnostics;
}

/**
 * Validate all analyzed aggregates
 */
export function validate(
  aggregates: AggregateAnalysis[],
  existingDiagnostics: Diagnostic[] = []
): ValidationResult {
  const diagnostics = [...existingDiagnostics];

  // Validate unique aggregate names
  diagnostics.push(...validateUniqueAggregateNames(aggregates));

  // Validate each aggregate
  for (const aggregate of aggregates) {
    diagnostics.push(...validateCommandParams(aggregate));
    diagnostics.push(...validateEventVariants(aggregate));
  }

  // Sort diagnostics by file and line
  diagnostics.sort((a, b) => {
    if (a.location.filePath !== b.location.filePath) {
      return a.location.filePath.localeCompare(b.location.filePath);
    }
    return a.location.line - b.location.line;
  });

  // Check for errors (not just warnings)
  const hasErrors = diagnostics.some((d) => d.severity === "error");

  return {
    valid: !hasErrors,
    diagnostics,
    aggregates,
  };
}
