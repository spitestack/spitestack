import ts from "typescript";
import { relative, dirname, join, basename } from "node:path";
import { existsSync, readFileSync } from "node:fs";
import type {
  ParsedFile,
  AggregateAnalysis,
  CommandInfo,
  ParameterInfo,
  TypeInfo,
  EventTypeInfo,
  EventVariant,
  StateTypeInfo,
  Diagnostic,
} from "../types";
import { DiagnosticCode, DiagnosticMessages } from "../errors/codes";
import { getLineAndColumn, getEndLineAndColumn } from "./parse";

/**
 * Methods to exclude from command detection
 */
const EXCLUDED_METHODS = new Set([
  "constructor",
  "apply",
  "emit",
]);

/**
 * Analyze a TypeScript type node and extract TypeInfo
 */
export function extractTypeInfo(
  typeNode: ts.TypeNode | undefined,
  typeChecker: ts.TypeChecker,
  sourceFile: ts.SourceFile
): TypeInfo {
  if (!typeNode) {
    return { kind: "unknown" };
  }

  // String type
  if (ts.isTypeReferenceNode(typeNode)) {
    const typeName = typeNode.typeName.getText(sourceFile);

    if (typeName === "string") return { kind: "string" };
    if (typeName === "number") return { kind: "number" };
    if (typeName === "boolean") return { kind: "boolean" };

    // Array<T>
    if (typeName === "Array" && typeNode.typeArguments?.length === 1) {
      return {
        kind: "array",
        elementType: extractTypeInfo(typeNode.typeArguments[0], typeChecker, sourceFile),
      };
    }

    // Try to resolve the type
    const type = typeChecker.getTypeAtLocation(typeNode);
    return extractTypeFromType(type, typeChecker);
  }

  // Keyword types
  if (typeNode.kind === ts.SyntaxKind.StringKeyword) {
    return { kind: "string" };
  }
  if (typeNode.kind === ts.SyntaxKind.NumberKeyword) {
    return { kind: "number" };
  }
  if (typeNode.kind === ts.SyntaxKind.BooleanKeyword) {
    return { kind: "boolean" };
  }
  if (typeNode.kind === ts.SyntaxKind.NullKeyword) {
    return { kind: "null" };
  }
  if (typeNode.kind === ts.SyntaxKind.UndefinedKeyword) {
    return { kind: "undefined" };
  }

  // Array type T[]
  if (ts.isArrayTypeNode(typeNode)) {
    return {
      kind: "array",
      elementType: extractTypeInfo(typeNode.elementType, typeChecker, sourceFile),
    };
  }

  // Object literal type { prop: type }
  if (ts.isTypeLiteralNode(typeNode)) {
    const properties: Record<string, TypeInfo> = {};

    for (const member of typeNode.members) {
      if (ts.isPropertySignature(member) && member.name) {
        const propName = member.name.getText(sourceFile);
        properties[propName] = extractTypeInfo(member.type, typeChecker, sourceFile);
      }
    }

    return { kind: "object", properties };
  }

  // Union type A | B
  if (ts.isUnionTypeNode(typeNode)) {
    const types = typeNode.types.map((t) =>
      extractTypeInfo(t, typeChecker, sourceFile)
    );
    return { kind: "union", types };
  }

  // Literal types
  if (ts.isLiteralTypeNode(typeNode)) {
    if (ts.isStringLiteral(typeNode.literal)) {
      return { kind: "literal", literalValue: typeNode.literal.text };
    }
    if (ts.isNumericLiteral(typeNode.literal)) {
      return { kind: "literal", literalValue: Number(typeNode.literal.text) };
    }
    if (typeNode.literal.kind === ts.SyntaxKind.TrueKeyword) {
      return { kind: "literal", literalValue: true };
    }
    if (typeNode.literal.kind === ts.SyntaxKind.FalseKeyword) {
      return { kind: "literal", literalValue: false };
    }
  }

  return { kind: "unknown" };
}

/**
 * Extract TypeInfo from a ts.Type object
 */
function extractTypeFromType(type: ts.Type, typeChecker: ts.TypeChecker): TypeInfo {
  // String
  if (type.flags & ts.TypeFlags.String) {
    return { kind: "string" };
  }

  // Number
  if (type.flags & ts.TypeFlags.Number) {
    return { kind: "number" };
  }

  // Boolean
  if (type.flags & ts.TypeFlags.Boolean || type.flags & ts.TypeFlags.BooleanLiteral) {
    return { kind: "boolean" };
  }

  // Null
  if (type.flags & ts.TypeFlags.Null) {
    return { kind: "null" };
  }

  // Undefined
  if (type.flags & ts.TypeFlags.Undefined) {
    return { kind: "undefined" };
  }

  // String/Number literals
  if (type.flags & ts.TypeFlags.StringLiteral) {
    return { kind: "literal", literalValue: (type as ts.StringLiteralType).value };
  }
  if (type.flags & ts.TypeFlags.NumberLiteral) {
    return { kind: "literal", literalValue: (type as ts.NumberLiteralType).value };
  }

  // Union types
  if (type.isUnion()) {
    const types = type.types.map((t) => extractTypeFromType(t, typeChecker));
    return { kind: "union", types };
  }

  // Object types
  if (type.flags & ts.TypeFlags.Object) {
    const objectType = type as ts.ObjectType;

    // Check for array
    const symbol = type.getSymbol();
    if (symbol?.getName() === "Array") {
      const typeArgs = typeChecker.getTypeArguments(objectType as ts.TypeReference);
      if (typeArgs.length > 0) {
        return {
          kind: "array",
          elementType: extractTypeFromType(typeArgs[0], typeChecker),
        };
      }
    }

    // Regular object
    const properties: Record<string, TypeInfo> = {};
    const props = type.getProperties();

    for (const prop of props) {
      const propType = typeChecker.getTypeOfSymbolAtLocation(
        prop,
        prop.valueDeclaration!
      );
      properties[prop.getName()] = extractTypeFromType(propType, typeChecker);
    }

    return { kind: "object", properties };
  }

  return { kind: "unknown" };
}

/**
 * Find a type alias by name in the parsed file
 */
function findTypeAlias(
  name: string,
  parsedFile: ParsedFile
): ts.TypeAliasDeclaration | null {
  for (const typeAlias of parsedFile.typeAliases) {
    if (typeAlias.name.getText(parsedFile.sourceFile) === name) {
      return typeAlias;
    }
  }
  return null;
}

/**
 * Check if file is in the new folder structure (aggregate.ts)
 */
function isInFolderStructure(filePath: string): boolean {
  return basename(filePath) === "aggregate.ts";
}

/**
 * Get the aggregate folder name from the file path
 * e.g., /path/to/domain/Todo/aggregate.ts -> "Todo"
 */
function getAggregateFolderName(filePath: string): string {
  return basename(dirname(filePath));
}

/**
 * Find and parse the events.ts file in the same folder as aggregate.ts
 */
function findEventsFile(
  aggregateFilePath: string,
  program: ts.Program
): { sourceFile: ts.SourceFile; typeAliases: ts.TypeAliasDeclaration[] } | null {
  const eventsPath = join(dirname(aggregateFilePath), "events.ts");

  if (!existsSync(eventsPath)) {
    return null;
  }

  // Try to get the source file from the program
  let sourceFile = program.getSourceFile(eventsPath);

  // If not in program, create it manually
  if (!sourceFile) {
    const content = readFileSync(eventsPath, "utf-8");
    sourceFile = ts.createSourceFile(
      eventsPath,
      content,
      ts.ScriptTarget.ESNext,
      true,
      ts.ScriptKind.TS
    );
  }

  // Extract type aliases
  const typeAliases: ts.TypeAliasDeclaration[] = [];

  function visit(node: ts.Node) {
    if (ts.isTypeAliasDeclaration(node)) {
      typeAliases.push(node);
    }
    ts.forEachChild(node, visit);
  }

  visit(sourceFile);

  return { sourceFile, typeAliases };
}

/**
 * Find event type in events file
 */
function findEventTypeInEventsFile(
  eventsFile: { sourceFile: ts.SourceFile; typeAliases: ts.TypeAliasDeclaration[] },
  eventTypeName: string
): ts.TypeAliasDeclaration | null {
  for (const typeAlias of eventsFile.typeAliases) {
    if (typeAlias.name.getText(eventsFile.sourceFile) === eventTypeName) {
      return typeAlias;
    }
  }
  return null;
}

/**
 * Extract event type info from a union type
 */
function extractEventType(
  typeAlias: ts.TypeAliasDeclaration,
  typeChecker: ts.TypeChecker,
  sourceFile: ts.SourceFile
): EventTypeInfo {
  const typeName = typeAlias.name.getText(sourceFile);
  const variants: EventVariant[] = [];

  const typeNode = typeAlias.type;

  // Must be a union type
  if (!ts.isUnionTypeNode(typeNode)) {
    return { typeName, variants, node: typeAlias };
  }

  for (const member of typeNode.types) {
    // Each member should be an object literal with a 'type' property
    if (!ts.isTypeLiteralNode(member)) {
      continue;
    }

    const properties: Record<string, TypeInfo> = {};
    let discriminant: string | null = null;

    for (const prop of member.members) {
      if (!ts.isPropertySignature(prop) || !prop.name) {
        continue;
      }

      const propName = prop.name.getText(sourceFile);
      const propType = extractTypeInfo(prop.type, typeChecker, sourceFile);

      if (propName === "type" && propType.kind === "literal") {
        discriminant = String(propType.literalValue);
      } else {
        properties[propName] = propType;
      }
    }

    if (discriminant) {
      variants.push({ type: discriminant, properties });
    }
  }

  return { typeName, variants, node: typeAlias };
}

/**
 * Extract state type info
 */
function extractStateType(
  typeAlias: ts.TypeAliasDeclaration,
  typeChecker: ts.TypeChecker,
  sourceFile: ts.SourceFile
): StateTypeInfo {
  const typeName = typeAlias.name.getText(sourceFile);
  const type = extractTypeInfo(typeAlias.type, typeChecker, sourceFile);

  return { typeName, type, node: typeAlias };
}

/**
 * Check if a class is an aggregate (name ends with "Aggregate")
 */
function isAggregateClass(className: string): boolean {
  return className.endsWith("Aggregate");
}

/**
 * Derive aggregate name from folder name (lowercase)
 * e.g., /path/to/Todo/aggregate.ts -> "todo"
 */
function deriveAggregateName(filePath: string): string {
  const folderName = getAggregateFolderName(filePath);
  return folderName.toLowerCase();
}

/**
 * Check if a class has a required member
 */
function hasRequiredMember(
  classDecl: ts.ClassDeclaration,
  memberName: string,
  kind: "property" | "method" | "getter",
  isStatic: boolean = false
): boolean {
  for (const member of classDecl.members) {
    const modifiers = ts.canHaveModifiers(member) ? ts.getModifiers(member) : undefined;
    const memberIsStatic = modifiers?.some(
      (m: ts.Modifier) => m.kind === ts.SyntaxKind.StaticKeyword
    ) ?? false;

    if (memberIsStatic !== isStatic) continue;

    const name = member.name?.getText();

    if (name !== memberName) continue;

    switch (kind) {
      case "property":
        return ts.isPropertyDeclaration(member);
      case "method":
        return ts.isMethodDeclaration(member);
      case "getter":
        return ts.isGetAccessorDeclaration(member);
    }
  }

  return false;
}

/**
 * Extract command methods from a class
 */
function extractCommands(
  classDecl: ts.ClassDeclaration,
  typeChecker: ts.TypeChecker,
  sourceFile: ts.SourceFile
): CommandInfo[] {
  const commands: CommandInfo[] = [];

  for (const member of classDecl.members) {
    if (!ts.isMethodDeclaration(member)) continue;

    // Skip static methods
    const isStatic = member.modifiers?.some(
      (m) => m.kind === ts.SyntaxKind.StaticKeyword
    );
    if (isStatic) continue;

    // Skip private/protected methods
    const isPrivate = member.modifiers?.some(
      (m) =>
        m.kind === ts.SyntaxKind.PrivateKeyword ||
        m.kind === ts.SyntaxKind.ProtectedKeyword
    );
    if (isPrivate) continue;

    const methodName = member.name?.getText(sourceFile);

    if (!methodName) continue;

    // Skip excluded methods
    if (EXCLUDED_METHODS.has(methodName)) continue;

    // Skip methods starting with _
    if (methodName.startsWith("_")) continue;

    // Extract parameters
    const parameters: ParameterInfo[] = [];

    for (const param of member.parameters) {
      const paramName = param.name.getText(sourceFile);
      const paramType = extractTypeInfo(param.type, typeChecker, sourceFile);
      const optional = param.questionToken !== undefined;

      parameters.push({
        name: paramName,
        type: paramType,
        optional,
        node: param,
      });
    }

    commands.push({
      methodName,
      parameters,
      node: member,
    });
  }

  return commands;
}

/**
 * Infer the event type name from aggregate class name
 * e.g., TodoAggregate -> TodoEvent
 */
function inferEventTypeName(className: string): string {
  if (className.endsWith("Aggregate")) {
    return className.replace(/Aggregate$/, "Event");
  }
  return `${className}Event`;
}

/**
 * Infer the state type name from aggregate class name
 * e.g., TodoAggregate -> TodoState
 */
function inferStateTypeName(className: string): string {
  if (className.endsWith("Aggregate")) {
    return className.replace(/Aggregate$/, "State");
  }
  return `${className}State`;
}

/**
 * Analyze a single class declaration as an aggregate
 */
export function analyzeAggregate(
  classDecl: ts.ClassDeclaration,
  parsedFile: ParsedFile,
  typeChecker: ts.TypeChecker,
  program: ts.Program,
  domainDir: string
): { aggregate: AggregateAnalysis | null; diagnostics: Diagnostic[] } {
  const diagnostics: Diagnostic[] = [];
  const sourceFile = parsedFile.sourceFile;
  const className = classDecl.name?.getText(sourceFile);

  if (!className) {
    return { aggregate: null, diagnostics };
  }

  // Check if class name ends with "Aggregate"
  if (!isAggregateClass(className)) {
    // Not an aggregate class
    return { aggregate: null, diagnostics };
  }

  // Derive aggregate name from folder name
  const aggregateName = deriveAggregateName(parsedFile.filePath);

  // Helper to create diagnostics
  const createDiag = (
    code: keyof typeof DiagnosticCode,
    node: ts.Node
  ): Diagnostic => {
    const { line, column } = getLineAndColumn(sourceFile, node.getStart());
    const { endLine, endColumn } = getEndLineAndColumn(sourceFile, node);

    return {
      code: DiagnosticCode[code],
      severity: "error",
      message: DiagnosticMessages[DiagnosticCode[code]],
      location: {
        filePath: parsedFile.filePath,
        line,
        column,
        endLine,
        endColumn,
      },
    };
  };

  // Check required members
  if (!hasRequiredMember(classDecl, "initialState", "property", true)) {
    diagnostics.push(createDiag("MISSING_INITIAL_STATE", classDecl));
  }

  if (!hasRequiredMember(classDecl, "events", "property", false)) {
    diagnostics.push(createDiag("MISSING_EVENTS_ARRAY", classDecl));
  }

  if (!hasRequiredMember(classDecl, "emit", "method", false)) {
    diagnostics.push(createDiag("MISSING_EMIT_METHOD", classDecl));
  }

  if (!hasRequiredMember(classDecl, "currentState", "getter", false)) {
    diagnostics.push(createDiag("MISSING_STATE_GETTER", classDecl));
  }

  if (!hasRequiredMember(classDecl, "apply", "method", false)) {
    diagnostics.push(createDiag("MISSING_APPLY_METHOD", classDecl));
  }

  // Extract event type
  const eventTypeName = inferEventTypeName(className);

  // Check if we're in the new folder structure
  const inFolderStructure = isInFolderStructure(parsedFile.filePath);

  let eventType: EventTypeInfo;
  let eventTypeAlias: ts.TypeAliasDeclaration | null = null;
  let eventsSourceFile: ts.SourceFile = sourceFile;

  if (inFolderStructure) {
    // Look for events.ts in the same folder
    const eventsFile = findEventsFile(parsedFile.filePath, program);

    if (eventsFile) {
      eventTypeAlias = findEventTypeInEventsFile(eventsFile, eventTypeName);
      if (eventTypeAlias) {
        eventsSourceFile = eventsFile.sourceFile;
      }
    }
  }

  // Fall back to same file if not found in events.ts
  if (!eventTypeAlias) {
    eventTypeAlias = findTypeAlias(eventTypeName, parsedFile);
    eventsSourceFile = sourceFile;
  }

  if (eventTypeAlias) {
    eventType = extractEventType(eventTypeAlias, typeChecker, eventsSourceFile);
  } else {
    const { line, column } = getLineAndColumn(sourceFile, classDecl.getStart());
    const suggestion = inFolderStructure
      ? `Create an events.ts file in the same folder:\n\nexport type ${eventTypeName} =\n  | { type: 'Created'; /* ... */ }\n  | { type: 'Updated'; /* ... */ };`
      : `Define the event type:\n\nexport type ${eventTypeName} =\n  | { type: 'Created'; /* ... */ }\n  | { type: 'Updated'; /* ... */ };`;

    diagnostics.push({
      code: DiagnosticCode.EVENT_TYPE_NOT_FOUND,
      severity: "error",
      message: `Could not find event type '${eventTypeName}'`,
      location: { filePath: parsedFile.filePath, line, column },
      suggestion,
    });
    eventType = { typeName: eventTypeName, variants: [], node: null };
  }

  // Validate event type has discriminants
  if (eventType.variants.length === 0 && eventTypeAlias) {
    const { line, column } = getLineAndColumn(eventsSourceFile, eventTypeAlias.getStart());
    diagnostics.push({
      code: DiagnosticCode.EVENT_NOT_UNION,
      severity: "error",
      message: DiagnosticMessages[DiagnosticCode.EVENT_NOT_UNION],
      location: { filePath: parsedFile.filePath, line, column },
    });
  }

  // Extract state type
  const stateTypeName = inferStateTypeName(className);
  const stateTypeAlias = findTypeAlias(stateTypeName, parsedFile);

  let stateType: StateTypeInfo;

  if (stateTypeAlias) {
    stateType = extractStateType(stateTypeAlias, typeChecker, sourceFile);
  } else {
    const { line, column } = getLineAndColumn(sourceFile, classDecl.getStart());
    diagnostics.push({
      code: DiagnosticCode.STATE_TYPE_NOT_FOUND,
      severity: "error",
      message: `Could not find state type '${stateTypeName}'`,
      location: { filePath: parsedFile.filePath, line, column },
      suggestion: `Define the state type:\n\nexport type ${stateTypeName} = {\n  /* your state properties */\n};`,
    });
    stateType = { typeName: stateTypeName, type: { kind: "unknown" }, node: null };
  }

  // Extract commands
  const commands = extractCommands(classDecl, typeChecker, sourceFile);

  // Warn if no commands found
  if (commands.length === 0) {
    const { line, column } = getLineAndColumn(sourceFile, classDecl.getStart());
    diagnostics.push({
      code: DiagnosticCode.NO_COMMANDS_FOUND,
      severity: "warning",
      message: DiagnosticMessages[DiagnosticCode.NO_COMMANDS_FOUND],
      location: { filePath: parsedFile.filePath, line, column },
    });
  }

  const aggregate: AggregateAnalysis = {
    className,
    aggregateName,
    filePath: parsedFile.filePath,
    relativePath: relative(domainDir, parsedFile.filePath),
    stateType,
    eventType,
    commands,
    node: classDecl,
  };

  return { aggregate, diagnostics };
}

/**
 * Analyze all parsed files and extract aggregate information
 */
export function analyzeFiles(
  parsedFiles: ParsedFile[],
  typeChecker: ts.TypeChecker,
  program: ts.Program,
  domainDir: string
): { aggregates: AggregateAnalysis[]; diagnostics: Diagnostic[] } {
  const aggregates: AggregateAnalysis[] = [];
  const diagnostics: Diagnostic[] = [];

  for (const parsedFile of parsedFiles) {
    for (const classDecl of parsedFile.classes) {
      const result = analyzeAggregate(classDecl, parsedFile, typeChecker, program, domainDir);

      if (result.aggregate) {
        aggregates.push(result.aggregate);
      }

      diagnostics.push(...result.diagnostics);
    }
  }

  return { aggregates, diagnostics };
}
