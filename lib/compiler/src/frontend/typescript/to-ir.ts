/**
 * Convert TypeScript AST to language-agnostic IR.
 */

import { CompilerError } from "../../diagnostic/index.js";
import {
  type AccessLevel,
  type AggregateIR,
  type BinaryOp,
  type CommandIR,
  type ColumnDef,
  type DomainIR,
  type DomainType,
  type EventField,
  type EventTypeIR,
  type EventVariant,
  type ExpressionIR,
  type FieldDef,
  type IndexDef,
  type InitialValue,
  type ObjectType,
  type ParameterIR,
  type ProjectionIR,
  type ProjectionKind,
  type ProjectionSchema,
  type QueryMethodIR,
  type SqlType,
  type StateShape,
  type StatementIR,
  type SubscribedEvent,
  type TimeSeriesSignals,
  type UnaryOp,
  domainTypeToSqlType,
  isRangeParam,
  isTimeRelatedName,
  TIMESTAMP_FIELDS,
  TIME_STRING_METHODS,
} from "../../ir/index.js";
import type {
  ClassDecl,
  Expression,
  MethodDecl,
  ObjectProperty,
  Parameter,
  ParsedFile,
  PropertyDecl,
  Statement,
  TypeAlias,
  TypeNode,
} from "./ast.js";

/**
 * Converts a parsed file to IR and adds it to the domain.
 */
export function convertToIR(file: ParsedFile, domain: DomainIR): void {
  // Collect event types and state types from this file
  const eventTypes = file.typeAliases.filter((t) => t.name.endsWith("Event"));
  const stateTypes = file.typeAliases.filter((t) => t.name.endsWith("State"));

  // Find aggregate and projection classes
  for (const cls of file.classes) {
    if (isAggregate(cls)) {
      const aggregate = convertAggregate(cls, eventTypes, stateTypes, file.path);
      if (aggregate) {
        domain.aggregates.push(aggregate);
      }
    } else if (isProjection(cls)) {
      const projection = convertProjection(cls, eventTypes, file.path);
      if (projection) {
        domain.projections.push(projection);
      }
    }
  }
}

/**
 * Checks if a class is an aggregate (has initialState, events, emit, apply).
 */
function isAggregate(cls: ClassDecl): boolean {
  const hasInitialState = cls.properties.some((p) => p.name === "initialState" && p.isStatic);
  const hasEvents = cls.properties.some((p) => p.name === "events");
  const hasEmit = cls.methods.some((m) => m.name === "emit");
  const hasApply = cls.methods.some((m) => m.name === "apply");

  return hasInitialState && hasEvents && hasEmit && hasApply;
}

/**
 * Converts a class declaration to an AggregateIR.
 */
function convertAggregate(
  cls: ClassDecl,
  eventTypes: TypeAlias[],
  stateTypes: TypeAlias[],
  sourcePath: string
): AggregateIR | undefined {
  const name = cls.name.replace(/Aggregate$/, "");

  // Find matching event type
  const eventTypeName = `${name}Event`;
  const eventType = eventTypes.find((t) => t.name === eventTypeName);
  if (!eventType) {
    console.warn(`Missing event type ${eventTypeName} for aggregate ${cls.name}`);
    return undefined;
  }

  // Find matching state type
  const stateTypeName = `${name}State`;
  const stateType = stateTypes.find((t) => t.name === stateTypeName);
  if (!stateType) {
    console.warn(`Missing state type ${stateTypeName} for aggregate ${cls.name}`);
    return undefined;
  }

  // Convert event type
  const events = convertEventType(eventType);
  if (!events) {
    return undefined;
  }

  // Convert state type
  const state = convertStateType(stateType);
  if (!state) {
    return undefined;
  }

  // Extract initial state values
  const initialState = extractInitialState(cls);

  // Extract commands (public methods that aren't emit, apply, constructor, or getters/setters)
  const commands = cls.methods
    .filter(
      (m) =>
        m.visibility === "public" &&
        !["emit", "apply", "constructor"].includes(m.name) &&
        !m.name.startsWith("get_") &&
        !m.name.startsWith("set_")
    )
    .map((m) => convertCommand(m));

  // Extract raw apply body for TS->TS pass-through
  const rawApplyBody = cls.methods.find((m) => m.name === "apply")?.rawBody;

  return {
    name,
    sourcePath,
    state,
    initialState,
    events,
    commands,
    rawApplyBody,
  };
}

/**
 * Converts a type alias to an EventTypeIR.
 */
function convertEventType(typeAlias: TypeAlias): EventTypeIR | undefined {
  const typeNode = typeAlias.typeNode;

  let variants: EventVariant[];

  if (typeNode.kind === "union") {
    variants = typeNode.variants
      .map(convertEventVariant)
      .filter((v): v is EventVariant => v !== undefined);
  } else if (typeNode.kind === "objectLiteral") {
    const variant = convertObjectToVariant(typeNode.properties);
    if (variant) {
      variants = [variant];
    } else {
      return undefined;
    }
  } else {
    console.warn(`Invalid event type ${typeAlias.name}: must be a union or object type`);
    return undefined;
  }

  return {
    name: typeAlias.name,
    variants,
  };
}

/**
 * Converts a union member to an event variant.
 */
function convertEventVariant(typeNode: TypeNode): EventVariant | undefined {
  if (typeNode.kind === "objectLiteral") {
    return convertObjectToVariant(typeNode.properties);
  }
  return undefined;
}

/**
 * Converts an object literal to an event variant.
 */
function convertObjectToVariant(props: ObjectProperty[]): EventVariant | undefined {
  // Find the "type" discriminant field
  const typeProp = props.find((p) => p.name === "type");
  if (!typeProp) {
    return undefined;
  }

  // Extract variant name from literal type
  let variantName: string;
  if (typeProp.typeNode.kind === "primitive") {
    // Remove quotes if present
    variantName = typeProp.typeNode.name.replace(/^['"]|['"]$/g, "");
  } else {
    return undefined;
  }

  // Convert other fields
  const fields: EventField[] = props
    .filter((p) => p.name !== "type")
    .map((p) => ({
      name: p.name,
      type: convertTypeNode(p.typeNode),
    }));

  return {
    name: variantName,
    fields,
  };
}

/**
 * Converts a type alias to an ObjectType for state.
 */
function convertStateType(typeAlias: TypeAlias): ObjectType | undefined {
  if (typeAlias.typeNode.kind !== "objectLiteral") {
    console.warn(`Invalid state type ${typeAlias.name}: must be an object type`);
    return undefined;
  }

  const fields: FieldDef[] = typeAlias.typeNode.properties.map((p) => ({
    name: p.name,
    type: convertTypeNode(p.typeNode),
    optional: p.optional,
  }));

  return { fields };
}

/**
 * Converts a TypeNode to a DomainType.
 */
function convertTypeNode(node: TypeNode): DomainType {
  switch (node.kind) {
    case "primitive":
      switch (node.name) {
        case "string":
          return { kind: "string" };
        case "number":
          return { kind: "number" };
        case "boolean":
          return { kind: "boolean" };
        default:
          return { kind: "string" }; // Default for literals
      }
    case "array":
      return { kind: "array", element: convertTypeNode(node.element) };
    case "optional":
      return { kind: "option", inner: convertTypeNode(node.inner) };
    case "reference":
      return { kind: "reference", name: node.name };
    case "objectLiteral":
      const fields: FieldDef[] = node.properties.map((p) => ({
        name: p.name,
        type: convertTypeNode(p.typeNode),
        optional: p.optional,
      }));
      return { kind: "object", fields };
    case "indexSignature":
      // For index signatures, represent as the value type
      return convertTypeNode(node.valueType);
    case "union":
      // Check if it's T | undefined (optional)
      const nonUndefined = node.variants.filter(
        (v) => !(v.kind === "primitive" && v.name === "undefined")
      );
      if (nonUndefined.length === 1 && nonUndefined[0]) {
        return { kind: "option", inner: convertTypeNode(nonUndefined[0]) };
      }
      // Complex union - just use first type
      return convertTypeNode(node.variants[0]!);
  }
}

/**
 * Extracts initial state values from the class.
 */
function extractInitialState(cls: ClassDecl): Array<{ field: string; value: InitialValue }> {
  const initialStateProp = cls.properties.find(
    (p) => p.name === "initialState" && p.isStatic
  );

  if (!initialStateProp?.initializer) {
    return [];
  }

  return parseInitialStateObject(initialStateProp.initializer);
}

/**
 * Parses an initial state object literal string into field values.
 */
function parseInitialStateObject(init: string): Array<{ field: string; value: InitialValue }> {
  const values: Array<{ field: string; value: InitialValue }> = [];

  // Simple parsing of object literal
  const content = init.trim().replace(/^\{/, "").replace(/\}$/, "");

  for (const pair of content.split(",")) {
    const parts = pair.split(":").map((s) => s.trim());
    if (parts.length === 2) {
      const key = parts[0]!;
      const value = parts[1]!;

      let initValue: InitialValue;
      if (value === '""' || value === "''") {
        initValue = { kind: "string", value: "" };
      } else if (value.startsWith('"') || value.startsWith("'")) {
        initValue = { kind: "string", value: value.replace(/^['"]|['"]$/g, "") };
      } else if (value === "false") {
        initValue = { kind: "boolean", value: false };
      } else if (value === "true") {
        initValue = { kind: "boolean", value: true };
      } else if (value === "null" || value === "undefined") {
        initValue = { kind: "null" };
      } else if (value === "[]") {
        initValue = { kind: "emptyArray" };
      } else if (value === "{}") {
        initValue = { kind: "emptyObject" };
      } else {
        const num = parseFloat(value);
        if (!isNaN(num)) {
          initValue = { kind: "number", value: num };
        } else {
          initValue = { kind: "null" };
        }
      }

      values.push({ field: key, value: initValue });
    }
  }

  return values;
}

/**
 * Converts a method declaration to a CommandIR.
 */
function convertCommand(method: MethodDecl): CommandIR {
  const parameters: ParameterIR[] = method.parameters.map((p) => ({
    name: p.name,
    type: p.typeNode ? convertTypeNode(p.typeNode) : { kind: "string" },
  }));

  const body: StatementIR[] = method.body
    .map((s) => convertStatement(s))
    .filter((s): s is StatementIR => s !== undefined);

  return {
    name: method.name,
    parameters,
    body,
    access: "internal" as AccessLevel,
    roles: [],
  };
}

/**
 * Converts a statement to StatementIR.
 */
function convertStatement(stmt: Statement): StatementIR | undefined {
  switch (stmt.kind) {
    case "if":
      const condition = convertExpression(stmt.condition);
      if (!condition) return undefined;
      return {
        kind: "if",
        condition,
        thenBranch: stmt.thenBranch
          .map(convertStatement)
          .filter((s): s is StatementIR => s !== undefined),
        elseBranch: stmt.elseBranch
          ?.map(convertStatement)
          .filter((s): s is StatementIR => s !== undefined),
      };

    case "throw":
      const message = extractErrorMessage(stmt.argument);
      return { kind: "throw", message };

    case "expression":
      // Check if this is a this.emit() call
      const emit = tryConvertEmit(stmt.expression);
      if (emit) return emit;
      const expr = convertExpression(stmt.expression);
      if (!expr) return undefined;
      return { kind: "expression", expression: expr };

    case "variableDecl":
      const value = stmt.initializer
        ? convertExpression(stmt.initializer)
        : { kind: "identifier" as const, name: "undefined" };
      if (!value) return undefined;
      return { kind: "let", name: stmt.name, value };

    case "return":
      return {
        kind: "return",
        value: stmt.value ? convertExpression(stmt.value) : undefined,
      };

    case "block":
      const stmts = stmt.statements
        .map(convertStatement)
        .filter((s): s is StatementIR => s !== undefined);
      return stmts.length === 1 ? stmts[0] : stmts[0];

    case "switch":
      // Switch statements in apply() are handled differently
      return { kind: "expression", expression: { kind: "identifier", name: "match" } };
  }
}

/**
 * Tries to convert a call expression to an emit statement.
 */
function tryConvertEmit(expr: Expression): StatementIR | undefined {
  if (expr.kind !== "call") return undefined;
  if (expr.callee.kind !== "memberAccess") return undefined;
  if (expr.callee.object.kind !== "this") return undefined;
  if (expr.callee.property !== "emit") return undefined;
  if (expr.arguments.length !== 1) return undefined;

  const arg = expr.arguments[0]!;
  if (arg.kind !== "objectLiteral") return undefined;

  // Find the type field
  const typeProp = arg.properties.find((p) => p.key === "type");
  if (!typeProp || typeProp.value.kind !== "stringLiteral") return undefined;

  const eventType = typeProp.value.value;

  // Convert other fields
  const fields = arg.properties
    .filter((p) => p.key !== "type")
    .map((p) => {
      const value = convertExpression(p.value);
      return value ? { name: p.key, value } : undefined;
    })
    .filter((f): f is { name: string; value: ExpressionIR } => f !== undefined);

  return { kind: "emit", eventType, fields };
}

/**
 * Extracts error message from throw new Error("message").
 */
function extractErrorMessage(expr: Expression): string {
  if (expr.kind === "new" && expr.arguments.length > 0) {
    const arg = expr.arguments[0]!;
    if (arg.kind === "stringLiteral") {
      return arg.value;
    }
  }
  return "Unknown error";
}

/**
 * Converts an expression to ExpressionIR.
 */
function convertExpression(expr: Expression): ExpressionIR | undefined {
  switch (expr.kind) {
    case "stringLiteral":
      return { kind: "stringLiteral", value: expr.value };
    case "numberLiteral":
      return { kind: "numberLiteral", value: expr.value };
    case "booleanLiteral":
      return { kind: "booleanLiteral", value: expr.value };
    case "identifier":
      return { kind: "identifier", name: expr.name };
    case "this":
      return { kind: "identifier", name: "self" };
    case "memberAccess":
      // Check for this.state.field pattern
      if (
        expr.object.kind === "memberAccess" &&
        expr.object.object.kind === "this" &&
        expr.object.property === "state"
      ) {
        return { kind: "stateAccess", field: expr.property };
      }
      const obj = convertExpression(expr.object);
      if (!obj) return undefined;
      return { kind: "propertyAccess", object: obj, property: expr.property };
    case "call":
      // Check if it's a method call
      if (expr.callee.kind === "memberAccess") {
        const callObj = convertExpression(expr.callee.object);
        if (!callObj) return undefined;
        const args = expr.arguments
          .map(convertExpression)
          .filter((a): a is ExpressionIR => a !== undefined);
        return {
          kind: "methodCall",
          object: callObj,
          method: expr.callee.property,
          arguments: args,
        };
      }
      // Regular function call
      const calleeName = expr.callee.kind === "identifier" ? expr.callee.name : "unknown";
      const callArgs = expr.arguments
        .map(convertExpression)
        .filter((a): a is ExpressionIR => a !== undefined);
      return { kind: "call", callee: calleeName, arguments: callArgs };
    case "binary":
      const left = convertExpression(expr.left);
      const right = convertExpression(expr.right);
      if (!left || !right) return undefined;
      const op = stringToBinaryOp(expr.operator);
      if (!op) return undefined;
      return { kind: "binary", left, operator: op, right };
    case "unary":
      const operand = convertExpression(expr.argument);
      if (!operand) return undefined;
      const unaryOp = stringToUnaryOp(expr.operator);
      if (!unaryOp) return undefined;
      return { kind: "unary", operator: unaryOp, operand };
    case "objectLiteral":
      const fields = expr.properties
        .map((p) => {
          const value = convertExpression(p.value);
          return value ? { name: p.key, value } : undefined;
        })
        .filter((f): f is { name: string; value: ExpressionIR } => f !== undefined);
      return { kind: "object", fields };
    case "arrayLiteral":
      const elements = expr.elements
        .map(convertExpression)
        .filter((e): e is ExpressionIR => e !== undefined);
      return { kind: "array", elements };
    case "nullLiteral":
      return { kind: "identifier", name: "null" };
    case "new":
      const newCallee = expr.callee.kind === "identifier" ? expr.callee.name : "unknown";
      const newArgs = expr.arguments
        .map(convertExpression)
        .filter((a): a is ExpressionIR => a !== undefined);
      return { kind: "new", callee: newCallee, arguments: newArgs };
    default:
      return { kind: "identifier", name: "unknown" };
  }
}

/**
 * Converts a string operator to BinaryOp.
 */
function stringToBinaryOp(op: string): BinaryOp | undefined {
  switch (op) {
    case "===":
    case "==":
      return "eq";
    case "!==":
    case "!=":
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
 * Converts a string operator to UnaryOp.
 */
function stringToUnaryOp(op: string): UnaryOp | undefined {
  switch (op) {
    case "!":
      return "not";
    case "-":
      return "neg";
    default:
      return undefined;
  }
}

// ============================================================================
// Projection Detection and Conversion
// ============================================================================

/**
 * Checks if a class is a projection.
 */
function isProjection(cls: ClassDecl): boolean {
  const hasBuild = cls.methods.some((m) => m.name === "build");
  const hasStateProperty = cls.properties.some((p) => {
    if (!p.typeNode) return false;
    return p.typeNode.kind === "indexSignature" || p.typeNode.kind === "objectLiteral";
  });

  return hasBuild && hasStateProperty;
}

/**
 * Converts a class declaration to a ProjectionIR.
 */
function convertProjection(
  cls: ClassDecl,
  eventTypes: TypeAlias[],
  sourcePath: string
): ProjectionIR | undefined {
  const name = cls.name;

  // Find the state property
  const stateProp = cls.properties.find((p) => {
    if (!p.typeNode) return false;
    return p.typeNode.kind === "indexSignature" || p.typeNode.kind === "objectLiteral";
  });

  if (!stateProp?.typeNode) {
    console.warn(`Missing state property in projection ${name}`);
    return undefined;
  }

  // Analyze state shape and determine projection kind
  const stateShape = analyzeStateShape(stateProp.typeNode);
  const timeSignals = detectTimeSeriesSignals(cls, stateShape);
  const kind = determineProjectionKind(stateShape, timeSignals);

  // Extract subscribed events from build method parameter
  const subscribedEvents = extractSubscribedEvents(cls, eventTypes);

  // Extract schema from state type
  const schema = extractProjectionSchema(stateProp, stateProp.typeNode, cls);
  if (!schema) {
    return undefined;
  }

  // Extract query methods
  const queries = extractQueryMethods(cls);

  // Get raw build body
  const rawBuildBody = cls.methods.find((m) => m.name === "build")?.rawBody;

  return {
    name,
    sourcePath,
    kind,
    subscribedEvents,
    schema,
    queries,
    rawBuildBody,
    access: "internal" as AccessLevel,
    roles: [],
  };
}

/**
 * Analyzes the state type to determine its shape.
 */
function analyzeStateShape(typeNode: TypeNode): StateShape {
  if (typeNode.kind === "indexSignature") {
    // Check if value is a number
    if (typeNode.valueType.kind === "primitive" && typeNode.valueType.name === "number") {
      return { kind: "indexedNumber", keyName: typeNode.keyName };
    }
    return {
      kind: "indexedObject",
      keyName: typeNode.keyName,
      valueType: convertTypeNode(typeNode.valueType),
    };
  }

  if (typeNode.kind === "objectLiteral") {
    const fields = typeNode.properties.map((p) => ({
      name: p.name,
      type: convertTypeNode(p.typeNode),
    }));
    return { kind: "namedFields", fields };
  }

  return { kind: "namedFields", fields: [] };
}

/**
 * Detects time-series signals from the class.
 */
function detectTimeSeriesSignals(cls: ClassDecl, stateShape: StateShape): TimeSeriesSignals {
  const signals: TimeSeriesSignals = {
    hasTimestampDerivation: hasTimestampDerivedKey(cls),
    hasTimeRelatedKeyName:
      stateShape.kind === "indexedNumber" && isTimeRelatedName(stateShape.keyName),
    hasRangeQueryMethods: hasRangeQueryMethods(cls),
  };
  return signals;
}

/**
 * Determines the projection kind based on state shape and time signals.
 */
function determineProjectionKind(
  stateShape: StateShape,
  timeSignals: TimeSeriesSignals
): ProjectionKind {
  switch (stateShape.kind) {
    case "indexedObject":
      return "denormalizedView";
    case "indexedNumber":
      if (
        timeSignals.hasTimestampDerivation ||
        timeSignals.hasTimeRelatedKeyName ||
        timeSignals.hasRangeQueryMethods
      ) {
        return "timeSeries";
      }
      return "aggregator";
    case "namedFields":
      return "aggregator";
  }
}

/**
 * Detects if the key in build() is derived from a timestamp field.
 */
function hasTimestampDerivedKey(cls: ClassDecl): boolean {
  const buildMethod = cls.methods.find((m) => m.name === "build");
  if (!buildMethod) return false;

  for (const stmt of buildMethod.body) {
    if (stmt.kind === "variableDecl" && stmt.initializer) {
      if (isTimestampDerivation(stmt.initializer)) {
        return true;
      }
    }
  }
  return false;
}

/**
 * Checks if an expression is a timestamp derivation pattern.
 */
function isTimestampDerivation(expr: Expression): boolean {
  if (expr.kind !== "call") return false;
  if (expr.callee.kind !== "memberAccess") return false;

  const method = expr.callee.property.toLowerCase();
  const isStringMethod = TIME_STRING_METHODS.some((m) => method.includes(m.toLowerCase()));

  if (isStringMethod) {
    // Check if the object is accessing a timestamp field
    if (expr.callee.object.kind === "memberAccess") {
      const fieldName = expr.callee.object.property.toLowerCase();
      return TIMESTAMP_FIELDS.some((f) => fieldName.includes(f.toLowerCase()));
    }
    // Could be chained
    if (expr.callee.object.kind === "call") {
      const innerCall = expr.callee.object;
      if (innerCall.callee.kind === "memberAccess") {
        const innerMethod = innerCall.callee.property.toLowerCase();
        if (innerMethod === "toisostring") {
          if (innerCall.callee.object.kind === "memberAccess") {
            const fieldName = innerCall.callee.object.property.toLowerCase();
            return TIMESTAMP_FIELDS.some((f) => fieldName.includes(f.toLowerCase()));
          }
        }
      }
    }
  }
  return false;
}

/**
 * Checks if the class has range query methods.
 */
function hasRangeQueryMethods(cls: ClassDecl): boolean {
  return cls.methods.some(
    (m) =>
      m.visibility === "public" &&
      m.name !== "build" &&
      m.name !== "constructor" &&
      m.parameters.some((p) => isRangeParam(p.name))
  );
}

/**
 * Extracts subscribed events from build method parameter.
 */
function extractSubscribedEvents(cls: ClassDecl, _eventTypes: TypeAlias[]): SubscribedEvent[] {
  const buildMethod = cls.methods.find((m) => m.name === "build");
  if (!buildMethod) return [];

  const eventParam = buildMethod.parameters[0];
  if (!eventParam?.typeNode) return [];

  const eventNames = extractEventNamesFromType(eventParam.typeNode);

  return eventNames.map((name) => ({
    eventName: name,
    aggregate: inferAggregateFromEvent(name),
  }));
}

/**
 * Extracts event type names from a type node.
 */
function extractEventNamesFromType(typeNode: TypeNode): string[] {
  switch (typeNode.kind) {
    case "union":
      return typeNode.variants.flatMap(extractEventNamesFromType);
    case "reference":
      return [typeNode.name];
    case "objectLiteral":
      const typeProp = typeNode.properties.find((p) => p.name === "type");
      if (typeProp && typeProp.typeNode.kind === "primitive") {
        const name = typeProp.typeNode.name.replace(/^['"]|['"]$/g, "");
        if (name) return [name];
      }
      return [];
    default:
      return [];
  }
}

/**
 * Infers the aggregate name from an event name.
 */
function inferAggregateFromEvent(eventName: string): string | undefined {
  const suffixes = [
    "Created",
    "Updated",
    "Deleted",
    "Completed",
    "Cancelled",
    "Added",
    "Removed",
    "Changed",
  ];
  for (const suffix of suffixes) {
    if (eventName.endsWith(suffix)) {
      return eventName.slice(0, -suffix.length);
    }
  }
  return undefined;
}

/**
 * Extracts projection schema from state property.
 */
function extractProjectionSchema(
  stateProp: PropertyDecl,
  stateType: TypeNode,
  cls: ClassDecl
): ProjectionSchema | undefined {
  const statePropertyName = stateProp.name;

  let primaryKeys: ColumnDef[];
  let columns: ColumnDef[];

  if (stateType.kind === "indexSignature") {
    // Primary key is the index key
    const pk: ColumnDef = {
      name: toSnakeCase(stateType.keyName),
      sqlType: "text",
      nullable: false,
    };

    // Columns from value type
    columns = extractColumnsFromType(stateType.valueType);
    primaryKeys = [pk];
  } else if (stateType.kind === "objectLiteral") {
    // For aggregators with named fields, use a singleton key
    const pk: ColumnDef = {
      name: "id",
      sqlType: "text",
      nullable: false,
      default: "'singleton'",
    };

    columns = stateType.properties.map((p) => ({
      name: toSnakeCase(p.name),
      sqlType: domainTypeToSqlType(convertTypeNode(p.typeNode)),
      nullable: p.optional,
    }));
    primaryKeys = [pk];
  } else {
    return undefined;
  }

  // Derive indexes from query method parameters
  const indexes = deriveIndexesFromQueries(cls, columns);

  return {
    statePropertyName,
    primaryKeys,
    columns,
    indexes,
  };
}

/**
 * Extracts column definitions from a type node.
 */
function extractColumnsFromType(typeNode: TypeNode): ColumnDef[] {
  if (typeNode.kind === "objectLiteral") {
    return typeNode.properties.map((p) => ({
      name: toSnakeCase(p.name),
      sqlType: domainTypeToSqlType(convertTypeNode(p.typeNode)),
      nullable: p.optional,
    }));
  }
  if (typeNode.kind === "primitive" && typeNode.name === "number") {
    return [
      {
        name: "value",
        sqlType: "real",
        nullable: false,
      },
    ];
  }
  return [];
}

/**
 * Derives indexes from query method parameters.
 */
function deriveIndexesFromQueries(cls: ClassDecl, columns: ColumnDef[]): IndexDef[] {
  const indexes: IndexDef[] = [];
  const columnNames = columns.map((c) => c.name);

  for (const method of cls.methods) {
    if (
      method.visibility !== "public" ||
      method.name === "build" ||
      method.name === "constructor"
    ) {
      continue;
    }

    for (const param of method.parameters) {
      const paramSnake = toSnakeCase(param.name);
      if (columnNames.includes(paramSnake)) {
        // Check if we already have this index
        if (!indexes.some((idx) => idx.columns.length === 1 && idx.columns[0] === paramSnake)) {
          indexes.push({
            name: `idx_${paramSnake}`,
            columns: [paramSnake],
            unique: false,
          });
        }
      }
    }
  }

  return indexes;
}

/**
 * Extracts query methods from a class.
 */
function extractQueryMethods(cls: ClassDecl): QueryMethodIR[] {
  return cls.methods
    .filter(
      (m) =>
        m.visibility === "public" &&
        m.name !== "build" &&
        m.name !== "constructor" &&
        !m.name.startsWith("get_") &&
        !m.name.startsWith("set_")
    )
    .map((m) => {
      const parameters: ParameterIR[] = m.parameters.map((p) => ({
        name: p.name,
        type: p.typeNode ? convertTypeNode(p.typeNode) : { kind: "string" },
      }));

      const returnType = m.returnType ? convertTypeNode(m.returnType) : undefined;
      const indexedColumns = m.parameters.map((p) => toSnakeCase(p.name));
      const isRangeQuery = m.parameters.some((p) => isRangeParam(p.name));

      return {
        name: m.name,
        parameters,
        returnType,
        indexedColumns,
        isRangeQuery,
        rawBody: m.rawBody,
      };
    });
}

/**
 * Converts a camelCase string to snake_case.
 */
function toSnakeCase(s: string): string {
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
 * Applies access configuration from App registration to the domain IR.
 */
export function applyAccessConfig(
  domain: DomainIR,
  appConfig: import("../../ir/index.js").AppConfig
): void {
  for (const aggregate of domain.aggregates) {
    // Look for config by aggregate name
    let entityConfig = appConfig.entities.get(aggregate.name);
    if (!entityConfig) {
      entityConfig = appConfig.entities.get(`${aggregate.name}Aggregate`);
    }

    if (entityConfig) {
      // Apply access config to each command
      for (const cmd of aggregate.commands) {
        const methodConfig = entityConfig.methods.get(cmd.name);
        if (methodConfig) {
          cmd.access = methodConfig.access;
          cmd.roles = methodConfig.roles.length > 0 ? methodConfig.roles : entityConfig.roles;
        } else {
          cmd.access = entityConfig.access;
          cmd.roles = entityConfig.roles;
        }
      }
    }
  }
}
