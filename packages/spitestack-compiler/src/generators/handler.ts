import { relative, dirname, join, resolve } from "node:path";
import type { AggregateAnalysis, CommandInfo, TypeInfo, GeneratedFile, CompilerConfig } from "../types";

/**
 * Helper to capitalize first letter
 */
function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
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
 * Generate input type interface for a command
 */
function generateInputType(command: CommandInfo, aggregate: AggregateAnalysis): string {
  const typeName = `${capitalize(aggregate.aggregateName)}${capitalize(command.methodName)}Input`;

  const props: string[] = ["id: string"];
  for (const param of command.parameters) {
    const typeStr = typeInfoToTypeScript(param.type);
    const optionalMark = param.optional ? "?" : "";
    props.push(`${param.name}${optionalMark}: ${typeStr}`);
  }

  return `export interface ${typeName} {
  ${props.join(";\n  ")};
}`;
}

/**
 * Generate the context type
 */
function generateContextType(aggregate: AggregateAnalysis): string {
  const typeName = `${capitalize(aggregate.aggregateName)}CommandContext`;

  return `export interface ${typeName} {
  db: SpiteDbNapi;
  commandId: string;
  tenant: string;
}`;
}

/**
 * Generate the command result type
 */
function generateResultType(): string {
  return `export interface CommandResult {
  aggregateId: string;
  revision: number;
  events: unknown[];
}`;
}

/**
 * Generate the executeCommand helper function
 */
function generateExecuteCommand(aggregate: AggregateAnalysis): string {
  const aggClassName = aggregate.className;
  const aggName = aggregate.aggregateName;
  const contextType = `${capitalize(aggName)}CommandContext`;
  const eventTypeName = aggregate.eventType.typeName;

  return `/**
 * Load aggregate, execute command, extract events, persist to SpiteDB
 */
async function executeCommand<TInput extends { id: string }>(
  ctx: ${contextType},
  input: TInput,
  execute: (aggregate: ${aggClassName}) => void
): Promise<CommandResult> {
  // Load existing events for this aggregate (fromRev=0, limit=10000)
  const existingEvents = await ctx.db.readStream(input.id, 0, 10000, ctx.tenant);

  // Create aggregate and replay events to reconstruct state
  const aggregate = new ${aggClassName}();

  for (const event of existingEvents) {
    const parsed = JSON.parse(event.data.toString()) as ${eventTypeName};
    aggregate.apply(parsed);
  }

  // Execute the command (populates aggregate.events)
  execute(aggregate);

  // Extract emitted events
  const newEvents = aggregate.events;

  if (newEvents.length === 0) {
    return {
      aggregateId: input.id,
      revision: existingEvents.length,
      events: [],
    };
  }

  // Persist to SpiteDB
  // expectedRev: 0 means stream must not exist, -1 means any revision
  const expectedRev = existingEvents.length === 0 ? 0 : existingEvents.length;
  const eventBuffers = newEvents.map((e) => Buffer.from(JSON.stringify(e)));

  const result = await ctx.db.append(
    input.id,
    ctx.commandId,
    expectedRev,
    eventBuffers,
    ctx.tenant
  );

  return {
    aggregateId: input.id,
    revision: result.lastRev,
    events: newEvents,
  };
}`;
}

/**
 * Generate a single command handler
 */
function generateCommandHandler(command: CommandInfo, aggregate: AggregateAnalysis): string {
  const aggName = aggregate.aggregateName;
  const methodName = command.methodName;
  const contextType = `${capitalize(aggName)}CommandContext`;
  const inputType = `${capitalize(aggName)}${capitalize(methodName)}Input`;
  const validatorName = `validate${capitalize(aggName)}${capitalize(methodName)}`;

  // Build the method call with parameters
  const params = command.parameters.map((p) => `validated.${p.name}`).join(", ");
  const methodCall = params ? `agg.${methodName}(${params})` : `agg.${methodName}()`;

  return `async ${methodName}(ctx: ${contextType}, input: ${inputType}): Promise<CommandResult> {
    const result = ${validatorName}(input);
    if (!result.success) {
      throw new ValidationError(result.errors);
    }
    const validated = result.data;

    return executeCommand(ctx, validated, (agg) => {
      ${methodCall};
    });
  }`;
}

/**
 * Generate the handlers object
 */
function generateHandlersObject(aggregate: AggregateAnalysis): string {
  const aggName = aggregate.aggregateName;

  const handlers = aggregate.commands
    .map((cmd) => generateCommandHandler(cmd, aggregate))
    .join(",\n\n  ");

  return `export const ${aggName}Handlers = {
  ${handlers}
};`;
}

/**
 * Generate command type union
 */
function generateCommandTypeUnion(aggregate: AggregateAnalysis): string {
  const aggName = aggregate.aggregateName;

  const variants = aggregate.commands.map((cmd) => {
    const inputType = `${capitalize(aggName)}${capitalize(cmd.methodName)}Input`;
    return `| { type: "${aggName}.${cmd.methodName}"; payload: ${inputType} }`;
  });

  return `export type ${capitalize(aggName)}Command =
  ${variants.join("\n  ")};`;
}

/**
 * Calculate the relative import path from a generated handler file to a domain file
 */
function calculateDomainImportPath(
  aggregate: AggregateAnalysis,
  config: CompilerConfig
): string {
  // Handler file location: {outDir}/handlers/{aggName}.handler.ts
  const handlerDir = join(config.outDir, "handlers");

  // Domain file location: {domainDir}/{relativePath}
  const domainFile = join(config.domainDir, aggregate.relativePath);

  // Calculate relative path from handler directory to domain file
  let relativePath = relative(handlerDir, domainFile);

  // Remove .ts extension for import
  relativePath = relativePath.replace(/\.ts$/, "");

  // Ensure it starts with ./ or ../
  if (!relativePath.startsWith(".") && !relativePath.startsWith("/")) {
    relativePath = "./" + relativePath;
  }

  return relativePath;
}

/**
 * Generate handler file for an aggregate
 */
export function generateHandlerFile(
  aggregate: AggregateAnalysis,
  config: CompilerConfig
): GeneratedFile {
  const fileName = `${aggregate.aggregateName}.handler.ts`;
  const aggName = aggregate.aggregateName;

  // Generate input types for all commands
  const inputTypes = aggregate.commands
    .map((cmd) => generateInputType(cmd, aggregate))
    .join("\n\n");

  // Generate validator imports
  const validatorImports = aggregate.commands
    .map((cmd) => `validate${capitalize(aggName)}${capitalize(cmd.methodName)}`)
    .join(", ");

  // Calculate relative path from handlers/ to domain file
  const domainImportPath = calculateDomainImportPath(aggregate, config);

  // Check if using folder structure (aggregate.ts + events.ts)
  const isInFolderStructure = aggregate.relativePath.endsWith("/aggregate.ts") ||
    aggregate.relativePath.endsWith("\\aggregate.ts");

  // Build imports based on structure
  let aggregateImport: string;
  let typesImport: string;

  if (isInFolderStructure) {
    // Folder structure: import aggregate from aggregate.ts, events from events.ts
    const eventsImportPath = domainImportPath.replace(/\/aggregate$/, "/events");
    aggregateImport = `import { ${aggregate.className} } from "${domainImportPath}";`;
    typesImport = `import type { ${aggregate.eventType.typeName} } from "${eventsImportPath}";`;
  } else {
    // Single file: import everything from the same file
    aggregateImport = `import { ${aggregate.className} } from "${domainImportPath}";`;
    typesImport = `import type { ${aggregate.eventType.typeName} } from "${domainImportPath}";`;
  }

  const content = `/**
 * Auto-generated handler for ${aggregate.className}
 * DO NOT EDIT - regenerate with \`spitestack compile\`
 *
 * @generated from ${aggregate.relativePath}
 */

import type { SpiteDbNapi } from "@spitestack/db";
${aggregateImport}
${typesImport}
import {
  ${validatorImports},
  type ValidationError as ValidationErrorType,
} from "../validators/${aggregate.aggregateName}.validator";

/**
 * Error thrown when validation fails
 */
export class ValidationError extends Error {
  constructor(public readonly errors: ValidationErrorType[]) {
    super(\`Validation failed: \${errors.map((e) => e.message).join(", ")}\`);
    this.name = "ValidationError";
  }
}

${generateResultType()}

${generateContextType(aggregate)}

${inputTypes}

${generateExecuteCommand(aggregate)}

${generateHandlersObject(aggregate)}

${generateCommandTypeUnion(aggregate)}
`;

  return {
    path: `handlers/${fileName}`,
    content,
  };
}

/**
 * Generate all handler files
 */
export function generateHandlers(
  aggregates: AggregateAnalysis[],
  config: CompilerConfig
): GeneratedFile[] {
  return aggregates.map((agg) => generateHandlerFile(agg, config));
}
