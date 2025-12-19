import type { AggregateAnalysis, GeneratedFile } from "../types";

/**
 * Helper to capitalize first letter
 */
function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Generate the wiring file that connects all handlers
 */
export function generateWiringFile(aggregates: AggregateAnalysis[]): GeneratedFile {
  // Generate imports
  const handlerImports = aggregates
    .map((agg) => {
      const aggName = agg.aggregateName;
      const handlers = `${aggName}Handlers`;
      const commandType = `${capitalize(aggName)}Command`;
      return `import { ${handlers}, type ${commandType} } from "./handlers/${aggName}.handler";`;
    })
    .join("\n");

  // Generate Command union type
  const commandTypes = aggregates
    .map((agg) => `${capitalize(agg.aggregateName)}Command`)
    .join(" | ");

  // Generate switch cases for command routing
  const switchCases = aggregates.flatMap((agg) => {
    const aggName = agg.aggregateName;
    const handlers = `${aggName}Handlers`;

    return agg.commands.map((cmd) => {
      const caseType = `${aggName}.${cmd.methodName}`;
      return `    case "${caseType}":
      return ${handlers}.${cmd.methodName}(ctx, command.payload);`;
    });
  });

  const content = `/**
 * Auto-generated SpiteDB wiring
 * DO NOT EDIT - regenerate with \`spitestack compile\`
 */

import type { SpiteDB } from "@spitestack/db";
${handlerImports}

/**
 * Union of all command types
 */
export type Command = ${commandTypes};

/**
 * Context required for command execution
 */
export interface CommandContext {
  db: SpiteDB;
  commandId: string;
}

/**
 * Result of command execution
 */
export interface CommandResult {
  aggregateId: string;
  revision: number;
  events: unknown[];
}

/**
 * Execute a command and persist events to SpiteDB
 */
export async function executeCommand(
  ctx: CommandContext,
  command: Command
): Promise<CommandResult> {
  switch (command.type) {
${switchCases.join("\n\n")}

    default:
      const _exhaustive: never = command;
      throw new Error(\`Unknown command type: \${(command as any).type}\`);
  }
}

// Re-export handlers for direct access
${aggregates.map((agg) => `export { ${agg.aggregateName}Handlers } from "./handlers/${agg.aggregateName}.handler";`).join("\n")}
`;

  return {
    path: "wiring.ts",
    content,
  };
}

/**
 * Generate the index barrel file
 */
export function generateIndexFile(aggregates: AggregateAnalysis[]): GeneratedFile {
  const handlerExports = aggregates
    .map((agg) => {
      const aggName = agg.aggregateName;
      const handlers = `${aggName}Handlers`;
      const commandType = `${capitalize(aggName)}Command`;
      return `export { ${handlers}, type ${commandType} } from "./handlers/${aggName}.handler";`;
    })
    .join("\n");

  const validatorExports = aggregates
    .map((agg) => {
      const aggName = agg.aggregateName;
      const validators = agg.commands
        .map((cmd) => `validate${capitalize(aggName)}${capitalize(cmd.methodName)}`)
        .join(", ");
      return `export { ${validators} } from "./validators/${aggName}.validator";`;
    })
    .join("\n");

  const content = `/**
 * Auto-generated SpiteStack exports
 * DO NOT EDIT - regenerate with \`spitestack compile\`
 */

// Wiring
export { executeCommand, type Command, type CommandContext, type CommandResult } from "./wiring";

// Handlers
${handlerExports}

// Validators
${validatorExports}
`;

  return {
    path: "index.ts",
    content,
  };
}
