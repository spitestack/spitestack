/**
 * Auto-generated SpiteDB wiring
 * DO NOT EDIT - regenerate with `spitestack compile`
 */

import type { SpiteDB } from "@spitestack/db";
import { todoHandlers, type TodoCommand } from "./handlers/todo.handler";

/**
 * Union of all command types
 */
export type Command = TodoCommand;

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
    case "todo.create":
      return todoHandlers.create(ctx, command.payload);

    case "todo.complete":
      return todoHandlers.complete(ctx, command.payload);

    case "todo.rename":
      return todoHandlers.rename(ctx, command.payload);

    default:
      const _exhaustive: never = command;
      throw new Error(`Unknown command type: ${(command as any).type}`);
  }
}

// Re-export handlers for direct access
export { todoHandlers } from "./handlers/todo.handler";
