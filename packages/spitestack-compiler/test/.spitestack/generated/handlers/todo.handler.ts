/**
 * Auto-generated handler for TodoAggregate
 * DO NOT EDIT - regenerate with `spitestack compile`
 *
 * @generated from Todo/aggregate.ts
 */

import type { SpiteDbNapi } from "@spitestack/db";
import { TodoAggregate } from "../../../domain/aggregates/Todo/aggregate";
import type { TodoEvent } from "../../../domain/aggregates/Todo/events";
import {
  validateTodoCreate, validateTodoComplete, validateTodoRename,
  type ValidationError as ValidationErrorType,
} from "../validators/todo.validator";

/**
 * Error thrown when validation fails
 */
export class ValidationError extends Error {
  constructor(public readonly errors: ValidationErrorType[]) {
    super(`Validation failed: ${errors.map((e) => e.message).join(", ")}`);
    this.name = "ValidationError";
  }
}

export interface CommandResult {
  aggregateId: string;
  revision: number;
  events: unknown[];
}

export interface TodoCommandContext {
  db: SpiteDbNapi;
  commandId: string;
  tenant: string;
}

export interface TodoCreateInput {
  id: string;
  title: string;
}

export interface TodoCompleteInput {
  id: string;
}

export interface TodoRenameInput {
  id: string;
  title: string;
}

/**
 * Load aggregate, execute command, extract events, persist to SpiteDB
 */
async function executeCommand<TInput extends { id: string }>(
  ctx: TodoCommandContext,
  input: TInput,
  execute: (aggregate: TodoAggregate) => void
): Promise<CommandResult> {
  // Load existing events for this aggregate (fromRev=0, limit=10000)
  const existingEvents = await ctx.db.readStream(input.id, 0, 10000, ctx.tenant);

  // Create aggregate and replay events to reconstruct state
  const aggregate = new TodoAggregate();

  for (const event of existingEvents) {
    const parsed = JSON.parse(event.data.toString()) as TodoEvent;
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
}

export const todoHandlers = {
  async create(ctx: TodoCommandContext, input: TodoCreateInput): Promise<CommandResult> {
    const result = validateTodoCreate(input);
    if (!result.success) {
      throw new ValidationError(result.errors);
    }
    const validated = result.data;

    return executeCommand(ctx, validated, (agg) => {
      agg.create(validated.title);
    });
  },

  async complete(ctx: TodoCommandContext, input: TodoCompleteInput): Promise<CommandResult> {
    const result = validateTodoComplete(input);
    if (!result.success) {
      throw new ValidationError(result.errors);
    }
    const validated = result.data;

    return executeCommand(ctx, validated, (agg) => {
      agg.complete();
    });
  },

  async rename(ctx: TodoCommandContext, input: TodoRenameInput): Promise<CommandResult> {
    const result = validateTodoRename(input);
    if (!result.success) {
      throw new ValidationError(result.errors);
    }
    const validated = result.data;

    return executeCommand(ctx, validated, (agg) => {
      agg.rename(validated.title);
    });
  }
};

export type TodoCommand =
  | { type: "todo.create"; payload: TodoCreateInput }
  | { type: "todo.complete"; payload: TodoCompleteInput }
  | { type: "todo.rename"; payload: TodoRenameInput };
