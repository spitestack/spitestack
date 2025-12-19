/**
 * Todo Aggregate
 */

import type { TodoEvent } from "./events";

// State type
export type TodoState = {
  title: string;
  completed: boolean;
};

export class TodoAggregate {
  static readonly initialState: TodoState = {
    title: "",
    completed: false,
  };

  readonly events: TodoEvent[] = [];
  private state: TodoState;

  constructor(initialState: TodoState = TodoAggregate.initialState) {
    this.state = { ...initialState };
  }

  get currentState(): TodoState {
    return this.state;
  }

  protected emit(event: TodoEvent): void {
    this.events.push(event);
    this.apply(event);
  }

  apply(event: TodoEvent): void {
    switch (event.type) {
      case "TodoCreated":
        this.state.title = event.title;
        break;
      case "TodoCompleted":
        this.state.completed = true;
        break;
      case "TodoRenamed":
        this.state.title = event.title;
        break;
    }
  }

  // Commands
  create(title: string): void {
    if (this.state.title) {
      throw new Error("Todo already exists");
    }
    this.emit({ type: "TodoCreated", title });
  }

  complete(): void {
    if (!this.state.title) {
      throw new Error("Todo does not exist");
    }
    if (this.state.completed) {
      throw new Error("Todo already completed");
    }
    this.emit({ type: "TodoCompleted" });
  }

  rename(title: string): void {
    if (!this.state.title) {
      throw new Error("Todo does not exist");
    }
    this.emit({ type: "TodoRenamed", title });
  }
}
