/**
 * Task aggregate - represents a todo item.
 */

export type TaskState = {
  title: string;
  description: string;
  completed: boolean;
  assigneeId: string | undefined;
};

export type TaskEvent =
  | { type: "TaskCreated"; title: string; description: string }
  | { type: "TaskCompleted" }
  | { type: "TaskReopened" }
  | { type: "TaskAssigned"; assigneeId: string }
  | { type: "TaskUnassigned" }
  | { type: "TaskTitleChanged"; title: string }
  | { type: "TaskDescriptionChanged"; description: string };

export class Task {
  static initialState: TaskState = {
    title: "",
    description: "",
    completed: false,
    assigneeId: undefined,
  };
  events: TaskEvent[] = [];
  state: TaskState = Task.initialState;

  emit(event: TaskEvent): void {
    this.events.push(event);
  }

  apply(event: TaskEvent): void {
    switch (event.type) {
      case "TaskCreated":
        this.state.title = event.title;
        this.state.description = event.description;
        break;
      case "TaskCompleted":
        this.state.completed = true;
        break;
      case "TaskReopened":
        this.state.completed = false;
        break;
      case "TaskAssigned":
        this.state.assigneeId = event.assigneeId;
        break;
      case "TaskUnassigned":
        this.state.assigneeId = undefined;
        break;
      case "TaskTitleChanged":
        this.state.title = event.title;
        break;
      case "TaskDescriptionChanged":
        this.state.description = event.description;
        break;
    }
  }

  create(title: string, description: string = ""): void {
    if (!title.trim()) {
      throw new Error("Title cannot be empty");
    }
    this.emit({ type: "TaskCreated", title, description });
  }

  complete(): void {
    if (this.state.completed) {
      throw new Error("Task is already completed");
    }
    this.emit({ type: "TaskCompleted" });
  }

  reopen(): void {
    if (!this.state.completed) {
      throw new Error("Task is not completed");
    }
    this.emit({ type: "TaskReopened" });
  }

  assign(assigneeId: string): void {
    if (!assigneeId.trim()) {
      throw new Error("Assignee ID cannot be empty");
    }
    this.emit({ type: "TaskAssigned", assigneeId });
  }

  unassign(): void {
    if (!this.state.assigneeId) {
      throw new Error("Task is not assigned");
    }
    this.emit({ type: "TaskUnassigned" });
  }

  changeTitle(title: string): void {
    if (!title.trim()) {
      throw new Error("Title cannot be empty");
    }
    this.emit({ type: "TaskTitleChanged", title });
  }

  changeDescription(description: string): void {
    this.emit({ type: "TaskDescriptionChanged", description });
  }
}
