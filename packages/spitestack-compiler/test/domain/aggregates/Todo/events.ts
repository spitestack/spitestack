/**
 * Todo Domain Events
 */

export type TodoEvent =
  | { type: "TodoCreated"; title: string }
  | { type: "TodoCompleted" }
  | { type: "TodoRenamed"; title: string };
