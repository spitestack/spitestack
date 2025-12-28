/**
 * Todo App - app configuration.
 */

import { Task } from "./task";
import { User } from "./user";

export const app = {
  mode: "greenfield" as const,
};

export { Task, User };
