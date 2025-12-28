/**
 * User aggregate - represents a user who can be assigned tasks.
 */

export type UserState = {
  name: string;
  email: string;
  active: boolean;
};

export type UserEvent =
  | { type: "UserCreated"; name: string; email: string }
  | { type: "UserNameChanged"; name: string }
  | { type: "UserEmailChanged"; email: string }
  | { type: "UserDeactivated" }
  | { type: "UserReactivated" };

export class User {
  static initialState: UserState = {
    name: "",
    email: "",
    active: true,
  };
  events: UserEvent[] = [];
  state: UserState = User.initialState;

  emit(event: UserEvent): void {
    this.events.push(event);
  }

  apply(event: UserEvent): void {
    switch (event.type) {
      case "UserCreated":
        this.state.name = event.name;
        this.state.email = event.email;
        this.state.active = true;
        break;
      case "UserNameChanged":
        this.state.name = event.name;
        break;
      case "UserEmailChanged":
        this.state.email = event.email;
        break;
      case "UserDeactivated":
        this.state.active = false;
        break;
      case "UserReactivated":
        this.state.active = true;
        break;
    }
  }

  create(name: string, email: string): void {
    if (!name.trim()) {
      throw new Error("Name cannot be empty");
    }
    if (!email.includes("@")) {
      throw new Error("Invalid email address");
    }
    this.emit({ type: "UserCreated", name, email });
  }

  changeName(name: string): void {
    if (!name.trim()) {
      throw new Error("Name cannot be empty");
    }
    this.emit({ type: "UserNameChanged", name });
  }

  changeEmail(email: string): void {
    if (!email.includes("@")) {
      throw new Error("Invalid email address");
    }
    this.emit({ type: "UserEmailChanged", email });
  }

  deactivate(): void {
    if (!this.state.active) {
      throw new Error("User is already deactivated");
    }
    this.emit({ type: "UserDeactivated" });
  }

  reactivate(): void {
    if (this.state.active) {
      throw new Error("User is already active");
    }
    this.emit({ type: "UserReactivated" });
  }
}
