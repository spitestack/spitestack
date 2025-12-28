/**
 * Counter aggregate - minimal example.
 */

export type CounterState = {
  count: number;
};

export type CounterEvent =
  | { type: "Incremented"; amount: number }
  | { type: "Decremented"; amount: number }
  | { type: "Reset" };

export class Counter {
  static initialState: CounterState = { count: 0 };
  events: CounterEvent[] = [];
  state: CounterState = Counter.initialState;

  emit(event: CounterEvent): void {
    this.events.push(event);
  }

  apply(event: CounterEvent): void {
    switch (event.type) {
      case "Incremented":
        this.state.count += event.amount;
        break;
      case "Decremented":
        this.state.count -= event.amount;
        break;
      case "Reset":
        this.state.count = 0;
        break;
    }
  }

  increment(amount: number = 1): void {
    if (amount <= 0) {
      throw new Error("Amount must be positive");
    }
    this.emit({ type: "Incremented", amount });
  }

  decrement(amount: number = 1): void {
    if (amount <= 0) {
      throw new Error("Amount must be positive");
    }
    if (this.state.count - amount < 0) {
      throw new Error("Count cannot go negative");
    }
    this.emit({ type: "Decremented", amount });
  }

  reset(): void {
    this.emit({ type: "Reset" });
  }
}
