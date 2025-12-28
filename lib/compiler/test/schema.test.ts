/**
 * Schema evolution tests.
 */

import { describe, expect, it } from "bun:test";
import {
  createLockFile,
  diffSchema,
  validateNoBreakingChanges,
  type SchemaLockFile,
} from "../src/schema/index.js";
import type { DomainIR, AggregateIR } from "../src/ir/index.js";

function makeAggregate(overrides: Partial<AggregateIR>): AggregateIR {
  return {
    name: "Counter",
    sourcePath: "counter.ts",
    state: {
      fields: [{ name: "count", type: { kind: "number" }, optional: false }],
    },
    initialState: [],
    events: {
      name: "CounterEvent",
      variants: [
        {
          name: "Incremented",
          fields: [{ name: "amount", type: { kind: "number" } }],
        },
      ],
    },
    commands: [],
    ...overrides,
  };
}

function makeDomain(aggregates: AggregateIR[]): DomainIR {
  return {
    aggregates,
    projections: [],
    orchestrators: [],
    sourceDir: ".",
  };
}

describe("createLockFile", () => {
  it("creates lock file from domain", () => {
    const domain = makeDomain([
      makeAggregate({
        events: {
          name: "CounterEvent",
          variants: [
            {
              name: "Incremented",
              fields: [{ name: "amount", type: { kind: "number" } }],
            },
            {
              name: "Reset",
              fields: [],
            },
          ],
        },
      }),
    ]);

    const lock = createLockFile(domain);

    expect(lock.version).toBe(1);
    expect(lock.timestamp).toBeDefined();
    expect(lock.aggregates["Counter"]).toBeDefined();
    expect(lock.aggregates["Counter"]?.events).toHaveLength(2);
    expect(lock.aggregates["Counter"]?.events[0]?.name).toBe("Incremented");
    expect(lock.aggregates["Counter"]?.events[0]?.fields).toEqual([
      { name: "amount", type: "number" },
    ]);
    expect(lock.aggregates["Counter"]?.events[1]?.name).toBe("Reset");
    expect(lock.aggregates["Counter"]?.events[1]?.fields).toEqual([]);
  });

  it("serializes complex field types", () => {
    const domain = makeDomain([
      makeAggregate({
        name: "Test",
        events: {
          name: "TestEvent",
          variants: [
            {
              name: "Created",
              fields: [
                { name: "tags", type: { kind: "array", element: { kind: "string" } } },
                { name: "metadata", type: { kind: "option", inner: { kind: "string" } } },
              ],
            },
          ],
        },
      }),
    ]);

    const lock = createLockFile(domain);

    expect(lock.aggregates["Test"]?.events[0]?.fields).toEqual([
      { name: "tags", type: "string[]" },
      { name: "metadata", type: "string?" },
    ]);
  });
});

describe("diffSchema", () => {
  it("detects no changes", () => {
    const domain = makeDomain([
      makeAggregate({
        events: {
          name: "CounterEvent",
          variants: [{ name: "Incremented", fields: [] }],
        },
      }),
    ]);

    const lock: SchemaLockFile = {
      version: 1,
      timestamp: new Date().toISOString(),
      aggregates: {
        Counter: {
          events: [{ name: "Incremented", fields: [] }],
        },
      },
    };

    const diff = diffSchema(domain, lock);

    expect(diff.nonBreaking).toHaveLength(0);
    expect(diff.breaking).toHaveLength(0);
  });

  it("detects added event as non-breaking", () => {
    const domain = makeDomain([
      makeAggregate({
        events: {
          name: "CounterEvent",
          variants: [
            { name: "Incremented", fields: [] },
            { name: "Decremented", fields: [] },
          ],
        },
      }),
    ]);

    const lock: SchemaLockFile = {
      version: 1,
      timestamp: new Date().toISOString(),
      aggregates: {
        Counter: {
          events: [{ name: "Incremented", fields: [] }],
        },
      },
    };

    const diff = diffSchema(domain, lock);

    expect(diff.nonBreaking).toHaveLength(1);
    expect(diff.nonBreaking[0]).toEqual({
      kind: "eventAdded",
      aggregate: "Counter",
      event: "Decremented",
    });
    expect(diff.breaking).toHaveLength(0);
  });

  it("detects removed event as breaking", () => {
    const domain = makeDomain([
      makeAggregate({
        events: {
          name: "CounterEvent",
          variants: [{ name: "Incremented", fields: [] }],
        },
      }),
    ]);

    const lock: SchemaLockFile = {
      version: 1,
      timestamp: new Date().toISOString(),
      aggregates: {
        Counter: {
          events: [
            { name: "Incremented", fields: [] },
            { name: "Decremented", fields: [] },
          ],
        },
      },
    };

    const diff = diffSchema(domain, lock);

    expect(diff.breaking).toHaveLength(1);
    expect(diff.breaking[0]).toEqual({
      kind: "eventRemoved",
      aggregate: "Counter",
      event: "Decremented",
    });
  });

  it("detects added optional field as non-breaking", () => {
    const domain = makeDomain([
      makeAggregate({
        events: {
          name: "CounterEvent",
          variants: [
            {
              name: "Incremented",
              fields: [
                { name: "amount", type: { kind: "number" } },
                { name: "reason", type: { kind: "option", inner: { kind: "string" } } },
              ],
            },
          ],
        },
      }),
    ]);

    const lock: SchemaLockFile = {
      version: 1,
      timestamp: new Date().toISOString(),
      aggregates: {
        Counter: {
          events: [{ name: "Incremented", fields: [{ name: "amount", type: "number" }] }],
        },
      },
    };

    const diff = diffSchema(domain, lock);

    expect(diff.nonBreaking).toHaveLength(1);
    expect(diff.nonBreaking[0]).toEqual({
      kind: "fieldAdded",
      aggregate: "Counter",
      event: "Incremented",
      field: "reason",
      isOptional: true,
    });
  });

  it("detects added required field as breaking", () => {
    const domain = makeDomain([
      makeAggregate({
        events: {
          name: "CounterEvent",
          variants: [
            {
              name: "Incremented",
              fields: [
                { name: "amount", type: { kind: "number" } },
                { name: "userId", type: { kind: "string" } },
              ],
            },
          ],
        },
      }),
    ]);

    const lock: SchemaLockFile = {
      version: 1,
      timestamp: new Date().toISOString(),
      aggregates: {
        Counter: {
          events: [{ name: "Incremented", fields: [{ name: "amount", type: "number" }] }],
        },
      },
    };

    const diff = diffSchema(domain, lock);

    expect(diff.breaking).toHaveLength(1);
    expect(diff.breaking[0]).toEqual({
      kind: "fieldAdded",
      aggregate: "Counter",
      event: "Incremented",
      field: "userId",
      isOptional: false,
    });
  });

  it("detects removed field as breaking", () => {
    const domain = makeDomain([
      makeAggregate({
        events: {
          name: "CounterEvent",
          variants: [{ name: "Incremented", fields: [] }],
        },
      }),
    ]);

    const lock: SchemaLockFile = {
      version: 1,
      timestamp: new Date().toISOString(),
      aggregates: {
        Counter: {
          events: [{ name: "Incremented", fields: [{ name: "amount", type: "number" }] }],
        },
      },
    };

    const diff = diffSchema(domain, lock);

    expect(diff.breaking).toHaveLength(1);
    expect(diff.breaking[0]).toEqual({
      kind: "fieldRemoved",
      aggregate: "Counter",
      event: "Incremented",
      field: "amount",
    });
  });

  it("detects field type change as breaking", () => {
    const domain = makeDomain([
      makeAggregate({
        events: {
          name: "CounterEvent",
          variants: [
            {
              name: "Incremented",
              fields: [{ name: "amount", type: { kind: "string" } }],
            },
          ],
        },
      }),
    ]);

    const lock: SchemaLockFile = {
      version: 1,
      timestamp: new Date().toISOString(),
      aggregates: {
        Counter: {
          events: [{ name: "Incremented", fields: [{ name: "amount", type: "number" }] }],
        },
      },
    };

    const diff = diffSchema(domain, lock);

    expect(diff.breaking).toHaveLength(1);
    expect(diff.breaking[0]).toEqual({
      kind: "fieldTypeChanged",
      aggregate: "Counter",
      event: "Incremented",
      field: "amount",
      from: "number",
      to: "string",
    });
  });

  it("detects new aggregate as non-breaking", () => {
    const domain = makeDomain([
      makeAggregate({
        events: {
          name: "CounterEvent",
          variants: [{ name: "Incremented", fields: [] }],
        },
      }),
      makeAggregate({
        name: "User",
        events: {
          name: "UserEvent",
          variants: [{ name: "Created", fields: [] }],
        },
      }),
    ]);

    const lock: SchemaLockFile = {
      version: 1,
      timestamp: new Date().toISOString(),
      aggregates: {
        Counter: {
          events: [{ name: "Incremented", fields: [] }],
        },
      },
    };

    const diff = diffSchema(domain, lock);

    expect(diff.nonBreaking).toHaveLength(1);
    expect(diff.nonBreaking[0]).toEqual({
      kind: "eventAdded",
      aggregate: "User",
      event: "Created",
    });
  });

  it("detects removed aggregate as breaking", () => {
    const domain = makeDomain([]);

    const lock: SchemaLockFile = {
      version: 1,
      timestamp: new Date().toISOString(),
      aggregates: {
        Counter: {
          events: [{ name: "Incremented", fields: [] }],
        },
      },
    };

    const diff = diffSchema(domain, lock);

    expect(diff.breaking).toHaveLength(1);
    expect(diff.breaking[0]).toEqual({
      kind: "eventRemoved",
      aggregate: "Counter",
      event: "Incremented",
    });
  });
});

describe("validateNoBreakingChanges", () => {
  it("passes when no breaking changes", () => {
    const diff = {
      nonBreaking: [{ kind: "eventAdded" as const, aggregate: "Counter", event: "Reset" }],
      breaking: [],
    };

    expect(() => validateNoBreakingChanges(diff)).not.toThrow();
  });

  it("throws when breaking changes exist", () => {
    const diff = {
      nonBreaking: [],
      breaking: [{ kind: "eventRemoved" as const, aggregate: "Counter", event: "Incremented" }],
    };

    expect(() => validateNoBreakingChanges(diff)).toThrow(/removed/i);
  });
});
