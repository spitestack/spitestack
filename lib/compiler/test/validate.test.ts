/**
 * Validation tests.
 */

import { describe, expect, it } from "bun:test";
import { validateAggregatePurity, validateStructure, validateDomain } from "../src/validate/index.js";
import type { AggregateIR, DomainIR } from "../src/ir/index.js";

describe("validateAggregatePurity", () => {
  function makeAggregate(commandBody: string[]): AggregateIR {
    return {
      name: "TestAggregate",
      sourcePath: "test.ts",
      state: {
        fields: [{ name: "count", type: { kind: "number" }, optional: false }],
      },
      initialState: [],
      events: {
        name: "TestEvent",
        variants: [{ name: "Created", fields: [] }],
      },
      commands: [
        {
          name: "create",
          parameters: [],
          body: commandBody.map((raw) => ({
            kind: "expression" as const,
            expression: { kind: "identifier" as const, name: raw },
          })),
          access: "internal",
          roles: [],
        },
      ],
    };
  }

  it("passes for pure aggregate", () => {
    const aggregate = makeAggregate(["this.count"]);
    expect(() => validateAggregatePurity(aggregate)).not.toThrow();
  });

  it("fails for console.log call", () => {
    const aggregate: AggregateIR = {
      name: "TestAggregate",
      sourcePath: "test.ts",
      state: {
        fields: [{ name: "count", type: { kind: "number" }, optional: false }],
      },
      initialState: [],
      events: {
        name: "TestEvent",
        variants: [{ name: "Created", fields: [] }],
      },
      commands: [
        {
          name: "create",
          parameters: [],
          body: [
            {
              kind: "expression" as const,
              expression: {
                kind: "methodCall" as const,
                object: { kind: "identifier" as const, name: "console" },
                method: "log",
                arguments: [],
              },
            },
          ],
          access: "internal",
          roles: [],
        },
      ],
    };
    expect(() => validateAggregatePurity(aggregate)).toThrow(/console.*side effects/i);
  });

  it("fails for fetch call", () => {
    const aggregate: AggregateIR = {
      name: "TestAggregate",
      sourcePath: "test.ts",
      state: {
        fields: [{ name: "count", type: { kind: "number" }, optional: false }],
      },
      initialState: [],
      events: {
        name: "TestEvent",
        variants: [{ name: "Created", fields: [] }],
      },
      commands: [
        {
          name: "create",
          parameters: [],
          body: [
            {
              kind: "expression" as const,
              expression: {
                kind: "call" as const,
                callee: "fetch",
                arguments: [],
              },
            },
          ],
          access: "internal",
          roles: [],
        },
      ],
    };
    expect(() => validateAggregatePurity(aggregate)).toThrow(/fetch.*side effects/i);
  });

  it("fails for Date.now call", () => {
    const aggregate: AggregateIR = {
      name: "TestAggregate",
      sourcePath: "test.ts",
      state: {
        fields: [{ name: "count", type: { kind: "number" }, optional: false }],
      },
      initialState: [],
      events: {
        name: "TestEvent",
        variants: [{ name: "Created", fields: [] }],
      },
      commands: [
        {
          name: "create",
          parameters: [],
          body: [
            {
              kind: "expression" as const,
              expression: {
                kind: "methodCall" as const,
                object: { kind: "identifier" as const, name: "Date" },
                method: "now",
                arguments: [],
              },
            },
          ],
          access: "internal",
          roles: [],
        },
      ],
    };
    expect(() => validateAggregatePurity(aggregate)).toThrow(/Date\.now.*side effects/i);
  });

  it("fails for Math.random call", () => {
    const aggregate: AggregateIR = {
      name: "TestAggregate",
      sourcePath: "test.ts",
      state: {
        fields: [{ name: "count", type: { kind: "number" }, optional: false }],
      },
      initialState: [],
      events: {
        name: "TestEvent",
        variants: [{ name: "Created", fields: [] }],
      },
      commands: [
        {
          name: "create",
          parameters: [],
          body: [
            {
              kind: "expression" as const,
              expression: {
                kind: "methodCall" as const,
                object: { kind: "identifier" as const, name: "Math" },
                method: "random",
                arguments: [],
              },
            },
          ],
          access: "internal",
          roles: [],
        },
      ],
    };
    expect(() => validateAggregatePurity(aggregate)).toThrow(/Math\.random.*side effects/i);
  });
});

describe("validateStructure", () => {
  function makeDomain(aggregateOverrides: Partial<AggregateIR>): DomainIR {
    const aggregate: AggregateIR = {
      name: "TestAggregate",
      sourcePath: "test.ts",
      state: {
        fields: [{ name: "count", type: { kind: "number" }, optional: false }],
      },
      initialState: [],
      events: {
        name: "TestEvent",
        variants: [{ name: "Created", fields: [] }],
      },
      commands: [
        {
          name: "create",
          parameters: [],
          body: [],
          access: "internal",
          roles: [],
        },
      ],
      ...aggregateOverrides,
    };
    return {
      aggregates: [aggregate],
      projections: [],
      orchestrators: [],
      sourceDir: ".",
    };
  }

  it("passes for valid aggregate", () => {
    const domain = makeDomain({});
    expect(() => validateStructure(domain)).not.toThrow();
  });

  it("fails for aggregate without events", () => {
    const domain = makeDomain({
      events: {
        name: "TestEvent",
        variants: [],
      },
    });
    expect(() => validateStructure(domain)).toThrow(/event/i);
  });

  it("fails for aggregate without state fields", () => {
    const domain = makeDomain({
      state: {
        fields: [],
      },
    });
    expect(() => validateStructure(domain)).toThrow(/state/i);
  });
});

describe("validateDomain", () => {
  it("validates complete domain", () => {
    const domain: DomainIR = {
      aggregates: [
        {
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
          commands: [
            {
              name: "increment",
              parameters: [{ name: "amount", type: { kind: "number" } }],
              body: [],
              access: "internal",
              roles: [],
            },
          ],
        },
      ],
      projections: [],
      orchestrators: [],
      sourceDir: ".",
    };

    expect(() => validateDomain(domain)).not.toThrow();
  });

  it("throws for impure aggregate in domain", () => {
    const domain: DomainIR = {
      aggregates: [
        {
          name: "BadAggregate",
          sourcePath: "bad.ts",
          state: {
            fields: [{ name: "data", type: { kind: "string" }, optional: false }],
          },
          initialState: [],
          events: {
            name: "BadEvent",
            variants: [{ name: "Created", fields: [] }],
          },
          commands: [
            {
              name: "create",
              parameters: [],
              body: [
                {
                  kind: "expression",
                  expression: {
                    kind: "methodCall",
                    object: { kind: "identifier", name: "console" },
                    method: "log",
                    arguments: [{ kind: "stringLiteral", value: "impure" }],
                  },
                },
              ],
              access: "internal",
              roles: [],
            },
          ],
        },
      ],
      projections: [],
      orchestrators: [],
      sourceDir: ".",
    };

    expect(() => validateDomain(domain)).toThrow(/console.*side effects/i);
  });
});
