/**
 * Code generation tests.
 */

import { describe, expect, it } from "bun:test";
import { toSnakeCase, toPascalCase, typeToTs, generate } from "../src/codegen/index.js";
import type { DomainIR, DomainType } from "../src/ir/index.js";

describe("toSnakeCase", () => {
  it("converts PascalCase to snake_case", () => {
    expect(toSnakeCase("PascalCase")).toBe("pascal_case");
    expect(toSnakeCase("UserProfile")).toBe("user_profile");
  });

  it("converts camelCase to snake_case", () => {
    expect(toSnakeCase("camelCase")).toBe("camel_case");
    expect(toSnakeCase("getUserById")).toBe("get_user_by_id");
  });

  it("handles single words", () => {
    expect(toSnakeCase("word")).toBe("word");
    expect(toSnakeCase("Word")).toBe("word");
  });

  it("handles empty string", () => {
    expect(toSnakeCase("")).toBe("");
  });
});

describe("toPascalCase", () => {
  it("converts snake_case to PascalCase", () => {
    expect(toPascalCase("snake_case")).toBe("SnakeCase");
    expect(toPascalCase("user_profile")).toBe("UserProfile");
  });

  it("converts camelCase to PascalCase", () => {
    expect(toPascalCase("camelCase")).toBe("CamelCase");
  });

  it("handles single words", () => {
    expect(toPascalCase("word")).toBe("Word");
  });

  it("handles empty string", () => {
    expect(toPascalCase("")).toBe("");
  });
});

describe("typeToTs", () => {
  it("converts primitive types", () => {
    expect(typeToTs({ kind: "string" })).toBe("string");
    expect(typeToTs({ kind: "number" })).toBe("number");
    expect(typeToTs({ kind: "boolean" })).toBe("boolean");
  });

  it("converts array types", () => {
    expect(typeToTs({ kind: "array", element: { kind: "string" } })).toBe("string[]");
    expect(
      typeToTs({
        kind: "array",
        element: { kind: "array", element: { kind: "number" } },
      })
    ).toBe("number[][]");
  });

  it("converts option types", () => {
    expect(typeToTs({ kind: "option", inner: { kind: "string" } })).toBe("string | undefined");
  });

  it("converts object types", () => {
    const objectType: DomainType = {
      kind: "object",
      fields: [
        { name: "x", type: { kind: "number" }, optional: false },
        { name: "y", type: { kind: "number" }, optional: false },
      ],
    };
    expect(typeToTs(objectType)).toBe("{ x: number; y: number }");
  });

  it("converts reference types", () => {
    expect(typeToTs({ kind: "reference", name: "UserId" })).toBe("UserId");
  });
});

describe("generate", () => {
  it("generates code for a simple aggregate", () => {
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
              {
                name: "Reset",
                fields: [],
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
            {
              name: "reset",
              parameters: [],
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

    const result = generate(domain, "./domain");

    // Check that we have generated files
    expect(result.files.length).toBeGreaterThan(0);

    // Check validators exist
    const validatorsFile = result.files.find((f) => f.path === "validators/counter.validator.ts");
    expect(validatorsFile).toBeDefined();
    expect(validatorsFile?.content).toContain("validateIncrement");
    expect(validatorsFile?.content).toContain("validateReset");

    // Check handlers exist
    const handlersFile = result.files.find((f) => f.path === "handlers/counter.handlers.ts");
    expect(handlersFile).toBeDefined();
    expect(handlersFile?.content).toContain("handleIncrement");
    expect(handlersFile?.content).toContain("handleReset");

    // Check router exists
    const routerFile = result.files.find((f) => f.path === "router.ts");
    expect(routerFile).toBeDefined();
    expect(routerFile?.content).toContain("/counter");
  });

  it("generates code for multiple aggregates", () => {
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
            variants: [{ name: "Incremented", fields: [] }],
          },
          commands: [
            {
              name: "increment",
              parameters: [],
              body: [],
              access: "internal",
              roles: [],
            },
          ],
        },
        {
          name: "User",
          sourcePath: "user.ts",
          state: {
            fields: [
              { name: "name", type: { kind: "string" }, optional: false },
              { name: "email", type: { kind: "string" }, optional: false },
            ],
          },
          initialState: [],
          events: {
            name: "UserEvent",
            variants: [
              {
                name: "Created",
                fields: [
                  { name: "name", type: { kind: "string" } },
                  { name: "email", type: { kind: "string" } },
                ],
              },
            ],
          },
          commands: [
            {
              name: "create",
              parameters: [
                { name: "name", type: { kind: "string" } },
                { name: "email", type: { kind: "string" } },
              ],
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

    const result = generate(domain, "./domain");

    // Check validators for both aggregates
    expect(result.files.some((f) => f.path === "validators/counter.validator.ts")).toBe(true);
    expect(result.files.some((f) => f.path === "validators/user.validator.ts")).toBe(true);

    // Check handlers for both aggregates
    expect(result.files.some((f) => f.path === "handlers/counter.handlers.ts")).toBe(true);
    expect(result.files.some((f) => f.path === "handlers/user.handlers.ts")).toBe(true);

    // Check router includes both routes
    const routerFile = result.files.find((f) => f.path === "router.ts");
    expect(routerFile?.content).toContain("/counter");
    expect(routerFile?.content).toContain("/user");
  });

  it("handles empty domain", () => {
    const domain: DomainIR = {
      aggregates: [],
      projections: [],
      orchestrators: [],
      sourceDir: ".",
    };

    const result = generate(domain, "./domain");

    // Should still generate router (empty)
    const routerFile = result.files.find((f) => f.path === "router.ts");
    expect(routerFile).toBeDefined();
  });

  it("generates proper TypeScript types in validators", () => {
    const domain: DomainIR = {
      aggregates: [
        {
          name: "Order",
          sourcePath: "order.ts",
          state: {
            fields: [{ name: "items", type: { kind: "array", element: { kind: "string" } }, optional: false }],
          },
          initialState: [],
          events: {
            name: "OrderEvent",
            variants: [
              {
                name: "ItemAdded",
                fields: [{ name: "itemId", type: { kind: "string" } }],
              },
            ],
          },
          commands: [
            {
              name: "addItem",
              parameters: [
                { name: "itemId", type: { kind: "string" } },
                { name: "quantity", type: { kind: "number" } },
                { name: "metadata", type: { kind: "option", inner: { kind: "string" } } },
              ],
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

    const result = generate(domain, "./domain");

    const validatorsFile = result.files.find((f) => f.path === "validators/order.validator.ts");
    expect(validatorsFile).toBeDefined();
    expect(validatorsFile?.content).toContain("itemId");
    expect(validatorsFile?.content).toContain("quantity");
    expect(validatorsFile?.content).toContain("metadata");
  });
});
