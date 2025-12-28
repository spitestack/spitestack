/**
 * Parser tests.
 */

import { describe, expect, it } from "bun:test";
import { TypeScriptParser, createParser } from "../src/frontend/typescript/parser.js";

describe("TypeScriptParser", () => {
  const parser = createParser();

  describe("imports", () => {
    it("parses named imports", () => {
      const source = `import { Foo, Bar as Baz } from "./module";`;
      const result = parser.parse(source, "test.ts");

      expect(result.imports).toHaveLength(1);
      expect(result.imports[0]?.source).toBe("./module");
      expect(result.imports[0]?.specifiers).toEqual([
        { name: "Foo" },
        { name: "Bar", alias: "Baz" },
      ]);
    });

    it("parses default imports", () => {
      const source = `import Foo from "./module";`;
      const result = parser.parse(source, "test.ts");

      expect(result.imports).toHaveLength(1);
      expect(result.imports[0]?.specifiers).toEqual([{ name: "Foo" }]);
    });

    it("parses namespace imports", () => {
      const source = `import * as Foo from "./module";`;
      const result = parser.parse(source, "test.ts");

      expect(result.imports).toHaveLength(1);
      expect(result.imports[0]?.specifiers).toEqual([{ name: "*", alias: "Foo" }]);
    });
  });

  describe("type aliases", () => {
    it("parses simple type alias", () => {
      const source = `type UserId = string;`;
      const result = parser.parse(source, "test.ts");

      expect(result.typeAliases).toHaveLength(1);
      expect(result.typeAliases[0]?.name).toBe("UserId");
      expect(result.typeAliases[0]?.typeNode).toEqual({ kind: "primitive", name: "string" });
    });

    it("parses union type alias", () => {
      const source = `type Status = "pending" | "approved" | "rejected";`;
      const result = parser.parse(source, "test.ts");

      expect(result.typeAliases).toHaveLength(1);
      expect(result.typeAliases[0]?.name).toBe("Status");
      expect(result.typeAliases[0]?.typeNode.kind).toBe("union");
    });

    it("parses object type alias", () => {
      const source = `type Point = { x: number; y: number; };`;
      const result = parser.parse(source, "test.ts");

      expect(result.typeAliases).toHaveLength(1);
      expect(result.typeAliases[0]?.typeNode.kind).toBe("objectLiteral");
    });

    it("parses optional union type (T | undefined)", () => {
      const source = `type MaybeString = string | undefined;`;
      const result = parser.parse(source, "test.ts");

      expect(result.typeAliases).toHaveLength(1);
      expect(result.typeAliases[0]?.typeNode).toEqual({
        kind: "optional",
        inner: { kind: "primitive", name: "string" },
      });
    });

    it("parses array type", () => {
      const source = `type Numbers = number[];`;
      const result = parser.parse(source, "test.ts");

      expect(result.typeAliases).toHaveLength(1);
      expect(result.typeAliases[0]?.typeNode).toEqual({
        kind: "array",
        element: { kind: "primitive", name: "number" },
      });
    });

    it("tracks export status", () => {
      const source = `export type Exported = string;\ntype NotExported = number;`;
      const result = parser.parse(source, "test.ts");

      expect(result.typeAliases).toHaveLength(2);
      expect(result.typeAliases[0]?.exported).toBe(true);
      expect(result.typeAliases[1]?.exported).toBe(false);
    });

    it("parses index signature type", () => {
      const source = `type Dict = { [key: string]: number };`;
      const result = parser.parse(source, "test.ts");

      expect(result.typeAliases).toHaveLength(1);
      expect(result.typeAliases[0]?.typeNode.kind).toBe("indexSignature");
    });
  });

  describe("classes", () => {
    it("parses class with properties", () => {
      const source = `
        class Counter {
          count: number = 0;
          readonly name: string;
          static instances: number = 0;
        }
      `;
      const result = parser.parse(source, "test.ts");

      expect(result.classes).toHaveLength(1);
      expect(result.classes[0]?.name).toBe("Counter");
      expect(result.classes[0]?.properties).toHaveLength(3);
      expect(result.classes[0]?.properties[0]?.name).toBe("count");
      expect(result.classes[0]?.properties[1]?.isReadonly).toBe(true);
      expect(result.classes[0]?.properties[2]?.isStatic).toBe(true);
    });

    it("parses class with methods", () => {
      const source = `
        class Calculator {
          add(a: number, b: number): number {
            return a + b;
          }

          async fetchData(): Promise<string> {
            return "data";
          }

          private helper(): void {}
        }
      `;
      const result = parser.parse(source, "test.ts");

      expect(result.classes).toHaveLength(1);
      expect(result.classes[0]?.methods).toHaveLength(3);

      const addMethod = result.classes[0]?.methods[0];
      expect(addMethod?.name).toBe("add");
      expect(addMethod?.isAsync).toBe(false);
      expect(addMethod?.parameters).toHaveLength(2);

      const fetchMethod = result.classes[0]?.methods[1];
      expect(fetchMethod?.isAsync).toBe(true);

      const helperMethod = result.classes[0]?.methods[2];
      expect(helperMethod?.visibility).toBe("private");
    });

    it("parses exported class", () => {
      const source = `export class Exported {}`;
      const result = parser.parse(source, "test.ts");

      expect(result.classes[0]?.exported).toBe(true);
    });

    it("parses method parameters with defaults", () => {
      const source = `
        class Greeter {
          greet(name: string = "World", loud?: boolean): string {
            return "Hello";
          }
        }
      `;
      const result = parser.parse(source, "test.ts");

      const method = result.classes[0]?.methods[0];
      expect(method?.parameters[0]?.defaultValue).toBe('"World"');
      expect(method?.parameters[1]?.optional).toBe(true);
    });
  });

  describe("statements", () => {
    it("parses if statements", () => {
      const source = `
        class Test {
          check(x: number): string {
            if (x > 0) {
              return "positive";
            } else {
              return "non-positive";
            }
          }
        }
      `;
      const result = parser.parse(source, "test.ts");

      const method = result.classes[0]?.methods[0];
      expect(method?.body[0]?.kind).toBe("if");
    });

    it("parses switch statements", () => {
      const source = `
        class Test {
          handle(status: string): void {
            switch (status) {
              case "pending":
                break;
              case "done":
                break;
              default:
                break;
            }
          }
        }
      `;
      const result = parser.parse(source, "test.ts");

      const method = result.classes[0]?.methods[0];
      expect(method?.body[0]?.kind).toBe("switch");
    });

    it("parses return statements", () => {
      const source = `
        class Test {
          getValue(): number {
            return 42;
          }
        }
      `;
      const result = parser.parse(source, "test.ts");

      const method = result.classes[0]?.methods[0];
      expect(method?.body[0]?.kind).toBe("return");
    });

    it("parses variable declarations", () => {
      const source = `
        class Test {
          compute(): void {
            const x = 1;
            let y = 2;
          }
        }
      `;
      const result = parser.parse(source, "test.ts");

      const method = result.classes[0]?.methods[0];
      expect(method?.body[0]?.kind).toBe("variableDecl");
      expect((method?.body[0] as any).varKind).toBe("const");
      expect(method?.body[1]?.kind).toBe("variableDecl");
      expect((method?.body[1] as any).varKind).toBe("let");
    });

    it("parses throw statements", () => {
      const source = `
        class Test {
          fail(): never {
            throw new Error("oops");
          }
        }
      `;
      const result = parser.parse(source, "test.ts");

      const method = result.classes[0]?.methods[0];
      expect(method?.body[0]?.kind).toBe("throw");
    });
  });

  describe("expressions", () => {
    it("parses identifiers", () => {
      const source = `
        class Test {
          test(): void {
            foo;
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0];
      expect(stmt?.kind).toBe("expression");
      expect((stmt as any).expression.kind).toBe("identifier");
    });

    it("parses string literals", () => {
      const source = `
        class Test {
          test(): string {
            return "hello";
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0] as any;
      expect(stmt.value.kind).toBe("stringLiteral");
      expect(stmt.value.value).toBe("hello");
    });

    it("parses number literals", () => {
      const source = `
        class Test {
          test(): number {
            return 42.5;
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0] as any;
      expect(stmt.value.kind).toBe("numberLiteral");
      expect(stmt.value.value).toBe(42.5);
    });

    it("parses boolean literals", () => {
      const source = `
        class Test {
          test(): boolean {
            return true;
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0] as any;
      expect(stmt.value.kind).toBe("booleanLiteral");
      expect(stmt.value.value).toBe(true);
    });

    it("parses array literals", () => {
      const source = `
        class Test {
          test(): number[] {
            return [1, 2, 3];
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0] as any;
      expect(stmt.value.kind).toBe("arrayLiteral");
      expect(stmt.value.elements).toHaveLength(3);
    });

    it("parses object literals", () => {
      const source = `
        class Test {
          test(): { x: number } {
            return { x: 1, y: 2 };
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0] as any;
      expect(stmt.value.kind).toBe("objectLiteral");
      expect(stmt.value.properties).toHaveLength(2);
    });

    it("parses member access", () => {
      const source = `
        class Test {
          test(): void {
            this.foo.bar;
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0] as any;
      expect(stmt.expression.kind).toBe("memberAccess");
      expect(stmt.expression.property).toBe("bar");
    });

    it("parses call expressions", () => {
      const source = `
        class Test {
          test(): void {
            this.doSomething(1, "two");
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0] as any;
      expect(stmt.expression.kind).toBe("call");
      expect(stmt.expression.arguments).toHaveLength(2);
    });

    it("parses new expressions", () => {
      const source = `
        class Test {
          test(): void {
            new Error("oops");
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0] as any;
      expect(stmt.expression.kind).toBe("new");
    });

    it("parses binary expressions", () => {
      const source = `
        class Test {
          test(): boolean {
            return 1 + 2 > 0;
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0] as any;
      expect(stmt.value.kind).toBe("binary");
    });

    it("parses unary expressions", () => {
      const source = `
        class Test {
          test(): boolean {
            return !true;
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0] as any;
      expect(stmt.value.kind).toBe("unary");
      expect(stmt.value.operator).toBe("!");
    });

    it("parses await expressions", () => {
      const source = `
        class Test {
          async test(): Promise<void> {
            await this.fetch();
          }
        }
      `;
      const result = parser.parse(source, "test.ts");
      const stmt = result.classes[0]?.methods[0]?.body[0] as any;
      expect(stmt.expression.kind).toBe("await");
    });
  });

  describe("aggregate domain file", () => {
    it("parses a complete aggregate file", () => {
      const source = `
        /**
         * Counter aggregate for counting things.
         */
        export type CounterEvent =
          | { type: "Incremented"; amount: number }
          | { type: "Decremented"; amount: number }
          | { type: "Reset" };

        export class Counter {
          count: number = 0;

          increment(amount: number): CounterEvent {
            if (amount <= 0) {
              throw new Error("Amount must be positive");
            }
            return { type: "Incremented", amount };
          }

          decrement(amount: number): CounterEvent {
            if (amount <= 0) {
              throw new Error("Amount must be positive");
            }
            if (this.count - amount < 0) {
              throw new Error("Cannot go below zero");
            }
            return { type: "Decremented", amount };
          }

          reset(): CounterEvent {
            return { type: "Reset" };
          }

          apply(event: CounterEvent): void {
            switch (event.type) {
              case "Incremented":
                this.count += event.amount;
                break;
              case "Decremented":
                this.count -= event.amount;
                break;
              case "Reset":
                this.count = 0;
                break;
            }
          }
        }
      `;

      const result = parser.parse(source, "counter.ts");

      // Check type alias (events)
      expect(result.typeAliases).toHaveLength(1);
      expect(result.typeAliases[0]?.name).toBe("CounterEvent");
      expect(result.typeAliases[0]?.exported).toBe(true);
      expect(result.typeAliases[0]?.typeNode.kind).toBe("union");

      // Check class
      expect(result.classes).toHaveLength(1);
      expect(result.classes[0]?.name).toBe("Counter");
      expect(result.classes[0]?.exported).toBe(true);

      // Check properties (state)
      expect(result.classes[0]?.properties).toHaveLength(1);
      expect(result.classes[0]?.properties[0]?.name).toBe("count");

      // Check methods (commands + apply)
      expect(result.classes[0]?.methods).toHaveLength(4);
      expect(result.classes[0]?.methods.map((m) => m.name)).toEqual([
        "increment",
        "decrement",
        "reset",
        "apply",
      ]);
    });
  });
});
