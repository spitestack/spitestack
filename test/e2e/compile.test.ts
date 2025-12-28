/**
 * E2E tests for compilation of example projects.
 */

import { describe, expect, it, beforeAll, afterAll } from "bun:test";
import { rm, access } from "fs/promises";
import { join, dirname } from "path";
import { Compiler } from "../../lib/compiler/src/index";

// import.meta.dir is a Bun-specific property that gives the directory of this file
const TEST_DIR = import.meta.dir;
const ROOT_DIR = dirname(dirname(TEST_DIR));
const EXAMPLES_DIR = join(ROOT_DIR, "examples");
const TEMP_OUT_DIR = join(ROOT_DIR, ".test-output");

describe("E2E: Compilation", () => {
  beforeAll(async () => {
    // Clean up any previous test output
    try {
      await rm(TEMP_OUT_DIR, { recursive: true });
    } catch {
      // Directory doesn't exist, that's fine
    }
  });

  afterAll(async () => {
    // Clean up test output
    try {
      await rm(TEMP_OUT_DIR, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  describe("counter example", () => {
    const counterDomain = join(EXAMPLES_DIR, "counter/domain");
    const counterOut = join(TEMP_OUT_DIR, "counter");

    it("parses the counter domain", async () => {
      const compiler = new Compiler({
        domainDir: counterDomain,
        outDir: counterOut,
      });

      const domain = await compiler.parse();

      expect(domain.aggregates.length).toBe(1);
      expect(domain.aggregates[0].name).toBe("Counter");
    });

    it("validates the counter domain", async () => {
      const compiler = new Compiler({
        domainDir: counterDomain,
        outDir: counterOut,
      });

      // Should not throw
      await compiler.check();
    });

    it("generates code for counter domain", async () => {
      const compiler = new Compiler({
        domainDir: counterDomain,
        outDir: counterOut,
      });

      const result = await compiler.compile();

      // Should generate validators, handlers, and router
      expect(result.files.some((f) => f.path.includes("validators"))).toBe(true);
      expect(result.files.some((f) => f.path.includes("handlers"))).toBe(true);
      expect(result.files.some((f) => f.path === "router.ts")).toBe(true);
    });

    it("writes output files", async () => {
      const compiler = new Compiler({
        domainDir: counterDomain,
        outDir: counterOut,
      });

      await compiler.compileProject();

      // Check that files exist
      await access(join(counterOut, "router.ts"));
      await access(join(counterOut, "validators"));
      await access(join(counterOut, "handlers"));
    });
  });

  describe("todo-app example", () => {
    const todoAppDomain = join(EXAMPLES_DIR, "todo-app/domain");
    const todoAppOut = join(TEMP_OUT_DIR, "todo-app");

    it("parses the todo-app domain", async () => {
      const compiler = new Compiler({
        domainDir: todoAppDomain,
        outDir: todoAppOut,
      });

      const domain = await compiler.parse();

      expect(domain.aggregates.length).toBe(2);
      expect(domain.aggregates.map((a) => a.name).sort()).toEqual(["Task", "User"]);
    });

    it("validates the todo-app domain", async () => {
      const compiler = new Compiler({
        domainDir: todoAppDomain,
        outDir: todoAppOut,
      });

      // Should not throw
      await compiler.check();
    });

    it("generates code for multiple aggregates", async () => {
      const compiler = new Compiler({
        domainDir: todoAppDomain,
        outDir: todoAppOut,
      });

      const result = await compiler.compile();

      // Should have validators and handlers for both aggregates
      expect(result.files.some((f) => f.path.includes("task"))).toBe(true);
      expect(result.files.some((f) => f.path.includes("user"))).toBe(true);
    });

    it("writes output files for both aggregates", async () => {
      const compiler = new Compiler({
        domainDir: todoAppDomain,
        outDir: todoAppOut,
      });

      await compiler.compileProject();

      // Check that files exist
      await access(join(todoAppOut, "router.ts"));
      await access(join(todoAppOut, "validators/task.validator.ts"));
      await access(join(todoAppOut, "validators/user.validator.ts"));
      await access(join(todoAppOut, "handlers/task.handlers.ts"));
      await access(join(todoAppOut, "handlers/user.handlers.ts"));
    });
  });
});
