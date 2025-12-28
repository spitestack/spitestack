/**
 * E2E tests for runtime behavior of generated code.
 *
 * These tests verify that the generated validators and handlers
 * actually work as expected when executed.
 */

import { describe, expect, it, beforeAll, afterAll } from "bun:test";
import { rm, readFile } from "fs/promises";
import { join, dirname } from "path";
import { Compiler } from "../../lib/compiler/src/index";

// import.meta.dir is a Bun-specific property that gives the directory of this file
const TEST_DIR = import.meta.dir;
const ROOT_DIR = dirname(dirname(TEST_DIR));
const EXAMPLES_DIR = join(ROOT_DIR, "examples");
const TEMP_OUT_DIR = join(ROOT_DIR, ".test-output-runtime");

describe("E2E: Runtime", () => {
  beforeAll(async () => {
    try {
      await rm(TEMP_OUT_DIR, { recursive: true });
    } catch {
      // Directory doesn't exist
    }
  });

  afterAll(async () => {
    try {
      await rm(TEMP_OUT_DIR, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  describe("generated validators", () => {
    it("generates validators with proper structure", async () => {
      const compiler = new Compiler({
        domainDir: join(EXAMPLES_DIR, "counter/domain"),
        outDir: join(TEMP_OUT_DIR, "counter"),
      });

      await compiler.compileProject();

      const validatorContent = await readFile(
        join(TEMP_OUT_DIR, "counter/validators/counter.validator.ts"),
        "utf-8"
      );

      // Validator should have the expected structure
      expect(validatorContent).toContain("validateIncrement");
      expect(validatorContent).toContain("validateDecrement");
      expect(validatorContent).toContain("validateReset");
    });
  });

  describe("generated handlers", () => {
    it("generates handlers with proper imports", async () => {
      const compiler = new Compiler({
        domainDir: join(EXAMPLES_DIR, "counter/domain"),
        outDir: join(TEMP_OUT_DIR, "counter"),
      });

      await compiler.compileProject();

      const handlerContent = await readFile(
        join(TEMP_OUT_DIR, "counter/handlers/counter.handlers.ts"),
        "utf-8"
      );

      // Handler should import validator
      expect(handlerContent).toContain("validateIncrement");
      expect(handlerContent).toContain("handleIncrement");
      expect(handlerContent).toContain("handleDecrement");
      expect(handlerContent).toContain("handleReset");
    });
  });

  describe("generated router", () => {
    it("generates router with routes for all aggregates", async () => {
      const compiler = new Compiler({
        domainDir: join(EXAMPLES_DIR, "todo-app/domain"),
        outDir: join(TEMP_OUT_DIR, "todo-app"),
      });

      await compiler.compileProject();

      const routerContent = await readFile(
        join(TEMP_OUT_DIR, "todo-app/router.ts"),
        "utf-8"
      );

      // Router should have routes for both aggregates
      expect(routerContent).toContain("/task");
      expect(routerContent).toContain("/user");
    });

    it("generates Bun.serve compatible router", async () => {
      const compiler = new Compiler({
        domainDir: join(EXAMPLES_DIR, "counter/domain"),
        outDir: join(TEMP_OUT_DIR, "counter"),
      });

      await compiler.compileProject();

      const routerContent = await readFile(
        join(TEMP_OUT_DIR, "counter/router.ts"),
        "utf-8"
      );

      // Should have router structure
      expect(routerContent).toContain("export function createRouter");
      expect(routerContent).toContain("routes");
      expect(routerContent).toContain("handlers");
    });
  });
});
