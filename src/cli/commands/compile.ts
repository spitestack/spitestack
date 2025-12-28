/**
 * spite compile - Compiles domain code and generates scaffolding.
 */

import { relative } from "path";
import { logger } from "../utils/logger";

interface CompileOptions {
  domainDir: string;
  outDir: string;
  skipPurityCheck?: boolean;
}

export async function compile(options: CompileOptions): Promise<void> {
  const { Compiler } = await import("../../../lib/compiler/src/index");

  const startTime = performance.now();

  logger.header("spite compile");
  logger.info(`Domain: ${relative(process.cwd(), options.domainDir) || "."}`);
  logger.info(`Output: ${relative(process.cwd(), options.outDir)}`);
  logger.raw("");

  try {
    const compiler = new Compiler({
      domainDir: options.domainDir,
      outDir: options.outDir,
      skipPurityCheck: options.skipPurityCheck,
    });

    const result = await compiler.compile();

    logger.step("Parsed domain files");
    logger.step("Validated aggregate purity");
    logger.step(`Generated ${result.files.length} files`);

    await compiler.compileProject();

    const elapsed = ((performance.now() - startTime) / 1000).toFixed(2);
    logger.raw("");
    logger.success(`Compiled in ${elapsed}s`);
    logger.raw("");

    for (const file of result.files) {
      logger.file(file.path);
    }
  } catch (error) {
    if (error instanceof Error) {
      logger.error(error.message);
    } else {
      logger.error(String(error));
    }
    process.exit(1);
  }
}
