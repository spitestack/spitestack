/**
 * spite check - Validates domain code without generating output.
 */

import { relative } from "path";
import { logger } from "../utils/logger";

interface CheckOptions {
  domainDir: string;
}

export async function check(options: CheckOptions): Promise<void> {
  const { Compiler } = await import("../../../lib/compiler/src/index");

  const startTime = performance.now();

  logger.header("spite check");
  logger.info(`Domain: ${relative(process.cwd(), options.domainDir) || "."}`);
  logger.raw("");

  try {
    const compiler = new Compiler({
      domainDir: options.domainDir,
      outDir: "/dev/null", // Not used for check
    });

    await compiler.check();

    const elapsed = ((performance.now() - startTime) / 1000).toFixed(2);
    logger.success(`Valid domain in ${elapsed}s`);
  } catch (error) {
    if (error instanceof Error) {
      logger.error(error.message);
    } else {
      logger.error(String(error));
    }
    process.exit(1);
  }
}
