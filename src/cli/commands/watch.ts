/**
 * spite watch - Watch for changes and recompile (without running server).
 */

import { watch } from "fs";
import { resolve, relative } from "path";
import { logger } from "../utils/logger";
import type { WatchOptions } from "../args";

export async function watchCommand(options: WatchOptions): Promise<void> {
  const { Compiler } = await import("../../../lib/compiler/src/index");

  const domainDir = resolve(process.cwd(), options.domain);
  const outDir = resolve(process.cwd(), options.output);

  logger.header("spite watch");
  logger.info(`Domain:   ${relative(process.cwd(), domainDir) || "."}`);
  logger.info(`Output:   ${relative(process.cwd(), outDir)}`);
  logger.info(`Language: ${options.language}`);
  logger.raw("");

  const compileOnce = async (): Promise<boolean> => {
    const startTime = performance.now();
    try {
      const compiler = new Compiler({
        domainDir,
        outDir,
      });

      await compiler.compileProject();

      const elapsed = ((performance.now() - startTime) / 1000).toFixed(2);
      logger.success(`Compiled in ${elapsed}s`);
      return true;
    } catch (error) {
      if (error instanceof Error) {
        logger.error(error.message);
      } else {
        logger.error(String(error));
      }
      return false;
    }
  };

  // Initial compile
  await compileOnce();

  // Watch for changes
  let debounceTimer: ReturnType<typeof setTimeout> | null = null;
  let isCompiling = false;

  const watcher = watch(
    domainDir,
    { recursive: true },
    (eventType, filename) => {
      if (!filename?.endsWith(".ts")) return;
      if (isCompiling) return;

      // Debounce to avoid multiple rapid recompiles
      if (debounceTimer) {
        clearTimeout(debounceTimer);
      }

      debounceTimer = setTimeout(async () => {
        isCompiling = true;
        logger.raw("");
        logger.info(`Change detected: ${filename}`);
        await compileOnce();
        isCompiling = false;
      }, 100);
    }
  );

  // Handle graceful shutdown
  process.on("SIGINT", () => {
    logger.raw("\n");
    logger.info("Stopping watch mode");
    watcher.close();
    process.exit(0);
  });

  logger.info("Watching for changes... (Ctrl+C to stop)");

  // Keep process alive
  await new Promise(() => {});
}
