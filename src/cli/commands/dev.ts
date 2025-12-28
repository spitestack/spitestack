/**
 * spite dev - Development server with hot reload.
 *
 * Compiles domain, starts the generated server, and watches for changes.
 */

import { resolve, relative, join } from "path";
import { watch } from "fs";
import { existsSync } from "fs";
import { logger } from "../utils/logger";

interface DevOptions {
  domainDir: string;
  outDir: string;
  skipPurityCheck?: boolean;
  port?: number;
}

export async function dev(options: DevOptions): Promise<void> {
  const { Compiler } = await import("../../../lib/compiler/src/index");

  const port = options.port ?? 3000;

  logger.header("spite dev");
  logger.info(`Domain:   ${relative(process.cwd(), options.domainDir) || "."}`);
  logger.info(`Output:   ${relative(process.cwd(), options.outDir)}`);
  logger.info(`Port:     ${port}`);
  logger.raw("");

  let serverProcess: ReturnType<typeof Bun.spawn> | null = null;
  let isCompiling = false;

  const compileOnce = async (): Promise<boolean> => {
    const startTime = performance.now();
    try {
      const compiler = new Compiler({
        domainDir: options.domainDir,
        outDir: options.outDir,
        skipPurityCheck: options.skipPurityCheck,
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

  const startServer = async (): Promise<void> => {
    // Check if server.ts exists in output
    const serverPath = join(options.outDir, "server.ts");

    if (!existsSync(serverPath)) {
      logger.warn("No server.ts generated - skipping server start");
      return;
    }

    // Kill existing server if running
    if (serverProcess) {
      serverProcess.kill();
      serverProcess = null;
    }

    // Start the server
    logger.info(`Starting server on port ${port}...`);

    serverProcess = Bun.spawn(["bun", "run", serverPath], {
      env: { ...process.env, PORT: String(port) },
      stdio: ["ignore", "inherit", "inherit"],
    });

    // Give it a moment to start
    await new Promise((resolve) => setTimeout(resolve, 500));

    if (serverProcess.exitCode === null) {
      logger.success(`Server running at http://localhost:${port}`);
    }
  };

  // Initial compile
  const success = await compileOnce();
  if (success) {
    await startServer();
  }

  // Watch for changes
  let debounceTimer: ReturnType<typeof setTimeout> | null = null;

  const watcher = watch(
    options.domainDir,
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

        const success = await compileOnce();
        if (success) {
          await startServer();
        }

        isCompiling = false;
      }, 100);
    }
  );

  // Handle graceful shutdown
  process.on("SIGINT", () => {
    logger.raw("\n");
    logger.info("Stopping dev server...");

    if (serverProcess) {
      serverProcess.kill();
    }
    watcher.close();

    logger.success("Stopped");
    process.exit(0);
  });

  logger.raw("");
  logger.info("Watching for changes... (Ctrl+C to stop)");

  // Keep process alive
  await new Promise(() => {});
}
