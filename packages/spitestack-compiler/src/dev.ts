/**
 * Dev server with hot reloading
 */

import { spawn, type Subprocess } from "bun";
import { watch, existsSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { loadConfig } from "./config";
import type { CLIOptions } from "./index";

/**
 * Run the dev server with hot reloading
 */
export async function runDev(
  args: string[],
  options: CLIOptions,
  runCompile: (options: CLIOptions, checkOnly: boolean) => Promise<number>
): Promise<number> {
  const cwd = process.cwd();

  // Load config
  let config;
  let configPath;
  try {
    const result = await loadConfig(cwd);
    config = result.config;
    configPath = result.configPath;
  } catch (error) {
    console.error(`Error loading config: ${error instanceof Error ? error.message : error}`);
    return 1;
  }

  // Find server entry point
  const entry = args[0] ?? findServerEntry(configPath);
  if (!entry) {
    console.error("Could not find server entry point.");
    console.error("Create server.ts or pass entry: spitestack dev ./server.ts");
    return 1;
  }

  if (!existsSync(entry)) {
    console.error(`Server entry point not found: ${entry}`);
    return 1;
  }

  // Initial compile
  console.log("Initial compile...\n");
  const compileResult = await runCompile(options, false);
  if (compileResult !== 0) {
    console.error("\nInitial compile failed. Fix errors and try again.");
    return compileResult;
  }

  // Spawn bun with --hot
  let serverProcess: Subprocess | null = spawn({
    cmd: ["bun", "--hot", entry],
    stdout: "inherit",
    stderr: "inherit",
    cwd,
  });

  console.log(`\nDev server running: ${relative(cwd, entry)}`);
  console.log(`Watching: ${relative(cwd, config.domainDir)}`);
  console.log("Press Ctrl+C to stop\n");

  // Watch domain files
  let compiling = false;
  let pendingRecompile = false;

  const watcher = watch(config.domainDir, { recursive: true }, async (event, filename) => {
    if (!filename?.endsWith(".ts")) return;

    // Debounce: if already compiling, mark as pending
    if (compiling) {
      pendingRecompile = true;
      return;
    }

    compiling = true;
    console.log(`\n[${event}] ${filename}`);

    try {
      await runCompile({ ...options, colors: options.colors }, false);
      console.log("Recompiled. Bun will hot reload.\n");
    } catch (error) {
      console.error(`Compile error: ${error instanceof Error ? error.message : error}`);
    }

    compiling = false;

    // If changes happened while compiling, recompile again
    if (pendingRecompile) {
      pendingRecompile = false;
      // Trigger another compile via a small delay
      setTimeout(() => {
        watcher.emit("change", "change", "pending");
      }, 100);
    }
  });

  // Handle shutdown
  const cleanup = () => {
    console.log("\nShutting down...");
    watcher.close();
    if (serverProcess) {
      serverProcess.kill();
    }
    process.exit(0);
  };

  process.on("SIGINT", cleanup);
  process.on("SIGTERM", cleanup);

  // Wait for server to exit (shouldn't happen unless crashed)
  const exitCode = await serverProcess.exited;

  if (exitCode !== 0) {
    console.error(`\nServer exited with code ${exitCode}`);
    console.log("Waiting for file changes to restart...\n");

    // Wait for a file change to restart
    await new Promise<void>((resolve) => {
      const restartWatcher = watch(config.domainDir, { recursive: true }, async (event, filename) => {
        if (!filename?.endsWith(".ts")) return;
        restartWatcher.close();
        resolve();
      });
    });

    // Restart the dev server
    return runDev(args, options, runCompile);
  }

  return 0;
}

/**
 * Find server entry point by convention
 */
function findServerEntry(appPath: string | null): string | null {
  const dir = appPath ? dirname(appPath) : process.cwd();
  const candidates = [
    join(dir, "server.ts"),
    join(dir, "server.js"),
    join(dir, "src", "server.ts"),
    join(dir, "src", "server.js"),
  ];

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  return null;
}
