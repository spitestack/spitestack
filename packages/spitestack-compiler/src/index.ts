#!/usr/bin/env bun

/**
 * SpiteStack CLI Compiler
 *
 * Usage:
 *   spitestack compile    - Compile aggregates and generate handlers
 *   spitestack check      - Validate without generating code
 *   spitestack init       - Create a config file
 *   spitestack --help     - Show help
 */

import { parseArgs } from "util";
import { existsSync } from "node:fs";
import { writeFile } from "node:fs/promises";
import { join, relative } from "node:path";
import { loadConfig, getDefaultAppContent } from "./config";
import { discoverFiles, parseFiles, analyzeFiles, validate, generate, SchemaEvolutionError, ApiEvolutionError } from "./pipeline";
import { DiagnosticReporter } from "./errors/reporter";
import { DiagnosticCode, DiagnosticMessages } from "./errors/codes";
import { apiLockExists, bumpApiVersion, getVersionedApiLocks } from "./api/lock";
import { runDev } from "./dev";

const VERSION = "0.0.1";

const HELP = `
SpiteStack Compiler v${VERSION}

Usage:
  spitestack <command> [options]

Commands:
  compile           Compile aggregates and generate handlers (default)
  dev [entry]       Start dev server with hot reloading
  check             Validate without generating code
  init              Create a spitestack.app.ts file
  version api <v>   Bump API version (e.g., "spitestack version api v2")

Options:
  --no-colors           Disable colored output
  --force               Bypass schema evolution checks (WARNING: may corrupt data)
  -h, --help            Show this help
  -v, --version         Show version

Examples:
  spitestack compile          # Compile with App() entrypoint
  spitestack dev              # Start dev server (auto-detects server.ts)
  spitestack dev ./server.ts  # Start dev server with custom entry
  spitestack check            # Validate only
  spitestack init             # Create spitestack.app.ts
`;

export interface CLIOptions {
  colors: boolean;
  force: boolean;
  help: boolean;
  version: boolean;
}

function parseCliArgs(): { command: string; args: string[]; options: CLIOptions } {
  const { values, positionals } = parseArgs({
    args: Bun.argv.slice(2),
    options: {
      colors: { type: "boolean", default: true },
      "no-colors": { type: "boolean" },
      force: { type: "boolean" },
      help: { type: "boolean", short: "h" },
      version: { type: "boolean", short: "v" },
    },
    allowPositionals: true,
  });

  return {
    command: positionals[0] ?? "compile",
    args: positionals.slice(1),
    options: {
      colors: !values["no-colors"],
      force: values.force ?? false,
      help: values.help ?? false,
      version: values.version ?? false,
    },
  };
}

/**
 * Run the compile command
 */
async function runCompile(options: CLIOptions, checkOnly: boolean = false): Promise<number> {
  const cwd = process.cwd();
  const reporter = new DiagnosticReporter({ colors: options.colors, cwd });

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

  if (configPath) {
    console.log(`Using config: ${relative(cwd, configPath)}`);
  } else {
    console.log("Using default configuration");
  }

  // Check domain directory exists
  if (!existsSync(config.domainDir)) {
    reporter.report({
      code: DiagnosticCode.DOMAIN_DIR_NOT_FOUND,
      severity: "error",
      message: `${DiagnosticMessages[DiagnosticCode.DOMAIN_DIR_NOT_FOUND]}: ${config.domainDir}`,
      location: {
        filePath: configPath ?? cwd,
        line: 1,
        column: 1,
      },
      suggestion: `Create the domain directory: mkdir -p ${relative(cwd, config.domainDir)}`,
    });
    reporter.summary();
    return 1;
  }

  console.log(`Discovering files in: ${relative(cwd, config.domainDir)}`);

  // Discover files
  const files = await discoverFiles(config);

  if (files.length === 0) {
    reporter.report({
      code: DiagnosticCode.EMPTY_DOMAIN_DIR,
      severity: "warning",
      message: DiagnosticMessages[DiagnosticCode.EMPTY_DOMAIN_DIR],
      location: {
        filePath: config.domainDir,
        line: 1,
        column: 1,
      },
    });
    reporter.summary();
    return 0;
  }

  console.log(`Found ${files.length} candidate file(s)`);

  // Parse files
  const { program, parsedFiles } = parseFiles(files);

  if (parsedFiles.length === 0) {
    console.log("No aggregate classes found");
    return 0;
  }

  // Get type checker
  const typeChecker = program.getTypeChecker();

  // Analyze files
  const { aggregates, diagnostics: analysisdiagnostics } = analyzeFiles(
    parsedFiles,
    typeChecker,
    program,
    config.domainDir
  );

  if (aggregates.length === 0) {
    reporter.report({
      code: DiagnosticCode.NO_AGGREGATES_FOUND,
      severity: "error",
      message: DiagnosticMessages[DiagnosticCode.NO_AGGREGATES_FOUND],
      location: {
        filePath: config.domainDir,
        line: 1,
        column: 1,
      },
      suggestion: `Create an aggregate class with a static 'aggregateName' property`,
    });
    reporter.reportAll(analysisdiagnostics);
    reporter.summary();
    return 1;
  }

  console.log(`Found ${aggregates.length} aggregate(s): ${aggregates.map((a) => a.className).join(", ")}`);

  // Validate
  const validation = validate(aggregates, analysisdiagnostics);

  // Report diagnostics
  reporter.reportAll(validation.diagnostics);

  if (!validation.valid) {
    reporter.summary();
    return 1;
  }

  if (checkOnly) {
    console.log("\nValidation successful!");
    reporter.summary();
    return 0;
  }

  // Generate code
  console.log(`\nGenerating code to: ${relative(cwd, config.outDir)}`);
  console.log(`Mode: ${config.mode}`);

  try {
    const result = await generate(aggregates, config, { force: options.force });

    // Report schema lock status
    if (result.schemaLock.created) {
      console.log("\nSchema lock created (first production compile)");
    } else if (result.schemaLock.updated) {
      console.log("\nSchema lock updated with additive changes:");
      console.log(result.schemaLock.diffMessage);
    }

    // Report API lock status
    if (result.apiLock.created) {
      console.log("\nAPI lock created (first compile with api.versioning enabled)");
    } else if (result.apiLock.updated) {
      console.log("\nAPI lock updated with additive changes:");
      console.log(result.apiLock.diffMessage);
    }

    const fileCount =
      result.handlers.length +
      result.validators.length +
      (result.wiring ? 1 : 0) +
      (result.index ? 1 : 0);

    console.log(`Generated ${fileCount} file(s)`);
    reporter.summary();
    return 0;
  } catch (error) {
    if (error instanceof SchemaEvolutionError) {
      console.error(`\nSchema evolution error:\n${error.message}`);
      console.error("\nTo resolve breaking changes, you need to:");
      console.error("  1. Create upcasters for the affected events");
      console.error("  2. Or switch to greenfield mode (mode: 'greenfield' in config)");
      console.error("  3. Or use --force to bypass (WARNING: may corrupt existing data)");
      return 1;
    }
    if (error instanceof ApiEvolutionError) {
      console.error(`\nAPI evolution error:\n${error.message}`);
      return 1;
    }
    console.error(`Error generating code: ${error instanceof Error ? error.message : error}`);
    return 1;
  }
}

/**
 * Run the init command
 */
async function runInit(): Promise<number> {
  const appPath = join(process.cwd(), "spitestack.app.ts");

  if (existsSync(appPath)) {
    console.error("App file already exists: spitestack.app.ts");
    return 1;
  }

  await writeFile(appPath, getDefaultAppContent(), "utf-8");
  console.log("Created: spitestack.app.ts");
  return 0;
}

/**
 * Run the version api command
 */
async function runVersionApi(args: string[], options: CLIOptions): Promise<number> {
  const cwd = process.cwd();

  // Load config to get outDir
  let config;
  try {
    const result = await loadConfig(cwd);
    config = result.config;
  } catch (error) {
    console.error(`Error loading config: ${error instanceof Error ? error.message : error}`);
    return 1;
  }

  // Check if API versioning is enabled
  if (!config.api.versioning) {
    console.error("API versioning is not enabled.");
    console.error("Enable it in your config: api: { versioning: true }");
    return 1;
  }

  // Get the subcommand (should be "api")
  const subcommand = args[0];
  if (subcommand !== "api") {
    console.error(`Unknown version subcommand: ${subcommand}`);
    console.error("Usage: spitestack version api <version>");
    return 1;
  }

  // Get the new version
  const newVersion = args[1];
  if (!newVersion) {
    console.error("Missing version argument.");
    console.error("Usage: spitestack version api <version>");
    console.error("Example: spitestack version api v2");
    return 1;
  }

  // Validate version format
  if (!newVersion.startsWith("v") || !/^v\d+$/.test(newVersion)) {
    console.error(`Invalid version format: ${newVersion}`);
    console.error("Version must be in format 'v1', 'v2', etc.");
    return 1;
  }

  // Check if api.lock exists
  if (!apiLockExists(config.outDir)) {
    console.error("No API lock file exists.");
    console.error("Run 'spitestack compile' first to create the initial API lock.");
    return 1;
  }

  // Check if version already exists
  const existingVersions = getVersionedApiLocks(config.outDir);
  if (existingVersions.includes(newVersion)) {
    console.error(`API version ${newVersion} already exists.`);
    return 1;
  }

  // Bump the version
  try {
    bumpApiVersion(config.outDir, newVersion);
    console.log(`API version bumped to ${newVersion}`);
    console.log("Previous version has been frozen.");
    console.log("Run 'spitestack compile' to regenerate routes with version support.");
    return 0;
  } catch (error) {
    console.error(`Error bumping API version: ${error instanceof Error ? error.message : error}`);
    return 1;
  }
}

/**
 * Main entry point
 */
async function main(): Promise<void> {
  const { command, args, options } = parseCliArgs();

  if (options.help) {
    console.log(HELP);
    process.exit(0);
  }

  if (options.version) {
    console.log(`SpiteStack Compiler v${VERSION}`);
    process.exit(0);
  }

  let exitCode: number;

  switch (command) {
    case "compile":
      exitCode = await runCompile(options, false);
      break;

    case "dev":
      exitCode = await runDev(args, options, runCompile);
      break;

    case "check":
      exitCode = await runCompile(options, true);
      break;

    case "init":
      exitCode = await runInit();
      break;

    case "version":
      exitCode = await runVersionApi(args, options);
      break;

    case "help":
      console.log(HELP);
      exitCode = 0;
      break;

    default:
      console.error(`Unknown command: ${command}`);
      console.log(HELP);
      exitCode = 1;
  }

  process.exit(exitCode);
}

// Run
main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});

// Export types for library usage
export type { CompilerConfig, PartialConfig } from "./types";
