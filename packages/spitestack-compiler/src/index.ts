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
import { loadConfig, getDefaultConfigContent } from "./config";
import { discoverFiles, parseFiles, analyzeFiles, validate, generate } from "./pipeline";
import { DiagnosticReporter } from "./errors/reporter";
import { DiagnosticCode, DiagnosticMessages } from "./errors/codes";

const VERSION = "0.0.1";

const HELP = `
SpiteStack Compiler v${VERSION}

Usage:
  spitestack <command> [options]

Commands:
  compile     Compile aggregates and generate handlers (default)
  check       Validate without generating code
  init        Create a spitestack.config.ts file

Options:
  -c, --config <path>   Path to config file
  --no-colors           Disable colored output
  -h, --help            Show this help
  -v, --version         Show version

Examples:
  spitestack compile          # Compile with default or found config
  spitestack check            # Validate only
  spitestack init             # Create config file
  spitestack -c custom.ts     # Use specific config file
`;

interface CLIOptions {
  config?: string;
  colors: boolean;
  help: boolean;
  version: boolean;
}

function parseCliArgs(): { command: string; options: CLIOptions } {
  const { values, positionals } = parseArgs({
    args: Bun.argv.slice(2),
    options: {
      config: { type: "string", short: "c" },
      colors: { type: "boolean", default: true },
      "no-colors": { type: "boolean" },
      help: { type: "boolean", short: "h" },
      version: { type: "boolean", short: "v" },
    },
    allowPositionals: true,
  });

  return {
    command: positionals[0] ?? "compile",
    options: {
      config: values.config,
      colors: !values["no-colors"],
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

  try {
    const result = await generate(aggregates, config);

    const fileCount =
      result.handlers.length +
      result.validators.length +
      (result.wiring ? 1 : 0) +
      (result.index ? 1 : 0);

    console.log(`Generated ${fileCount} file(s)`);
    reporter.summary();
    return 0;
  } catch (error) {
    console.error(`Error generating code: ${error instanceof Error ? error.message : error}`);
    return 1;
  }
}

/**
 * Run the init command
 */
async function runInit(): Promise<number> {
  const configPath = join(process.cwd(), "spitestack.config.ts");

  if (existsSync(configPath)) {
    console.error("Config file already exists: spitestack.config.ts");
    return 1;
  }

  await writeFile(configPath, getDefaultConfigContent(), "utf-8");
  console.log("Created: spitestack.config.ts");
  return 0;
}

/**
 * Main entry point
 */
async function main(): Promise<void> {
  const { command, options } = parseCliArgs();

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

    case "check":
      exitCode = await runCompile(options, true);
      break;

    case "init":
      exitCode = await runInit();
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

// Export for library usage
export { defineConfig } from "./config";
export type { CompilerConfig, PartialConfig } from "./types";
