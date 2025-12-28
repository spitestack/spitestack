/**
 * CLI argument parsing framework.
 *
 * Provides typed argument parsing with short flags, defaults, and help generation.
 */

// Default values matching Rust CLI
export const DEFAULTS = {
  domain: "src/domain",
  output: ".spitestack",
  language: "typescript",
  port: 3000,
} as const;

// Command types
export type Command =
  | { type: "help" }
  | { type: "version" }
  | { type: "compile"; options: CompileOptions }
  | { type: "build"; options: BuildOptions }
  | { type: "check"; options: CheckOptions }
  | { type: "dev"; options: DevOptions }
  | { type: "watch"; options: WatchOptions }
  | { type: "init"; options: InitOptions }
  | { type: "schema"; action: SchemaAction }
  | { type: "unknown"; command: string };

export interface CompileOptions {
  domain: string;
  output: string;
  language: string;
  port: number;
  skipPurity: boolean;
}

export interface BuildOptions {
  input: string;
  output: string;
}

export interface CheckOptions {
  domain: string;
  language: string;
}

export interface DevOptions {
  domain: string;
  output: string;
  language: string;
  port: number;
  skipPurity: boolean;
}

export interface WatchOptions {
  domain: string;
  output: string;
  language: string;
}

export interface InitOptions {
  name: string;
  domain: string;
  language: string;
  port: number;
}

export type SchemaAction =
  | { type: "status"; domain: string }
  | { type: "sync"; domain: string; force: boolean }
  | { type: "diff"; domain: string }
  | { type: "reset"; domain: string; confirmed: boolean }
  | { type: "help" };

/**
 * Parse command line arguments into a typed Command.
 */
export function parseArgs(argv: string[] = process.argv.slice(2)): Command {
  if (argv.length === 0 || argv[0] === "-h" || argv[0] === "--help") {
    return { type: "help" };
  }

  if (argv[0] === "-v" || argv[0] === "--version") {
    return { type: "version" };
  }

  const command = argv[0];
  const args = argv.slice(1);

  switch (command) {
    case "compile":
      return { type: "compile", options: parseCompileOptions(args) };
    case "build":
      return { type: "build", options: parseBuildOptions(args) };
    case "check":
      return { type: "check", options: parseCheckOptions(args) };
    case "dev":
      return { type: "dev", options: parseDevOptions(args) };
    case "watch":
      return { type: "watch", options: parseWatchOptions(args) };
    case "init":
      return { type: "init", options: parseInitOptions(args) };
    case "schema":
      return { type: "schema", action: parseSchemaAction(args) };
    default:
      return { type: "unknown", command };
  }
}

/**
 * Parse a flag value from args, supporting both --flag value and --flag=value.
 */
function getFlag(args: string[], long: string, short?: string): string | undefined {
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    // --flag=value
    if (arg.startsWith(`--${long}=`)) {
      return arg.slice(`--${long}=`.length);
    }

    // --flag value
    if (arg === `--${long}` || (short && arg === `-${short}`)) {
      return args[i + 1];
    }
  }
  return undefined;
}

/**
 * Check if a boolean flag is present.
 */
function hasFlag(args: string[], long: string, short?: string): boolean {
  return args.some((arg) => arg === `--${long}` || (short && arg === `-${short}`));
}

/**
 * Get positional argument (first arg that doesn't start with -).
 */
function getPositional(args: string[]): string | undefined {
  for (const arg of args) {
    if (!arg.startsWith("-")) {
      return arg;
    }
  }
  return undefined;
}

function parseCompileOptions(args: string[]): CompileOptions {
  if (hasFlag(args, "help", "h")) {
    // Will be handled by showing help
  }

  return {
    domain: getPositional(args) ?? getFlag(args, "domain", "d") ?? DEFAULTS.domain,
    output: getFlag(args, "out", "o") ?? DEFAULTS.output,
    language: getFlag(args, "language", "l") ?? DEFAULTS.language,
    port: parseInt(getFlag(args, "port", "p") ?? String(DEFAULTS.port), 10),
    skipPurity: hasFlag(args, "skip-purity"),
  };
}

function parseBuildOptions(args: string[]): BuildOptions {
  return {
    input: getPositional(args) ?? getFlag(args, "input", "i") ?? DEFAULTS.output,
    output: getFlag(args, "out", "o") ?? "dist",
  };
}

function parseCheckOptions(args: string[]): CheckOptions {
  return {
    domain: getPositional(args) ?? getFlag(args, "domain", "d") ?? DEFAULTS.domain,
    language: getFlag(args, "language", "l") ?? DEFAULTS.language,
  };
}

function parseDevOptions(args: string[]): DevOptions {
  return {
    domain: getPositional(args) ?? getFlag(args, "domain", "d") ?? DEFAULTS.domain,
    output: getFlag(args, "out", "o") ?? DEFAULTS.output,
    language: getFlag(args, "language", "l") ?? DEFAULTS.language,
    port: parseInt(getFlag(args, "port", "p") ?? String(DEFAULTS.port), 10),
    skipPurity: hasFlag(args, "skip-purity"),
  };
}

function parseWatchOptions(args: string[]): WatchOptions {
  return {
    domain: getPositional(args) ?? getFlag(args, "domain", "d") ?? DEFAULTS.domain,
    output: getFlag(args, "out", "o") ?? DEFAULTS.output,
    language: getFlag(args, "language", "l") ?? DEFAULTS.language,
  };
}

function parseInitOptions(args: string[]): InitOptions {
  return {
    name: getPositional(args) ?? "my-spite-app",
    domain: getFlag(args, "domain", "d") ?? DEFAULTS.domain,
    language: getFlag(args, "language", "l") ?? DEFAULTS.language,
    port: parseInt(getFlag(args, "port", "p") ?? String(DEFAULTS.port), 10),
  };
}

function parseSchemaAction(args: string[]): SchemaAction {
  const action = args[0];
  const actionArgs = args.slice(1);

  switch (action) {
    case "status":
      return {
        type: "status",
        domain: getPositional(actionArgs) ?? getFlag(actionArgs, "domain", "d") ?? DEFAULTS.domain,
      };
    case "sync":
      return {
        type: "sync",
        domain: getPositional(actionArgs) ?? getFlag(actionArgs, "domain", "d") ?? DEFAULTS.domain,
        force: hasFlag(actionArgs, "force"),
      };
    case "diff":
      return {
        type: "diff",
        domain: getPositional(actionArgs) ?? getFlag(actionArgs, "domain", "d") ?? DEFAULTS.domain,
      };
    case "reset":
      return {
        type: "reset",
        domain: getPositional(actionArgs) ?? getFlag(actionArgs, "domain", "d") ?? DEFAULTS.domain,
        confirmed: hasFlag(actionArgs, "i-know-what-im-doing"),
      };
    default:
      return { type: "help" };
  }
}

/**
 * Generate help text for the CLI.
 */
export function getHelpText(): string {
  return `
spite - Code angry.

Usage:
  spite <command> [options]

Commands:
  compile [dir]     Compile domain and generate scaffolding
  build [dir]       Bundle .spitestack for production
  check [dir]       Validate domain without generating
  dev [dir]         Start dev server with hot reload
  watch [dir]       Watch for changes and recompile
  init [name]       Create a new Spite project
  schema <action>   Manage event schema evolution

Schema Actions:
  status            Show current schema status
  sync              Generate or update schema lock file
  diff              Show diff between code and lock file
  reset             Reset lock file (dangerous!)

Common Options:
  -d, --domain      Domain source directory (default: ${DEFAULTS.domain})
  -o, --out         Output directory (default: ${DEFAULTS.output})
  -l, --language    Source language (default: ${DEFAULTS.language})
  -p, --port        Server port (default: ${DEFAULTS.port})
  --skip-purity     Skip aggregate purity checks
  -h, --help        Show help for command
  -v, --version     Show version

Examples:
  spite init my-app           Create a new project
  spite compile               Compile src/domain to .spitestack
  spite compile ./domain      Compile custom domain directory
  spite compile -o ./gen      Output to custom directory
  spite dev -p 8080           Start dev server on port 8080
  spite watch                 Watch and recompile on changes
  spite schema status         Show schema evolution status
  spite schema sync           Update events.lock.json
  spite schema diff           Show pending schema changes
`;
}

/**
 * Generate help text for a specific command.
 */
export function getCommandHelp(command: string): string {
  switch (command) {
    case "compile":
      return `
spite compile - Compile domain and generate scaffolding

Usage:
  spite compile [dir] [options]

Arguments:
  dir               Domain source directory (default: ${DEFAULTS.domain})

Options:
  -o, --out <dir>   Output directory (default: ${DEFAULTS.output})
  -l, --language    Source language (default: ${DEFAULTS.language})
  -p, --port        Server port for generated code (default: ${DEFAULTS.port})
  --skip-purity     Skip aggregate purity checks
  -h, --help        Show this help
`;

    case "dev":
      return `
spite dev - Start dev server with hot reload

Usage:
  spite dev [dir] [options]

Arguments:
  dir               Domain source directory (default: ${DEFAULTS.domain})

Options:
  -o, --out <dir>   Output directory (default: ${DEFAULTS.output})
  -l, --language    Source language (default: ${DEFAULTS.language})
  -p, --port        Server port (default: ${DEFAULTS.port})
  --skip-purity     Skip aggregate purity checks
  -h, --help        Show this help
`;

    case "schema":
      return `
spite schema - Manage event schema evolution

Usage:
  spite schema <action> [options]

Actions:
  status            Show current schema status and pending changes
  sync              Generate or update events.lock.json
  diff              Show diff between current code and lock file
  reset             Reset lock file (requires --i-know-what-im-doing)

Options:
  -d, --domain      Domain source directory (default: ${DEFAULTS.domain})
  --force           Force sync even with breaking changes (sync only)
  --i-know-what-im-doing  Confirm reset (reset only)
  -h, --help        Show this help

Examples:
  spite schema status
  spite schema sync
  spite schema sync --force
  spite schema diff
  spite schema reset --i-know-what-im-doing
`;

    default:
      return getHelpText();
  }
}
