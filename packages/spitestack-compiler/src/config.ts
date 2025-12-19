import { existsSync } from "node:fs";
import { resolve, join } from "node:path";
import type { CompilerConfig, PartialConfig } from "./types";

const DEFAULT_CONFIG: CompilerConfig = {
  domainDir: "./src/domain/aggregates",
  outDir: "./.spitestack/generated",
  include: ["**/*.ts"],
  exclude: ["**/*.test.ts", "**/*.spec.ts"],
  generate: {
    handlers: true,
    validators: true,
    wiring: true,
  },
  diagnostics: {
    colors: true,
    maxErrors: 50,
  },
};

const CONFIG_FILES = [
  "spitestack.config.ts",
  "spitestack.config.js",
  "spitestack.config.mjs",
];

/**
 * Define a SpiteStack compiler configuration.
 * Use this in your spitestack.config.ts file.
 */
export function defineConfig(config: PartialConfig): PartialConfig {
  return config;
}

/**
 * Merge partial config with defaults
 */
function mergeConfig(partial: PartialConfig): CompilerConfig {
  return {
    domainDir: partial.domainDir ?? DEFAULT_CONFIG.domainDir,
    outDir: partial.outDir ?? DEFAULT_CONFIG.outDir,
    include: partial.include ?? DEFAULT_CONFIG.include,
    exclude: partial.exclude ?? DEFAULT_CONFIG.exclude,
    generate: {
      handlers: partial.generate?.handlers ?? DEFAULT_CONFIG.generate.handlers,
      validators: partial.generate?.validators ?? DEFAULT_CONFIG.generate.validators,
      wiring: partial.generate?.wiring ?? DEFAULT_CONFIG.generate.wiring,
    },
    diagnostics: {
      colors: partial.diagnostics?.colors ?? DEFAULT_CONFIG.diagnostics.colors,
      maxErrors: partial.diagnostics?.maxErrors ?? DEFAULT_CONFIG.diagnostics.maxErrors,
    },
  };
}

/**
 * Find and load the config file from the current directory or parents
 */
export async function loadConfig(cwd: string = process.cwd()): Promise<{
  config: CompilerConfig;
  configPath: string | null;
}> {
  // Search for config file
  let searchDir = cwd;
  let configPath: string | null = null;

  while (searchDir !== "/") {
    for (const configFile of CONFIG_FILES) {
      const candidate = join(searchDir, configFile);
      if (existsSync(candidate)) {
        configPath = candidate;
        break;
      }
    }
    if (configPath) break;
    searchDir = resolve(searchDir, "..");
  }

  // If no config found, use defaults
  if (!configPath) {
    return {
      config: DEFAULT_CONFIG,
      configPath: null,
    };
  }

  // Load and merge config
  try {
    const imported = await import(configPath);
    const partial: PartialConfig = imported.default ?? imported;
    const config = mergeConfig(partial);

    // Resolve paths relative to config file
    const configDir = resolve(configPath, "..");
    config.domainDir = resolve(configDir, config.domainDir);
    config.outDir = resolve(configDir, config.outDir);

    return { config, configPath };
  } catch (error) {
    throw new Error(
      `Failed to load config from ${configPath}: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

/**
 * Get default config (for `spitestack init`)
 */
export function getDefaultConfigContent(): string {
  return `import { defineConfig } from "@spitestack/compiler";

export default defineConfig({
  domainDir: "./src/domain/aggregates",
  outDir: "./.spitestack/generated",
  include: ["**/*.ts"],
  exclude: ["**/*.test.ts", "**/*.spec.ts"],
});
`;
}
