import { existsSync } from "node:fs";
import { resolve, join } from "node:path";
import type {
  CompilerConfig,
  PartialConfig,
  SpiteStackAppConfig,
  SpiteStackRegistration,
} from "./types";

const DEFAULT_CONFIG: CompilerConfig = {
  mode: "greenfield",
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
  routes: {
    basePath: "/api",
    publicSessionHeader: "x-session-id",
    publicSessionRequired: true,
    publicTenantId: undefined,
  },
  schemaEvolution: {
    upcasterDir: "./.spitestack/upcasters",
  },
  api: {
    versioning: false,
    deprecationWarnings: true,
  },
};

const APP_FILES = [
  "spitestack.app.ts",
  "spitestack.app.js",
  "spitestack.app.mjs",
  "index.ts",
  "index.js",
  "index.mjs",
];

/**
 * Merge partial config with defaults
 */
function mergeConfig(partial: PartialConfig): CompilerConfig {
  return {
    mode: partial.mode ?? DEFAULT_CONFIG.mode,
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
    routes: {
      basePath: partial.routes?.basePath ?? DEFAULT_CONFIG.routes.basePath,
      publicSessionHeader:
        partial.routes?.publicSessionHeader ?? DEFAULT_CONFIG.routes.publicSessionHeader,
      publicSessionRequired:
        partial.routes?.publicSessionRequired ?? DEFAULT_CONFIG.routes.publicSessionRequired,
      publicTenantId: partial.routes?.publicTenantId ?? DEFAULT_CONFIG.routes.publicTenantId,
    },
    schemaEvolution: {
      upcasterDir: partial.schemaEvolution?.upcasterDir ?? DEFAULT_CONFIG.schemaEvolution.upcasterDir,
    },
    api: {
      versioning: partial.api?.versioning ?? DEFAULT_CONFIG.api.versioning,
      latestAlias: partial.api?.latestAlias ?? DEFAULT_CONFIG.api.latestAlias,
      deprecationWarnings: partial.api?.deprecationWarnings ?? DEFAULT_CONFIG.api.deprecationWarnings,
    },
  };
}

/**
 * Find and load the App entrypoint from the current directory or parents
 */
export async function loadConfig(cwd: string = process.cwd()): Promise<{
  config: CompilerConfig;
  configPath: string | null;
}> {
  // Search for app file
  let searchDir = cwd;
  let appPath: string | null = null;

  while (searchDir !== "/") {
    for (const appFile of APP_FILES) {
      const candidate = join(searchDir, appFile);
      if (existsSync(candidate)) {
        appPath = candidate;
        break;
      }
    }
    if (appPath) break;
    searchDir = resolve(searchDir, "..");
  }

  // If no app found, use defaults
  if (!appPath) {
    return {
      config: DEFAULT_CONFIG,
      configPath: null,
    };
  }

  // Load and merge config from App instance
  try {
    const imported = await import(appPath);
    const raw = imported.default ?? imported;
    const isAppInstance = raw && typeof raw === "object" && "config" in raw;

    if (!isAppInstance) {
      throw new Error(
        `Expected default export to be an App() instance. Got: ${typeof raw}`
      );
    }

    const appConfig = (raw as { config?: SpiteStackAppConfig }).config ?? null;
    const registrations = (raw as { registrations?: SpiteStackRegistration[] }).registrations ?? null;

    // Merge with defaults to ensure all fields have values
    const partial: PartialConfig = (appConfig ?? {}) as PartialConfig;
    const config = mergeConfig(partial);

    // Resolve paths relative to app file
    const configDir = resolve(appPath, "..");
    config.domainDir = resolve(configDir, config.domainDir);
    config.outDir = resolve(configDir, config.outDir);
    config.appPath = appPath;
    config.appConfig = appConfig;
    config.registrations = registrations;

    return { config, configPath: appPath };
  } catch (error) {
    throw new Error(
      `Failed to load App from ${appPath}: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

/**
 * Get default app content (for `spitestack init`)
 */
export function getDefaultAppContent(): string {
  return `import App from "@spitestack/compiler/app";

export default App({
  // Mode determines schema evolution behavior:
  // - "greenfield": No lock files, iterate freely (default)
  // - "production": Lock files enforced, breaking changes require upcasters
  mode: "greenfield",

  domainDir: "./src/domain/aggregates",
});
`;
}
