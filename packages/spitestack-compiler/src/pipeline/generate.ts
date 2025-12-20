import { mkdir, writeFile } from "node:fs/promises";
import { join, dirname, relative, extname } from "node:path";
import type {
  AggregateAnalysis,
  CompilerConfig,
  GenerationResult,
  GeneratedFile,
  SpiteStackRegistration,
  SpiteStackScope,
} from "../types";
import { generateValidators } from "../generators/validator";
import { generateHandlers } from "../generators/handler";
import { generateWiringFile, generateIndexFile } from "../generators/wiring";
import { generateAuthFile } from "../generators/auth";
import { generateRoutesFile } from "../generators/routes";
import {
  schemaLockExists,
  readSchemaLock,
  writeSchemaLock,
  generateSchemaLock,
  mergeSchemaLock,
} from "../schema/lock";
import { compareSchemaLocks, formatSchemaDiff, getBreakingChanges } from "../schema/diff";
import {
  apiLockExists,
  readApiLock,
  writeApiLock,
  generateApiLock,
  mergeApiLock,
  getVersionedApiLocks,
} from "../api/lock";
import { compareApiLocks, formatApiDiff, getApiBreakingChanges } from "../api/diff";

// ============================================================================
// Lock Errors
// ============================================================================

export class SchemaEvolutionError extends Error {
  constructor(
    message: string,
    public readonly breakingChanges: {
      aggregate: string;
      event: string;
      changes: { propertyName: string; message: string }[];
    }[]
  ) {
    super(message);
    this.name = "SchemaEvolutionError";
  }
}

export class ApiEvolutionError extends Error {
  constructor(
    message: string,
    public readonly breakingChanges: {
      command: string;
      changes: { parameterName: string; message: string }[];
    }[]
  ) {
    super(message);
    this.name = "ApiEvolutionError";
  }
}

// ============================================================================
// Schema Lock Integration
// ============================================================================

export interface SchemaLockResult {
  created: boolean;
  updated: boolean;
  hasBreakingChanges: boolean;
  diffMessage?: string;
}

export interface CheckSchemaLockOptions {
  force?: boolean;
}

/**
 * Check and update schema lock based on mode
 */
export function checkSchemaLock(
  aggregates: AggregateAnalysis[],
  config: CompilerConfig,
  options: CheckSchemaLockOptions = {}
): SchemaLockResult {
  // In greenfield mode, skip all lock checking
  if (config.mode === "greenfield") {
    return { created: false, updated: false, hasBreakingChanges: false };
  }

  const lockExists = schemaLockExists(config.outDir);

  // Production mode: create lock if it doesn't exist
  if (!lockExists) {
    const lock = generateSchemaLock(aggregates);
    writeSchemaLock(config.outDir, lock);
    return { created: true, updated: false, hasBreakingChanges: false };
  }

  // Lock exists: compare with current schema
  const existingLock = readSchemaLock(config.outDir);
  if (!existingLock) {
    throw new Error("Failed to read existing schema lock file");
  }

  const currentLock = generateSchemaLock(aggregates);
  const diff = compareSchemaLocks(existingLock, currentLock);

  // No changes
  if (!diff.hasChanges) {
    return { created: false, updated: false, hasBreakingChanges: false };
  }

  // Has breaking changes
  if (diff.hasBreakingChanges) {
    // If force is enabled, update the lock anyway with a warning
    if (options.force) {
      const mergedLock = mergeSchemaLock(existingLock, aggregates);
      // Increment schema versions for breaking events
      const breakingChanges = getBreakingChanges(diff);
      for (const change of breakingChanges) {
        if (change.event !== "*" && mergedLock.aggregates[change.aggregate]?.events[change.event]) {
          mergedLock.aggregates[change.aggregate].events[change.event].schemaVersion += 1;
        }
      }
      writeSchemaLock(config.outDir, mergedLock);
      return {
        created: false,
        updated: true,
        hasBreakingChanges: true,
        diffMessage: `WARNING: Forced update with breaking changes!\n\n${formatSchemaDiff(diff)}`,
      };
    }

    // Otherwise, error
    const breakingChanges = getBreakingChanges(diff);
    throw new SchemaEvolutionError(
      `Breaking schema changes detected. Upcasters required.\n\n${formatSchemaDiff(diff)}`,
      breakingChanges
    );
  }

  // Only additive changes: update lock
  const mergedLock = mergeSchemaLock(existingLock, aggregates);
  writeSchemaLock(config.outDir, mergedLock);

  return {
    created: false,
    updated: true,
    hasBreakingChanges: false,
    diffMessage: formatSchemaDiff(diff),
  };
}

// ============================================================================
// API Lock Integration
// ============================================================================

export interface ApiLockResult {
  created: boolean;
  updated: boolean;
  hasBreakingChanges: boolean;
  diffMessage?: string;
  currentVersion: string;
  frozenVersions: string[];
}

/**
 * Check and update API lock based on config
 */
export function checkApiLock(
  aggregates: AggregateAnalysis[],
  commandPolicies: Map<string, { scope: string; roles?: string[] }>,
  config: CompilerConfig,
  options: CheckSchemaLockOptions = {}
): ApiLockResult {
  // API versioning is opt-in
  if (!config.api.versioning) {
    return {
      created: false,
      updated: false,
      hasBreakingChanges: false,
      currentVersion: "v1",
      frozenVersions: [],
    };
  }

  const lockExists = apiLockExists(config.outDir);
  const frozenVersions = getVersionedApiLocks(config.outDir);

  // Production mode with API versioning: create lock if it doesn't exist
  if (!lockExists) {
    const lock = generateApiLock(aggregates, commandPolicies);
    writeApiLock(config.outDir, lock);
    return {
      created: true,
      updated: false,
      hasBreakingChanges: false,
      currentVersion: "v1",
      frozenVersions,
    };
  }

  // Lock exists: compare with current API
  const existingLock = readApiLock(config.outDir);
  if (!existingLock) {
    throw new Error("Failed to read existing API lock file");
  }

  const currentLock = generateApiLock(aggregates, commandPolicies);
  const diff = compareApiLocks(existingLock, currentLock);

  // No changes
  if (!diff.hasChanges) {
    return {
      created: false,
      updated: false,
      hasBreakingChanges: false,
      currentVersion: existingLock.version,
      frozenVersions,
    };
  }

  // Has breaking changes
  if (diff.hasBreakingChanges) {
    // If force is enabled, update the lock anyway with a warning
    if (options.force) {
      const mergedLock = mergeApiLock(existingLock, aggregates, commandPolicies);
      writeApiLock(config.outDir, mergedLock);
      return {
        created: false,
        updated: true,
        hasBreakingChanges: true,
        diffMessage: `WARNING: Forced update with breaking API changes!\n\n${formatApiDiff(diff)}`,
        currentVersion: existingLock.version,
        frozenVersions,
      };
    }

    // Otherwise, error
    const breakingChanges = getApiBreakingChanges(diff);
    throw new ApiEvolutionError(
      `Breaking API changes detected. Version bump required.\n\n${formatApiDiff(diff)}\n\nTo fix:\n  1. Run 'spitestack version api v${parseInt(existingLock.version.slice(1)) + 1}' to bump the version\n  2. Or use --force to bypass (WARNING: may break API consumers)`,
      breakingChanges
    );
  }

  // Only additive changes: update lock
  const mergedLock = mergeApiLock(existingLock, aggregates, commandPolicies);
  writeApiLock(config.outDir, mergedLock);

  return {
    created: false,
    updated: true,
    hasBreakingChanges: false,
    diffMessage: formatApiDiff(diff),
    currentVersion: existingLock.version,
    frozenVersions,
  };
}

export interface GenerateCodeOptions {
  apiVersioning?: {
    enabled: boolean;
    currentVersion: string;
    frozenVersions: string[];
    latestAlias?: string;
  };
}

/**
 * Generate all code from analyzed aggregates
 */
export function generateCode(
  aggregates: AggregateAnalysis[],
  config: CompilerConfig,
  options: GenerateCodeOptions = {}
): GenerationResult {
  const registrationResult = applyRegistrations(aggregates, config.registrations);
  const result: GenerationResult = {
    handlers: [],
    validators: [],
    wiring: null,
    index: null,
    auth: null,
    routes: null,
  };

  if (registrationResult.aggregates.length === 0) {
    return result;
  }

  // Generate validators
  if (config.generate.validators) {
    result.validators = generateValidators(registrationResult.aggregates);
  }

  // Generate handlers
  if (config.generate.handlers) {
    result.handlers = generateHandlers(registrationResult.aggregates, config);
  }

  // Generate wiring
  if (config.generate.wiring) {
    result.wiring = generateWiringFile(registrationResult.aggregates, {
      allowedCommands: registrationResult.allowedCommands,
    });
    result.index = generateIndexFile(registrationResult.aggregates, {
      allowedCommands: registrationResult.allowedCommands,
    });
  }

  if (config.generate.wiring) {
    const appImportPath = config.appPath
      ? toImportPath(config.outDir, config.appPath)
      : null;
    result.auth = generateAuthFile(appImportPath);
    result.routes = generateRoutesFile(
      registrationResult.commandPolicies,
      appImportPath,
      options.apiVersioning
    );
  }

  return result;
}

function toImportPath(fromDir: string, filePath: string): string {
  let relPath = relative(fromDir, filePath);
  relPath = relPath.replace(/\\/g, "/");
  const extension = extname(relPath);
  if (extension) {
    relPath = relPath.slice(0, -extension.length);
  }
  if (!relPath.startsWith(".")) {
    relPath = `./${relPath}`;
  }
  return relPath;
}

type CommandPolicy = {
  scope: SpiteStackScope;
  roles?: string[];
};

type RegistrationResult = {
  aggregates: AggregateAnalysis[];
  allowedCommands: Map<string, Set<string>>;
  commandPolicies: Map<string, CommandPolicy>;
};

function applyRegistrations(
  aggregates: AggregateAnalysis[],
  registrations?: SpiteStackRegistration[] | null
): RegistrationResult {
  const commandPolicies = new Map<string, CommandPolicy>();
  const allowedCommands = new Map<string, Set<string>>();

  if (!registrations || registrations.length === 0) {
    for (const aggregate of aggregates) {
      const allowed = new Set<string>();
      for (const cmd of aggregate.commands) {
        allowed.add(cmd.methodName);
        commandPolicies.set(`${aggregate.aggregateName}.${cmd.methodName}`, {
          scope: "auth",
        });
      }
      allowedCommands.set(aggregate.aggregateName, allowed);
    }

    return {
      aggregates,
      allowedCommands,
      commandPolicies,
    };
  }

  const registrationMap = new Map<string, SpiteStackRegistration>();
  for (const registration of registrations) {
    registrationMap.set(registration.aggregate.toLowerCase(), registration);
  }

  const filtered: AggregateAnalysis[] = [];

  for (const aggregate of aggregates) {
    const registration = registrationMap.get(aggregate.aggregateName.toLowerCase());
    if (!registration) {
      continue;
    }

    const allowed = new Set<string>();
    const defaultScope = registration.scope ?? "auth";
    const defaultRoles = registration.roles;

    for (const cmd of aggregate.commands) {
      const override = registration.methods?.[cmd.methodName];
      if (override === false) {
        continue;
      }

      let scope = defaultScope;
      let roles = defaultRoles;

      if (typeof override === "string") {
        scope = override;
      } else if (override && typeof override === "object") {
        if (override.scope) {
          scope = override.scope;
        }
        if (override.roles) {
          roles = override.roles;
        }
      }

      allowed.add(cmd.methodName);
      commandPolicies.set(`${aggregate.aggregateName}.${cmd.methodName}`, {
        scope,
        roles,
      });
    }

    allowedCommands.set(aggregate.aggregateName, allowed);
    filtered.push(aggregate);
  }

  return {
    aggregates: filtered,
    allowedCommands,
    commandPolicies,
  };
}

/**
 * Write generated files to disk
 */
export async function writeGeneratedFiles(
  result: GenerationResult,
  outDir: string
): Promise<void> {
  // Create output directories
  await mkdir(join(outDir, "handlers"), { recursive: true });
  await mkdir(join(outDir, "validators"), { recursive: true });

  // Helper to write a file
  async function writeGenFile(file: GeneratedFile) {
    const fullPath = join(outDir, file.path);
    await mkdir(dirname(fullPath), { recursive: true });
    await writeFile(fullPath, file.content, "utf-8");
  }

  // Write all files
  const allFiles: GeneratedFile[] = [
    ...result.handlers,
    ...result.validators,
  ];

  if (result.wiring) {
    allFiles.push(result.wiring);
  }

  if (result.index) {
    allFiles.push(result.index);
  }

  if (result.auth) {
    allFiles.push(result.auth);
  }

  if (result.routes) {
    allFiles.push(result.routes);
  }

  await Promise.all(allFiles.map(writeGenFile));
}

export interface GenerateOptions {
  force?: boolean;
}

export interface GenerateResult extends GenerationResult {
  schemaLock: SchemaLockResult;
  apiLock: ApiLockResult;
}

/**
 * Generate and write all code
 */
export async function generate(
  aggregates: AggregateAnalysis[],
  config: CompilerConfig,
  options: GenerateOptions = {}
): Promise<GenerateResult> {
  // Check schema lock before generating code
  const schemaLockResult = checkSchemaLock(aggregates, config, { force: options.force });

  // Get command policies from registration result for API lock checking
  const registrationResult = applyRegistrations(aggregates, config.registrations);

  // Check API lock (only if api.versioning is enabled)
  const apiLockResult = checkApiLock(
    aggregates,
    registrationResult.commandPolicies,
    config,
    { force: options.force }
  );

  // Generate code with API versioning info
  const result = generateCode(aggregates, config, {
    apiVersioning: config.api.versioning
      ? {
          enabled: true,
          currentVersion: apiLockResult.currentVersion,
          frozenVersions: apiLockResult.frozenVersions,
          latestAlias: config.api.latestAlias,
        }
      : undefined,
  });

  await writeGeneratedFiles(result, config.outDir);

  return { ...result, schemaLock: schemaLockResult, apiLock: apiLockResult };
}
