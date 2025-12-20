import { existsSync, readFileSync, writeFileSync, mkdirSync, readdirSync, renameSync } from "node:fs";
import { dirname, join } from "node:path";
import type { AggregateAnalysis, TypeInfo } from "../types";

// ============================================================================
// Lock File Types
// ============================================================================

export interface ApiParameterSchema {
  name: string;
  type: string;
  optional: boolean;
}

export interface ApiCommandSchema {
  parameters: ApiParameterSchema[];
  scope: string;
  roles?: string[];
}

export interface ApiLock {
  version: string;
  lockedAt: string;
  commands: Record<string, ApiCommandSchema>;
}

// ============================================================================
// Lock File I/O
// ============================================================================

const API_LOCK_FILENAME = "api.lock";

/**
 * Get the path to the API lock file
 */
export function getApiLockPath(outDir: string): string {
  return join(dirname(outDir), ".spitestack", API_LOCK_FILENAME);
}

/**
 * Get the path to a versioned API lock file
 */
export function getVersionedApiLockPath(outDir: string, version: string): string {
  return join(dirname(outDir), ".spitestack", `api.${version}.lock`);
}

/**
 * Check if an API lock file exists
 */
export function apiLockExists(outDir: string): boolean {
  return existsSync(getApiLockPath(outDir));
}

/**
 * Read the API lock file
 */
export function readApiLock(outDir: string): ApiLock | null {
  const lockPath = getApiLockPath(outDir);
  if (!existsSync(lockPath)) {
    return null;
  }

  try {
    const content = readFileSync(lockPath, "utf-8");
    return JSON.parse(content) as ApiLock;
  } catch (error) {
    throw new Error(
      `Failed to read API lock file at ${lockPath}: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

/**
 * Read a versioned API lock file
 */
export function readVersionedApiLock(outDir: string, version: string): ApiLock | null {
  const lockPath = getVersionedApiLockPath(outDir, version);
  if (!existsSync(lockPath)) {
    return null;
  }

  try {
    const content = readFileSync(lockPath, "utf-8");
    return JSON.parse(content) as ApiLock;
  } catch (error) {
    throw new Error(
      `Failed to read versioned API lock file at ${lockPath}: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

/**
 * Write the API lock file
 */
export function writeApiLock(outDir: string, lock: ApiLock): void {
  const lockPath = getApiLockPath(outDir);
  const lockDir = dirname(lockPath);

  if (!existsSync(lockDir)) {
    mkdirSync(lockDir, { recursive: true });
  }

  writeFileSync(lockPath, JSON.stringify(lock, null, 2) + "\n", "utf-8");
}

/**
 * Get all versioned API locks
 */
export function getVersionedApiLocks(outDir: string): string[] {
  const lockDir = join(dirname(outDir), ".spitestack");
  if (!existsSync(lockDir)) {
    return [];
  }

  const files = readdirSync(lockDir);
  const versionedLocks = files
    .filter((f) => f.startsWith("api.v") && f.endsWith(".lock"))
    .map((f) => f.slice(4, -5)); // Extract version from "api.v1.lock" -> "v1"

  return versionedLocks.sort();
}

/**
 * Bump API version - moves current lock to versioned and creates new lock
 */
export function bumpApiVersion(outDir: string, newVersion: string): void {
  const currentLock = readApiLock(outDir);
  if (!currentLock) {
    throw new Error("No API lock file exists to bump");
  }

  const oldVersion = currentLock.version;

  // Move current lock to versioned
  const currentPath = getApiLockPath(outDir);
  const versionedPath = getVersionedApiLockPath(outDir, oldVersion);
  renameSync(currentPath, versionedPath);

  // Create new lock with new version
  const newLock: ApiLock = {
    version: newVersion,
    lockedAt: new Date().toISOString(),
    commands: currentLock.commands,
  };

  writeApiLock(outDir, newLock);
}

// ============================================================================
// Lock File Generation
// ============================================================================

/**
 * Convert TypeInfo to a simple type string
 */
function typeInfoToString(typeInfo: TypeInfo): string {
  switch (typeInfo.kind) {
    case "string":
      return "string";
    case "number":
      return "number";
    case "boolean":
      return "boolean";
    case "null":
      return "null";
    case "undefined":
      return "undefined";
    case "array":
      return typeInfo.elementType
        ? `${typeInfoToString(typeInfo.elementType)}[]`
        : "unknown[]";
    case "object":
      if (typeInfo.properties) {
        const props = Object.entries(typeInfo.properties)
          .map(([key, value]) => `${key}: ${typeInfoToString(value)}`)
          .join("; ");
        return `{ ${props} }`;
      }
      return "object";
    case "union":
      if (typeInfo.types) {
        return typeInfo.types.map(typeInfoToString).join(" | ");
      }
      return "unknown";
    case "literal":
      return typeInfo.literalValue !== undefined
        ? JSON.stringify(typeInfo.literalValue)
        : "unknown";
    default:
      return "unknown";
  }
}

/**
 * Generate an API lock from aggregate analysis results
 */
export function generateApiLock(
  aggregates: AggregateAnalysis[],
  commandPolicies: Map<string, { scope: string; roles?: string[] }>
): ApiLock {
  const lock: ApiLock = {
    version: "v1",
    lockedAt: new Date().toISOString(),
    commands: {},
  };

  for (const aggregate of aggregates) {
    for (const command of aggregate.commands) {
      const commandKey = `${aggregate.aggregateName}.${command.methodName}`;
      const policy = commandPolicies.get(commandKey);

      lock.commands[commandKey] = {
        parameters: command.parameters.map((p) => ({
          name: p.name,
          type: typeInfoToString(p.type),
          optional: p.optional,
        })),
        scope: policy?.scope ?? "auth",
        roles: policy?.roles,
      };
    }
  }

  return lock;
}

/**
 * Update an existing lock with new commands while preserving existing
 */
export function mergeApiLock(
  existing: ApiLock,
  aggregates: AggregateAnalysis[],
  commandPolicies: Map<string, { scope: string; roles?: string[] }>
): ApiLock {
  const newLock = generateApiLock(aggregates, commandPolicies);

  // Preserve version
  newLock.version = existing.version;
  newLock.lockedAt = new Date().toISOString();

  return newLock;
}
