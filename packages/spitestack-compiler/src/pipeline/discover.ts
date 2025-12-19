import { readdir, stat, readFile } from "node:fs/promises";
import { join, relative, extname } from "node:path";
import type { DiscoveredFile, CompilerConfig } from "../types";

/**
 * Simple glob pattern matcher
 * Supports: *, **, ?
 *
 * Pattern **\/*.ts matches:
 * - foo.ts (root level)
 * - dir/foo.ts
 * - dir/sub/foo.ts
 */
function matchGlob(pattern: string, path: string): boolean {
  // Handle common patterns directly for simplicity and correctness
  if (pattern === "**/*.ts") {
    // Match any .ts file at any depth
    return path.endsWith(".ts");
  }

  // Convert glob pattern to regex for other patterns
  let regexStr = pattern
    .replace(/[.+^${}()|[\]\\]/g, "\\$&") // Escape regex special chars
    .replace(/\*\*\//g, "(?:.*/)?") // **/ matches any directory including none
    .replace(/\*\*/g, ".*") // ** matches anything
    .replace(/\*/g, "[^/]*") // * matches anything except /
    .replace(/\?/g, "[^/]"); // ? matches single char except /

  const regex = new RegExp(`^${regexStr}$`);
  return regex.test(path);
}

/**
 * Check if a path matches any of the patterns
 */
function matchesAny(patterns: string[], path: string): boolean {
  return patterns.some((pattern) => matchGlob(pattern, path));
}

/**
 * Check if a file is an aggregate.ts file with an aggregate class
 */
async function isAggregateFile(filePath: string): Promise<boolean> {
  // Only aggregate.ts files in aggregate folders
  if (!filePath.endsWith("/aggregate.ts") && !filePath.endsWith("\\aggregate.ts")) {
    return false;
  }

  try {
    const content = await readFile(filePath, "utf-8");
    // Look for a class ending with "Aggregate"
    return /class\s+\w+Aggregate\b/.test(content);
  } catch {
    return false;
  }
}

/**
 * Recursively walk a directory
 */
async function walkDir(dir: string): Promise<string[]> {
  const files: string[] = [];
  const entries = await readdir(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = join(dir, entry.name);

    if (entry.isDirectory()) {
      // Skip node_modules and hidden directories
      if (entry.name === "node_modules" || entry.name.startsWith(".")) {
        continue;
      }
      files.push(...(await walkDir(fullPath)));
    } else if (entry.isFile() && extname(entry.name) === ".ts") {
      files.push(fullPath);
    }
  }

  return files;
}

/**
 * Discover TypeScript files in the domain directory that might contain aggregates
 */
export async function discoverFiles(
  config: CompilerConfig
): Promise<DiscoveredFile[]> {
  const { domainDir, include, exclude } = config;

  // Get all TypeScript files in the domain directory
  const allFiles = await walkDir(domainDir);

  const discovered: DiscoveredFile[] = [];

  for (const filePath of allFiles) {
    const relativePath = relative(domainDir, filePath);

    // Check include patterns
    if (!matchesAny(include, relativePath)) {
      continue;
    }

    // Check exclude patterns
    if (matchesAny(exclude, relativePath)) {
      continue;
    }

    // Only include aggregate.ts files
    if (!(await isAggregateFile(filePath))) {
      continue;
    }

    discovered.push({
      path: filePath,
      relativePath,
    });
  }

  return discovered;
}
