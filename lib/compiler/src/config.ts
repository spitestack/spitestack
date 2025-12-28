/**
 * Compiler configuration.
 */

/**
 * Configuration for the SpiteStack compiler.
 */
export interface CompilerConfig {
  /** Directory containing domain source files. */
  domainDir: string;

  /** Directory to write generated code. */
  outDir: string;

  /** Skip purity checks (for testing). */
  skipPurityCheck: boolean;

  /** Source language (default: "typescript"). */
  language: string;
}

/**
 * Creates default compiler configuration.
 */
export function defaultConfig(): CompilerConfig {
  return {
    domainDir: "domain",
    outDir: "src/generated",
    skipPurityCheck: false,
    language: "typescript",
  };
}

/**
 * Merges partial config with defaults.
 */
export function mergeConfig(partial: Partial<CompilerConfig>): CompilerConfig {
  return {
    ...defaultConfig(),
    ...partial,
  };
}
