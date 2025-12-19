import { mkdir, writeFile } from "node:fs/promises";
import { join, dirname } from "node:path";
import type { AggregateAnalysis, CompilerConfig, GenerationResult, GeneratedFile } from "../types";
import { generateValidators } from "../generators/validator";
import { generateHandlers } from "../generators/handler";
import { generateWiringFile, generateIndexFile } from "../generators/wiring";

/**
 * Generate all code from analyzed aggregates
 */
export function generateCode(
  aggregates: AggregateAnalysis[],
  config: CompilerConfig
): GenerationResult {
  const result: GenerationResult = {
    handlers: [],
    validators: [],
    wiring: null,
    index: null,
  };

  if (aggregates.length === 0) {
    return result;
  }

  // Generate validators
  if (config.generate.validators) {
    result.validators = generateValidators(aggregates);
  }

  // Generate handlers
  if (config.generate.handlers) {
    result.handlers = generateHandlers(aggregates, config);
  }

  // Generate wiring
  if (config.generate.wiring) {
    result.wiring = generateWiringFile(aggregates);
    result.index = generateIndexFile(aggregates);
  }

  return result;
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

  await Promise.all(allFiles.map(writeGenFile));
}

/**
 * Generate and write all code
 */
export async function generate(
  aggregates: AggregateAnalysis[],
  config: CompilerConfig
): Promise<GenerationResult> {
  const result = generateCode(aggregates, config);
  await writeGeneratedFiles(result, config.outDir);
  return result;
}
