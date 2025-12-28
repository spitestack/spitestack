/**
 * spite build - Compiles .spitestack into a standalone executable.
 *
 * Takes the readable generated code from `spite compile` and creates
 * a standalone binary with the Bun runtime embedded. No dependencies
 * required to run the output.
 */

import { existsSync } from "fs";
import { mkdir } from "fs/promises";
import { join, relative, basename } from "path";
import { logger } from "../utils/logger";

interface BuildOptions {
  inputDir: string;
  outputDir: string;
}

export async function build(options: BuildOptions): Promise<void> {
  const startTime = performance.now();

  logger.header("spite build");
  logger.info(`Input: ${relative(process.cwd(), options.inputDir) || "."}`);
  logger.info(`Output: ${relative(process.cwd(), options.outputDir)}`);
  logger.raw("");

  // Verify input directory exists
  if (!existsSync(options.inputDir)) {
    logger.error(`Input directory not found: ${options.inputDir}`);
    logger.raw("");
    logger.raw("Run 'spite compile' first to generate the .spitestack directory.");
    process.exit(1);
  }

  // Verify server.ts exists (entry point)
  const entryPoint = join(options.inputDir, "server.ts");
  if (!existsSync(entryPoint)) {
    logger.error(`Entry point not found: ${entryPoint}`);
    logger.raw("");
    logger.raw("The .spitestack directory appears incomplete. Run 'spite compile' to regenerate.");
    process.exit(1);
  }

  try {
    // Ensure output directory exists
    await mkdir(options.outputDir, { recursive: true });

    logger.step("Compiling standalone executable...");

    const outputName = "server";
    const outputPath = join(options.outputDir, outputName);

    // Use bun build --compile to create standalone executable
    const proc = Bun.spawn([
      "bun",
      "build",
      entryPoint,
      "--compile",
      "--minify",
      "--outfile",
      outputPath,
    ], {
      stdout: "pipe",
      stderr: "pipe",
    });

    const exitCode = await proc.exited;

    if (exitCode !== 0) {
      const stderr = await new Response(proc.stderr).text();
      logger.error("Build failed:");
      logger.raw(stderr);
      process.exit(1);
    }

    logger.step("Embedded Bun runtime");

    // Get output size
    const outputFile = Bun.file(outputPath);
    const sizeMb = (outputFile.size / (1024 * 1024)).toFixed(1);

    const elapsed = ((performance.now() - startTime) / 1000).toFixed(2);
    logger.raw("");
    logger.success(`Built in ${elapsed}s`);
    logger.raw("");
    logger.file(`${outputName} (${sizeMb} MB)`);
    logger.raw("");
    logger.raw(`  Run with: ./${relative(process.cwd(), outputPath)}`);
  } catch (error) {
    if (error instanceof Error) {
      logger.error(error.message);
    } else {
      logger.error(String(error));
    }
    process.exit(1);
  }
}
