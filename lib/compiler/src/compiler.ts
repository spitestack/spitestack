/**
 * Main SpiteStack compiler.
 */

import { readdir, readFile, mkdir, writeFile } from "fs/promises";
import { existsSync } from "fs";
import { join, relative, dirname } from "path";

import { CompilerError } from "./diagnostic/index.js";
import { createDomainIR, type DomainIR } from "./ir/index.js";
import { TypeScriptParser, convertToIR, parseAppConfig, applyAccessConfig } from "./frontend/index.js";
import { validateDomain } from "./validate/index.js";
import { generate, type GeneratedCode } from "./codegen/index.js";
import { type CompilerConfig, defaultConfig, mergeConfig } from "./config.js";
import { SPITEDB_JS, SPITEDB_DTS } from "./runtime/embedded.js";

/**
 * SpiteStack compiler.
 */
export class Compiler {
  private config: CompilerConfig;
  private parser: TypeScriptParser;

  constructor(config?: Partial<CompilerConfig>) {
    this.config = mergeConfig(config ?? {});
    this.parser = new TypeScriptParser();
  }

  /**
   * Compiles domain source files and generates code.
   */
  async compile(): Promise<GeneratedCode> {
    // Parse all TypeScript files
    const domain = await this.parse();

    // Validate domain IR
    if (!this.config.skipPurityCheck) {
      validateDomain(domain);
    }

    // Generate code
    const domainImportPath = this.calculateDomainImportPath();
    return generate(domain, domainImportPath);
  }

  /**
   * Compiles and writes output to the configured output directory.
   */
  async compileProject(): Promise<void> {
    const generated = await this.compile();

    // Ensure output directory exists
    await mkdir(this.config.outDir, { recursive: true });

    // Vendor the runtime (copy spitedb.js and spitedb.d.ts)
    await this.vendorRuntime();

    // Write generated files
    for (const { path: filename, content } of generated.files) {
      const outputPath = join(this.config.outDir, filename);

      // Ensure parent directory exists
      const dir = dirname(outputPath);
      if (!existsSync(dir)) {
        await mkdir(dir, { recursive: true });
      }

      await writeFile(outputPath, content, "utf-8");
    }
  }


  /**
   * Vendors the SpiteDB runtime into the output directory.
   *
   * Copies the pre-built spitedb.js bundle and type declarations
   * so the generated code is self-contained.
   */
  private async vendorRuntime(): Promise<void> {
    const runtimeDir = join(this.config.outDir, "runtime");
    await mkdir(runtimeDir, { recursive: true });

    // Write embedded runtime to output directory
    if (!SPITEDB_JS) {
      throw new CompilerError(
        "SpiteDB runtime not embedded. Run 'bun run build' to generate the runtime bundle."
      );
    }

    await writeFile(join(runtimeDir, "spitedb.js"), SPITEDB_JS);
    await writeFile(join(runtimeDir, "spitedb.d.ts"), SPITEDB_DTS);
  }

  /**
   * Parses domain files without generating code.
   */
  async parse(): Promise<DomainIR> {
    const domain = createDomainIR(this.config.domainDir);
    const files = await this.findTypeScriptFiles(this.config.domainDir);

    for (const filePath of files) {
      // Skip index.ts (handled separately for app config)
      if (filePath.endsWith("index.ts")) {
        continue;
      }

      const source = await readFile(filePath, "utf-8");
      const parsed = this.parser.parse(source, filePath);
      convertToIR(parsed, domain);
    }

    // Parse app config if present
    const appConfig = await parseAppConfig(this.config.domainDir);
    if (appConfig) {
      domain.appConfig = appConfig;
      applyAccessConfig(domain, appConfig);
    }

    // Check for at least some domain content
    if (domain.aggregates.length === 0 && domain.projections.length === 0) {
      throw CompilerError.noAggregates();
    }

    return domain;
  }

  /**
   * Type-checks the domain without generating code.
   */
  async check(): Promise<void> {
    const domain = await this.parse();
    validateDomain(domain);
  }

  /**
   * Recursively finds all TypeScript files in a directory.
   */
  private async findTypeScriptFiles(dirPath: string): Promise<string[]> {
    if (!existsSync(dirPath)) {
      return [];
    }

    const files: string[] = [];
    const entries = await readdir(dirPath, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = join(dirPath, entry.name);

      if (entry.isDirectory()) {
        // Skip node_modules and hidden directories
        if (entry.name === "node_modules" || entry.name.startsWith(".")) {
          continue;
        }
        const subFiles = await this.findTypeScriptFiles(fullPath);
        files.push(...subFiles);
      } else if (entry.isFile() && entry.name.endsWith(".ts")) {
        files.push(fullPath);
      }
    }

    return files;
  }

  /**
   * Calculates the relative import path from generated handlers to domain source.
   */
  private calculateDomainImportPath(): string {
    // From outDir/handlers/foo.ts to domainDir
    // e.g., "../../../../domain" for typical project structure
    const outHandlersDir = join(this.config.outDir, "handlers");
    const relativePath = relative(outHandlersDir, this.config.domainDir);
    return relativePath.replace(/\\/g, "/");
  }
}

/**
 * Creates a new compiler with the given config.
 */
export function createCompiler(config?: Partial<CompilerConfig>): Compiler {
  return new Compiler(config);
}
