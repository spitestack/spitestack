/**
 * Frontend module - language-specific parsers.
 */

export * from "./typescript/index.js";

import { readdir, readFile } from "fs/promises";
import { join, relative } from "path";
import { CompilerError } from "../diagnostic/index.js";
import { createDomainIR, type DomainIR } from "../ir/index.js";
import { TypeScriptParser } from "./typescript/parser.js";
import { convertToIR } from "./typescript/to-ir.js";
import type { ParsedFile } from "./typescript/ast.js";

/**
 * Frontend interface for language-specific parsing.
 */
export interface Frontend {
  /**
   * Parses all files in a directory into a DomainIR.
   */
  parseDirectory(dirPath: string): Promise<DomainIR>;
}

/**
 * Creates a frontend for the specified language.
 */
export function createFrontend(language: string): Frontend {
  if (language === "typescript" || language === "ts") {
    return new TypeScriptFrontend();
  }
  throw CompilerError.unsupportedLanguage(language);
}

/**
 * TypeScript frontend implementation.
 */
class TypeScriptFrontend implements Frontend {
  private parser = new TypeScriptParser();

  async parseDirectory(dirPath: string): Promise<DomainIR> {
    const domain = createDomainIR(dirPath);
    const files = await this.findTypeScriptFiles(dirPath);

    const parsedFiles: ParsedFile[] = [];

    for (const filePath of files) {
      // Skip index.ts (handled separately for app config)
      if (filePath.endsWith("index.ts")) {
        continue;
      }

      const source = await readFile(filePath, "utf-8");
      const parsed = this.parser.parse(source, filePath);
      parsedFiles.push(parsed);
    }

    // Convert parsed files to IR
    for (const parsed of parsedFiles) {
      convertToIR(parsed, domain);
    }

    return domain;
  }

  /**
   * Recursively finds all TypeScript files in a directory.
   */
  private async findTypeScriptFiles(dirPath: string): Promise<string[]> {
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
}
