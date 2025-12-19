import ts from "typescript";
import { readFileSync } from "node:fs";
import type { DiscoveredFile, ParsedFile } from "../types";

/**
 * Create a TypeScript program from discovered files
 */
export function createProgram(files: DiscoveredFile[]): ts.Program {
  const filePaths = files.map((f) => f.path);

  // Create compiler options
  const compilerOptions: ts.CompilerOptions = {
    target: ts.ScriptTarget.ESNext,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    strict: true,
    esModuleInterop: true,
    skipLibCheck: true,
    noEmit: true,
  };

  return ts.createProgram(filePaths, compilerOptions);
}

/**
 * Parse a single file and extract relevant declarations
 */
export function parseFile(
  file: DiscoveredFile,
  program: ts.Program
): ParsedFile | null {
  const sourceFile = program.getSourceFile(file.path);

  if (!sourceFile) {
    return null;
  }

  const classes: ts.ClassDeclaration[] = [];
  const typeAliases: ts.TypeAliasDeclaration[] = [];

  // Walk the AST to find class declarations and type aliases
  function visit(node: ts.Node) {
    if (ts.isClassDeclaration(node) && node.name) {
      classes.push(node);
    }

    if (ts.isTypeAliasDeclaration(node)) {
      typeAliases.push(node);
    }

    ts.forEachChild(node, visit);
  }

  visit(sourceFile);

  return {
    filePath: file.path,
    sourceFile,
    classes,
    typeAliases,
  };
}

/**
 * Parse all discovered files
 */
export function parseFiles(files: DiscoveredFile[]): {
  program: ts.Program;
  parsedFiles: ParsedFile[];
} {
  const program = createProgram(files);
  const parsedFiles: ParsedFile[] = [];

  for (const file of files) {
    const parsed = parseFile(file, program);
    if (parsed && parsed.classes.length > 0) {
      parsedFiles.push(parsed);
    }
  }

  return { program, parsedFiles };
}

/**
 * Get line and column from a position in source file
 */
export function getLineAndColumn(
  sourceFile: ts.SourceFile,
  pos: number
): { line: number; column: number } {
  const { line, character } = sourceFile.getLineAndCharacterOfPosition(pos);
  return {
    line: line + 1, // 1-indexed
    column: character + 1, // 1-indexed
  };
}

/**
 * Get the end position line and column
 */
export function getEndLineAndColumn(
  sourceFile: ts.SourceFile,
  node: ts.Node
): { endLine: number; endColumn: number } {
  const { line, character } = sourceFile.getLineAndCharacterOfPosition(node.getEnd());
  return {
    endLine: line + 1,
    endColumn: character + 1,
  };
}

/**
 * Get the full text of a node without leading trivia
 */
export function getNodeText(node: ts.Node, sourceFile: ts.SourceFile): string {
  return node.getText(sourceFile);
}
