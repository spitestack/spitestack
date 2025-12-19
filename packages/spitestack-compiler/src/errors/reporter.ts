import { readFileSync } from "node:fs";
import { relative } from "node:path";
import type { Diagnostic, DiagnosticSeverity } from "../types";
import { DiagnosticSuggestions, type DiagnosticCodeType } from "./codes";

/**
 * ANSI color codes for terminal output
 */
const colors = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",

  // Severity colors
  error: "\x1b[1;31m", // Bold red
  warning: "\x1b[1;33m", // Bold yellow
  info: "\x1b[1;36m", // Bold cyan

  // UI colors
  lineNumber: "\x1b[1;34m", // Bold blue
  caret: "\x1b[1;31m", // Bold red
  caretWarning: "\x1b[1;33m", // Bold yellow
  help: "\x1b[1;32m", // Bold green
  note: "\x1b[1;34m", // Bold blue
  dim: "\x1b[2m", // Dim
  path: "\x1b[1;36m", // Bold cyan
} as const;

export interface ReporterOptions {
  /** Enable ANSI colors in output */
  colors: boolean;
  /** Stop reporting after this many errors */
  maxErrors: number;
  /** Current working directory for relative paths */
  cwd: string;
}

/**
 * Rust-style diagnostic reporter
 *
 * Produces output like:
 * ```
 * error[SS001]: Missing required static property 'aggregateName'
 *   --> src/domain/todo.ts:5:1
 *    |
 *  5 | export class TodoAggregate {
 *    | ^^^^^^^^^^^^^^^^^^^^^^^^^^
 *    |
 *    = help: Add a static 'aggregateName' property:
 *    |
 *    |   static readonly aggregateName = 'todo';
 *    |
 * ```
 */
export class DiagnosticReporter {
  private errorCount = 0;
  private warningCount = 0;
  private infoCount = 0;
  private options: ReporterOptions;
  private fileCache = new Map<string, string[]>();

  constructor(options: Partial<ReporterOptions> = {}) {
    this.options = {
      colors: options.colors ?? true,
      maxErrors: options.maxErrors ?? 50,
      cwd: options.cwd ?? process.cwd(),
    };
  }

  /**
   * Color a string if colors are enabled
   */
  private color(text: string, colorCode: string): string {
    if (!this.options.colors) return text;
    return `${colorCode}${text}${colors.reset}`;
  }

  /**
   * Get source lines from a file (cached)
   */
  private getSourceLines(filePath: string): string[] {
    if (!this.fileCache.has(filePath)) {
      try {
        const content = readFileSync(filePath, "utf-8");
        this.fileCache.set(filePath, content.split("\n"));
      } catch {
        this.fileCache.set(filePath, []);
      }
    }
    return this.fileCache.get(filePath)!;
  }

  /**
   * Format the severity label
   */
  private formatSeverity(severity: DiagnosticSeverity): string {
    switch (severity) {
      case "error":
        return this.color("error", colors.error);
      case "warning":
        return this.color("warning", colors.warning);
      case "info":
        return this.color("info", colors.info);
    }
  }

  /**
   * Format the file location header
   */
  private formatLocation(diagnostic: Diagnostic): string {
    const relativePath = relative(this.options.cwd, diagnostic.location.filePath);
    const location = `${relativePath}:${diagnostic.location.line}:${diagnostic.location.column}`;
    return `  ${this.color("-->", colors.lineNumber)} ${location}`;
  }

  /**
   * Format the code snippet with underline carets
   */
  private formatCodeSnippet(diagnostic: Diagnostic): string {
    const lines = this.getSourceLines(diagnostic.location.filePath);
    const lineNum = diagnostic.location.line;
    const line = lines[lineNum - 1];

    if (!line) return "";

    // Calculate gutter width based on line number
    const gutterWidth = String(lineNum).length;
    const emptyGutter = " ".repeat(gutterWidth);

    // Build output
    const output: string[] = [];

    // Empty line with gutter
    output.push(`${emptyGutter} ${this.color("|", colors.lineNumber)}`);

    // Source line
    const lineNumStr = this.color(String(lineNum).padStart(gutterWidth), colors.lineNumber);
    output.push(`${lineNumStr} ${this.color("|", colors.lineNumber)} ${line}`);

    // Caret line
    const startCol = diagnostic.location.column - 1;
    const endCol = diagnostic.location.endColumn
      ? diagnostic.location.endColumn - 1
      : line.length;
    const caretLength = Math.max(1, endCol - startCol);

    const caretColor = diagnostic.severity === "error" ? colors.caret : colors.caretWarning;
    const carets = this.color("^".repeat(caretLength), caretColor);
    const caretLine = " ".repeat(startCol) + carets;

    output.push(`${emptyGutter} ${this.color("|", colors.lineNumber)} ${caretLine}`);

    return output.join("\n");
  }

  /**
   * Format the help suggestion
   */
  private formatHelp(diagnostic: Diagnostic): string {
    const suggestion =
      diagnostic.suggestion ??
      DiagnosticSuggestions[diagnostic.code as DiagnosticCodeType];

    if (!suggestion) return "";

    const gutterWidth = String(diagnostic.location.line).length;
    const emptyGutter = " ".repeat(gutterWidth);

    const lines = suggestion.split("\n");
    const output: string[] = [
      `${emptyGutter} ${this.color("|", colors.lineNumber)}`,
      `${emptyGutter} ${this.color("=", colors.note)} ${this.color("help", colors.help)}: ${lines[0]}`,
    ];

    for (let i = 1; i < lines.length; i++) {
      output.push(`${emptyGutter} ${this.color("|", colors.lineNumber)} ${lines[i]}`);
    }

    output.push(`${emptyGutter} ${this.color("|", colors.lineNumber)}`);

    return output.join("\n");
  }

  /**
   * Format a single diagnostic
   */
  format(diagnostic: Diagnostic): string {
    const severity = this.formatSeverity(diagnostic.severity);
    const code = this.color(`[${diagnostic.code}]`, colors.bold);
    const message = diagnostic.message;

    const header = `${severity}${code}: ${message}`;
    const location = this.formatLocation(diagnostic);
    const snippet = this.formatCodeSnippet(diagnostic);
    const help = this.formatHelp(diagnostic);

    return [header, location, snippet, help].filter(Boolean).join("\n");
  }

  /**
   * Report a single diagnostic
   */
  report(diagnostic: Diagnostic): void {
    // Track counts
    switch (diagnostic.severity) {
      case "error":
        this.errorCount++;
        break;
      case "warning":
        this.warningCount++;
        break;
      case "info":
        this.infoCount++;
        break;
    }

    // Check if we've hit the max errors
    if (diagnostic.severity === "error" && this.errorCount > this.options.maxErrors) {
      return;
    }

    console.log(this.format(diagnostic));
    console.log(); // Empty line between diagnostics
  }

  /**
   * Report multiple diagnostics
   */
  reportAll(diagnostics: Diagnostic[]): void {
    for (const diagnostic of diagnostics) {
      this.report(diagnostic);
    }
  }

  /**
   * Print the final summary
   */
  summary(): void {
    const parts: string[] = [];

    if (this.errorCount > 0) {
      const label = this.errorCount === 1 ? "error" : "errors";
      parts.push(this.color(`${this.errorCount} ${label}`, colors.error));
    }

    if (this.warningCount > 0) {
      const label = this.warningCount === 1 ? "warning" : "warnings";
      parts.push(this.color(`${this.warningCount} ${label}`, colors.warning));
    }

    if (parts.length === 0) {
      console.log(this.color("Compilation successful!", colors.help));
      return;
    }

    if (this.errorCount > this.options.maxErrors) {
      console.log(
        this.color(
          `... and ${this.errorCount - this.options.maxErrors} more errors`,
          colors.dim
        )
      );
    }

    const prefix = this.errorCount > 0 ? "error" : "warning";
    const message = `${prefix}: could not compile due to ${parts.join("; ")}`;

    console.log(
      this.color(message, this.errorCount > 0 ? colors.error : colors.warning)
    );
  }

  /**
   * Get current counts
   */
  getCounts(): { errors: number; warnings: number; info: number } {
    return {
      errors: this.errorCount,
      warnings: this.warningCount,
      info: this.infoCount,
    };
  }

  /**
   * Check if there were any errors
   */
  hasErrors(): boolean {
    return this.errorCount > 0;
  }
}

/**
 * Create a diagnostic object with location from a TypeScript node
 */
export function createDiagnostic(
  code: DiagnosticCodeType,
  severity: DiagnosticSeverity,
  message: string,
  filePath: string,
  line: number,
  column: number,
  endLine?: number,
  endColumn?: number,
  suggestion?: string
): Diagnostic {
  return {
    code,
    severity,
    message,
    location: {
      filePath,
      line,
      column,
      endLine,
      endColumn,
    },
    suggestion,
  };
}
