/**
 * Source location tracking.
 */

/**
 * A span in the source code.
 */
export interface Span {
  file: string;
  startLine: number;
  startCol: number;
  endLine: number;
  endCol: number;
}

/**
 * Creates a new span.
 */
export function createSpan(
  file: string,
  startLine: number,
  startCol: number,
  endLine: number,
  endCol: number
): Span {
  return {
    file,
    startLine,
    startCol,
    endLine,
    endCol,
  };
}

/**
 * Creates a span from a single position (start and end are the same).
 */
export function createPointSpan(file: string, line: number, col: number): Span {
  return createSpan(file, line, col, line, col);
}

/**
 * Formats a span for display.
 */
export function formatSpan(span: Span): string {
  if (span.startLine === span.endLine) {
    return `${span.file}:${span.startLine + 1}:${span.startCol + 1}`;
  }
  return `${span.file}:${span.startLine + 1}:${span.startCol + 1}-${span.endLine + 1}:${span.endCol + 1}`;
}
