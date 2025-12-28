/**
 * Diagnostic module - error types and source location tracking.
 */

export { type Span, createSpan, createPointSpan, formatSpan } from "./span.js";
export { CompilerError, ErrorCode } from "./error.js";
