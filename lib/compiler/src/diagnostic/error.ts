/**
 * Compiler error types.
 */

/**
 * Error codes for compiler errors.
 */
export const ErrorCode = {
  // IO Errors
  IO_READ_ERROR: "spitestack::io::read_error",

  // Parse Errors
  PARSE_INIT_FAILED: "spitestack::parse::init_failed",
  PARSE_FAILED: "spitestack::parse::parse_failed",
  SYNTAX_ERROR: "spitestack::parse::syntax_error",

  // Structure Errors
  MISSING_MEMBER: "spitestack::structure::missing_member",
  INVALID_EVENT_TYPE: "spitestack::structure::invalid_event_type",
  INVALID_STATE_TYPE: "spitestack::structure::invalid_state_type",
  INVALID_PROJECTION: "spitestack::structure::invalid_projection",

  // Purity Errors
  FORBIDDEN_CALL: "spitestack::purity::forbidden_call",
  FORBIDDEN_AWAIT: "spitestack::purity::forbidden_await",
  FORBIDDEN_IMPORT: "spitestack::purity::forbidden_import",

  // Type Errors
  NOT_SERIALIZABLE: "spitestack::types::not_serializable",
  UNKNOWN_TYPE_REFERENCE: "spitestack::types::unknown_reference",

  // Code Generation Errors
  CODEGEN_FAILED: "spitestack::codegen::generation_failed",
  FORMAT_FAILED: "spitestack::codegen::format_failed",

  // Schema Evolution Errors
  BREAKING_SCHEMA_CHANGE: "spitestack::schema::breaking_change",
  EVENT_REMOVED: "spitestack::schema::event_removed",
  LOCK_FILE_REQUIRED: "spitestack::schema::lock_file_required",

  // Analysis Errors
  NO_AGGREGATES: "spitestack::analysis::no_aggregates",
  DUPLICATE_AGGREGATE: "spitestack::analysis::duplicate_aggregate",
  CIRCULAR_IMPORT: "spitestack::analysis::circular_import",

  // Frontend Errors
  UNSUPPORTED_LANGUAGE: "spitestack::frontend::unsupported_language",
} as const;

export type ErrorCode = (typeof ErrorCode)[keyof typeof ErrorCode];

/**
 * Base class for compiler errors.
 */
export class CompilerError extends Error {
  readonly code: ErrorCode;
  readonly help?: string;
  readonly file?: string;
  readonly line?: number;
  readonly column?: number;

  constructor(
    message: string,
    code: ErrorCode,
    options?: {
      help?: string;
      file?: string;
      line?: number;
      column?: number;
      cause?: Error;
    }
  ) {
    super(message, { cause: options?.cause });
    this.name = "CompilerError";
    this.code = code;
    this.help = options?.help;
    this.file = options?.file;
    this.line = options?.line;
    this.column = options?.column;
  }

  /**
   * Formats the error for display.
   */
  format(): string {
    const parts: string[] = [];

    // Error code and message
    parts.push(`error[${this.code}]: ${this.message}`);

    // Location
    if (this.file) {
      const location = this.line !== undefined ? `${this.file}:${this.line + 1}:${(this.column ?? 0) + 1}` : this.file;
      parts.push(`  --> ${location}`);
    }

    // Help text
    if (this.help) {
      parts.push("");
      parts.push(`  help: ${this.help}`);
    }

    return parts.join("\n");
  }

  // =========================================================================
  // Factory methods for specific error types
  // =========================================================================

  static io(path: string, message: string): CompilerError {
    return new CompilerError(`Failed to read file '${path}': ${message}`, ErrorCode.IO_READ_ERROR, {
      file: path,
    });
  }

  static parserInitFailed(): CompilerError {
    return new CompilerError("Failed to initialize parser", ErrorCode.PARSE_INIT_FAILED);
  }

  static parseFailed(path: string): CompilerError {
    return new CompilerError(`Failed to parse file: ${path}`, ErrorCode.PARSE_FAILED, {
      file: path,
    });
  }

  static syntaxError(message: string, file: string, line: number, column: number): CompilerError {
    return new CompilerError(`Syntax error: ${message}`, ErrorCode.SYNTAX_ERROR, {
      file,
      line,
      column,
    });
  }

  static missingMember(member: string, aggregate: string): CompilerError {
    return new CompilerError(`Aggregate '${aggregate}' is missing required member: ${member}`, ErrorCode.MISSING_MEMBER, {
      help: "Aggregates must have: initialState (static), state, events, emit(), apply()",
    });
  }

  static invalidEventType(typeName: string): CompilerError {
    return new CompilerError(
      `Event type '${typeName}' must be a discriminated union with 'type' field`,
      ErrorCode.INVALID_EVENT_TYPE,
      {
        help: "Events should be defined as: type FooEvent = { type: 'Created', ... } | { type: 'Updated', ... }",
      }
    );
  }

  static invalidStateType(typeName: string): CompilerError {
    return new CompilerError(`State type '${typeName}' must be an object type`, ErrorCode.INVALID_STATE_TYPE);
  }

  static invalidProjection(name: string, reason: string): CompilerError {
    return new CompilerError(`Invalid projection '${name}': ${reason}`, ErrorCode.INVALID_PROJECTION, {
      help: "Projections must have a build(event) method and a state property with type annotation",
    });
  }

  static forbiddenCall(name: string, file: string, line: number): CompilerError {
    return new CompilerError(`Domain logic cannot use '${name}' - it has side effects`, ErrorCode.FORBIDDEN_CALL, {
      file,
      line,
      help: "Domain logic must be pure. Move side effects to adapters.",
    });
  }

  static forbiddenAwait(file: string, line: number): CompilerError {
    return new CompilerError("Domain logic cannot use 'await' in aggregates", ErrorCode.FORBIDDEN_AWAIT, {
      file,
      line,
      help: "Async operations are only allowed in orchestrators. Move async logic to adapters.",
    });
  }

  static forbiddenImport(packageName: string, file: string, line: number): CompilerError {
    return new CompilerError(`Cannot import external package '${packageName}'`, ErrorCode.FORBIDDEN_IMPORT, {
      file,
      line,
      help: "Only relative imports within the domain directory are allowed.",
    });
  }

  static notSerializable(typeDesc: string): CompilerError {
    return new CompilerError(`Cannot serialize type '${typeDesc}' to JSON`, ErrorCode.NOT_SERIALIZABLE, {
      help: "Event and state types must be JSON-serializable. Avoid functions, symbols, etc.",
    });
  }

  static unknownTypeReference(name: string): CompilerError {
    return new CompilerError(`Unknown type reference: ${name}`, ErrorCode.UNKNOWN_TYPE_REFERENCE);
  }

  static codegenFailed(message: string): CompilerError {
    return new CompilerError(`Failed to generate code: ${message}`, ErrorCode.CODEGEN_FAILED);
  }

  static formatFailed(message: string): CompilerError {
    return new CompilerError(`Failed to format generated code: ${message}`, ErrorCode.FORMAT_FAILED);
  }

  static breakingSchemaChange(aggregate: string, event: string, changes: string): CompilerError {
    return new CompilerError(
      `Breaking schema change detected in ${aggregate}.${event}`,
      ErrorCode.BREAKING_SCHEMA_CHANGE,
      {
        help: `Breaking changes are not allowed in production mode.

Options:
1. Create a new event type (e.g., '${event}V2') with the new schema
2. Switch to greenfield mode: new App({ mode: 'greenfield' })
3. Run \`spitestack schema reset --i-know-what-im-doing\` (WARNING: existing events won't replay correctly)

Changes detected:
${changes}

Learn more: https://spitestack.dev/docs/event-evolution`,
      }
    );
  }

  static eventRemoved(aggregate: string, event: string): CompilerError {
    return new CompilerError(`Event '${aggregate}.${event}' was removed`, ErrorCode.EVENT_REMOVED, {
      help: `Removing events is a breaking change in production mode.
If this event is no longer needed, you can:
1. Keep the event type but deprecate it
2. Switch to greenfield mode for development
3. Run \`spitestack schema reset --i-know-what-im-doing\``,
    });
  }

  static lockFileRequired(): CompilerError {
    return new CompilerError("Lock file generation required for production mode", ErrorCode.LOCK_FILE_REQUIRED, {
      help: `Run \`spitestack schema sync\` to generate the initial events.lock.json file.
This captures your current event schemas and enables safe evolution.`,
    });
  }

  static noAggregates(): CompilerError {
    return new CompilerError("No aggregates found in domain directory", ErrorCode.NO_AGGREGATES, {
      help: "Create aggregate files in the domain directory following the pattern: domain/Todo/aggregate.ts",
    });
  }

  static duplicateAggregate(name: string, first: string, second: string): CompilerError {
    return new CompilerError(
      `Duplicate aggregate name: ${name}`,
      ErrorCode.DUPLICATE_AGGREGATE,
      {
        help: `First defined in: ${first}\nAlso defined in: ${second}`,
      }
    );
  }

  static circularImport(cycle: string[]): CompilerError {
    return new CompilerError("Circular import detected", ErrorCode.CIRCULAR_IMPORT, {
      help: `Break the circular dependency by restructuring the imports.\nCycle: ${cycle.join(" -> ")}`,
    });
  }

  static unsupportedLanguage(language: string): CompilerError {
    return new CompilerError(`Unsupported language: ${language}`, ErrorCode.UNSUPPORTED_LANGUAGE);
  }
}
