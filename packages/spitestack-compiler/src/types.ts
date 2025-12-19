import type ts from "typescript";

// ============================================================================
// Configuration Types
// ============================================================================

export interface CompilerConfig {
  /** Directory containing aggregate classes */
  domainDir: string;
  /** Output directory for generated code */
  outDir: string;
  /** Glob patterns to include */
  include: string[];
  /** Glob patterns to exclude */
  exclude: string[];
  /** Code generation options */
  generate: {
    handlers: boolean;
    validators: boolean;
    wiring: boolean;
  };
  /** Error reporting options */
  diagnostics: {
    colors: boolean;
    maxErrors: number;
  };
}

export type PartialConfig = Partial<Omit<CompilerConfig, "generate" | "diagnostics">> & {
  generate?: Partial<CompilerConfig["generate"]>;
  diagnostics?: Partial<CompilerConfig["diagnostics"]>;
};

// ============================================================================
// Analysis Types
// ============================================================================

export interface DiscoveredFile {
  path: string;
  relativePath: string;
}

export interface ParsedFile {
  filePath: string;
  sourceFile: ts.SourceFile;
  classes: ts.ClassDeclaration[];
  typeAliases: ts.TypeAliasDeclaration[];
}

export interface TypeInfo {
  kind:
    | "string"
    | "number"
    | "boolean"
    | "null"
    | "undefined"
    | "object"
    | "array"
    | "union"
    | "literal"
    | "unknown";
  properties?: Record<string, TypeInfo>;
  elementType?: TypeInfo;
  types?: TypeInfo[];
  literalValue?: string | number | boolean;
}

export interface ParameterInfo {
  name: string;
  type: TypeInfo;
  optional: boolean;
  node: ts.ParameterDeclaration;
}

export interface CommandInfo {
  methodName: string;
  parameters: ParameterInfo[];
  node: ts.MethodDeclaration;
}

export interface EventVariant {
  type: string; // discriminant value
  properties: Record<string, TypeInfo>;
}

export interface EventTypeInfo {
  typeName: string;
  variants: EventVariant[];
  node: ts.TypeAliasDeclaration | null;
}

export interface StateTypeInfo {
  typeName: string;
  type: TypeInfo;
  node: ts.TypeAliasDeclaration | null;
}

export interface AggregateAnalysis {
  className: string;
  aggregateName: string;
  filePath: string;
  relativePath: string;
  stateType: StateTypeInfo;
  eventType: EventTypeInfo;
  commands: CommandInfo[];
  node: ts.ClassDeclaration;
}

// ============================================================================
// Diagnostic Types
// ============================================================================

export type DiagnosticSeverity = "error" | "warning" | "info";

export interface DiagnosticLocation {
  filePath: string;
  line: number;
  column: number;
  endLine?: number;
  endColumn?: number;
}

export interface DiagnosticRelatedInfo {
  message: string;
  location: DiagnosticLocation;
}

export interface Diagnostic {
  code: string;
  severity: DiagnosticSeverity;
  message: string;
  location: DiagnosticLocation;
  suggestion?: string;
  relatedInfo?: DiagnosticRelatedInfo[];
}

// ============================================================================
// Validation Types
// ============================================================================

export interface ValidationResult {
  valid: boolean;
  diagnostics: Diagnostic[];
  aggregates: AggregateAnalysis[];
}

// ============================================================================
// Generation Types
// ============================================================================

export interface GeneratedFile {
  path: string;
  content: string;
}

export interface GenerationResult {
  handlers: GeneratedFile[];
  validators: GeneratedFile[];
  wiring: GeneratedFile | null;
  index: GeneratedFile | null;
}
