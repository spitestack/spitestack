/**
 * TypeScript frontend module.
 */

export { TypeScriptParser, createParser } from "./parser.js";
export { convertToIR, applyAccessConfig } from "./to-ir.js";
export { parseAppConfig } from "./app-parser.js";
export type {
  ParsedFile,
  ImportDecl,
  ImportSpecifier,
  TypeAlias,
  TypeNode,
  ObjectProperty,
  ClassDecl,
  PropertyDecl,
  MethodDecl,
  Visibility,
  Parameter,
  Statement,
  VarKind,
  SwitchCase,
  Expression,
} from "./ast.js";
