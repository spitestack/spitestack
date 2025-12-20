export { discoverFiles } from "./discover";
export { createProgram, parseFiles, parseFile, getLineAndColumn, getEndLineAndColumn } from "./parse";
export { analyzeFiles, analyzeAggregate, extractTypeInfo } from "./analyze";
export { validate } from "./validate";
export {
  generate,
  generateCode,
  writeGeneratedFiles,
  checkSchemaLock,
  checkApiLock,
  SchemaEvolutionError,
  ApiEvolutionError,
  type SchemaLockResult,
  type ApiLockResult,
  type GenerateOptions,
  type GenerateResult,
  type CheckSchemaLockOptions,
} from "./generate";
