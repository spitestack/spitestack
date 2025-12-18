#!/usr/bin/env node

/**
 * Fixes TypeScript declaration files after build:
 * 1. Appends JS exports to the NAPI-generated index.d.ts
 * 2. Adds node types reference to js/types.d.ts for Buffer type
 */

import { readFileSync, writeFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const rootDir = join(__dirname, '..');

// Fix index.d.ts - add JS exports
const indexDts = join(rootDir, 'index.d.ts');
let indexContent = readFileSync(indexDts, 'utf8');

const jsExports = `
// Re-export projection system types and functions
export type {
  ColumnDef,
  ColumnType,
  ErrorStrategy,
  Projection,
  ProjectionEvent,
  ProjectionOptions,
  ProjectionTable,
  RowData,
  SchemaDefinition,
  SchemaToRow,
} from './js/types';

export { projection, createProjectionProxy, ProjectionRunner } from './js/index';
`;

if (!indexContent.includes('Re-export projection system')) {
  indexContent += jsExports;
  writeFileSync(indexDts, indexContent);
  console.log('Added JS exports to index.d.ts');
} else {
  console.log('JS exports already present in index.d.ts');
}

// Fix js/types.d.ts - add node types reference for Buffer
const typesDts = join(rootDir, 'js', 'types.d.ts');
let typesContent = readFileSync(typesDts, 'utf8');

const nodeRef = '/// <reference types="node" />\n';
if (!typesContent.includes('<reference types="node"')) {
  typesContent = nodeRef + typesContent;
  writeFileSync(typesDts, typesContent);
  console.log('Added node types reference to js/types.d.ts');
} else {
  console.log('Node types reference already present in js/types.d.ts');
}
