/**
 * Parser for App registration configuration from index.ts.
 *
 * This module parses the FastAPI-style App registration to extract
 * access configuration for aggregates and orchestrators.
 *
 * @example
 * ```typescript
 * // index.ts
 * const app = new App();
 *
 * app.register(OrderAggregate, {
 *   access: 'private',
 *   roles: ['user'],
 *   methods: {
 *     create: { access: 'public' },
 *     cancel: { access: 'internal', roles: ['admin'] }
 *   }
 * });
 *
 * app.start();
 * ```
 */

import { readFile } from "fs/promises";
import { existsSync } from "fs";
import { join } from "path";
import ts from "typescript";
import { CompilerError } from "../../diagnostic/index.js";
import {
  type AccessLevel,
  type AppConfig,
  type AppMode,
  type EntityAccessConfig,
  type MethodAccessConfig,
  parseAccessLevel,
  parseAppMode,
  defaultAppConfig,
  defaultEntityAccessConfig,
  defaultMethodAccessConfig,
} from "../../ir/index.js";

/**
 * Parses App configuration from index.ts in the given source directory.
 *
 * Returns `undefined` if no index.ts exists or no App registration is found.
 */
export async function parseAppConfig(
  sourceDir: string
): Promise<AppConfig | undefined> {
  const indexPath = join(sourceDir, "index.ts");
  if (!existsSync(indexPath)) {
    return undefined;
  }

  const source = await readFile(indexPath, "utf-8");

  const sourceFile = ts.createSourceFile(
    indexPath,
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS
  );

  const extractor = new AppConfigExtractor(source, sourceFile);
  extractor.visit(sourceFile);

  // Return config if we found an App registration
  if (extractor.appVar || extractor.entities.size > 0) {
    return {
      mode: extractor.mode,
      apiVersioning: extractor.apiVersioning,
      entities: extractor.entities,
    };
  }

  return undefined;
}

/**
 * Extracts App configuration from TypeScript AST.
 */
class AppConfigExtractor {
  private source: string;
  private sourceFile: ts.SourceFile;

  entities = new Map<string, EntityAccessConfig>();
  appVar: string | undefined;
  mode: AppMode = "greenfield";
  apiVersioning = false;

  constructor(source: string, sourceFile: ts.SourceFile) {
    this.source = source;
    this.sourceFile = sourceFile;
  }

  private getText(node: ts.Node): string {
    return node.getText(this.sourceFile);
  }

  visit(node: ts.Node): void {
    if (ts.isVariableStatement(node)) {
      this.visitVariableStatement(node);
    } else if (ts.isExpressionStatement(node)) {
      this.visitExpressionStatement(node);
    }

    ts.forEachChild(node, (child) => this.visit(child));
  }

  /**
   * Look for: const app = new App()
   */
  private visitVariableStatement(node: ts.VariableStatement): void {
    for (const decl of node.declarationList.declarations) {
      if (!ts.isIdentifier(decl.name)) continue;
      if (!decl.initializer) continue;

      if (ts.isNewExpression(decl.initializer)) {
        const expr = decl.initializer.expression;
        if (ts.isIdentifier(expr) && expr.text === "App") {
          this.appVar = decl.name.text;

          // Parse constructor arguments: new App({ mode: '...', apiVersioning: ... })
          if (decl.initializer.arguments && decl.initializer.arguments.length > 0) {
            const arg = decl.initializer.arguments[0];
            if (arg && ts.isObjectLiteralExpression(arg)) {
              this.parseAppConfigObject(arg);
            }
          }
        }
      }
    }
  }

  /**
   * Parse the config object passed to App constructor.
   */
  private parseAppConfigObject(node: ts.ObjectLiteralExpression): void {
    for (const prop of node.properties) {
      if (!ts.isPropertyAssignment(prop)) continue;
      const keyName = ts.isIdentifier(prop.name)
        ? prop.name.text
        : this.getText(prop.name).replace(/^['"]|['"]$/g, "");

      if (keyName === "mode") {
        this.mode = this.parseMode(prop.initializer);
      } else if (keyName === "apiVersioning") {
        this.apiVersioning = this.parseBoolean(prop.initializer);
      }
    }
  }

  private parseMode(node: ts.Expression): AppMode {
    const text = this.getText(node).replace(/^['"]|['"]$/g, "");
    return parseAppMode(text) ?? "greenfield";
  }

  private parseBoolean(node: ts.Expression): boolean {
    return this.getText(node) === "true";
  }

  /**
   * Look for: app.register(EntityClass, { ... })
   */
  private visitExpressionStatement(node: ts.ExpressionStatement): void {
    if (!ts.isCallExpression(node.expression)) return;
    const call = node.expression;

    if (!ts.isPropertyAccessExpression(call.expression)) return;
    const propAccess = call.expression;

    if (!ts.isIdentifier(propAccess.expression)) return;
    const objName = propAccess.expression.text;
    const methodName = propAccess.name.text;

    // Check if this is app.register(...)
    if (this.appVar === objName && methodName === "register") {
      this.parseRegisterCall(call);
    }
  }

  /**
   * Parse: register(EntityClass, { access: '...', roles: [...], methods: { ... } })
   */
  private parseRegisterCall(call: ts.CallExpression): void {
    if (call.arguments.length < 1) return;

    // First argument: entity class name
    const firstArg = call.arguments[0]!;
    if (!ts.isIdentifier(firstArg)) return;
    const entityName = firstArg.text;

    // Second argument: config object (optional)
    let config: EntityAccessConfig = defaultEntityAccessConfig();
    if (call.arguments.length >= 2) {
      const secondArg = call.arguments[1];
      if (secondArg && ts.isObjectLiteralExpression(secondArg)) {
        config = this.parseEntityConfig(secondArg);
      }
    }

    this.entities.set(entityName, config);
  }

  /**
   * Parse: { access: '...', roles: [...], methods: { ... } }
   */
  private parseEntityConfig(node: ts.ObjectLiteralExpression): EntityAccessConfig {
    const config: EntityAccessConfig = {
      access: "internal",
      roles: [],
      methods: new Map(),
    };

    for (const prop of node.properties) {
      if (!ts.isPropertyAssignment(prop)) continue;
      const keyName = ts.isIdentifier(prop.name)
        ? prop.name.text
        : this.getText(prop.name).replace(/^['"]|['"]$/g, "");

      if (keyName === "access") {
        config.access = this.parseAccessLevel(prop.initializer);
      } else if (keyName === "roles") {
        config.roles = this.parseStringArray(prop.initializer);
      } else if (keyName === "methods" && ts.isObjectLiteralExpression(prop.initializer)) {
        config.methods = this.parseMethodsConfig(prop.initializer);
      }
    }

    return config;
  }

  /**
   * Parse: 'public' | 'internal' | 'private'
   */
  private parseAccessLevel(node: ts.Expression): AccessLevel {
    const text = this.getText(node).replace(/^['"]|['"]$/g, "");
    return parseAccessLevel(text) ?? "internal";
  }

  /**
   * Parse: ['role1', 'role2']
   */
  private parseStringArray(node: ts.Expression): string[] {
    const result: string[] = [];

    if (ts.isArrayLiteralExpression(node)) {
      for (const element of node.elements) {
        if (ts.isStringLiteral(element)) {
          result.push(element.text);
        }
      }
    }

    return result;
  }

  /**
   * Parse: { methodName: { access: '...', roles: [...] }, ... }
   */
  private parseMethodsConfig(
    node: ts.ObjectLiteralExpression
  ): Map<string, MethodAccessConfig> {
    const methods = new Map<string, MethodAccessConfig>();

    for (const prop of node.properties) {
      if (!ts.isPropertyAssignment(prop)) continue;
      const methodName = ts.isIdentifier(prop.name)
        ? prop.name.text
        : this.getText(prop.name).replace(/^['"]|['"]$/g, "");

      if (ts.isObjectLiteralExpression(prop.initializer)) {
        methods.set(methodName, this.parseMethodConfig(prop.initializer));
      }
    }

    return methods;
  }

  /**
   * Parse: { access: '...', roles: [...] }
   */
  private parseMethodConfig(node: ts.ObjectLiteralExpression): MethodAccessConfig {
    const config: MethodAccessConfig = {
      access: "internal",
      roles: [],
    };

    for (const prop of node.properties) {
      if (!ts.isPropertyAssignment(prop)) continue;
      const keyName = ts.isIdentifier(prop.name)
        ? prop.name.text
        : this.getText(prop.name).replace(/^['"]|['"]$/g, "");

      if (keyName === "access") {
        config.access = this.parseAccessLevel(prop.initializer);
      } else if (keyName === "roles") {
        config.roles = this.parseStringArray(prop.initializer);
      }
    }

    return config;
  }
}
