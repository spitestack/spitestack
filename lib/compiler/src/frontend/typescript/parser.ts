/**
 * TypeScript parser using the TypeScript compiler API.
 */

import ts from "typescript";
import { CompilerError } from "../../diagnostic/index.js";
import { createSpan, type Span } from "../../diagnostic/span.js";
import type {
  ClassDecl,
  Expression,
  ImportDecl,
  ImportSpecifier,
  MethodDecl,
  ObjectProperty,
  Parameter,
  ParsedFile,
  PropertyDecl,
  Statement,
  SwitchCase,
  TypeAlias,
  TypeNode,
  VarKind,
  Visibility,
} from "./ast.js";

/**
 * TypeScript parser class.
 */
export class TypeScriptParser {
  /**
   * Parses a TypeScript source file.
   */
  parse(source: string, path: string): ParsedFile {
    const sourceFile = ts.createSourceFile(
      path,
      source,
      ts.ScriptTarget.Latest,
      true,
      ts.ScriptKind.TS
    );

    const visitor = new Visitor(sourceFile, path);
    visitor.visitSourceFile(sourceFile);

    return {
      path,
      imports: visitor.imports,
      typeAliases: visitor.typeAliases,
      classes: visitor.classes,
    };
  }
}

/**
 * AST visitor that extracts declarations from TypeScript AST nodes.
 */
class Visitor {
  private sourceFile: ts.SourceFile;
  private path: string;

  imports: ImportDecl[] = [];
  typeAliases: TypeAlias[] = [];
  classes: ClassDecl[] = [];

  constructor(sourceFile: ts.SourceFile, path: string) {
    this.sourceFile = sourceFile;
    this.path = path;
  }

  /**
   * Creates a span from a TypeScript node.
   */
  private span(node: ts.Node): Span {
    const start = this.sourceFile.getLineAndCharacterOfPosition(node.getStart());
    const end = this.sourceFile.getLineAndCharacterOfPosition(node.getEnd());
    return createSpan(this.path, start.line, start.character, end.line, end.character);
  }

  /**
   * Gets the text of a node.
   */
  private nodeText(node: ts.Node): string {
    return node.getText(this.sourceFile);
  }

  /**
   * Visits the source file.
   */
  visitSourceFile(sourceFile: ts.SourceFile): void {
    for (const statement of sourceFile.statements) {
      if (ts.isImportDeclaration(statement)) {
        this.visitImport(statement);
      } else if (ts.isTypeAliasDeclaration(statement)) {
        const alias = this.visitTypeAlias(statement, false);
        if (alias) {
          this.typeAliases.push(alias);
        }
      } else if (ts.isClassDeclaration(statement)) {
        const cls = this.visitClass(statement, false);
        if (cls) {
          this.classes.push(cls);
        }
      } else if (ts.isExportDeclaration(statement) || this.hasExportModifier(statement)) {
        // Handle exported declarations
        if (ts.isTypeAliasDeclaration(statement)) {
          const alias = this.visitTypeAlias(statement, true);
          if (alias) {
            this.typeAliases.push(alias);
          }
        } else if (ts.isClassDeclaration(statement)) {
          const cls = this.visitClass(statement, true);
          if (cls) {
            this.classes.push(cls);
          }
        }
      }
    }
  }

  /**
   * Checks if a node has an export modifier.
   */
  private hasExportModifier(node: ts.Node): boolean {
    const modifiers = ts.canHaveModifiers(node) ? ts.getModifiers(node) : undefined;
    return modifiers?.some((m) => m.kind === ts.SyntaxKind.ExportKeyword) ?? false;
  }

  /**
   * Visits an import declaration.
   */
  private visitImport(node: ts.ImportDeclaration): void {
    const specifiers: ImportSpecifier[] = [];
    let source = "";

    // Get the module specifier
    if (ts.isStringLiteral(node.moduleSpecifier)) {
      source = node.moduleSpecifier.text;
    }

    // Get import specifiers
    const importClause = node.importClause;
    if (importClause) {
      // Default import
      if (importClause.name) {
        specifiers.push({ name: importClause.name.text });
      }

      // Named imports
      const namedBindings = importClause.namedBindings;
      if (namedBindings) {
        if (ts.isNamedImports(namedBindings)) {
          for (const element of namedBindings.elements) {
            const name = element.propertyName?.text ?? element.name.text;
            const alias = element.propertyName ? element.name.text : undefined;
            specifiers.push({ name, alias });
          }
        } else if (ts.isNamespaceImport(namedBindings)) {
          specifiers.push({ name: "*", alias: namedBindings.name.text });
        }
      }
    }

    this.imports.push({
      specifiers,
      source,
      span: this.span(node),
    });
  }

  /**
   * Visits a type alias declaration.
   */
  private visitTypeAlias(
    node: ts.TypeAliasDeclaration,
    exported: boolean
  ): TypeAlias | undefined {
    const name = node.name.text;
    const typeNode = this.visitTypeNode(node.type);

    return {
      name,
      typeNode,
      exported: exported || this.hasExportModifier(node),
      span: this.span(node),
    };
  }

  /**
   * Visits a type node.
   */
  private visitTypeNode(node: ts.TypeNode): TypeNode {
    // Predefined types (string, number, boolean, etc.)
    if (ts.isTypeReferenceNode(node)) {
      const name = this.nodeText(node.typeName);
      if (["string", "number", "boolean", "void", "null", "undefined"].includes(name)) {
        return { kind: "primitive", name };
      }
      return { kind: "reference", name };
    }

    if (node.kind === ts.SyntaxKind.StringKeyword) {
      return { kind: "primitive", name: "string" };
    }
    if (node.kind === ts.SyntaxKind.NumberKeyword) {
      return { kind: "primitive", name: "number" };
    }
    if (node.kind === ts.SyntaxKind.BooleanKeyword) {
      return { kind: "primitive", name: "boolean" };
    }
    if (node.kind === ts.SyntaxKind.VoidKeyword) {
      return { kind: "primitive", name: "void" };
    }
    if (node.kind === ts.SyntaxKind.NullKeyword) {
      return { kind: "primitive", name: "null" };
    }
    if (node.kind === ts.SyntaxKind.UndefinedKeyword) {
      return { kind: "primitive", name: "undefined" };
    }
    if (node.kind === ts.SyntaxKind.AnyKeyword) {
      return { kind: "primitive", name: "any" };
    }

    // Array types
    if (ts.isArrayTypeNode(node)) {
      const element = this.visitTypeNode(node.elementType);
      return { kind: "array", element };
    }

    // Union types
    if (ts.isUnionTypeNode(node)) {
      const variants: TypeNode[] = [];
      let hasUndefined = false;

      for (const type of node.types) {
        if (type.kind === ts.SyntaxKind.UndefinedKeyword) {
          hasUndefined = true;
          continue;
        }
        const variant = this.visitTypeNode(type);
        // Flatten nested unions
        if (variant.kind === "union") {
          for (const nested of variant.variants) {
            if (nested.kind === "primitive" && nested.name === "undefined") {
              hasUndefined = true;
            } else {
              variants.push(nested);
            }
          }
        } else {
          if (variant.kind === "primitive" && variant.name === "undefined") {
            hasUndefined = true;
          } else {
            variants.push(variant);
          }
        }
      }

      // T | undefined becomes Optional<T>
      if (hasUndefined && variants.length === 1) {
        return { kind: "optional", inner: variants[0]! };
      }

      return { kind: "union", variants };
    }

    // Object/Type literal
    if (ts.isTypeLiteralNode(node)) {
      const properties: ObjectProperty[] = [];
      let indexSignature:
        | { keyName: string; keyType: TypeNode; valueType: TypeNode }
        | undefined;

      for (const member of node.members) {
        if (ts.isPropertySignature(member) && member.name) {
          const name = ts.isIdentifier(member.name)
            ? member.name.text
            : this.nodeText(member.name);
          const typeNode = member.type
            ? this.visitTypeNode(member.type)
            : { kind: "primitive" as const, name: "unknown" };
          const optional = !!member.questionToken;
          properties.push({ name, typeNode, optional });
        } else if (ts.isIndexSignatureDeclaration(member)) {
          // Parse { [key: KeyType]: ValueType }
          const param = member.parameters[0];
          const keyName = param?.name && ts.isIdentifier(param.name) ? param.name.text : "key";
          const keyType = param?.type
            ? this.visitTypeNode(param.type)
            : { kind: "primitive" as const, name: "string" };
          const valueType = member.type
            ? this.visitTypeNode(member.type)
            : { kind: "primitive" as const, name: "unknown" };
          indexSignature = { keyName, keyType, valueType };
        }
      }

      // If we have an index signature and no other properties, return IndexSignature
      if (indexSignature && properties.length === 0) {
        return {
          kind: "indexSignature",
          keyName: indexSignature.keyName,
          keyType: indexSignature.keyType,
          valueType: indexSignature.valueType,
        };
      }

      return { kind: "objectLiteral", properties };
    }

    // Literal types (string literals like "Created")
    if (ts.isLiteralTypeNode(node)) {
      const text = this.nodeText(node);
      return { kind: "primitive", name: text };
    }

    // Parenthesized types
    if (ts.isParenthesizedTypeNode(node)) {
      return this.visitTypeNode(node.type);
    }

    // Fallback
    return { kind: "primitive", name: this.nodeText(node) };
  }

  /**
   * Visits a class declaration.
   */
  private visitClass(node: ts.ClassDeclaration, exported: boolean): ClassDecl | undefined {
    const name = node.name?.text;
    if (!name) {
      return undefined;
    }

    const properties: PropertyDecl[] = [];
    const methods: MethodDecl[] = [];

    for (const member of node.members) {
      if (ts.isPropertyDeclaration(member)) {
        const prop = this.visitPropertyDecl(member);
        if (prop) {
          properties.push(prop);
        }
      } else if (ts.isMethodDeclaration(member)) {
        const method = this.visitMethodDecl(member);
        if (method) {
          methods.push(method);
        }
      } else if (ts.isGetAccessor(member) || ts.isSetAccessor(member)) {
        const method = this.visitAccessorDecl(member);
        if (method) {
          methods.push(method);
        }
      }
    }

    return {
      name,
      properties,
      methods,
      exported: exported || this.hasExportModifier(node),
      span: this.span(node),
    };
  }

  /**
   * Visits a property declaration.
   */
  private visitPropertyDecl(node: ts.PropertyDeclaration): PropertyDecl | undefined {
    const name = ts.isIdentifier(node.name) ? node.name.text : this.nodeText(node.name);

    const modifiers = ts.canHaveModifiers(node) ? ts.getModifiers(node) : undefined;
    const isStatic = modifiers?.some((m) => m.kind === ts.SyntaxKind.StaticKeyword) ?? false;
    const isReadonly = modifiers?.some((m) => m.kind === ts.SyntaxKind.ReadonlyKeyword) ?? false;

    const typeNode = node.type ? this.visitTypeNode(node.type) : undefined;
    const initializer = node.initializer ? this.nodeText(node.initializer) : undefined;

    return {
      name,
      typeNode,
      isStatic,
      isReadonly,
      initializer,
      span: this.span(node),
    };
  }

  /**
   * Visits a method declaration.
   */
  private visitMethodDecl(node: ts.MethodDeclaration): MethodDecl | undefined {
    const name = ts.isIdentifier(node.name) ? node.name.text : this.nodeText(node.name);

    const modifiers = ts.canHaveModifiers(node) ? ts.getModifiers(node) : undefined;
    const isAsync = modifiers?.some((m) => m.kind === ts.SyntaxKind.AsyncKeyword) ?? false;

    let visibility: Visibility = "public";
    for (const mod of modifiers ?? []) {
      if (mod.kind === ts.SyntaxKind.PrivateKeyword) {
        visibility = "private";
      } else if (mod.kind === ts.SyntaxKind.ProtectedKeyword) {
        visibility = "protected";
      }
    }

    const parameters = node.parameters.map((p) => this.visitParameter(p));
    const returnType = node.type ? this.visitTypeNode(node.type) : undefined;

    const body: Statement[] = [];
    let rawBody: string | undefined;

    if (node.body) {
      rawBody = this.nodeText(node.body);
      for (const stmt of node.body.statements) {
        const s = this.visitStatement(stmt);
        if (s) {
          body.push(s);
        }
      }
    }

    return {
      name,
      parameters,
      returnType,
      body,
      rawBody,
      isAsync,
      visibility,
      span: this.span(node),
    };
  }

  /**
   * Visits an accessor declaration (getter/setter).
   */
  private visitAccessorDecl(
    node: ts.GetAccessorDeclaration | ts.SetAccessorDeclaration
  ): MethodDecl | undefined {
    const baseName = ts.isIdentifier(node.name) ? node.name.text : this.nodeText(node.name);
    const prefix = ts.isGetAccessor(node) ? "get_" : "set_";
    const name = prefix + baseName;

    const modifiers = ts.canHaveModifiers(node) ? ts.getModifiers(node) : undefined;
    const isAsync = false; // Accessors can't be async

    let visibility: Visibility = "public";
    for (const mod of modifiers ?? []) {
      if (mod.kind === ts.SyntaxKind.PrivateKeyword) {
        visibility = "private";
      } else if (mod.kind === ts.SyntaxKind.ProtectedKeyword) {
        visibility = "protected";
      }
    }

    const parameters = node.parameters.map((p) => this.visitParameter(p));
    const returnType = ts.isGetAccessor(node) && node.type ? this.visitTypeNode(node.type) : undefined;

    const body: Statement[] = [];
    let rawBody: string | undefined;

    if (node.body) {
      rawBody = this.nodeText(node.body);
      for (const stmt of node.body.statements) {
        const s = this.visitStatement(stmt);
        if (s) {
          body.push(s);
        }
      }
    }

    return {
      name,
      parameters,
      returnType,
      body,
      rawBody,
      isAsync,
      visibility,
      span: this.span(node),
    };
  }

  /**
   * Visits a parameter.
   */
  private visitParameter(node: ts.ParameterDeclaration): Parameter {
    const name = ts.isIdentifier(node.name) ? node.name.text : this.nodeText(node.name);
    const typeNode = node.type ? this.visitTypeNode(node.type) : undefined;
    const optional = !!node.questionToken;
    const defaultValue = node.initializer ? this.nodeText(node.initializer) : undefined;

    return { name, typeNode, optional, defaultValue };
  }

  /**
   * Visits a statement.
   */
  private visitStatement(node: ts.Statement): Statement | undefined {
    if (ts.isIfStatement(node)) {
      return this.visitIfStatement(node);
    }
    if (ts.isSwitchStatement(node)) {
      return this.visitSwitchStatement(node);
    }
    if (ts.isReturnStatement(node)) {
      return this.visitReturnStatement(node);
    }
    if (ts.isThrowStatement(node)) {
      return this.visitThrowStatement(node);
    }
    if (ts.isExpressionStatement(node)) {
      return this.visitExpressionStatement(node);
    }
    if (ts.isVariableStatement(node)) {
      return this.visitVariableStatement(node);
    }
    if (ts.isBlock(node)) {
      const statements: Statement[] = [];
      for (const stmt of node.statements) {
        const s = this.visitStatement(stmt);
        if (s) {
          statements.push(s);
        }
      }
      return { kind: "block", statements, span: this.span(node) };
    }
    return undefined;
  }

  /**
   * Visits an if statement.
   */
  private visitIfStatement(node: ts.IfStatement): Statement {
    const condition = this.visitExpression(node.expression);
    const thenBranch: Statement[] = [];
    let elseBranch: Statement[] | undefined;

    if (ts.isBlock(node.thenStatement)) {
      for (const stmt of node.thenStatement.statements) {
        const s = this.visitStatement(stmt);
        if (s) {
          thenBranch.push(s);
        }
      }
    } else {
      const s = this.visitStatement(node.thenStatement);
      if (s) {
        thenBranch.push(s);
      }
    }

    if (node.elseStatement) {
      elseBranch = [];
      if (ts.isBlock(node.elseStatement)) {
        for (const stmt of node.elseStatement.statements) {
          const s = this.visitStatement(stmt);
          if (s) {
            elseBranch.push(s);
          }
        }
      } else {
        const s = this.visitStatement(node.elseStatement);
        if (s) {
          elseBranch.push(s);
        }
      }
    }

    return {
      kind: "if",
      condition,
      thenBranch,
      elseBranch,
      span: this.span(node),
    };
  }

  /**
   * Visits a switch statement.
   */
  private visitSwitchStatement(node: ts.SwitchStatement): Statement {
    const discriminant = this.visitExpression(node.expression);
    const cases: SwitchCase[] = [];

    for (const clause of node.caseBlock.clauses) {
      const test = ts.isCaseClause(clause) ? this.visitExpression(clause.expression) : undefined;
      const consequent: Statement[] = [];
      for (const stmt of clause.statements) {
        const s = this.visitStatement(stmt);
        if (s) {
          consequent.push(s);
        }
      }
      cases.push({ test, consequent });
    }

    return { kind: "switch", discriminant, cases, span: this.span(node) };
  }

  /**
   * Visits a return statement.
   */
  private visitReturnStatement(node: ts.ReturnStatement): Statement {
    const value = node.expression ? this.visitExpression(node.expression) : undefined;
    return { kind: "return", value, span: this.span(node) };
  }

  /**
   * Visits a throw statement.
   */
  private visitThrowStatement(node: ts.ThrowStatement): Statement {
    const argument = this.visitExpression(node.expression);
    return { kind: "throw", argument, span: this.span(node) };
  }

  /**
   * Visits an expression statement.
   */
  private visitExpressionStatement(node: ts.ExpressionStatement): Statement {
    const expression = this.visitExpression(node.expression);
    return { kind: "expression", expression, span: this.span(node) };
  }

  /**
   * Visits a variable statement.
   */
  private visitVariableStatement(node: ts.VariableStatement): Statement | undefined {
    const decl = node.declarationList.declarations[0];
    if (!decl) {
      return undefined;
    }

    const name = ts.isIdentifier(decl.name) ? decl.name.text : this.nodeText(decl.name);
    const typeNode = decl.type ? this.visitTypeNode(decl.type) : undefined;
    const initializer = decl.initializer ? this.visitExpression(decl.initializer) : undefined;

    let varKind: VarKind = "let";
    if (node.declarationList.flags & ts.NodeFlags.Const) {
      varKind = "const";
    } else if (node.declarationList.flags & ts.NodeFlags.Let) {
      varKind = "let";
    } else {
      varKind = "var";
    }

    return {
      kind: "variableDecl",
      varKind,
      name,
      typeNode,
      initializer,
      span: this.span(node),
    };
  }

  /**
   * Visits an expression.
   */
  private visitExpression(node: ts.Expression): Expression {
    const span = this.span(node);

    // Identifier
    if (ts.isIdentifier(node)) {
      return { kind: "identifier", name: node.text, span };
    }

    // String literal
    if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
      return { kind: "stringLiteral", value: node.text, span };
    }

    // Number literal
    if (ts.isNumericLiteral(node)) {
      return { kind: "numberLiteral", value: parseFloat(node.text), span };
    }

    // Boolean literals
    if (node.kind === ts.SyntaxKind.TrueKeyword) {
      return { kind: "booleanLiteral", value: true, span };
    }
    if (node.kind === ts.SyntaxKind.FalseKeyword) {
      return { kind: "booleanLiteral", value: false, span };
    }

    // Null literal
    if (node.kind === ts.SyntaxKind.NullKeyword) {
      return { kind: "nullLiteral", span };
    }

    // This
    if (node.kind === ts.SyntaxKind.ThisKeyword) {
      return { kind: "this", span };
    }

    // Array literal
    if (ts.isArrayLiteralExpression(node)) {
      const elements = node.elements.map((e) => this.visitExpression(e));
      return { kind: "arrayLiteral", elements, span };
    }

    // Object literal
    if (ts.isObjectLiteralExpression(node)) {
      const properties: Array<{ key: string; value: Expression }> = [];
      for (const prop of node.properties) {
        if (ts.isPropertyAssignment(prop)) {
          const key = ts.isIdentifier(prop.name)
            ? prop.name.text
            : this.nodeText(prop.name);
          const value = this.visitExpression(prop.initializer);
          properties.push({ key, value });
        } else if (ts.isShorthandPropertyAssignment(prop)) {
          const key = prop.name.text;
          properties.push({
            key,
            value: { kind: "identifier", name: key, span: this.span(prop) },
          });
        }
      }
      return { kind: "objectLiteral", properties, span };
    }

    // Property access
    if (ts.isPropertyAccessExpression(node)) {
      const object = this.visitExpression(node.expression);
      const property = node.name.text;
      return { kind: "memberAccess", object, property, span };
    }

    // Element access (obj["key"] or obj[0])
    if (ts.isElementAccessExpression(node)) {
      const object = this.visitExpression(node.expression);
      const property = this.nodeText(node.argumentExpression);
      return { kind: "memberAccess", object, property, span };
    }

    // Call expression
    if (ts.isCallExpression(node)) {
      const callee = this.visitExpression(node.expression);
      const args = node.arguments.map((a) => this.visitExpression(a));
      return { kind: "call", callee, arguments: args, span };
    }

    // New expression
    if (ts.isNewExpression(node)) {
      const callee = this.visitExpression(node.expression);
      const args = node.arguments?.map((a) => this.visitExpression(a)) ?? [];
      return { kind: "new", callee, arguments: args, span };
    }

    // Binary expression
    if (ts.isBinaryExpression(node)) {
      const left = this.visitExpression(node.left);
      const right = this.visitExpression(node.right);
      const operator = this.binaryOperatorToString(node.operatorToken.kind);
      return { kind: "binary", left, operator, right, span };
    }

    // Prefix unary expression
    if (ts.isPrefixUnaryExpression(node)) {
      const operator = this.unaryOperatorToString(node.operator);
      const argument = this.visitExpression(node.operand);
      return { kind: "unary", operator, argument, prefix: true, span };
    }

    // Await expression
    if (ts.isAwaitExpression(node)) {
      const argument = this.visitExpression(node.expression);
      return { kind: "await", argument, span };
    }

    // Parenthesized expression
    if (ts.isParenthesizedExpression(node)) {
      return this.visitExpression(node.expression);
    }

    // Spread element
    if (ts.isSpreadElement(node)) {
      const argument = this.visitExpression(node.expression);
      return { kind: "spread", argument, span };
    }

    // Fallback: treat as identifier
    return { kind: "identifier", name: this.nodeText(node), span };
  }

  /**
   * Converts a binary operator kind to its string representation.
   */
  private binaryOperatorToString(kind: ts.BinaryOperator): string {
    switch (kind) {
      case ts.SyntaxKind.PlusToken:
        return "+";
      case ts.SyntaxKind.MinusToken:
        return "-";
      case ts.SyntaxKind.AsteriskToken:
        return "*";
      case ts.SyntaxKind.SlashToken:
        return "/";
      case ts.SyntaxKind.PercentToken:
        return "%";
      case ts.SyntaxKind.EqualsEqualsToken:
        return "==";
      case ts.SyntaxKind.EqualsEqualsEqualsToken:
        return "===";
      case ts.SyntaxKind.ExclamationEqualsToken:
        return "!=";
      case ts.SyntaxKind.ExclamationEqualsEqualsToken:
        return "!==";
      case ts.SyntaxKind.LessThanToken:
        return "<";
      case ts.SyntaxKind.LessThanEqualsToken:
        return "<=";
      case ts.SyntaxKind.GreaterThanToken:
        return ">";
      case ts.SyntaxKind.GreaterThanEqualsToken:
        return ">=";
      case ts.SyntaxKind.AmpersandAmpersandToken:
        return "&&";
      case ts.SyntaxKind.BarBarToken:
        return "||";
      case ts.SyntaxKind.EqualsToken:
        return "=";
      default:
        return "??";
    }
  }

  /**
   * Converts a unary operator kind to its string representation.
   */
  private unaryOperatorToString(kind: ts.PrefixUnaryOperator): string {
    switch (kind) {
      case ts.SyntaxKind.ExclamationToken:
        return "!";
      case ts.SyntaxKind.MinusToken:
        return "-";
      case ts.SyntaxKind.PlusToken:
        return "+";
      case ts.SyntaxKind.TildeToken:
        return "~";
      default:
        return "??";
    }
  }
}

/**
 * Creates a new TypeScript parser.
 */
export function createParser(): TypeScriptParser {
  return new TypeScriptParser();
}
