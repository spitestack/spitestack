/**
 * IR types and utilities tests.
 */

import { describe, expect, it } from "bun:test";
import {
  stringType,
  numberType,
  booleanType,
  arrayType,
  optionType,
  objectType,
  referenceType,
  createDomainIR,
  parseAppMode,
  parseAccessLevel,
} from "../src/ir/index.js";

describe("DomainType constructors", () => {
  it("creates string type", () => {
    expect(stringType()).toEqual({ kind: "string" });
  });

  it("creates number type", () => {
    expect(numberType()).toEqual({ kind: "number" });
  });

  it("creates boolean type", () => {
    expect(booleanType()).toEqual({ kind: "boolean" });
  });

  it("creates array type", () => {
    expect(arrayType(stringType())).toEqual({
      kind: "array",
      element: { kind: "string" },
    });
  });

  it("creates option type", () => {
    expect(optionType(numberType())).toEqual({
      kind: "option",
      inner: { kind: "number" },
    });
  });

  it("creates object type", () => {
    const obj = objectType([
      { name: "x", type: numberType(), optional: false },
      { name: "y", type: numberType(), optional: false },
    ]);
    expect(obj).toEqual({
      kind: "object",
      fields: [
        { name: "x", type: { kind: "number" }, optional: false },
        { name: "y", type: { kind: "number" }, optional: false },
      ],
    });
  });

  it("creates reference type", () => {
    expect(referenceType("UserId")).toEqual({
      kind: "reference",
      name: "UserId",
    });
  });

  it("creates nested types", () => {
    const nested = arrayType(optionType(objectType([{ name: "id", type: stringType(), optional: false }])));
    expect(nested).toEqual({
      kind: "array",
      element: {
        kind: "option",
        inner: {
          kind: "object",
          fields: [{ name: "id", type: { kind: "string" }, optional: false }],
        },
      },
    });
  });
});

describe("createDomainIR", () => {
  it("creates empty domain IR", () => {
    const domain = createDomainIR("./domain");
    expect(domain).toEqual({
      aggregates: [],
      projections: [],
      orchestrators: [],
      sourceDir: "./domain",
    });
  });
});

describe("parseAppMode", () => {
  it("parses 'greenfield' mode", () => {
    expect(parseAppMode("greenfield")).toBe("greenfield");
  });

  it("parses 'production' mode", () => {
    expect(parseAppMode("production")).toBe("production");
  });

  it("returns undefined for unknown values", () => {
    expect(parseAppMode("unknown")).toBe(undefined);
    expect(parseAppMode("")).toBe(undefined);
  });
});

describe("parseAccessLevel", () => {
  it("parses 'public' level", () => {
    expect(parseAccessLevel("public")).toBe("public");
  });

  it("parses 'internal' level", () => {
    expect(parseAccessLevel("internal")).toBe("internal");
  });

  it("parses 'private' level", () => {
    expect(parseAccessLevel("private")).toBe("private");
  });

  it("returns undefined for unknown values", () => {
    expect(parseAccessLevel("unknown")).toBe(undefined);
    expect(parseAccessLevel("")).toBe(undefined);
  });
});
