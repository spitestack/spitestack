/**
 * Mode determines how schema evolution and API contracts are handled.
 * - "greenfield": No lock files, schemas can change freely. Use during early development.
 * - "production": Lock files enforced. Breaking changes require upcasters/version bumps.
 */
export type SpiteStackMode = "greenfield" | "production";

export interface SpiteStackAppConfig {
  /**
   * Mode determines schema evolution behavior.
   * - "greenfield": No lock files, iterate freely (default)
   * - "production": Lock files enforced, breaking changes require upcasters
   */
  mode?: SpiteStackMode;
  domainDir?: string;
  outDir?: string;
  include?: string[];
  exclude?: string[];
  generate?: {
    handlers?: boolean;
    validators?: boolean;
    wiring?: boolean;
  };
  diagnostics?: {
    colors?: boolean;
    maxErrors?: number;
  };
  routes?: {
    basePath?: string;
    publicSessionHeader?: string;
    publicSessionRequired?: boolean;
    publicTenantId?: string;
  };
  /** Schema evolution options (relevant in production mode) */
  schemaEvolution?: {
    /** Directory for upcaster files */
    upcasterDir?: string;
  };
  /** API versioning options (opt-in for API products) */
  api?: {
    /** Enable API contract versioning (default: false) */
    versioning?: boolean;
    /** Optional alias for latest version (e.g., "/api") */
    latestAlias?: string;
    /** Warn when deprecated API versions are used */
    deprecationWarnings?: boolean;
  };
  auth?: unknown;
}

export interface SpiteStackResolvedConfig {
  mode: SpiteStackMode;
  domainDir: string;
  outDir: string;
  include: string[];
  exclude: string[];
  generate: {
    handlers: boolean;
    validators: boolean;
    wiring: boolean;
  };
  diagnostics: {
    colors: boolean;
    maxErrors: number;
  };
  routes: {
    basePath: string;
    publicSessionHeader: string;
    publicSessionRequired: boolean;
    publicTenantId?: string;
  };
  schemaEvolution: {
    upcasterDir: string;
  };
  api: {
    versioning: boolean;
    latestAlias?: string;
    deprecationWarnings: boolean;
  };
  auth?: unknown;
}

export type SpiteStackScope = "public" | "auth" | "internal";

export type SpiteStackMethodScope =
  | SpiteStackScope
  | false
  | {
      scope?: SpiteStackScope;
      roles?: string[];
    };

export interface SpiteStackRegistration {
  aggregate: string;
  scope?: SpiteStackScope;
  roles?: string[];
  methods?: Record<string, SpiteStackMethodScope>;
}

const DEFAULT_CONFIG: SpiteStackResolvedConfig = {
  mode: "greenfield",
  domainDir: "./src/domain/aggregates",
  outDir: "./.spitestack/generated",
  include: ["**/*.ts"],
  exclude: ["**/*.test.ts", "**/*.spec.ts"],
  generate: {
    handlers: true,
    validators: true,
    wiring: true,
  },
  diagnostics: {
    colors: true,
    maxErrors: 50,
  },
  routes: {
    basePath: "/api",
    publicSessionHeader: "x-session-id",
    publicSessionRequired: true,
    publicTenantId: undefined,
  },
  schemaEvolution: {
    upcasterDir: "./.spitestack/upcasters",
  },
  api: {
    versioning: false,
    deprecationWarnings: true,
  },
};

function mergeConfig(config: SpiteStackAppConfig = {}): SpiteStackResolvedConfig {
  return {
    mode: config.mode ?? DEFAULT_CONFIG.mode,
    domainDir: config.domainDir ?? DEFAULT_CONFIG.domainDir,
    outDir: config.outDir ?? DEFAULT_CONFIG.outDir,
    include: config.include ?? DEFAULT_CONFIG.include,
    exclude: config.exclude ?? DEFAULT_CONFIG.exclude,
    generate: {
      handlers: config.generate?.handlers ?? DEFAULT_CONFIG.generate.handlers,
      validators: config.generate?.validators ?? DEFAULT_CONFIG.generate.validators,
      wiring: config.generate?.wiring ?? DEFAULT_CONFIG.generate.wiring,
    },
    diagnostics: {
      colors: config.diagnostics?.colors ?? DEFAULT_CONFIG.diagnostics.colors,
      maxErrors: config.diagnostics?.maxErrors ?? DEFAULT_CONFIG.diagnostics.maxErrors,
    },
    routes: {
      basePath: config.routes?.basePath ?? DEFAULT_CONFIG.routes.basePath,
      publicSessionHeader:
        config.routes?.publicSessionHeader ?? DEFAULT_CONFIG.routes.publicSessionHeader,
      publicSessionRequired:
        config.routes?.publicSessionRequired ?? DEFAULT_CONFIG.routes.publicSessionRequired,
      publicTenantId: config.routes?.publicTenantId ?? DEFAULT_CONFIG.routes.publicTenantId,
    },
    schemaEvolution: {
      upcasterDir: config.schemaEvolution?.upcasterDir ?? DEFAULT_CONFIG.schemaEvolution.upcasterDir,
    },
    api: {
      versioning: config.api?.versioning ?? DEFAULT_CONFIG.api.versioning,
      latestAlias: config.api?.latestAlias ?? DEFAULT_CONFIG.api.latestAlias,
      deprecationWarnings: config.api?.deprecationWarnings ?? DEFAULT_CONFIG.api.deprecationWarnings,
    },
    auth: config.auth,
  };
}

export class SpiteStackApp {
  readonly config: SpiteStackResolvedConfig;
  readonly registrations: SpiteStackRegistration[] = [];

  constructor(config: SpiteStackAppConfig = {}) {
    this.config = mergeConfig(config);
  }

  register(
    aggregate: string | { name: string },
    options: Omit<SpiteStackRegistration, "aggregate"> = {}
  ): this {
    const aggregateName = resolveAggregateName(aggregate);
    this.registrations.push({
      aggregate: aggregateName,
      scope: options.scope,
      roles: options.roles,
      methods: options.methods,
    });
    return this;
  }
}

export default function App(config: SpiteStackAppConfig = {}): SpiteStackApp {
  return new SpiteStackApp(config);
}

function resolveAggregateName(aggregate: string | { name: string }): string {
  if (typeof aggregate === "string") {
    return aggregate.toLowerCase();
  }
  const rawName = aggregate.name;
  if (rawName.endsWith("Aggregate")) {
    return rawName.slice(0, -"Aggregate".length).toLowerCase();
  }
  return rawName.toLowerCase();
}
