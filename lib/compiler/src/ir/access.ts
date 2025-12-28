/**
 * Access control configuration for aggregate and orchestrator methods.
 */

/**
 * Application mode controlling schema evolution behavior.
 */
export type AppMode =
  /** Development mode - schemas can change freely without constraints. */
  | "greenfield"
  /** Locked mode - event schemas are captured in a lock file. */
  | "production";

/**
 * Parse an app mode from a string.
 */
export function parseAppMode(s: string): AppMode | undefined {
  const lower = s.toLowerCase();
  if (lower === "greenfield") return "greenfield";
  if (lower === "production") return "production";
  return undefined;
}

/**
 * Access level for an endpoint.
 */
export type AccessLevel =
  /** No authentication required - anyone can call this endpoint. */
  | "public"
  /** Requires authentication and system-tenant membership. */
  | "internal"
  /** Requires authentication and tenant membership. */
  | "private";

/**
 * Parse an access level from a string.
 */
export function parseAccessLevel(s: string): AccessLevel | undefined {
  const lower = s.toLowerCase();
  if (lower === "public") return "public";
  if (lower === "internal") return "internal";
  if (lower === "private") return "private";
  return undefined;
}

/**
 * Access configuration for a single method.
 */
export interface MethodAccessConfig {
  /** The access level for this method. */
  access: AccessLevel;

  /**
   * Required roles to access this method.
   * Only applicable for `internal` and `private` access levels.
   */
  roles: string[];
}

/**
 * Creates default method access config.
 */
export function defaultMethodAccessConfig(): MethodAccessConfig {
  return {
    access: "internal",
    roles: [],
  };
}

/**
 * Access configuration for an aggregate or orchestrator.
 */
export interface EntityAccessConfig {
  /** Default access level for all methods on this entity. */
  access: AccessLevel;

  /** Default required roles for all methods on this entity. */
  roles: string[];

  /** Per-method configuration overrides. */
  methods: Map<string, MethodAccessConfig>;
}

/**
 * Creates default entity access config.
 */
export function defaultEntityAccessConfig(): EntityAccessConfig {
  return {
    access: "internal",
    roles: [],
    methods: new Map(),
  };
}

/**
 * Resolve the effective access configuration for a method.
 * Method-level configuration overrides entity-level defaults.
 */
export function resolveMethodAccess(
  entityConfig: EntityAccessConfig,
  methodName: string
): MethodAccessConfig {
  const methodConfig = entityConfig.methods.get(methodName);
  if (methodConfig) {
    return {
      access: methodConfig.access,
      roles: methodConfig.roles.length > 0 ? methodConfig.roles : entityConfig.roles,
    };
  }
  return {
    access: entityConfig.access,
    roles: entityConfig.roles,
  };
}

/**
 * Configuration parsed from App registration in index.ts.
 */
export interface AppConfig {
  /** Application mode controlling schema evolution. */
  mode: AppMode;

  /**
   * Whether API versioning is enabled.
   * When true, routes are prefixed with version and contract changes are locked.
   */
  apiVersioning: boolean;

  /** Access configurations keyed by entity name (aggregate or orchestrator). */
  entities: Map<string, EntityAccessConfig>;
}

/**
 * Creates default app config.
 */
export function defaultAppConfig(): AppConfig {
  return {
    mode: "greenfield",
    apiVersioning: false,
    entities: new Map(),
  };
}

/**
 * Get the access configuration for an entity, or default if not configured.
 */
export function getEntityConfig(
  appConfig: AppConfig,
  name: string
): EntityAccessConfig {
  return appConfig.entities.get(name) ?? defaultEntityAccessConfig();
}
