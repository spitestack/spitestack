import type { GeneratedFile, CompilerConfig } from "../types";

type CommandPolicy = {
  scope: "public" | "auth" | "internal";
  roles?: string[];
};

export interface RoutesGeneratorOptions {
  commandPolicies: Map<string, CommandPolicy>;
  appImportPath?: string | null;
  apiVersioning?: {
    enabled: boolean;
    currentVersion?: string;
    frozenVersions?: string[];
    latestAlias?: string;
  };
}

export function generateRoutesFile(
  commandPolicies: Map<string, CommandPolicy>,
  appImportPath?: string | null,
  apiVersioning?: RoutesGeneratorOptions["apiVersioning"]
): GeneratedFile {
  const appImport = appImportPath ? `import app from "${appImportPath}";` : "";
  const appConfigInit = appImportPath
    ? "const appConfig = app?.config ?? app;"
    : "const appConfig = null;";
  const appAuthInit = appImportPath
    ? "const appAuth: SpiteStackAuthOptions = appConfig?.auth ?? {};"
    : "const appAuth: SpiteStackAuthOptions = {};";
  const appRoutesInit = appImportPath
    ? "const appRoutes: SpiteStackRouteConfig = appConfig?.routes ?? {};"
    : "const appRoutes: SpiteStackRouteConfig = {};";

  const routes = Array.from(commandPolicies.entries())
    .map(([command, policy]) => {
      const roles =
        policy.roles && policy.roles.length > 0
          ? `, roles: [${policy.roles.map((role) => `"${role}"`).join(", ")}]`
          : "";
      return `  ["${command}", { scope: "${policy.scope}"${roles} }]`;
    })
    .join(",\n");

  const content = `/**
 * Auto-generated SpiteStack routes
 * DO NOT EDIT - regenerate with \`spitestack compile\`
 */

import type { SpiteDbNapi } from "@spitestack/db";
import { DEFAULT_TENANT } from "@spitestack/db";
import type { Auth } from "better-auth";
import { executeCommand, type Command } from "./wiring";
${appImport}

type RouteScope = "public" | "auth" | "internal";

type RoutePolicy = {
  scope: RouteScope;
  roles?: string[];
};

interface SpiteStackAuthOptions {
  tenantPrefix?: {
    org?: string;
    user?: string;
  };
  internalOrgId?: string;
}

interface SpiteStackRouteConfig {
  basePath?: string;
  publicSessionHeader?: string;
  publicSessionRequired?: boolean;
  publicTenantId?: string;
}

${appConfigInit}
${appAuthInit}
${appRoutesInit}

const COMMAND_ROUTES = new Map<string, RoutePolicy>([
${routes}
]);

const DEFAULT_BASE_PATH = "/api";
const DEFAULT_PUBLIC_SESSION_HEADER = "x-session-id";
const DEFAULT_PUBLIC_SESSION_REQUIRED = true;
const DEFAULT_PUBLIC_SESSION_COOKIE = "spitestack_session";
${apiVersioning?.enabled ? `
// API Versioning
const API_VERSIONING_ENABLED = true;
const CURRENT_API_VERSION = "${apiVersioning.currentVersion ?? "v1"}";
const SUPPORTED_API_VERSIONS = new Set([${
  apiVersioning.frozenVersions?.length
    ? [...apiVersioning.frozenVersions, apiVersioning.currentVersion ?? "v1"].map(v => `"${v}"`).join(", ")
    : `"${apiVersioning.currentVersion ?? "v1"}"`
}]);
${apiVersioning.latestAlias ? `const LATEST_API_ALIAS = "${apiVersioning.latestAlias}";` : "const LATEST_API_ALIAS: string | null = null;"}
` : `
// API Versioning disabled
const API_VERSIONING_ENABLED = false;
const CURRENT_API_VERSION = "v1";
const SUPPORTED_API_VERSIONS = new Set<string>();
const LATEST_API_ALIAS: string | null = null;
`}

export interface RouteOptions {
  db: SpiteDbNapi;
  auth?: Auth;
  basePath?: string;
  publicTenantId?: string;
  internalOrgId?: string;
}

export function createCommandHandler(options: RouteOptions) {
  const basePath = options.basePath ?? appRoutes.basePath ?? DEFAULT_BASE_PATH;
  const publicSessionHeader =
    appRoutes.publicSessionHeader ?? DEFAULT_PUBLIC_SESSION_HEADER;
  const publicSessionRequired =
    appRoutes.publicSessionRequired ?? DEFAULT_PUBLIC_SESSION_REQUIRED;
  const publicTenantId =
    options.publicTenantId ?? appRoutes.publicTenantId ?? DEFAULT_TENANT;
  const internalOrgId = options.internalOrgId ?? appAuth.internalOrgId ?? null;

  return async function handleRequest(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (!url.pathname.startsWith(basePath)) {
      return new Response("Not found", { status: 404 });
    }

    const path = url.pathname.slice(basePath.length);
    const segments = path.split("/").filter(Boolean);

    // Handle API versioning
    let apiVersion: string | null = null;
    let aggregate: string;
    let command: string;

    if (API_VERSIONING_ENABLED) {
      // Check for version prefix (e.g., /v1/todo/create)
      if (segments[0]?.startsWith("v") && /^v\\d+$/.test(segments[0])) {
        apiVersion = segments[0];
        if (!SUPPORTED_API_VERSIONS.has(apiVersion)) {
          return new Response(\`Unsupported API version: \${apiVersion}\`, { status: 400 });
        }
        [, aggregate, command] = segments;
      } else {
        // No version prefix - use current version
        [aggregate, command] = segments;
        apiVersion = CURRENT_API_VERSION;
      }

      if (!aggregate || !command) {
        return new Response("Not found", { status: 404 });
      }
    } else {
      [aggregate, command] = segments;
      if (!aggregate || !command || segments.length !== 2) {
        return new Response("Not found", { status: 404 });
      }
    }

    if (req.method !== "POST") {
      return new Response("Method not allowed", { status: 405 });
    }

    const commandType = \`\${aggregate}.\${command}\`;
    const policy = COMMAND_ROUTES.get(commandType);
    if (!policy) {
      return new Response("Not found", { status: 404 });
    }

    const authResult = await authorizeRequest({
      req,
      policy,
      auth: options.auth,
      publicTenantId,
      publicSessionHeader,
      publicSessionRequired,
      internalOrgId,
    });

    if (!authResult.ok) {
      return authResult.response;
    }

    let payload: unknown;
    try {
      payload = await req.json();
    } catch {
      return new Response("Invalid JSON body", { status: 400 });
    }

    if (!payload || typeof payload !== "object" || !("id" in payload)) {
      return new Response("Missing id in payload", { status: 400 });
    }

    const commandId = req.headers.get("x-command-id") ?? randomId("cmd");
    const commandInput = { type: commandType, payload } as Command;

    const result = await executeCommand(
      {
        db: options.db,
        commandId,
        tenant: authResult.tenantId,
        actorId: authResult.actorId ?? undefined,
      },
      commandInput
    );

    return Response.json(result);
  };
}

async function authorizeRequest(input: {
  req: Request;
  policy: RoutePolicy;
  auth?: Auth;
  publicTenantId: string;
  publicSessionHeader: string;
  publicSessionRequired: boolean;
  internalOrgId: string | null;
}): Promise<
  | { ok: true; tenantId: string; actorId?: string | null }
  | { ok: false; response: Response }
> {
  if (input.policy.scope === "public") {
    const sessionId = getPublicSessionId(input.req, input.publicSessionHeader);
    if (input.publicSessionRequired && !sessionId) {
      return {
        ok: false,
        response: new Response("Missing session id", { status: 400 }),
      };
    }

    return {
      ok: true,
      tenantId: input.publicTenantId,
      actorId: sessionId ?? null,
    };
  }

  if (!input.auth?.api?.getSession) {
    return {
      ok: false,
      response: new Response("Authentication required", { status: 401 }),
    };
  }

  const session = (await input.auth.api.getSession({ headers: input.req.headers })) as any;
  if (!session || !session.user?.id) {
    return {
      ok: false,
      response: new Response("Authentication required", { status: 401 }),
    };
  }

  const tenantId = resolveTenantIdFromSession(session, appAuth.tenantPrefix);
  if (!tenantId) {
    return {
      ok: false,
      response: new Response("No active tenant", { status: 400 }),
    };
  }

  if (input.policy.scope === "internal") {
    if (!input.internalOrgId) {
      return {
        ok: false,
        response: new Response("Internal org not configured", { status: 500 }),
      };
    }
    const activeOrgId = session.session?.activeOrganizationId ?? null;
    if (activeOrgId !== input.internalOrgId) {
      return {
        ok: false,
        response: new Response("Forbidden", { status: 403 }),
      };
    }
  }

  if (input.policy.roles && input.policy.roles.length > 0) {
    const role = await resolveActiveRole(input.auth, input.req.headers);
    if (!role || !input.policy.roles.includes(role)) {
      return {
        ok: false,
        response: new Response("Forbidden", { status: 403 }),
      };
    }
  }

  return {
    ok: true,
    tenantId,
    actorId: session.user?.id ?? null,
  };
}

async function resolveActiveRole(auth: Auth, headers: Headers): Promise<string | null> {
  const api = auth.api as { getActiveMemberRole?: (input: any) => Promise<any> };
  if (!api?.getActiveMemberRole) {
    return null;
  }
  const result = await api.getActiveMemberRole({ headers });
  return result?.role ?? null;
}

function resolveTenantIdFromSession(session: any, prefix?: SpiteStackAuthOptions["tenantPrefix"]): string | null {
  const activeOrgId = session.session?.activeOrganizationId ?? null;
  if (activeOrgId) {
    return resolveTenantId("org", activeOrgId, prefix);
  }
  const userId = session.user?.id ?? null;
  if (!userId) {
    return null;
  }
  return resolveTenantId("user", userId, prefix);
}

function resolveTenantId(
  kind: "org" | "user",
  id: string,
  prefix?: SpiteStackAuthOptions["tenantPrefix"]
): string {
  const effectivePrefix = kind === "org" ? prefix?.org ?? "org:" : prefix?.user ?? "user:";
  return \`\${effectivePrefix}\${id}\`;
}

function getPublicSessionId(req: Request, headerName: string): string | null {
  const headerValue = req.headers.get(headerName);
  if (headerValue) {
    return headerValue;
  }
  const cookieHeader = req.headers.get("cookie");
  if (!cookieHeader) {
    return null;
  }
  return (
    readCookie(cookieHeader, headerName) ??
    readCookie(cookieHeader, DEFAULT_PUBLIC_SESSION_COOKIE)
  );
}

function readCookie(cookieHeader: string, name: string): string | null {
  const parts = cookieHeader.split(";").map((part) => part.trim());
  for (const part of parts) {
    if (part.startsWith(\`\${name}=\`)) {
      return decodeURIComponent(part.slice(name.length + 1));
    }
  }
  return null;
}

function randomId(prefix: string): string {
  const uuid = globalThis.crypto?.randomUUID?.() ?? \`\${Date.now()}-\${Math.random()}\`;
  return \`\${prefix}:\${uuid}\`;
}
`;

  return {
    path: "routes.ts",
    content,
  };
}
