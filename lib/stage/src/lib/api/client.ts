/**
 * Admin API Client
 *
 * HTTP client for the spite-stage admin dashboard.
 * Connects to the backend admin API endpoints.
 */

// =============================================================================
// Types
// =============================================================================

export interface AdminStatusResponse {
  connected: boolean;
  uptime: number;
  version: string;
  environment: string;
  projectionsCount: number;
}

export interface AdminMetricsResponse {
  eventsPerSec: { read: number; write: number };
  totalEvents: number;
  admission: {
    currentLimit: number;
    observedP99Ms: number;
    targetP99Ms: number;
    requestsAccepted: number;
    requestsRejected: number;
    rejectionRate: number;
    adjustments: number;
  };
}

export interface AdminProjectionStatus {
  name: string;
  health: 'healthy' | 'warning' | 'error';
  checkpoint: number;
  lag: number;
  lastProcessed: number;
}

export interface AdminProjectionsResponse {
  projections: AdminProjectionStatus[];
  globalHead: number;
}

export interface AdminLogEntry {
  id: string;
  timestamp: number;
  severity: 'debug' | 'info' | 'warn' | 'error';
  message: string;
  service?: string;
  traceId?: string;
  spanId?: string;
  attrs?: Record<string, unknown>;
}

export interface AdminLogsResponse {
  logs: AdminLogEntry[];
  hasMore: boolean;
}

export interface AdminEventEntry {
  globalPos: number;
  streamId: string;
  streamRev: number;
  timestamp: number;
  tenantHash: number;
  dataPreview: string;
}

export interface AdminEventsResponse {
  events: AdminEventEntry[];
  hasMore: boolean;
  globalHead: number;
}

export interface AdminLogsQuery {
  limit?: number;
  severity?: 'debug' | 'info' | 'warn' | 'error';
  startMs?: number;
  endMs?: number;
  service?: string;
  traceId?: string;
}

export interface AdminEventsQuery {
  limit?: number;
  fromPos?: number;
  direction?: 'forward' | 'backward';
}

// =============================================================================
// Client
// =============================================================================

export interface AdminClientConfig {
  baseUrl?: string;
  /** Function to retrieve the current auth token */
  getToken?: () => string | null;
}

export interface AdminClient {
  getStatus(): Promise<AdminStatusResponse>;
  getMetrics(): Promise<AdminMetricsResponse>;
  getProjections(): Promise<AdminProjectionsResponse>;
  getLogs(query?: AdminLogsQuery): Promise<AdminLogsResponse>;
  getEvents(query?: AdminEventsQuery): Promise<AdminEventsResponse>;
}

export function createAdminClient(config: AdminClientConfig = {}): AdminClient {
  const baseUrl = config.baseUrl ?? '';
  const getToken = config.getToken;

  async function fetchJson<T>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
    const url = new URL(path, baseUrl || window.location.origin);

    if (params) {
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined) {
          url.searchParams.set(key, String(value));
        }
      }
    }

    const headers: Record<string, string> = {};

    // Add Authorization header if token is available
    const token = getToken?.();
    if (token) {
      headers['Authorization'] = `Bearer ${token}`;
    }

    const response = await fetch(url.toString(), {
      credentials: 'include',
      headers,
    });

    if (!response.ok) {
      const errorBody = await response.text();
      throw new Error(`API error ${response.status}: ${errorBody}`);
    }

    return response.json();
  }

  return {
    async getStatus(): Promise<AdminStatusResponse> {
      return fetchJson('/admin/api/status');
    },

    async getMetrics(): Promise<AdminMetricsResponse> {
      return fetchJson('/admin/api/metrics');
    },

    async getProjections(): Promise<AdminProjectionsResponse> {
      return fetchJson('/admin/api/projections');
    },

    async getLogs(query?: AdminLogsQuery): Promise<AdminLogsResponse> {
      return fetchJson('/admin/api/logs', query as Record<string, string | number | undefined>);
    },

    async getEvents(query?: AdminEventsQuery): Promise<AdminEventsResponse> {
      return fetchJson('/admin/api/events', query as Record<string, string | number | undefined>);
    },
  };
}
