/**
 * Telemetry Store
 *
 * Real-time metrics from Spitestack.
 * Events flowing, projections processing, system breathing.
 */

import { writable, derived } from 'svelte/store';
import type {
  TelemetryState,
  TelemetryMetrics,
  ProjectionStatus,
  LogEntry,
  EventNode
} from '$lib/types/telemetry';
import {
  createAdminClient,
  createAdminWebSocket,
  type AdminClient,
  type AdminWebSocket,
  type AdminMetricsResponse,
  type AdminLogEntry,
  type AdminProjectionStatus,
} from '$lib/api';
import { auth } from './auth';

const MAX_LOGS = 1000;

// Global instances for connection management
let client: AdminClient | null = null;
let ws: AdminWebSocket | null = null;

function createTelemetryStore() {
  const { subscribe, set, update } = writable<TelemetryState>({
    connected: false,
    connecting: false,
    error: null,
    eventsPerSec: { read: 0, write: 0 },
    totalEvents: 0,
    projections: new Map(),
    logs: [],
    eventGraph: [],
  });

  // Helper to map API projection to store projection
  function mapProjection(apiProj: AdminProjectionStatus & { globalHead?: number }): ProjectionStatus {
    return {
      id: apiProj.name,
      name: apiProj.name,
      health: apiProj.health,
      eventsPerSec: 0, // Will be updated via metrics
      avgLatencyMs: 0, // Will be updated via metrics
      lastCheckpoint: apiProj.lastProcessed,
      eventSubscriptions: [], // Not currently provided by API
    };
  }

  // Helper to map API log to store log
  function mapLog(apiLog: AdminLogEntry): LogEntry {
    return {
      id: apiLog.id,
      timestamp: apiLog.timestamp,
      severity: apiLog.severity,
      message: apiLog.message,
      service: apiLog.service,
      traceId: apiLog.traceId,
    };
  }

  const store = {
    subscribe,

    setConnected: (connected: boolean) => {
      update(state => ({ ...state, connected, connecting: false }));
    },

    setConnecting: (connecting: boolean) => {
      update(state => ({ ...state, connecting }));
    },

    setError: (error: string | null) => {
      update(state => ({ ...state, error }));
    },

    updateMetrics: (metrics: TelemetryMetrics) => {
      update(state => ({
        ...state,
        eventsPerSec: metrics,
      }));
    },

    setTotalEvents: (totalEvents: number) => {
      update(state => ({ ...state, totalEvents }));
    },

    updateProjection: (projection: ProjectionStatus) => {
      update(state => {
        const newProjections = new Map(state.projections);
        newProjections.set(projection.id, projection);
        return { ...state, projections: newProjections };
      });
    },

    addLog: (log: LogEntry) => {
      update(state => ({
        ...state,
        logs: [log, ...state.logs].slice(0, MAX_LOGS),
      }));
    },

    clearLogs: () => {
      update(state => ({ ...state, logs: [] }));
    },

    updateEventGraph: (nodes: EventNode[]) => {
      update(state => ({ ...state, eventGraph: nodes }));
    },

    /**
     * Connect to the backend API and WebSocket.
     * Fetches initial data and sets up real-time streaming.
     */
    async connect(): Promise<void> {
      update(state => ({ ...state, connecting: true, error: null }));

      try {
        // Create API client with auth token
        client = createAdminClient({
          getToken: () => auth.getToken(),
        });

        // Fetch initial data in parallel
        const [metricsRes, projectionsRes, logsRes] = await Promise.all([
          client.getMetrics(),
          client.getProjections(),
          client.getLogs({ limit: 100 }),
        ]);

        // Update state with initial data
        update(state => {
          const newProjections = new Map<string, ProjectionStatus>();
          for (const p of projectionsRes.projections) {
            const proj = mapProjection(p);
            newProjections.set(proj.id, proj);
          }

          return {
            ...state,
            eventsPerSec: metricsRes.eventsPerSec,
            totalEvents: metricsRes.totalEvents,
            projections: newProjections,
            logs: logsRes.logs.map(mapLog),
          };
        });

        // Create WebSocket for real-time updates (with auth token)
        ws = createAdminWebSocket({ getToken: () => auth.getToken() }, {
          onConnect: () => {
            update(state => ({ ...state, connected: true, connecting: false }));
          },
          onDisconnect: () => {
            update(state => ({ ...state, connected: false }));
          },
          onError: (err) => {
            update(state => ({ ...state, error: err.message }));
          },
          onMetrics: (data: AdminMetricsResponse) => {
            update(state => ({
              ...state,
              eventsPerSec: data.eventsPerSec,
              totalEvents: data.totalEvents,
            }));
          },
          onLog: (data: AdminLogEntry) => {
            const log = mapLog(data);
            update(state => ({
              ...state,
              logs: [log, ...state.logs].slice(0, MAX_LOGS),
            }));
          },
          onProjection: (data: AdminProjectionStatus & { globalHead: number }) => {
            const proj = mapProjection(data);
            update(state => {
              const newProjections = new Map(state.projections);
              newProjections.set(proj.id, proj);
              return { ...state, projections: newProjections };
            });
          },
        });

        // Connect WebSocket
        ws.connect();

      } catch (err) {
        const message = err instanceof Error ? err.message : 'Failed to connect';
        update(state => ({
          ...state,
          connecting: false,
          error: message,
        }));
        throw err;
      }
    },

    /**
     * Disconnect from the backend.
     */
    disconnect(): void {
      if (ws) {
        ws.disconnect();
        ws = null;
      }
      client = null;
      update(state => ({ ...state, connected: false, connecting: false }));
    },

    // Mock data for development/demo
    populateMockData: () => {
      const mockProjections: ProjectionStatus[] = [
        {
          id: 'user-list',
          name: 'user-list',
          health: 'healthy',
          eventsPerSec: 234,
          avgLatencyMs: 45,
          lastCheckpoint: Date.now() - 1000,
          eventSubscriptions: ['UserCreated', 'UserUpdated', 'UserDeleted'],
        },
        {
          id: 'billing-summary',
          name: 'billing-summary',
          health: 'warning',
          eventsPerSec: 89,
          avgLatencyMs: 340,
          lastCheckpoint: Date.now() - 5000,
          eventSubscriptions: ['InvoiceCreated', 'PaymentProcessed'],
        },
        {
          id: 'activity-feed',
          name: 'activity-feed',
          health: 'healthy',
          eventsPerSec: 567,
          avgLatencyMs: 12,
          lastCheckpoint: Date.now() - 500,
          eventSubscriptions: ['*'],
        },
        {
          id: 'search-index',
          name: 'search-index',
          health: 'healthy',
          eventsPerSec: 123,
          avgLatencyMs: 8,
          lastCheckpoint: Date.now() - 200,
          eventSubscriptions: ['UserCreated', 'PostCreated', 'CommentCreated'],
        },
        {
          id: 'analytics',
          name: 'analytics',
          health: 'error',
          eventsPerSec: 0,
          avgLatencyMs: 0,
          lastCheckpoint: Date.now() - 60000,
          eventSubscriptions: ['PageView', 'Click', 'Conversion'],
        },
        {
          id: 'notifications',
          name: 'notifications',
          health: 'healthy',
          eventsPerSec: 45,
          avgLatencyMs: 23,
          lastCheckpoint: Date.now() - 1500,
          eventSubscriptions: ['UserMentioned', 'CommentReplied', 'PostLiked'],
        },
        {
          id: 'audit-log',
          name: 'audit-log',
          health: 'healthy',
          eventsPerSec: 890,
          avgLatencyMs: 5,
          lastCheckpoint: Date.now() - 100,
          eventSubscriptions: ['*'],
        },
        {
          id: 'reports',
          name: 'reports',
          health: 'warning',
          eventsPerSec: 12,
          avgLatencyMs: 1200,
          lastCheckpoint: Date.now() - 30000,
          eventSubscriptions: ['ReportRequested', 'DataExported'],
        },
      ];

      const newProjections = new Map<string, ProjectionStatus>();
      mockProjections.forEach(p => newProjections.set(p.id, p));

      set({
        connected: true,
        connecting: false,
        error: null,
        eventsPerSec: { read: 1247, write: 456 },
        totalEvents: 1503247,
        projections: newProjections,
        logs: [],
        eventGraph: [],
      });
    },
  };

  return store;
}

export const telemetry = createTelemetryStore();

// Derived stores for convenience
export const projectionsList = derived(telemetry, $t => 
  Array.from($t.projections.values())
);

export const healthSummary = derived(telemetry, $t => {
  const projections = Array.from($t.projections.values());
  return {
    healthy: projections.filter(p => p.health === 'healthy').length,
    warning: projections.filter(p => p.health === 'warning').length,
    error: projections.filter(p => p.health === 'error').length,
    total: projections.length,
  };
});

export const totalEventsPerSec = derived(telemetry, $t => 
  $t.eventsPerSec.read + $t.eventsPerSec.write
);
