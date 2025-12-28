/**
 * Telemetry Types
 *
 * Data structures for real-time monitoring of Spitestack projections and events.
 */

export type ProjectionHealth = 'healthy' | 'warning' | 'error';

export interface ProjectionStatus {
  id: string;
  name: string;
  health: ProjectionHealth;
  eventsPerSec: number;
  avgLatencyMs: number;
  lastCheckpoint: number;
  eventSubscriptions: string[];
}

export interface LogEntry {
  id: string;
  timestamp: number;
  severity: 'debug' | 'info' | 'warn' | 'error';
  message: string;
  service?: string;
  traceId?: string;
}

export interface EventNode {
  id: string;
  type: string;
  timestamp: number;
  causedBy?: string;
  triggered: string[];
}

export interface TelemetryMetrics {
  read: number;
  write: number;
}

export interface TelemetryState {
  connected: boolean;
  connecting: boolean;
  error: string | null;
  eventsPerSec: TelemetryMetrics;
  totalEvents: number;
  projections: Map<string, ProjectionStatus>;
  logs: LogEntry[];
  eventGraph: EventNode[];
}
