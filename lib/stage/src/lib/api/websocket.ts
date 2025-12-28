/**
 * Admin WebSocket Manager
 *
 * Real-time connection manager for the spite-stage admin dashboard.
 * Handles auto-reconnect and message routing.
 */

import type {
  AdminMetricsResponse,
  AdminLogEntry,
  AdminProjectionStatus,
  AdminEventEntry,
} from './client';

// =============================================================================
// Types
// =============================================================================

export type Channel = 'metrics' | 'logs' | 'projections' | 'events';

interface ServerMessage {
  type: 'connected' | 'subscribed' | 'unsubscribed' | 'metrics' | 'log' | 'projection' | 'event' | 'error';
  data?: unknown;
  channels?: Channel[];
  message?: string;
}

export interface AdminWebSocketConfig {
  url?: string;
  reconnectDelay?: number;
  maxReconnectAttempts?: number;
  /** Function to retrieve the current auth token */
  getToken?: () => string | null;
}

export interface AdminWebSocketCallbacks {
  onConnect?: () => void;
  onDisconnect?: () => void;
  onError?: (error: Error) => void;
  onMetrics?: (data: AdminMetricsResponse) => void;
  onLog?: (data: AdminLogEntry) => void;
  onProjection?: (data: AdminProjectionStatus & { globalHead: number }) => void;
  onEvent?: (data: AdminEventEntry) => void;
}

export interface AdminWebSocket {
  connect(): void;
  disconnect(): void;
  subscribe(channels: Channel[]): void;
  unsubscribe(channels: Channel[]): void;
  isConnected(): boolean;
}

// =============================================================================
// WebSocket Manager
// =============================================================================

export function createAdminWebSocket(
  config: AdminWebSocketConfig,
  callbacks: AdminWebSocketCallbacks
): AdminWebSocket {
  const reconnectDelay = config.reconnectDelay ?? 2000;
  const maxReconnectAttempts = config.maxReconnectAttempts ?? 10;

  let ws: WebSocket | null = null;
  let reconnectAttempts = 0;
  let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;
  let isManualDisconnect = false;

  function getWebSocketUrl(): string {
    let baseUrl: string;
    if (config.url) {
      baseUrl = config.url;
    } else {
      // Auto-detect WebSocket URL from current page
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const host = window.location.host;
      baseUrl = `${protocol}//${host}/admin/ws`;
    }

    // Add token as query parameter if available
    const token = config.getToken?.();
    if (token) {
      const separator = baseUrl.includes('?') ? '&' : '?';
      return `${baseUrl}${separator}token=${encodeURIComponent(token)}`;
    }

    return baseUrl;
  }

  function handleMessage(event: MessageEvent): void {
    try {
      const message = JSON.parse(event.data) as ServerMessage;

      switch (message.type) {
        case 'connected':
          reconnectAttempts = 0;
          callbacks.onConnect?.();
          break;

        case 'subscribed':
          // Server confirmed subscription
          break;

        case 'unsubscribed':
          // Server confirmed unsubscription
          break;

        case 'metrics':
          callbacks.onMetrics?.(message.data as AdminMetricsResponse);
          break;

        case 'log':
          callbacks.onLog?.(message.data as AdminLogEntry);
          break;

        case 'projection':
          callbacks.onProjection?.(message.data as AdminProjectionStatus & { globalHead: number });
          break;

        case 'event':
          callbacks.onEvent?.(message.data as AdminEventEntry);
          break;

        case 'error':
          callbacks.onError?.(new Error(message.message ?? 'Unknown error'));
          break;
      }
    } catch (err) {
      callbacks.onError?.(err instanceof Error ? err : new Error('Failed to parse message'));
    }
  }

  function handleOpen(): void {
    reconnectAttempts = 0;
    // Connection opened, wait for 'connected' message from server
  }

  function handleClose(): void {
    ws = null;
    callbacks.onDisconnect?.();

    // Attempt reconnect unless manually disconnected
    if (!isManualDisconnect && reconnectAttempts < maxReconnectAttempts) {
      reconnectTimeout = setTimeout(() => {
        reconnectAttempts++;
        connect();
      }, reconnectDelay * Math.min(reconnectAttempts + 1, 5)); // Exponential backoff capped at 5x
    }
  }

  function handleError(event: Event): void {
    callbacks.onError?.(new Error('WebSocket connection error'));
  }

  function connect(): void {
    if (ws && ws.readyState === WebSocket.OPEN) {
      return; // Already connected
    }

    isManualDisconnect = false;

    try {
      ws = new WebSocket(getWebSocketUrl());
      ws.addEventListener('open', handleOpen);
      ws.addEventListener('close', handleClose);
      ws.addEventListener('error', handleError);
      ws.addEventListener('message', handleMessage);
    } catch (err) {
      callbacks.onError?.(err instanceof Error ? err : new Error('Failed to create WebSocket'));
    }
  }

  function disconnect(): void {
    isManualDisconnect = true;

    if (reconnectTimeout) {
      clearTimeout(reconnectTimeout);
      reconnectTimeout = null;
    }

    if (ws) {
      ws.removeEventListener('open', handleOpen);
      ws.removeEventListener('close', handleClose);
      ws.removeEventListener('error', handleError);
      ws.removeEventListener('message', handleMessage);
      ws.close();
      ws = null;
    }

    callbacks.onDisconnect?.();
  }

  function subscribe(channels: Channel[]): void {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'subscribe', channels }));
    }
  }

  function unsubscribe(channels: Channel[]): void {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'unsubscribe', channels }));
    }
  }

  function isConnected(): boolean {
    return ws !== null && ws.readyState === WebSocket.OPEN;
  }

  return {
    connect,
    disconnect,
    subscribe,
    unsubscribe,
    isConnected,
  };
}
