/**
 * Event data as stored in segment files.
 *
 * This is the internal representation used by the storage layer.
 * The public API uses different types that get converted to/from this.
 */
export interface StoredEvent {
  /** Stream identifier */
  streamId: string;
  /** Event type name */
  type: string;
  /** Event data payload */
  data: unknown;
  /** Optional event metadata */
  metadata?: unknown;
  /** Stream-local revision (0-indexed) */
  revision: number;
  /** Global position across all streams */
  globalPosition: bigint;
  /** Unix timestamp in milliseconds */
  timestamp: number;
  /** Tenant identifier for multi-tenancy */
  tenantId: string;
}
