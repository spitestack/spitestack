import { Packr, Unpackr } from 'msgpackr';
import type { Serializer } from '../../ports/serialization/serializer';
import type { StoredEvent } from '../../domain/events/stored-event';

const BATCH_MARKER = '__spite_batch';
const BATCH_VERSION = 1;

interface ColumnarBatch {
  [BATCH_MARKER]: number;
  streamIds: string[];
  types: string[];
  data: unknown[];
  metadata: unknown[];
  revisions: number[];
  globalPositions: number[];
  timestamps: number[];
  tenantIds: Array<string | undefined>;
}

function isStoredEvent(value: unknown): value is StoredEvent {
  if (!value || typeof value !== 'object') return false;
  const record = value as Record<string, unknown>;
  return (
    typeof record['streamId'] === 'string' &&
    typeof record['type'] === 'string' &&
    typeof record['revision'] === 'number' &&
    typeof record['globalPosition'] === 'number' &&
    typeof record['timestamp'] === 'number'
  );
}

function toColumnarBatch(events: StoredEvent[]): ColumnarBatch {
  const len = events.length;
  const streamIds = new Array<string>(len);
  const types = new Array<string>(len);
  const data = new Array<unknown>(len);
  const metadata = new Array<unknown>(len);
  const revisions = new Array<number>(len);
  const globalPositions = new Array<number>(len);
  const timestamps = new Array<number>(len);
  const tenantIds = new Array<string | undefined>(len);

  for (let i = 0; i < len; i++) {
    const event = events[i]!;
    streamIds[i] = event.streamId;
    types[i] = event.type;
    data[i] = event.data;
    metadata[i] = event.metadata;
    revisions[i] = event.revision;
    globalPositions[i] = event.globalPosition;
    timestamps[i] = event.timestamp;
    tenantIds[i] = event.tenantId;
  }

  return {
    [BATCH_MARKER]: BATCH_VERSION,
    streamIds,
    types,
    data,
    metadata,
    revisions,
    globalPositions,
    timestamps,
    tenantIds,
  };
}

function fromColumnarBatch(batch: ColumnarBatch): StoredEvent[] {
  const len = batch.streamIds.length;
  const events = new Array<StoredEvent>(len);

  for (let i = 0; i < len; i++) {
    events[i] = {
      streamId: batch.streamIds[i] ?? '',
      type: batch.types[i] ?? '',
      data: batch.data[i],
      metadata: batch.metadata[i],
      revision: batch.revisions[i] ?? 0,
      globalPosition: batch.globalPositions[i] ?? 0,
      timestamp: batch.timestamps[i] ?? 0,
      tenantId: batch.tenantIds[i] ?? 'default',
    };
  }

  return events;
}

/**
 * Fast serializer for event batches using a columnar layout.
 *
 * This reduces per-event object overhead during decode by storing
 * batch fields as arrays in MessagePack.
 */
export class FastEventSerializer implements Serializer {
  private readonly packr = new Packr({ useRecords: true });
  private readonly unpackr = new Unpackr({ useRecords: true });

  encode<T>(value: T): Uint8Array {
    if (Array.isArray(value) && value.length > 0 && isStoredEvent(value[0])) {
      return this.packr.pack(toColumnarBatch(value as StoredEvent[]));
    }

    return this.packr.pack(value);
  }

  decode<T>(data: Uint8Array): T {
    const decoded = this.unpackr.unpack(data) as T | ColumnarBatch;
    if (
      decoded &&
      typeof decoded === 'object' &&
      (decoded as ColumnarBatch)[BATCH_MARKER] === BATCH_VERSION
    ) {
      return fromColumnarBatch(decoded as ColumnarBatch) as T;
    }
    return decoded as T;
  }
}
