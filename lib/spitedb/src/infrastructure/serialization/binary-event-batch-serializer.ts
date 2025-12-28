import { Packr, Unpackr } from 'msgpackr';
import type { Serializer } from '../../ports/serialization/serializer';
import type { StoredEvent } from '../../domain/events/stored-event';

const BATCH_MAGIC = 0x53505442; // "SPTB" in ASCII
const BATCH_VERSION = 1;
const BATCH_HEADER_SIZE = 12; // magic (4) + version (2) + flags (2) + count (4)
const NULL_LENGTH = 0xffffffff;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

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

function ensureSafeUint(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${label} must be a non-negative safe integer`);
  }
}

function encodeEventBatch(
  events: StoredEvent[],
  packr: Packr
): Uint8Array {
  const encoded = events.map((event) => {
    const streamId = textEncoder.encode(event.streamId);
    const type = textEncoder.encode(event.type);
    const tenantId = event.tenantId !== undefined ? textEncoder.encode(event.tenantId) : null;
    const metadata = event.metadata !== undefined ? packr.pack(event.metadata) : null;
    const data = packr.pack(event.data);

    ensureSafeUint(event.revision, 'Revision');
    ensureSafeUint(event.globalPosition, 'Global position');
    ensureSafeUint(event.timestamp, 'Timestamp');

    return {
      event,
      streamId,
      type,
      tenantId,
      metadata,
      data,
    };
  });

  let totalSize = BATCH_HEADER_SIZE;
  for (const entry of encoded) {
    totalSize += 4 + entry.streamId.length;
    totalSize += 4 + entry.type.length;
    totalSize += 4; // revision
    totalSize += 8; // globalPosition
    totalSize += 8; // timestamp
    totalSize += 4 + (entry.tenantId ? entry.tenantId.length : 0);
    totalSize += 4 + (entry.metadata ? entry.metadata.length : 0);
    totalSize += 4 + entry.data.length;
  }

  const buffer = new Uint8Array(totalSize);
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);

  view.setUint32(0, BATCH_MAGIC, false);
  view.setUint16(4, BATCH_VERSION, false);
  view.setUint16(6, 0, false);
  view.setUint32(8, events.length, false);

  let offset = BATCH_HEADER_SIZE;
  for (const entry of encoded) {
    view.setUint32(offset, entry.streamId.length, false);
    offset += 4;
    buffer.set(entry.streamId, offset);
    offset += entry.streamId.length;

    view.setUint32(offset, entry.type.length, false);
    offset += 4;
    buffer.set(entry.type, offset);
    offset += entry.type.length;

    view.setUint32(offset, entry.event.revision, false);
    offset += 4;

    view.setBigUint64(offset, BigInt(entry.event.globalPosition), false);
    offset += 8;
    view.setBigUint64(offset, BigInt(entry.event.timestamp), false);
    offset += 8;

    if (entry.tenantId) {
      view.setUint32(offset, entry.tenantId.length, false);
      offset += 4;
      buffer.set(entry.tenantId, offset);
      offset += entry.tenantId.length;
    } else {
      view.setUint32(offset, NULL_LENGTH, false);
      offset += 4;
    }

    if (entry.metadata) {
      view.setUint32(offset, entry.metadata.length, false);
      offset += 4;
      buffer.set(entry.metadata, offset);
      offset += entry.metadata.length;
    } else {
      view.setUint32(offset, NULL_LENGTH, false);
      offset += 4;
    }

    view.setUint32(offset, entry.data.length, false);
    offset += 4;
    buffer.set(entry.data, offset);
    offset += entry.data.length;
  }

  return buffer;
}

function decodeEventBatch(
  data: Uint8Array,
  unpackr: Unpackr
): StoredEvent[] {
  if (data.length < BATCH_HEADER_SIZE) {
    throw new Error('Batch payload is too small');
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const magic = view.getUint32(0, false);
  if (magic !== BATCH_MAGIC) {
    throw new Error('Invalid batch payload magic');
  }

  const version = view.getUint16(4, false);
  if (version !== BATCH_VERSION) {
    throw new Error(`Unsupported batch payload version: ${version}`);
  }

  const eventCount = view.getUint32(8, false);
  let offset = BATCH_HEADER_SIZE;
  const events: StoredEvent[] = [];

  for (let i = 0; i < eventCount; i += 1) {
    if (offset + 4 > data.length) {
      throw new Error('Batch payload truncated (streamId length)');
    }
    const streamIdLength = view.getUint32(offset, false);
    offset += 4;
    const streamIdEnd = offset + streamIdLength;
    if (streamIdEnd > data.length) {
      throw new Error('Batch payload truncated (streamId)');
    }
    const streamId = textDecoder.decode(data.subarray(offset, streamIdEnd));
    offset = streamIdEnd;

    if (offset + 4 > data.length) {
      throw new Error('Batch payload truncated (type length)');
    }
    const typeLength = view.getUint32(offset, false);
    offset += 4;
    const typeEnd = offset + typeLength;
    if (typeEnd > data.length) {
      throw new Error('Batch payload truncated (type)');
    }
    const type = textDecoder.decode(data.subarray(offset, typeEnd));
    offset = typeEnd;

    if (offset + 20 > data.length) {
      throw new Error('Batch payload truncated (position fields)');
    }
    const revision = view.getUint32(offset, false);
    offset += 4;
    const globalPosition = Number(view.getBigUint64(offset, false));
    offset += 8;
    const timestamp = Number(view.getBigUint64(offset, false));
    offset += 8;

    if (!Number.isSafeInteger(globalPosition) || !Number.isSafeInteger(timestamp)) {
      throw new Error('Batch payload contains unsafe integer positions');
    }

    if (offset + 4 > data.length) {
      throw new Error('Batch payload truncated (tenant length)');
    }
    const tenantLength = view.getUint32(offset, false);
    offset += 4;
    let tenantId: string | undefined;
    if (tenantLength !== NULL_LENGTH) {
      const tenantEnd = offset + tenantLength;
      if (tenantEnd > data.length) {
        throw new Error('Batch payload truncated (tenant id)');
      }
      tenantId = textDecoder.decode(data.subarray(offset, tenantEnd));
      offset = tenantEnd;
    }

    if (offset + 4 > data.length) {
      throw new Error('Batch payload truncated (metadata length)');
    }
    const metadataLength = view.getUint32(offset, false);
    offset += 4;
    let metadata: unknown;
    if (metadataLength !== NULL_LENGTH) {
      const metadataEnd = offset + metadataLength;
      if (metadataEnd > data.length) {
        throw new Error('Batch payload truncated (metadata)');
      }
      metadata = unpackr.unpack(data.subarray(offset, metadataEnd));
      offset = metadataEnd;
    }

    if (offset + 4 > data.length) {
      throw new Error('Batch payload truncated (data length)');
    }
    const dataLength = view.getUint32(offset, false);
    offset += 4;
    const dataEnd = offset + dataLength;
    if (dataEnd > data.length) {
      throw new Error('Batch payload truncated (data)');
    }
    const payload = unpackr.unpack(data.subarray(offset, dataEnd));
    offset = dataEnd;

    events.push({
      streamId,
      type,
      data: payload,
      metadata,
      revision,
      globalPosition,
      timestamp,
      tenantId: tenantId ?? 'default',
    });
  }

  return events;
}

/**
 * Binary serializer for event batches.
 *
 * Uses a simple length-prefixed binary layout for event envelopes and
 * msgpack for data/metadata payloads. This keeps decoding fast while
 * preserving schema evolution flexibility inside the event body.
 */
export class BinaryEventBatchSerializer implements Serializer {
  private readonly packr = new Packr({ useRecords: true });
  private readonly unpackr = new Unpackr({ useRecords: true });

  encode<T>(value: T): Uint8Array {
    if (!Array.isArray(value)) {
      throw new Error('BinaryEventBatchSerializer only supports event batches');
    }

    if (value.length > 0 && !isStoredEvent(value[0])) {
      throw new Error('BinaryEventBatchSerializer expects StoredEvent[]');
    }

    return encodeEventBatch(value as StoredEvent[], this.packr);
  }

  decode<T>(data: Uint8Array): T {
    if (data.length < BATCH_HEADER_SIZE) {
      throw new Error('Batch payload is too small');
    }

    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    if (view.getUint32(0, false) !== BATCH_MAGIC) {
      throw new Error('Invalid batch payload magic');
    }

    return decodeEventBatch(data, this.unpackr) as T;
  }
}
