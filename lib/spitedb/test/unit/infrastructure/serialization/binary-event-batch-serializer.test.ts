import { describe, test, expect } from 'bun:test';
import { BinaryEventBatchSerializer } from '../../../../src/infrastructure/serialization/binary-event-batch-serializer';
import type { StoredEvent } from '../../../../src/domain/events/stored-event';

describe('BinaryEventBatchSerializer', () => {
  const serializer = new BinaryEventBatchSerializer();

  function createEvent(overrides: Partial<StoredEvent> = {}): StoredEvent {
    return {
      streamId: 'stream-1',
      type: 'TestEvent',
      data: { value: 1 },
      metadata: { trace: 'abc' },
      revision: 0,
      globalPosition: 0,
      timestamp: 123456789,
      tenantId: 'default',
      ...overrides,
    };
  }

  test('round-trips a batch of events', () => {
    const events = [
      createEvent(),
      createEvent({ streamId: 'stream-2', revision: 1, globalPosition: 1, data: { value: 2 } }),
    ];

    const encoded = serializer.encode(events);
    const decoded = serializer.decode<StoredEvent[]>(encoded);

    expect(decoded).toHaveLength(2);
    expect(decoded[0]).toEqual(events[0]);
    expect(decoded[1]).toEqual(events[1]);
  });

  test('handles missing metadata and tenantId', () => {
    // Create event without metadata and tenantId fields
    const events = [
      createEvent({}),
    ];

    const encoded = serializer.encode(events);
    const decoded = serializer.decode<StoredEvent[]>(encoded);

    // Decoder provides default 'default' tenantId
    expect(decoded[0]!.tenantId).toBe('default');
  });

  test('encodes and decodes empty batches', () => {
    const encoded = serializer.encode<StoredEvent[]>([]);
    const decoded = serializer.decode<StoredEvent[]>(encoded);

    expect(decoded).toEqual([]);
  });

  test('throws on invalid magic', () => {
    const bad = new Uint8Array([0, 1, 2, 3, 0, 1, 0, 0, 0, 0, 0, 0]);
    expect(() => serializer.decode(bad)).toThrow('Invalid batch payload magic');
  });

  test('throws when encoding non-event data', () => {
    expect(() => serializer.encode({} as unknown as StoredEvent[])).toThrow(
      'BinaryEventBatchSerializer only supports event batches'
    );
    expect(() => serializer.encode([{}] as unknown as StoredEvent[])).toThrow(
      'BinaryEventBatchSerializer expects StoredEvent[]'
    );
  });
});
