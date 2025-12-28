/**
 * Test fixtures and data generators for DST testing.
 *
 * Provides seeded random generators for creating test data
 * that is deterministic based on the seed.
 *
 * @example
 * ```ts
 * const random = new SeededRandom(12345);
 * const events = generateRandomEvents(random, 100, 5);
 * // Always generates the same 100 events across 5 streams
 * ```
 */

import type { InputEvent } from '../../src/application/event-store';
import type { StoredEvent } from '../../src/domain/events/stored-event';
import type { SeededRandom } from './seeded-random';

/**
 * Common event types for testing.
 */
export const EVENT_TYPES = {
  // User events
  UserCreated: 'UserCreated',
  UserUpdated: 'UserUpdated',
  UserDeleted: 'UserDeleted',

  // Order events
  OrderCreated: 'OrderCreated',
  OrderCompleted: 'OrderCompleted',
  OrderCancelled: 'OrderCancelled',

  // Generic events
  ItemAdded: 'ItemAdded',
  ItemRemoved: 'ItemRemoved',
  StateChanged: 'StateChanged',
} as const;

export type EventType = (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES];

/**
 * Generate a random InputEvent.
 *
 * @param random - Seeded random generator
 * @param options - Optional overrides
 */
export function generateRandomEvent(
  random: SeededRandom,
  options: {
    type?: string;
    streamId?: string;
    includeMetadata?: boolean;
  } = {}
): InputEvent {
  const eventTypes = Object.values(EVENT_TYPES);
  const type = options.type ?? random.choice(eventTypes);

  // Generate data based on event type
  let data: Record<string, unknown>;

  switch (type) {
    case EVENT_TYPES.UserCreated:
      data = {
        id: random.uuid(),
        name: `User ${random.string(8)}`,
        email: `${random.string(8)}@example.com`,
        createdAt: Date.now(),
      };
      break;

    case EVENT_TYPES.UserUpdated:
      data = {
        id: random.uuid(),
        changes: {
          name: `Updated ${random.string(8)}`,
        },
        updatedAt: Date.now(),
      };
      break;

    case EVENT_TYPES.UserDeleted:
      data = {
        id: random.uuid(),
        deletedAt: Date.now(),
      };
      break;

    case EVENT_TYPES.OrderCreated:
      data = {
        id: random.uuid(),
        customerId: random.uuid(),
        items: Array.from({ length: random.int(1, 5) }, () => ({
          productId: random.uuid(),
          quantity: random.int(1, 10),
          price: random.int(100, 10000),
        })),
        total: random.int(1000, 100000),
      };
      break;

    case EVENT_TYPES.OrderCompleted:
      data = {
        id: random.uuid(),
        completedAt: Date.now(),
        amount: random.int(1000, 100000),
      };
      break;

    case EVENT_TYPES.OrderCancelled:
      data = {
        id: random.uuid(),
        reason: random.choice(['customer_request', 'out_of_stock', 'payment_failed']),
        cancelledAt: Date.now(),
      };
      break;

    default:
      data = {
        id: random.uuid(),
        value: random.int(1, 1000),
        timestamp: Date.now(),
      };
  }

  const event: InputEvent = { type, data };

  if (options.includeMetadata ?? random.bool(0.3)) {
    event.metadata = {
      correlationId: random.uuid(),
      causationId: random.uuid(),
      userId: random.uuid(),
      timestamp: Date.now(),
    };
  }

  return event;
}

/**
 * Generate multiple random events.
 *
 * @param random - Seeded random generator
 * @param count - Number of events to generate
 * @param options - Optional configuration
 */
export function generateRandomEvents(
  random: SeededRandom,
  count: number,
  options: {
    types?: string[];
    includeMetadata?: boolean;
  } = {}
): InputEvent[] {
  const events: InputEvent[] = [];

  for (let i = 0; i < count; i++) {
    const type = options.types ? random.choice(options.types) : undefined;
    const eventOptions: { type?: string; includeMetadata?: boolean } = {};
    if (type !== undefined) {
      eventOptions.type = type;
    }
    if (options.includeMetadata !== undefined) {
      eventOptions.includeMetadata = options.includeMetadata;
    }
    events.push(generateRandomEvent(random, eventOptions));
  }

  return events;
}

/**
 * Generate events distributed across multiple streams.
 *
 * @param random - Seeded random generator
 * @param totalEvents - Total number of events
 * @param streamCount - Number of streams to distribute across
 * @returns Map of streamId to events
 */
export function generateEventsAcrossStreams(
  random: SeededRandom,
  totalEvents: number,
  streamCount: number
): Map<string, InputEvent[]> {
  const streams = new Map<string, InputEvent[]>();

  // Generate stream IDs
  const streamIds: string[] = [];
  for (let i = 0; i < streamCount; i++) {
    streamIds.push(`stream-${random.string(8)}`);
    streams.set(streamIds[i]!, []);
  }

  // Distribute events
  for (let i = 0; i < totalEvents; i++) {
    const streamId = random.choice(streamIds);
    const event = generateRandomEvent(random);
    streams.get(streamId)!.push(event);
  }

  return streams;
}

/**
 * Generate a workload of operations (appends and reads).
 */
export interface WorkloadOperation {
  type: 'append' | 'read' | 'readGlobal';
  streamId?: string;
  events?: InputEvent[];
  fromPosition?: number;
}

/**
 * Generate a random workload of operations.
 *
 * @param random - Seeded random generator
 * @param totalOps - Total number of operations
 * @param config - Workload configuration
 */
export function generateRandomWorkload(
  random: SeededRandom,
  totalOps: number,
  config: {
    appendProbability?: number; // Default: 0.7
    streamCount?: number; // Default: 5
    eventsPerAppend?: { min: number; max: number }; // Default: 1-5
  } = {}
): WorkloadOperation[] {
  const {
    appendProbability = 0.7,
    streamCount = 5,
    eventsPerAppend = { min: 1, max: 5 },
  } = config;

  // Generate stream IDs
  const streamIds: string[] = [];
  for (let i = 0; i < streamCount; i++) {
    streamIds.push(`stream-${random.string(8)}`);
  }

  const operations: WorkloadOperation[] = [];
  let globalPosition = 0;

  for (let i = 0; i < totalOps; i++) {
    if (random.bool(appendProbability)) {
      // Append operation
      const eventCount = random.int(eventsPerAppend.min, eventsPerAppend.max);
      const events = generateRandomEvents(random, eventCount);
      operations.push({
        type: 'append',
        streamId: random.choice(streamIds),
        events,
      });
      globalPosition += eventCount;
    } else {
      // Read operation
      if (random.bool(0.5) && globalPosition > 0) {
        // Read global
        const fromPos = random.int(0, globalPosition - 1);
        operations.push({
          type: 'readGlobal',
          fromPosition: fromPos,
        });
      } else {
        // Read stream
        operations.push({
          type: 'read',
          streamId: random.choice(streamIds),
        });
      }
    }
  }

  return operations;
}

/**
 * Create a StoredEvent from InputEvent for testing.
 */
export function createStoredEvent(
  input: InputEvent,
  streamId: string,
  revision: number,
  globalPosition: number,
  timestamp: number,
  tenantId: string = 'default'
): StoredEvent {
  return {
    streamId,
    type: input.type,
    data: input.data,
    metadata: input.metadata,
    revision,
    globalPosition,
    timestamp,
    tenantId,
  };
}

/**
 * Generate stored events for testing projections.
 */
export function generateStoredEvents(
  random: SeededRandom,
  count: number,
  streamId: string = 'test-stream'
): StoredEvent[] {
  const events: StoredEvent[] = [];
  const baseTimestamp = Date.now();

  for (let i = 0; i < count; i++) {
    const input = generateRandomEvent(random);
    events.push(
      createStoredEvent(
        input,
        streamId,
        i,
        i,
        baseTimestamp + i * 1000
      )
    );
  }

  return events;
}

/**
 * Common test scenarios for projections.
 */
export const ProjectionTestScenarios = {
  /**
   * Simple sequence of user events.
   */
  userLifecycle: (random: SeededRandom): StoredEvent[] => {
    const userId = random.uuid();
    const baseTimestamp = Date.now();

    return [
      createStoredEvent(
        {
          type: EVENT_TYPES.UserCreated,
          data: { id: userId, name: 'Test User', email: 'test@example.com' },
        },
        `user-${userId}`,
        0,
        0,
        baseTimestamp
      ),
      createStoredEvent(
        {
          type: EVENT_TYPES.UserUpdated,
          data: { id: userId, changes: { name: 'Updated User' } },
        },
        `user-${userId}`,
        1,
        1,
        baseTimestamp + 1000
      ),
      createStoredEvent(
        {
          type: EVENT_TYPES.UserDeleted,
          data: { id: userId },
        },
        `user-${userId}`,
        2,
        2,
        baseTimestamp + 2000
      ),
    ];
  },

  /**
   * Order events for aggregation testing.
   */
  orderSequence: (random: SeededRandom, count: number): StoredEvent[] => {
    const events: StoredEvent[] = [];
    const baseTimestamp = Date.now();

    for (let i = 0; i < count; i++) {
      const orderId = random.uuid();
      const amount = random.int(1000, 10000);

      events.push(
        createStoredEvent(
          {
            type: EVENT_TYPES.OrderCreated,
            data: { id: orderId, total: amount },
          },
          `order-${orderId}`,
          0,
          i * 2,
          baseTimestamp + i * 2000
        )
      );

      if (random.bool(0.8)) {
        events.push(
          createStoredEvent(
            {
              type: EVENT_TYPES.OrderCompleted,
              data: { id: orderId, amount },
            },
            `order-${orderId}`,
            1,
            i * 2 + 1,
            baseTimestamp + i * 2000 + 1000
          )
        );
      }
    }

    return events;
  },
};
