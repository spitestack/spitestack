/**
 * Property-based tests for EventStore.
 *
 * These tests use fast-check to generate random operation sequences
 * and verify that invariants hold across all possible inputs.
 *
 * Run with specific seed: SEED=12345 bun test test/property/event-store.property.test.ts
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import * as fc from 'fast-check';
import {
  createTestEnvironment,
  createTestEventStore,
  cleanupTestEnvironment,
  type TestEnvironment,
} from '../setup/test-helpers';
import { EventStoreInvariants, checkInvariants } from '../setup/dst-scenarios';
import { getSeedFromEnv } from '../setup/seeded-random';

describe('Property Tests: EventStore', () => {
  let env: TestEnvironment;

  beforeEach(() => {
    env = createTestEnvironment(getSeedFromEnv());
  });

  afterEach(async () => {
    await cleanupTestEnvironment(env);
  });

  test('no duplicate global positions across any operation sequence', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.record({
            streamId: fc.stringOf(fc.constantFrom('a', 'b', 'c'), { minLength: 1, maxLength: 1 }),
            eventCount: fc.integer({ min: 1, max: 5 }),
          }),
          { minLength: 1, maxLength: 20 }
        ),
        async (operations) => {
          const testEnv = createTestEnvironment(Date.now());
          const store = await createTestEventStore(testEnv);

          try {
            for (const op of operations) {
              const events = Array.from({ length: op.eventCount }, (_, i) => ({
                type: 'TestEvent',
                data: { index: i },
              }));
              await store.append(`stream-${op.streamId}`, events);
            }
            await store.flush();

            const allEvents = await store.readGlobal(0);
            checkInvariants(allEvents, [
              EventStoreInvariants.noDuplicatePositions,
              EventStoreInvariants.monotonicPositions,
              EventStoreInvariants.sequentialRevisions,
            ]);
          } finally {
            await store.close();
            await cleanupTestEnvironment(testEnv);
          }
        }
      ),
      { numRuns: 50, seed: getSeedFromEnv() }
    );
  });

  test('stream revisions are always sequential within a stream', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.record({
            streamId: fc.constantFrom('stream-1', 'stream-2', 'stream-3'),
            eventCount: fc.integer({ min: 1, max: 3 }),
          }),
          { minLength: 5, maxLength: 30 }
        ),
        async (operations) => {
          const testEnv = createTestEnvironment(Date.now());
          const store = await createTestEventStore(testEnv);

          try {
            for (const op of operations) {
              const events = Array.from({ length: op.eventCount }, (_, i) => ({
                type: 'TestEvent',
                data: { stream: op.streamId, index: i },
              }));
              await store.append(op.streamId, events);
            }
            await store.flush();

            // Check each stream individually
            for (const streamId of ['stream-1', 'stream-2', 'stream-3']) {
              const streamEvents = await store.readStream(streamId);
              if (streamEvents.length > 0) {
                // Verify revisions start at 0 and increment by 1
                for (let i = 0; i < streamEvents.length; i++) {
                  expect(streamEvents[i]!.revision).toBe(i);
                }
              }
            }
          } finally {
            await store.close();
            await cleanupTestEnvironment(testEnv);
          }
        }
      ),
      { numRuns: 50, seed: getSeedFromEnv() }
    );
  });

  test('global positions are always monotonically increasing', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.record({
            streamId: fc.constantFrom('s1', 's2'),
            eventCount: fc.integer({ min: 1, max: 4 }),
            flushAfter: fc.boolean(),
          }),
          { minLength: 3, maxLength: 15 }
        ),
        async (operations) => {
          const testEnv = createTestEnvironment(Date.now());
          const store = await createTestEventStore(testEnv);

          try {
            for (const op of operations) {
              const events = Array.from({ length: op.eventCount }, (_, i) => ({
                type: 'Event',
                data: { i },
              }));
              await store.append(op.streamId, events);
              if (op.flushAfter) {
                await store.flush();
              }
            }
            await store.flush();

            const allEvents = await store.readGlobal(0);
            checkInvariants(allEvents, [EventStoreInvariants.monotonicPositions]);
          } finally {
            await store.close();
            await cleanupTestEnvironment(testEnv);
          }
        }
      ),
      { numRuns: 50, seed: getSeedFromEnv() }
    );
  });

  test('append is atomic - all events succeed or none', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.record({
            streamId: fc.constant('test-stream'),
            eventCount: fc.integer({ min: 1, max: 5 }),
            expectedRevision: fc.option(fc.integer({ min: -1, max: 10 }), { nil: undefined }),
          }),
          { minLength: 1, maxLength: 10 }
        ),
        async (operations) => {
          const testEnv = createTestEnvironment(Date.now());
          const store = await createTestEventStore(testEnv);

          try {
            let expectedNextRevision = 0;

            for (const op of operations) {
              const events = Array.from({ length: op.eventCount }, (_, i) => ({
                type: 'TestEvent',
                data: { index: i },
              }));

              try {
                const result = await store.append(op.streamId, events, {
                  expectedRevision: op.expectedRevision,
                });

                // If append succeeded, all events should be present
                const streamEvents = await store.readStream(op.streamId);
                expect(streamEvents.length).toBe(result.streamRevision + 1);

                expectedNextRevision = result.streamRevision + 1;
              } catch {
                // If append failed (e.g., concurrency error), no events should be added
                const streamEvents = await store.readStream(op.streamId);
                expect(streamEvents.length).toBe(expectedNextRevision);
              }
            }

            await store.flush();
          } finally {
            await store.close();
            await cleanupTestEnvironment(testEnv);
          }
        }
      ),
      { numRuns: 30, seed: getSeedFromEnv() }
    );
  });

  test('read after write consistency', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.record({
            streamId: fc.constantFrom('alpha', 'beta', 'gamma'),
            events: fc.array(
              fc.record({
                type: fc.constantFrom('Created', 'Updated', 'Deleted'),
                value: fc.integer({ min: 0, max: 1000 }),
              }),
              { minLength: 1, maxLength: 3 }
            ),
          }),
          { minLength: 1, maxLength: 10 }
        ),
        async (batches) => {
          const testEnv = createTestEnvironment(Date.now());
          const store = await createTestEventStore(testEnv);

          try {
            const expectedEvents: Map<string, Array<{ type: string; data: unknown }>> = new Map();

            for (const batch of batches) {
              const events = batch.events.map((e) => ({
                type: e.type,
                data: { value: e.value },
              }));

              await store.append(batch.streamId, events);

              // Track expected events
              const existing = expectedEvents.get(batch.streamId) ?? [];
              existing.push(...events);
              expectedEvents.set(batch.streamId, existing);
            }

            await store.flush();

            // Verify each stream has exactly the expected events
            for (const [streamId, expected] of expectedEvents) {
              const actual = await store.readStream(streamId);
              expect(actual.length).toBe(expected.length);

              for (let i = 0; i < expected.length; i++) {
                expect(actual[i]!.type).toBe(expected[i]!.type);
                expect(actual[i]!.data).toEqual(expected[i]!.data);
              }
            }
          } finally {
            await store.close();
            await cleanupTestEnvironment(testEnv);
          }
        }
      ),
      { numRuns: 50, seed: getSeedFromEnv() }
    );
  });
});
