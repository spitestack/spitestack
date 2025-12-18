/**
 * Projection System Tests
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { SpiteDbNapi } from '../index.js';
import { projection, ProjectionRunner } from '../js/index';
import { randomUUID } from 'crypto';
import { mkdtemp, rm } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';

describe('Projection System', () => {
  let tempDir: string;
  let db: SpiteDbNapi;

  beforeAll(async () => {
    // Create temp directory for test files
    tempDir = await mkdtemp(join(tmpdir(), 'spitedb-test-'));
  });

  afterAll(async () => {
    // Clean up temp directory
    await rm(tempDir, { recursive: true, force: true });
  });

  test('should open database and append events', async () => {
    db = await SpiteDbNapi.open(join(tempDir, 'events.db'));

    const result = await db.append(
      'user-123',
      randomUUID(),
      0, // Stream must not exist
      [Buffer.from(JSON.stringify({ type: 'UserCreated', name: 'Alice' }))]
    );

    expect(result.firstPos).toBe(1);
    expect(result.lastPos).toBe(1);
    expect(result.firstRev).toBe(1);
    expect(result.lastRev).toBe(1);
  });

  test('should append more events', async () => {
    const result = await db.append(
      'user-123',
      randomUUID(),
      1, // Expected revision
      [
        Buffer.from(JSON.stringify({ type: 'UserLoggedIn' })),
        Buffer.from(JSON.stringify({ type: 'UserLoggedIn' })),
      ]
    );

    expect(result.firstRev).toBe(2);
    expect(result.lastRev).toBe(3);
  });

  test('should read events from stream', async () => {
    const events = await db.readStream('user-123', 0, 100);

    expect(events.length).toBe(3);
    expect(events[0].streamRev).toBe(1);
    expect(events[1].streamRev).toBe(2);
    expect(events[2].streamRev).toBe(3);
  });

  test('should read events from global log', async () => {
    // GlobalPos starts at 1, not 0
    const events = await db.readGlobal(1, 100);

    expect(events.length).toBe(3);
    expect(events[0].globalPos).toBe(1);
    expect(events[1].globalPos).toBe(2);
    expect(events[2].globalPos).toBe(3);
  });

  test('should initialize projections', async () => {
    await db.initProjections(join(tempDir, 'projections.db'));
  });

  test('should register a projection', async () => {
    await db.registerProjection('user_stats', [
      { name: 'user_id', colType: 'text', primaryKey: true, nullable: false },
      { name: 'login_count', colType: 'integer', primaryKey: false, nullable: false, defaultValue: '0' },
    ]);
  });

  test('should apply projection batch', async () => {
    await db.applyProjectionBatch({
      projectionName: 'user_stats',
      operations: [
        {
          opType: 'upsert',
          key: 'user-123',
          value: JSON.stringify({ login_count: 2 }),
        },
      ],
      lastGlobalPos: 3,
    });
  });

  test('should read projection row', () => {
    const json = db.readProjectionRow('user_stats', 'user-123');
    expect(json).not.toBeNull();

    const row = JSON.parse(json!);
    expect(row.user_id).toBe('user-123');
    expect(row.login_count).toBe(2);
  });

  test('should get projection checkpoint', async () => {
    const checkpoint = await db.getProjectionCheckpoint('user_stats');
    expect(checkpoint).toBe(3);
  });

  test('should return null for non-existent row', () => {
    const json = db.readProjectionRow('user_stats', 'non-existent');
    expect(json).toBeNull();
  });
});

describe('Projection Definition', () => {
  test('should create projection definition', () => {
    const userStats = projection('user_stats', {
      schema: {
        user_id: { type: 'text', primaryKey: true },
        login_count: 'integer',
        total_spent: 'real',
      },
      apply(event, table) {
        const data = JSON.parse(event.data.toString());
        if (data.type === 'UserCreated') {
          table[event.streamId] = { login_count: 0, total_spent: 0 };
        }
      },
    });

    expect(userStats.name).toBe('user_stats');
    expect(userStats.options.schema.user_id).toEqual({ type: 'text', primaryKey: true });
    expect(userStats.options.schema.login_count).toBe('integer');
  });
});

describe('Magic Proxy', () => {
  let db: SpiteDbNapi;
  let tempDir: string;

  beforeAll(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'spitedb-proxy-test-'));
    db = await SpiteDbNapi.open(join(tempDir, 'events.db'));
    await db.initProjections(join(tempDir, 'projections.db'));
    await db.registerProjection('counters', [
      { name: 'id', colType: 'text', primaryKey: true, nullable: false },
      { name: 'count', colType: 'integer', primaryKey: false, nullable: false },
    ]);
  });

  afterAll(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  test('proxy creates row on assignment', async () => {
    const { createProjectionProxy } = await import('../js/proxy');

    const { proxy, flush } = createProjectionProxy<{ id?: string; count: number }>('counters', 'id', db);

    // Create a new row
    proxy['counter-1'] = { count: 0 };

    const ops = flush();
    expect(ops.length).toBe(1);
    expect(ops[0].opType).toBe('upsert');
    expect(ops[0].key).toBe('counter-1');
    expect(JSON.parse(ops[0].value!).count).toBe(0);
  });

  test('proxy tracks increments', async () => {
    const { createProjectionProxy } = await import('../js/proxy');

    // First, insert initial data
    await db.applyProjectionBatch({
      projectionName: 'counters',
      operations: [{ opType: 'upsert', key: 'counter-2', value: JSON.stringify({ count: 5 }) }],
      lastGlobalPos: 1,
    });

    const { proxy, flush } = createProjectionProxy<{ id?: string; count: number }>('counters', 'id', db);

    // Read and increment
    const row = proxy['counter-2'];
    expect(row).toBeDefined();
    expect(row!.count).toBe(5);

    // Increment the count
    row!.count++;

    const ops = flush();
    expect(ops.length).toBe(1);
    expect(ops[0].opType).toBe('upsert');
    expect(JSON.parse(ops[0].value!).count).toBe(6);
  });

  test('proxy tracks deletes', async () => {
    const { createProjectionProxy } = await import('../js/proxy');

    const { proxy, flush } = createProjectionProxy<{ id?: string; count: number }>('counters', 'id', db);

    // Delete a row
    delete proxy['counter-2'];

    const ops = flush();
    expect(ops.length).toBe(1);
    expect(ops[0].opType).toBe('delete');
    expect(ops[0].key).toBe('counter-2');
  });
});
