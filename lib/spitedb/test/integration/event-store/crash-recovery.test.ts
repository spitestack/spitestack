import { describe, test, expect, beforeEach } from 'bun:test';
import { EventStore } from '../../../src/application/event-store';
import { SegmentManager } from '../../../src/infrastructure/storage/segments/segment-manager';
import { SegmentIndexFile, INDEX_MAGIC, INDEX_VERSION, INDEX_HEADER_SIZE } from '../../../src/infrastructure/storage/segments/segment-index-file';
import { SimulatedFileSystem } from '../../../src/testing/simulated-filesystem';
import { SimulatedClock } from '../../../src/testing/simulated-clock';
import { MsgpackSerializer } from '../../../src/infrastructure/serialization/msgpack-serializer';
import { ZstdCompressor } from '../../../src/infrastructure/serialization/zstd-compressor';

/**
 * Integration tests for the Phase 2.5 indexing architecture crash recovery.
 *
 * Tests scenarios:
 * 1. Missing .idx file → rebuild from .log
 * 2. Corrupted .idx file (bad checksum) → rebuild from .log
 * 3. Partial/truncated .idx file → rebuild from .log
 * 4. Power loss during .idx write (tmp file exists) → rebuild from .log
 * 5. StreamMap correctly rebuilt on startup
 * 6. Multiple segments with mixed index states
 */
describe('Index Crash Recovery', () => {
  let fs: SimulatedFileSystem;
  let clock: SimulatedClock;
  let serializer: MsgpackSerializer;
  let compressor: ZstdCompressor;

  beforeEach(() => {
    fs = new SimulatedFileSystem();
    clock = new SimulatedClock();
    serializer = new MsgpackSerializer();
    compressor = new ZstdCompressor();
  });

  function createStore(): EventStore {
    return new EventStore({
      fs,
      serializer,
      compressor,
      clock,
      autoFlushCount: 0,
    });
  }

  describe('missing .idx file recovery', () => {
    test('should rebuild index from .log when .idx file is missing', async () => {
      // Create store and write some events
      const store1 = createStore();
      await store1.open('/data/events');

      await store1.append('stream-a', [
        { type: 'Event1', data: { value: 1 } },
        { type: 'Event2', data: { value: 2 } },
      ]);
      await store1.append('stream-b', [
        { type: 'Event3', data: { value: 3 } },
      ]);
      await store1.flush();
      await store1.close();

      // Verify .idx file was created
      expect(fs.getFileContent('/data/events/segment-00000000.idx')).toBeDefined();

      // Delete the .idx file (simulating corruption or disk issue)
      await fs.unlink('/data/events/segment-00000000.idx');
      expect(fs.getFileContent('/data/events/segment-00000000.idx')).toBeUndefined();

      // Reopen store - should rebuild index from .log
      const store2 = createStore();
      await store2.open('/data/events');

      // Verify events are still readable
      const eventsA = await store2.readStream('stream-a');
      expect(eventsA).toHaveLength(2);
      expect(eventsA[0]!.type).toBe('Event1');
      expect(eventsA[1]!.type).toBe('Event2');

      const eventsB = await store2.readStream('stream-b');
      expect(eventsB).toHaveLength(1);
      expect(eventsB[0]!.type).toBe('Event3');

      // Verify .idx file was rebuilt
      expect(fs.getFileContent('/data/events/segment-00000000.idx')).toBeDefined();

      await store2.close();
    });

    test('should correctly rebuild StreamMap from recovered index', async () => {
      // Create store and write events across multiple streams
      const store1 = createStore();
      await store1.open('/data/events');

      await store1.append('user-1', [
        { type: 'Created', data: {} },
        { type: 'Updated', data: {} },
      ]);
      await store1.append('user-2', [
        { type: 'Created', data: {} },
      ]);
      await store1.append('user-1', [
        { type: 'Deleted', data: {} },
      ]);
      await store1.flush();
      await store1.close();

      // Delete .idx file
      await fs.unlink('/data/events/segment-00000000.idx');

      // Reopen store
      const store2 = createStore();
      await store2.open('/data/events');

      // Verify stream revisions are correct (StreamMap rebuilt)
      expect(store2.getStreamRevision('user-1')).toBe(2); // Events 0, 1, 2
      expect(store2.getStreamRevision('user-2')).toBe(0); // Event 0
      expect(store2.getStreamRevision('nonexistent')).toBe(-1);

      await store2.close();
    });
  });

  describe('corrupted .idx file recovery', () => {
    test('should detect and recover from corrupted checksum', async () => {
      // Create store and write events
      const store1 = createStore();
      await store1.open('/data/events');

      await store1.append('stream-a', [
        { type: 'Event1', data: { value: 100 } },
      ]);
      await store1.flush();
      await store1.close();

      // Corrupt the .idx file by modifying bytes
      const idxContent = fs.getFileContent('/data/events/segment-00000000.idx')!;
      const corrupted = new Uint8Array(idxContent);
      // Corrupt some data bytes (after the header)
      if (corrupted.length > INDEX_HEADER_SIZE + 10) {
        corrupted[INDEX_HEADER_SIZE + 5] = corrupted[INDEX_HEADER_SIZE + 5]! ^ 0xFF;
        corrupted[INDEX_HEADER_SIZE + 6] = corrupted[INDEX_HEADER_SIZE + 6]! ^ 0xFF;
      }
      fs.setFileContent('/data/events/segment-00000000.idx', corrupted);

      // Reopen store - should detect corruption and rebuild
      const store2 = createStore();
      await store2.open('/data/events');

      // Verify events are still readable (index was rebuilt)
      const events = await store2.readStream('stream-a');
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toEqual({ value: 100 });

      await store2.close();
    });

    test('should detect and recover from corrupted magic number', async () => {
      // Create store and write events
      const store1 = createStore();
      await store1.open('/data/events');

      await store1.append('test-stream', [
        { type: 'TestEvent', data: { msg: 'hello' } },
      ]);
      await store1.flush();
      await store1.close();

      // Corrupt the magic number in the .idx file
      const idxContent = fs.getFileContent('/data/events/segment-00000000.idx')!;
      const corrupted = new Uint8Array(idxContent);
      corrupted[0] = 0x00; // Corrupt first byte of magic
      corrupted[1] = 0x00;
      fs.setFileContent('/data/events/segment-00000000.idx', corrupted);

      // Reopen store - should detect bad magic and rebuild
      const store2 = createStore();
      await store2.open('/data/events');

      // Verify events are still readable
      const events = await store2.readStream('test-stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toEqual({ msg: 'hello' });

      await store2.close();
    });

    test('should detect and recover from truncated .idx file', async () => {
      // Create store and write events
      const store1 = createStore();
      await store1.open('/data/events');

      await store1.append('stream-x', [
        { type: 'Event', data: { id: 1 } },
        { type: 'Event', data: { id: 2 } },
      ]);
      await store1.flush();
      await store1.close();

      // Truncate the .idx file to half its size
      const idxContent = fs.getFileContent('/data/events/segment-00000000.idx')!;
      const truncated = idxContent.slice(0, Math.floor(idxContent.length / 2));
      fs.setFileContent('/data/events/segment-00000000.idx', truncated);

      // Reopen store - should detect truncation and rebuild
      const store2 = createStore();
      await store2.open('/data/events');

      // Verify events are still readable
      const events = await store2.readStream('stream-x');
      expect(events).toHaveLength(2);

      await store2.close();
    });
  });

  describe('power loss during .idx write', () => {
    test('should handle .idx.tmp file left behind', async () => {
      // Create store and write events
      const store1 = createStore();
      await store1.open('/data/events');

      await store1.append('my-stream', [
        { type: 'ImportantEvent', data: { key: 'value' } },
      ]);
      await store1.flush();
      await store1.close();

      // Simulate crash during .idx write by:
      // 1. Keeping the .log file
      // 2. Leaving a partial .idx.tmp file
      // 3. Removing the actual .idx file
      const idxContent = fs.getFileContent('/data/events/segment-00000000.idx')!;
      const partialTmp = idxContent.slice(0, INDEX_HEADER_SIZE + 5); // Partial write

      await fs.unlink('/data/events/segment-00000000.idx');
      fs.setFileContent('/data/events/segment-00000000.idx.tmp', partialTmp);

      // Reopen store - should ignore .tmp and rebuild from .log
      const store2 = createStore();
      await store2.open('/data/events');

      // Verify events are readable
      const events = await store2.readStream('my-stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toEqual({ key: 'value' });

      await store2.close();
    });
  });

  describe('multiple segment recovery', () => {
    test('should recover each segment independently', async () => {
      // Create store with small segment size to force rotation
      const smallSegmentStore = new EventStore({
        fs,
        serializer,
        compressor,
        clock,
        autoFlushCount: 0,
        maxSegmentSize: 1024, // Very small to force rotation
      });
      await smallSegmentStore.open('/data/events');

      // Write enough data to create multiple segments
      for (let i = 0; i < 10; i++) {
        await smallSegmentStore.append(`stream-${i % 3}`, [
          { type: 'Event', data: { iteration: i, padding: 'x'.repeat(200) } },
        ]);
        await smallSegmentStore.flush();
      }
      await smallSegmentStore.close();

      // Check that multiple segments were created
      const files = await fs.readdir('/data/events');
      const logFiles = files.filter(f => f.endsWith('.log'));
      const idxFiles = files.filter(f => f.endsWith('.idx'));

      expect(logFiles.length).toBeGreaterThanOrEqual(1);
      expect(idxFiles.length).toBe(logFiles.length);

      // Corrupt/delete only some .idx files
      if (idxFiles.length >= 2) {
        // Delete the first .idx file
        await fs.unlink(`/data/events/${idxFiles[0]}`);
      }

      // Reopen store
      const store2 = createStore();
      await store2.open('/data/events');

      // All events should still be readable (indexes rebuilt as needed)
      const allEvents: string[] = [];
      for await (const event of store2.streamGlobal()) {
        allEvents.push(event.type);
      }

      expect(allEvents.length).toBe(10);

      await store2.close();
    });

    test('should rebuild StreamMap from multiple segments', async () => {
      // Create store with small segment size
      const smallSegmentStore = new EventStore({
        fs,
        serializer,
        compressor,
        clock,
        autoFlushCount: 0,
        maxSegmentSize: 512,
      });
      await smallSegmentStore.open('/data/events');

      // Write to same stream across multiple flushes
      await smallSegmentStore.append('persistent-stream', [
        { type: 'First', data: { padding: 'x'.repeat(200) } },
      ]);
      await smallSegmentStore.flush();

      await smallSegmentStore.append('persistent-stream', [
        { type: 'Second', data: { padding: 'x'.repeat(200) } },
      ]);
      await smallSegmentStore.flush();

      await smallSegmentStore.append('persistent-stream', [
        { type: 'Third', data: { padding: 'x'.repeat(200) } },
      ]);
      await smallSegmentStore.flush();

      await smallSegmentStore.close();

      // Delete all .idx files
      const files = await fs.readdir('/data/events');
      for (const file of files) {
        if (file.endsWith('.idx')) {
          await fs.unlink(`/data/events/${file}`);
        }
      }

      // Reopen and verify
      const store2 = createStore();
      await store2.open('/data/events');

      // Verify stream revision accounts for all events
      expect(store2.getStreamRevision('persistent-stream')).toBe(2); // Revisions 0, 1, 2

      // Verify all events readable in order
      const events = await store2.readStream('persistent-stream');
      expect(events).toHaveLength(3);
      expect(events[0]!.type).toBe('First');
      expect(events[1]!.type).toBe('Second');
      expect(events[2]!.type).toBe('Third');

      await store2.close();
    });
  });

  describe('crash during append', () => {
    test('should handle crash during append (unflushed)', async () => {
      const store1 = createStore();
      await store1.open('/data/events');

      // Flush one batch
      await store1.append('my-stream', [
        { type: 'Persisted', data: { safe: true } },
      ]);
      await store1.flush();

      // Append but don't flush
      await store1.append('my-stream', [
        { type: 'Lost', data: { safe: false } },
      ]);

      // Simulate crash
      fs.crash();

      // Reopen
      const store2 = createStore();
      await store2.open('/data/events');

      // Only persisted event should be visible
      const events = await store2.readStream('my-stream');
      expect(events).toHaveLength(1);
      expect(events[0]!.type).toBe('Persisted');

      await store2.close();
    });

    test('should maintain correct revision after crash recovery', async () => {
      const store1 = createStore();
      await store1.open('/data/events');

      // Write and flush 3 events
      await store1.append('counter-stream', [
        { type: 'Inc', data: {} },
        { type: 'Inc', data: {} },
        { type: 'Inc', data: {} },
      ]);
      await store1.flush();

      // Append 2 more but don't flush
      await store1.append('counter-stream', [
        { type: 'Inc', data: {} },
        { type: 'Inc', data: {} },
      ]);

      // Crash (losing the unflushed events)
      fs.crash();

      // Reopen
      const store2 = createStore();
      await store2.open('/data/events');

      // Revision should be 2 (three events: 0, 1, 2)
      expect(store2.getStreamRevision('counter-stream')).toBe(2);

      // Should be able to append at correct revision
      await store2.append('counter-stream', [{ type: 'Inc', data: {} }], {
        expectedRevision: 2,
      });

      expect(store2.getStreamRevision('counter-stream')).toBe(3);

      await store2.close();
    });
  });

  describe('SegmentIndexFile direct tests', () => {
    test('should load and validate index file correctly', async () => {
      // Create store and write events
      const store = createStore();
      await store.open('/data/events');

      await store.append('stream-1', [
        { type: 'E1', data: {} },
        { type: 'E2', data: {} },
      ]);
      await store.append('stream-2', [
        { type: 'E3', data: {} },
      ]);
      await store.flush();
      await store.close();

      // Load and verify index file directly
      const indexFile = new SegmentIndexFile();
      await indexFile.load(fs, '/data/events/segment-00000000.idx');

      expect(indexFile.getEntryCount()).toBe(3);
      expect(indexFile.validateChecksum()).toBe(true);

      const stream1Entries = indexFile.findByStream('stream-1');
      expect(stream1Entries).toHaveLength(2);
      expect(stream1Entries[0]!.revision).toBe(0);
      expect(stream1Entries[1]!.revision).toBe(1);

      const stream2Entries = indexFile.findByStream('stream-2');
      expect(stream2Entries).toHaveLength(1);
    });

    test('should detect invalid checksum and throw IndexCorruptedError', async () => {
      // Create a valid index file first
      const store = createStore();
      await store.open('/data/events');
      await store.append('test', [{ type: 'E', data: {} }]);
      await store.flush();
      await store.close();

      // Corrupt a data byte
      const content = fs.getFileContent('/data/events/segment-00000000.idx')!;
      const corrupted = new Uint8Array(content);
      corrupted[INDEX_HEADER_SIZE + 2] = corrupted[INDEX_HEADER_SIZE + 2]! ^ 0xFF;
      fs.setFileContent('/data/events/segment-00000000.idx', corrupted);

      // Load should throw IndexCorruptedError due to checksum mismatch
      const indexFile = new SegmentIndexFile();
      await expect(indexFile.load(fs, '/data/events/segment-00000000.idx'))
        .rejects.toThrow('Checksum mismatch');
    });
  });

  describe('unicode stream IDs', () => {
    test('should correctly recover indexes with unicode stream IDs', async () => {
      const store1 = createStore();
      await store1.open('/data/events');

      // Use unicode stream IDs
      await store1.append('用户-123', [
        { type: 'Created', data: { name: '张三' } },
      ]);
      await store1.append('пользователь-456', [
        { type: 'Registered', data: { name: 'Иван' } },
      ]);
      await store1.flush();
      await store1.close();

      // Delete .idx file
      await fs.unlink('/data/events/segment-00000000.idx');

      // Reopen
      const store2 = createStore();
      await store2.open('/data/events');

      // Verify events readable
      const chineseEvents = await store2.readStream('用户-123');
      expect(chineseEvents).toHaveLength(1);
      expect(chineseEvents[0]!.data).toEqual({ name: '张三' });

      const russianEvents = await store2.readStream('пользователь-456');
      expect(russianEvents).toHaveLength(1);
      expect(russianEvents[0]!.data).toEqual({ name: 'Иван' });

      await store2.close();
    });
  });

  describe('empty segment recovery', () => {
    test('should handle segment with no events', async () => {
      const store1 = createStore();
      await store1.open('/data/events');
      // Open creates a segment but we don't write anything
      await store1.close();

      // Reopen
      const store2 = createStore();
      await store2.open('/data/events');

      expect(store2.getStreamRevision('any-stream')).toBe(-1);

      await store2.close();
    });
  });
});
