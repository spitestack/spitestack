import { describe, it, expect, beforeEach } from 'bun:test';
import { SimulatedFileSystem } from '../../../../../src/testing/simulated-filesystem';
import { Manifest } from '../../../../../src/infrastructure/storage/support/manifest';

describe('Manifest', () => {
  let fs: SimulatedFileSystem;
  let manifest: Manifest;

  beforeEach(() => {
    fs = new SimulatedFileSystem();
    manifest = new Manifest(fs);
  });

  describe('load', () => {
    it('should return false when manifest does not exist', async () => {
      await fs.mkdir('/data', { recursive: true });
      const loaded = await manifest.load('/data');
      expect(loaded).toBe(false);
    });

    it('should load existing manifest', async () => {
      await fs.mkdir('/data', { recursive: true });

      // Create a manifest file
      const manifestData = JSON.stringify({
        version: 1,
        globalPosition: '100',
        nextSegmentId: '3',
        segments: [
          { id: '0', path: 'segment-00000000.log', basePosition: '0' },
          { id: '1', path: 'segment-00000001.log', basePosition: '50' },
        ],
      });
      fs.setFileContent('/data/.manifest', new TextEncoder().encode(manifestData));

      const loaded = await manifest.load('/data');

      expect(loaded).toBe(true);
      expect(manifest.getGlobalPosition()).toBe(100);
      expect(manifest.getNextSegmentId()).toBe(3n);
      expect(manifest.getSegmentCount()).toBe(2);
    });

    it('should return false for invalid JSON', async () => {
      await fs.mkdir('/data', { recursive: true });
      fs.setFileContent('/data/.manifest', new TextEncoder().encode('not json'));

      const loaded = await manifest.load('/data');
      expect(loaded).toBe(false);
    });

    it('should return false for wrong version', async () => {
      await fs.mkdir('/data', { recursive: true });

      const manifestData = JSON.stringify({
        version: 999,
        globalPosition: '0',
        nextSegmentId: '0',
        segments: [],
      });
      fs.setFileContent('/data/.manifest', new TextEncoder().encode(manifestData));

      const loaded = await manifest.load('/data');
      expect(loaded).toBe(false);
    });
  });

  describe('save', () => {
    it('should save manifest atomically', async () => {
      await fs.mkdir('/data', { recursive: true });
      await manifest.load('/data');

      manifest.addSegment({ id: 0n, path: 'segment-00000000.log', basePosition: 0 });
      manifest.setGlobalPosition(50);
      manifest.setNextSegmentId(1n);

      await manifest.save();

      // Verify manifest was saved
      expect(await fs.exists('/data/.manifest')).toBe(true);

      // Verify temp file was cleaned up
      expect(await fs.exists('/data/.manifest.tmp')).toBe(false);

      // Load in new manifest and verify contents
      const manifest2 = new Manifest(fs);
      const loaded = await manifest2.load('/data');

      expect(loaded).toBe(true);
      expect(manifest2.getGlobalPosition()).toBe(50);
      expect(manifest2.getNextSegmentId()).toBe(1n);
      expect(manifest2.getSegmentCount()).toBe(1);
    });

    it('should preserve segment data through save/load cycle', async () => {
      await fs.mkdir('/data', { recursive: true });
      await manifest.load('/data');

      manifest.addSegment({ id: 0n, path: 'segment-00000000.log', basePosition: 0 });
      manifest.addSegment({ id: 1n, path: 'segment-00000001.log', basePosition: 100 });
      manifest.addSegment({ id: 2n, path: 'segment-00000002.log', basePosition: 200 });
      manifest.setGlobalPosition(300);

      await manifest.save();

      const manifest2 = new Manifest(fs);
      await manifest2.load('/data');

      const segments = manifest2.getSegments();
      expect(segments.length).toBe(3);
      expect(segments[0]!.id).toBe(0n);
      expect(segments[0]!.basePosition).toBe(0);
      expect(segments[1]!.id).toBe(1n);
      expect(segments[1]!.basePosition).toBe(100);
      expect(segments[2]!.id).toBe(2n);
      expect(segments[2]!.basePosition).toBe(200);
    });
  });

  describe('segment management', () => {
    beforeEach(async () => {
      await fs.mkdir('/data', { recursive: true });
      await manifest.load('/data');
    });

    it('should add segment', () => {
      manifest.addSegment({ id: 0n, path: 'segment-00000000.log', basePosition: 0 });

      expect(manifest.getSegmentCount()).toBe(1);
      expect(manifest.getSegment(0n)).toEqual({
        id: 0n,
        path: 'segment-00000000.log',
        basePosition: 0,
      });
    });

    it('should auto-update nextSegmentId when adding segment', () => {
      expect(manifest.getNextSegmentId()).toBe(0n);

      manifest.addSegment({ id: 5n, path: 'segment-00000005.log', basePosition: 0 });

      expect(manifest.getNextSegmentId()).toBe(6n);
    });

    it('should remove segment', () => {
      manifest.addSegment({ id: 0n, path: 'segment-00000000.log', basePosition: 0 });
      expect(manifest.getSegmentCount()).toBe(1);

      const removed = manifest.removeSegment(0n);
      expect(removed).toBe(true);
      expect(manifest.getSegmentCount()).toBe(0);
    });

    it('should return false when removing non-existent segment', () => {
      const removed = manifest.removeSegment(999n);
      expect(removed).toBe(false);
    });

    it('should get segments sorted by id', () => {
      manifest.addSegment({ id: 2n, path: 'segment-00000002.log', basePosition: 200 });
      manifest.addSegment({ id: 0n, path: 'segment-00000000.log', basePosition: 0 });
      manifest.addSegment({ id: 1n, path: 'segment-00000001.log', basePosition: 100 });

      const segments = manifest.getSegments();
      expect(segments.map((s) => s.id)).toEqual([0n, 1n, 2n]);
    });
  });

  describe('state management', () => {
    beforeEach(async () => {
      await fs.mkdir('/data', { recursive: true });
      await manifest.load('/data');
    });

    it('should track global position', () => {
      expect(manifest.getGlobalPosition()).toBe(0);

      manifest.setGlobalPosition(100);
      expect(manifest.getGlobalPosition()).toBe(100);
    });

    it('should allocate segment ids', () => {
      expect(manifest.allocateSegmentId()).toBe(0n);
      expect(manifest.allocateSegmentId()).toBe(1n);
      expect(manifest.allocateSegmentId()).toBe(2n);
      expect(manifest.getNextSegmentId()).toBe(3n);
    });

    it('should track dirty state', () => {
      expect(manifest.isDirty()).toBe(false);

      manifest.addSegment({ id: 0n, path: 'test.log', basePosition: 0 });
      expect(manifest.isDirty()).toBe(true);
    });

    it('should clear dirty state after save', async () => {
      manifest.addSegment({ id: 0n, path: 'test.log', basePosition: 0 });
      expect(manifest.isDirty()).toBe(true);

      await manifest.save();
      expect(manifest.isDirty()).toBe(false);
    });

    it('should clear all state', () => {
      manifest.addSegment({ id: 0n, path: 'test.log', basePosition: 0 });
      manifest.setGlobalPosition(100);
      manifest.setNextSegmentId(5n);

      manifest.clear();

      expect(manifest.getSegmentCount()).toBe(0);
      expect(manifest.getGlobalPosition()).toBe(0);
      expect(manifest.getNextSegmentId()).toBe(0n);
    });
  });

  describe('initializeFromScan', () => {
    beforeEach(async () => {
      await fs.mkdir('/data', { recursive: true });
      await manifest.load('/data');
    });

    it('should initialize from scan results', () => {
      manifest.initializeFromScan(
        [
          { id: 0n, path: 'segment-00000000.log', basePosition: 0 },
          { id: 1n, path: 'segment-00000001.log', basePosition: 100 },
        ],
        200,
        2n
      );

      expect(manifest.getSegmentCount()).toBe(2);
      expect(manifest.getGlobalPosition()).toBe(200);
      expect(manifest.getNextSegmentId()).toBe(2n);
      expect(manifest.isDirty()).toBe(true);
    });
  });
});
