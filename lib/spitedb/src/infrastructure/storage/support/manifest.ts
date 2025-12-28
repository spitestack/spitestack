/**
 * Manifest file for tracking segments.
 *
 * Provides fast startup by maintaining a JSON manifest of all segments
 * instead of scanning the directory on every open.
 *
 * Features:
 * - Atomic writes (write to temp, fsync, rename)
 * - Automatic fallback to directory scan if manifest is missing/corrupt
 * - Tracks segment metadata for efficient lookups
 *
 * @example
 * ```ts
 * const manifest = new Manifest(fs);
 *
 * // Load existing manifest (or return empty)
 * await manifest.load('/data/events');
 *
 * // Add a segment
 * manifest.addSegment({ id: 1n, path: 'segment-00000001.log', basePosition: 0 });
 *
 * // Save atomically
 * await manifest.save();
 * ```
 */

import type { FileSystem } from '../../../ports/storage/filesystem';

/**
 * Version of the manifest format.
 * Increment when making breaking changes.
 */
const MANIFEST_VERSION = 1;

/**
 * Manifest file name.
 */
const MANIFEST_FILENAME = '.manifest';

/**
 * Temporary file name for atomic writes.
 */
const MANIFEST_TEMP_FILENAME = '.manifest.tmp';

/**
 * Segment entry in the manifest.
 */
export interface ManifestSegment {
  /** Segment ID (bigint stored as string in JSON) */
  id: bigint;
  /** Relative path to segment file */
  path: string;
  /** Base global position of first event in segment */
  basePosition: number;
}

/**
 * Manifest file structure (JSON serializable).
 */
interface ManifestData {
  /** Manifest format version */
  version: number;
  /** Current global position (next position to assign) */
  globalPosition: string; // number as string
  /** Next segment ID to assign */
  nextSegmentId: string; // bigint as string
  /** List of segments */
  segments: Array<{
    id: string; // bigint as string
    path: string;
    basePosition: string; // number as string
  }>;
}

/**
 * Manages the segment manifest file.
 */
export class Manifest {
  private segments = new Map<bigint, ManifestSegment>();
  private globalPosition = 0;
  private nextSegmentId: bigint = 0n;
  private dataDir: string = '';
  private dirty = false;
  private saveCounter = 0;

  constructor(private readonly fs: FileSystem) {}

  /**
   * Load manifest from disk.
   *
   * @param dataDir - Directory containing the manifest
   * @returns true if manifest was loaded, false if not found or corrupt
   */
  async load(dataDir: string): Promise<boolean> {
    this.dataDir = dataDir;
    const manifestPath = `${dataDir}/${MANIFEST_FILENAME}`;

    try {
      if (!(await this.fs.exists(manifestPath))) {
        return false;
      }

      const content = await this.fs.readFile(manifestPath);
      const json = new TextDecoder().decode(content);
      const data = JSON.parse(json) as ManifestData;

      // Validate version
      if (data.version !== MANIFEST_VERSION) {
        console.warn(
          `[spitedb] Manifest version mismatch: expected ${MANIFEST_VERSION}, got ${data.version}`
        );
        return false;
      }

      // Parse manifest
      this.globalPosition = Number(data.globalPosition);
      this.nextSegmentId = BigInt(data.nextSegmentId);
      this.segments.clear();

      for (const seg of data.segments) {
        const segment: ManifestSegment = {
          id: BigInt(seg.id),
          path: seg.path,
          basePosition: Number(seg.basePosition),
        };
        this.segments.set(segment.id, segment);
      }

      this.dirty = false;
      return true;
    } catch (error) {
      console.warn(`[spitedb] Failed to load manifest: ${error}`);
      return false;
    }
  }

  /**
   * Save manifest to disk atomically.
   *
   * Uses write-to-temp → fsync → rename pattern.
   */
  async save(): Promise<void> {
    if (!this.dataDir) {
      throw new Error('Manifest not initialized. Call load() first.');
    }

    const data: ManifestData = {
      version: MANIFEST_VERSION,
      globalPosition: this.globalPosition.toString(),
      nextSegmentId: this.nextSegmentId.toString(),
      segments: Array.from(this.segments.values()).map((seg) => ({
        id: seg.id.toString(),
        path: seg.path,
        basePosition: seg.basePosition.toString(),
      })),
    };

    const json = JSON.stringify(data, null, 2);
    const content = new TextEncoder().encode(json);

    const tempPath = `${this.dataDir}/${MANIFEST_TEMP_FILENAME}.${this.saveCounter++}`;
    const finalPath = `${this.dataDir}/${MANIFEST_FILENAME}`;

    // Write to temp file
    const handle = await this.fs.open(tempPath, 'write');
    await this.fs.write(handle, content);
    await this.fs.sync(handle);
    await this.fs.close(handle);

    // Atomic rename
    await this.fs.rename(tempPath, finalPath);

    this.dirty = false;
  }

  /**
   * Add a segment to the manifest.
   */
  addSegment(segment: ManifestSegment): void {
    this.segments.set(segment.id, segment);

    // Update nextSegmentId if needed
    if (segment.id >= this.nextSegmentId) {
      this.nextSegmentId = segment.id + 1n;
    }

    this.dirty = true;
  }

  /**
   * Remove a segment from the manifest.
   */
  removeSegment(segmentId: bigint): boolean {
    const removed = this.segments.delete(segmentId);
    if (removed) {
      this.dirty = true;
    }
    return removed;
  }

  /**
   * Get a segment by ID.
   */
  getSegment(segmentId: bigint): ManifestSegment | undefined {
    return this.segments.get(segmentId);
  }

  /**
   * Get all segments.
   */
  getSegments(): ManifestSegment[] {
    return Array.from(this.segments.values()).sort((a, b) =>
      a.id < b.id ? -1 : a.id > b.id ? 1 : 0
    );
  }

  /**
   * Get segment count.
   */
  getSegmentCount(): number {
    return this.segments.size;
  }

  /**
   * Get current global position.
   */
  getGlobalPosition(): number {
    return this.globalPosition;
  }

  /**
   * Set global position.
   */
  setGlobalPosition(position: number): void {
    if (position !== this.globalPosition) {
      this.globalPosition = position;
      this.dirty = true;
    }
  }

  /**
   * Get next segment ID.
   */
  getNextSegmentId(): bigint {
    return this.nextSegmentId;
  }

  /**
   * Set next segment ID.
   */
  setNextSegmentId(id: bigint): void {
    if (id !== this.nextSegmentId) {
      this.nextSegmentId = id;
      this.dirty = true;
    }
  }

  /**
   * Allocate and return the next segment ID.
   */
  allocateSegmentId(): bigint {
    const id = this.nextSegmentId;
    this.nextSegmentId++;
    this.dirty = true;
    return id;
  }

  /**
   * Check if manifest has unsaved changes.
   */
  isDirty(): boolean {
    return this.dirty;
  }

  /**
   * Clear all segments (for testing or rebuild).
   */
  clear(): void {
    this.segments.clear();
    this.globalPosition = 0;
    this.nextSegmentId = 0n;
    this.dirty = true;
  }

  /**
   * Initialize from directory scan results.
   *
   * @param segments - Segments found by directory scan
   * @param globalPosition - Current global position
   * @param nextSegmentId - Next segment ID to assign
   */
  initializeFromScan(
    segments: ManifestSegment[],
    globalPosition: number,
    nextSegmentId: bigint
  ): void {
    this.segments.clear();
    for (const seg of segments) {
      this.segments.set(seg.id, seg);
    }
    this.globalPosition = globalPosition;
    this.nextSegmentId = nextSegmentId;
    this.dirty = true;
  }
}
