/**
 * Manages multiple segment files with automatic rotation and caching.
 *
 * Responsibilities:
 * - Track all segment files in a directory
 * - Manage the active writer with automatic rotation
 * - Cache segment indexes with LRU eviction
 * - Handle crash recovery
 *
 * @example
 * ```ts
 * const manager = new SegmentManager(fs, serializer, compressor, {
 *   dataDir: '/data',
 *   maxSegmentSize: 128 * 1024 * 1024, // 128MB
 *   indexCacheSize: 10,
 * });
 *
 * await manager.initialize();
 *
 * // Get writer (auto-rotates when segment is full)
 * const writer = await manager.getWriter();
 * await writer.appendBatch(events);
 * await writer.sync();
 *
 * // Read with cached index
 * const index = await manager.getIndex(segmentId);
 * ```
 */

import type { FileSystem } from '../../../ports/storage/filesystem';
import type { Serializer } from '../../../ports/serialization/serializer';
import type { Compressor } from '../../../ports/serialization/compressor';
import { SegmentWriter, type BatchWriteResult } from './segment-writer';
import { SegmentReader, type ValidationResult } from './segment-reader';
import { SegmentIndex } from './segment-index';
import { SegmentIndexFile, IndexCorruptedError } from './segment-index-file';
import { StreamMap } from '../support/stream-map';
import { LRUCache } from '../support/lru-cache';
import { decodeSegmentHeader, SEGMENT_HEADER_SIZE } from './segment-header';
import type { StoredEvent } from '../../../domain/events/stored-event';
import { Manifest } from '../support/manifest';

/**
 * Configuration for the segment manager.
 */
export interface SegmentManagerConfig {
  /** Directory containing segment files */
  dataDir: string;
  /** Maximum size of a segment file before rotation (default: 128MB) */
  maxSegmentSize?: number | undefined;
  /** Number of segment indexes to keep in LRU cache (default: 10) */
  indexCacheSize?: number | undefined;
  /** Optional profiler for batch reads */
  readProfiler?: import('./segment-reader').ReadBatchProfiler;
}

/**
 * Information about a segment file.
 */
export interface SegmentInfo {
  /** Segment ID */
  id: bigint;
  /** Full path to segment file */
  path: string;
  /** File size in bytes */
  size: number;
  /** Global position of first event in segment */
  basePosition: number;
  /** Whether this is the active (writable) segment */
  isActive: boolean;
}

/**
 * Result of crash recovery.
 */
export interface RecoveryResult {
  /** Number of segments found */
  segmentCount: number;
  /** Number of segments that needed recovery */
  recoveredSegments: number;
  /** Total events recovered */
  totalEvents: number;
  /** Errors encountered during recovery */
  errors: string[];
}

const DEFAULT_MAX_SEGMENT_SIZE = 128 * 1024 * 1024; // 128MB
const DEFAULT_INDEX_CACHE_SIZE = 10;

/**
 * Manages segment files for an event store.
 */
export class SegmentManager {
  private readonly config: {
    dataDir: string;
    maxSegmentSize: number;
    indexCacheSize: number;
  };
  private readonly reader: SegmentReader;
  private readonly indexCache: LRUCache<bigint, SegmentIndex>;
  private readonly indexFileCache: LRUCache<bigint, SegmentIndexFile>;
  private readonly streamMap: StreamMap;
  private readonly manifest: Manifest;

  private segments = new Map<bigint, SegmentInfo>();
  private activeWriter: SegmentWriter | null = null;
  private activeSegmentId: bigint = 0n;
  private nextSegmentId: bigint = 0n;
  private globalPosition = 0;
  private initialized = false;
  private pendingCloses: Promise<void>[] = [];

  /**
   * Track pending (written but not synced) batch for idempotent writes.
   * This prevents duplicate writes when flush retries after sync failure.
   */
  private pendingBatch: {
    firstPosition: number;
    lastPosition: number;
    eventCount: number;
  } | null = null;

  constructor(
    private readonly fs: FileSystem,
    private readonly serializer: Serializer,
    private readonly compressor: Compressor,
    config: SegmentManagerConfig
  ) {
    this.config = {
      dataDir: config.dataDir,
      maxSegmentSize: config.maxSegmentSize ?? DEFAULT_MAX_SEGMENT_SIZE,
      indexCacheSize: config.indexCacheSize ?? DEFAULT_INDEX_CACHE_SIZE,
    };

    this.reader = new SegmentReader(fs, serializer, compressor, config.readProfiler);
    this.indexCache = new LRUCache(this.config.indexCacheSize);
    this.indexFileCache = new LRUCache(this.config.indexCacheSize);
    this.streamMap = new StreamMap();
    this.manifest = new Manifest(fs);
  }

  /**
   * Initialize the manager by loading manifest or scanning existing segments.
   *
   * Must be called before any other operations.
   *
   * Initialization order:
   * 1. Try to load manifest (fast path)
   * 2. If manifest loads, use it to populate segments
   * 3. If manifest fails, fall back to directory scan and create new manifest
   */
  async initialize(): Promise<void> {
    if (this.initialized) {
      throw new Error('SegmentManager already initialized');
    }

    // Ensure data directory exists
    if (!(await this.fs.exists(this.config.dataDir))) {
      await this.fs.mkdir(this.config.dataDir, { recursive: true });
    }

    // Try to load manifest (fast path)
    const manifestLoaded = await this.manifest.load(this.config.dataDir);

    if (manifestLoaded) {
      // Use manifest to populate segments
      for (const seg of this.manifest.getSegments()) {
        const path = `${this.config.dataDir}/${seg.path}`;

        // Verify segment file still exists
        if (await this.fs.exists(path)) {
          try {
            const stat = await this.fs.stat(path);
            this.segments.set(seg.id, {
              id: seg.id,
              path,
              size: stat.size,
              basePosition: seg.basePosition,
              isActive: false,
            });
          } catch {
            // Segment file invalid, will be handled by StreamMap rebuild
          }
        }
      }

      this.nextSegmentId = this.manifest.getNextSegmentId();
      this.globalPosition = this.manifest.getGlobalPosition();
    } else {
      // Fall back to directory scan (slow path)
      await this.scanDirectory();

      // Save manifest for next time
      this.manifest.initializeFromScan(
        Array.from(this.segments.values()).map((seg) => ({
          id: seg.id,
          path: seg.path.split('/').pop()!, // Store relative path
          basePosition: seg.basePosition,
        })),
        this.globalPosition,
        this.nextSegmentId
      );
      await this.manifest.save();
    }

    // Load .idx files and rebuild StreamMap
    await this.rebuildStreamMap();

    this.initialized = true;
  }

  /**
   * Scan directory for segment files (fallback when manifest unavailable).
   */
  private async scanDirectory(): Promise<void> {
    const files = await this.fs.readdir(this.config.dataDir);
    const segmentFiles = files.filter((f) => f.startsWith('segment-') && f.endsWith('.log'));

    for (const file of segmentFiles) {
      const path = `${this.config.dataDir}/${file}`;
      try {
        const stat = await this.fs.stat(path);
        const header = await this.reader.readHeader(path);

        this.segments.set(header.segmentId, {
          id: header.segmentId,
          path,
          size: stat.size,
          basePosition: header.basePosition,
          isActive: false,
        });

        // Track highest segment ID
        if (header.segmentId >= this.nextSegmentId) {
          this.nextSegmentId = header.segmentId + 1n;
        }
      } catch (error) {
        // Skip invalid segment files
        console.warn(`Skipping invalid segment file: ${file}`, error);
      }
    }
  }

  /**
   * Rebuild StreamMap from all segment index files.
   * If an .idx file is missing or corrupted, rebuild it from the .log file.
   */
  private async rebuildStreamMap(): Promise<void> {
    this.streamMap.clear();

    for (const [segmentId, info] of this.segments) {
      try {
        const indexFile = await this.loadOrRebuildIndexFile(segmentId, info.path);

        // Extract stream→segment mappings into StreamMap
        for (const streamId of indexFile.getAllStreamIds()) {
          const maxRevision = indexFile.getStreamMaxRevision(streamId);
          this.streamMap.updateStream(streamId, maxRevision, segmentId);

          // Also track global position
          const entries = indexFile.findByStream(streamId);
          for (const entry of entries) {
            if (entry.globalPosition >= this.globalPosition) {
              this.globalPosition = entry.globalPosition + 1;
            }
          }
        }
      } catch (error) {
        console.warn(`Failed to load index for segment ${segmentId}:`, error);
        try {
          await this.rebuildStreamMapFromLog(segmentId, info.path);
        } catch (scanError) {
          console.warn(`Failed to scan segment ${segmentId} for recovery:`, scanError);
        }
      }
    }
  }

  /**
   * Rebuild StreamMap and global position directly from a segment log.
   * Used as a fallback when index files are unavailable or corrupt.
   */
  private async rebuildStreamMapFromLog(segmentId: bigint, logPath: string): Promise<void> {
    for await (const batch of this.reader.readAllBatches(logPath)) {
      for (const event of batch) {
        this.streamMap.updateStream(event.streamId, event.revision, segmentId);
        if (event.globalPosition >= this.globalPosition) {
          this.globalPosition = event.globalPosition + 1;
        }
      }
    }
  }

  /**
   * Load an index file, rebuilding from .log if necessary.
   */
  private async loadOrRebuildIndexFile(
    segmentId: bigint,
    logPath: string
  ): Promise<SegmentIndexFile> {
    const idxPath = logPath.replace('.log', '.idx');

    // Try loading existing .idx file
    if (await this.fs.exists(idxPath)) {
      try {
        const indexFile = new SegmentIndexFile();
        await indexFile.load(this.fs, idxPath);
        return indexFile;
      } catch (error) {
        if (error instanceof IndexCorruptedError) {
          console.warn(`Index file corrupted for segment ${segmentId}, rebuilding...`);
        } else {
          throw error;
        }
      }
    }

    // Rebuild from .log file
    return await this.rebuildIndexFile(segmentId, logPath);
  }

  /**
   * Rebuild an index file from the segment .log file.
   */
  private async rebuildIndexFile(
    segmentId: bigint,
    logPath: string
  ): Promise<SegmentIndexFile> {
    const idxPath = logPath.replace('.log', '.idx');
    const entries: Array<{
      streamId: string;
      revision: number;
      globalPosition: number;
      batchOffset: number;
    }> = [];

    // Scan all valid batches in the segment
    const fileStat = await this.fs.stat(logPath);
    let offset = SEGMENT_HEADER_SIZE;

    while (offset < fileStat.size) {
      try {
        const result = await this.reader.readBatchWithMetadata(logPath, offset);
        const batchOffset = offset;

        for (const event of result.events) {
          entries.push({
            streamId: event.streamId,
            revision: event.revision,
            globalPosition: event.globalPosition,
            batchOffset,
          });
        }

        offset = result.nextOffset;
      } catch {
        // Stop at first invalid batch (truncated/corrupted)
        break;
      }
    }

    // Write new index file
    if (entries.length > 0) {
      await SegmentIndexFile.write(this.fs, idxPath, segmentId, entries);
    }

    // Load and return
    const indexFile = new SegmentIndexFile();
    if (entries.length > 0) {
      await indexFile.load(this.fs, idxPath);
    }
    return indexFile;
  }

  /**
   * Get or create the active segment writer.
   *
   * Automatically rotates to a new segment if:
   * - No active segment exists
   * - Active segment exceeds max size
   */
  async getWriter(basePosition?: number): Promise<SegmentWriter> {
    this.ensureInitialized();

    // Create new writer if needed
    if (!this.activeWriter) {
      await this.rotateSegment(basePosition);
    }

    // Rotate if current segment is too large
    const activeInfo = this.segments.get(this.activeSegmentId);
    if (activeInfo && activeInfo.size >= this.config.maxSegmentSize) {
      await this.rotateSegment(basePosition);
    }

    return this.activeWriter!;
  }

  /**
   * Close the active writer and create a new segment.
   *
   * The .log file is synced synchronously to ensure data durability,
   * but the .idx file write happens in the background to avoid latency spikes.
   */
  async rotateSegment(basePosition: number = this.globalPosition): Promise<void> {
    this.ensureInitialized();

    const oldWriter = this.activeWriter;
    const oldSegmentId = this.activeSegmentId;

    // Sync and update state for old segment (sync path - ensures durability)
    if (oldWriter) {
      await oldWriter.sync();

      // Capture size before we lose reference
      const finalSize = oldWriter.offset;

      // Update segment info immediately
      const activeInfo = this.segments.get(oldSegmentId);
      if (activeInfo) {
        activeInfo.isActive = false;
        activeInfo.size = finalSize;
      }
    }

    // Create new segment immediately (sync path - low latency)
    const segmentId = this.nextSegmentId++;
    const path = this.getSegmentPath(segmentId);

    this.activeWriter = new SegmentWriter(this.fs, this.serializer, this.compressor);
    await this.activeWriter.open(path, segmentId, basePosition);

    this.activeSegmentId = segmentId;
    this.segments.set(segmentId, {
      id: segmentId,
      path,
      size: SEGMENT_HEADER_SIZE,
      basePosition,
      isActive: true,
    });

    // Update manifest with new segment
    const segmentFileName = path.split('/').pop()!;
    this.manifest.addSegment({
      id: segmentId,
      path: segmentFileName,
      basePosition,
    });
    this.manifest.setNextSegmentId(this.nextSegmentId);

    // Save manifest (non-blocking - failure is recoverable via directory scan)
    this.manifest.save().catch((err) => {
      console.warn(`Failed to save manifest after rotation:`, err);
    });

    // Close old writer in background (writes .idx file)
    // This avoids blocking appends on potentially slow index writes
    if (oldWriter) {
      const closePromise = oldWriter.close().catch((err) => {
        // .idx write failed - will be rebuilt from .log on next read
        console.warn(`Background index write failed for segment ${oldSegmentId}:`, err);
      });
      this.pendingCloses.push(closePromise);
    }
  }

  /**
   * Write a batch of events to the active segment (idempotent).
   *
   * This method is idempotent - if called with the same events after a
   * sync failure, it will detect the retry and skip the write. This
   * prevents duplicate events when flushing is retried.
   *
   * @returns Write result with position info, including whether this was a retry
   */
  async writeBatch(events: StoredEvent[]): Promise<BatchWriteResult & { alreadyWritten?: boolean }> {
    const firstPosition = events[0]!.globalPosition;
    const lastPosition = events[events.length - 1]!.globalPosition;

    // Check if this is a retry of a pending (written but not synced) batch
    if (
      this.pendingBatch &&
      this.pendingBatch.firstPosition === firstPosition &&
      this.pendingBatch.lastPosition === lastPosition &&
      this.pendingBatch.eventCount === events.length
    ) {
      // This is a retry - the batch was already written, just needs sync
      return {
        batchId: 0n, // Not meaningful for retries
        offset: 0,
        length: 0,
        eventCount: events.length,
        alreadyWritten: true,
      };
    }

    const writer = await this.getWriter(events[0]!.globalPosition);
    const result = await writer.appendBatch(events);

    // Track this batch as pending (written but not synced)
    this.pendingBatch = {
      firstPosition,
      lastPosition,
      eventCount: events.length,
    };

    // Update segment size and global position
    const activeInfo = this.segments.get(this.activeSegmentId);
    if (activeInfo) {
      activeInfo.size = writer.offset;
    }

    // Update global position and StreamMap based on events
    for (const event of events) {
      if (event.globalPosition >= this.globalPosition) {
        this.globalPosition = event.globalPosition + 1;
      }

      // Update StreamMap for fast lookups
      this.streamMap.updateStream(event.streamId, event.revision, this.activeSegmentId);
    }

    return result;
  }

  /**
   * Sync the active writer to disk.
   *
   * After successful sync, clears the pending batch tracker to allow
   * new writes. This is part of the idempotent write mechanism.
   */
  async sync(): Promise<void> {
    if (this.activeWriter) {
      await this.activeWriter.sync();
      // Clear pending batch - data is now durable
      this.pendingBatch = null;
    }
  }

  /**
   * Get or build the index for a segment.
   *
   * Indexes are cached with LRU eviction.
   */
  async getIndex(segmentId: bigint): Promise<SegmentIndex> {
    this.ensureInitialized();

    // Check cache
    let index = this.indexCache.get(segmentId);
    if (index) {
      return index;
    }

    // Build index from segment file
    const segmentInfo = this.segments.get(segmentId);
    if (!segmentInfo) {
      throw new Error(`Segment ${segmentId} not found`);
    }

    index = new SegmentIndex();
    await index.rebuildFromSegment(this.reader, segmentInfo.path, segmentId);

    this.indexCache.set(segmentId, index);
    return index;
  }

  /**
   * Perform crash recovery on all segments.
   *
   * Validates segments and truncates corrupted tails.
   */
  async recover(): Promise<RecoveryResult> {
    this.ensureInitialized();

    const errors: string[] = [];
    let recoveredSegments = 0;
    let totalEvents = 0;

    for (const [segmentId, info] of this.segments) {
      try {
        const result = await this.reader.validateSegment(info.path);

        if (!result.valid) {
          // Truncate to last valid offset
          const handle = await this.fs.open(info.path, 'readwrite');
          await this.fs.truncate(handle, result.lastValidOffset);
          await this.fs.close(handle);

          recoveredSegments++;
          info.size = result.lastValidOffset;

          // Invalidate cached index
          this.indexCache.delete(segmentId);

          errors.push(
            `Segment ${segmentId}: truncated from ${info.size} to ${result.lastValidOffset} bytes. ` +
              `Errors: ${result.errors.join(', ')}`
          );
        }

        totalEvents += result.eventCount;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        errors.push(`Segment ${segmentId}: recovery failed - ${message}`);
      }
    }

    return {
      segmentCount: this.segments.size,
      recoveredSegments,
      totalEvents,
      errors,
    };
  }

  /**
   * Close the manager and release resources.
   */
  async close(): Promise<void> {
    // Wait for any pending background closes to complete
    if (this.pendingCloses.length > 0) {
      await Promise.all(this.pendingCloses);
      this.pendingCloses = [];
    }

    if (this.activeWriter) {
      await this.activeWriter.close();
      this.activeWriter = null;
    }

    // Save manifest with final state
    this.manifest.setGlobalPosition(this.globalPosition);
    try {
      await this.manifest.save();
    } catch (err) {
      console.warn(`Failed to save manifest on close:`, err);
    }

    this.indexCache.clear();
    this.indexFileCache.clear();
    this.streamMap.clear();
    this.segments.clear();
    this.initialized = false;
  }

  /**
   * Get the segment reader.
   */
  getReader(): SegmentReader {
    return this.reader;
  }

  /**
   * Get information about all segments.
   */
  getSegments(): SegmentInfo[] {
    return Array.from(this.segments.values()).sort((a, b) =>
      a.id < b.id ? -1 : a.id > b.id ? 1 : 0
    );
  }

  /**
   * Get the current global position.
   */
  getGlobalPosition(): number {
    return this.globalPosition;
  }

  /**
   * Get the next global position to assign.
   */
  getNextGlobalPosition(): number {
    return this.globalPosition;
  }

  /**
   * Increment and return the next global position.
   */
  allocateGlobalPosition(): number {
    return this.globalPosition++;
  }

  /**
   * Get the active segment ID.
   */
  getActiveSegmentId(): bigint {
    return this.activeSegmentId;
  }

  /**
   * Check if the manager is initialized.
   */
  isInitialized(): boolean {
    return this.initialized;
  }

  // ============================================================
  // StreamMap Accessors (O(1) lookups)
  // ============================================================

  /**
   * Get the latest revision for a stream (O(1) lookup).
   * @returns The latest revision, or -1 if stream doesn't exist
   */
  getStreamRevision(streamId: string): number {
    return this.streamMap.getRevision(streamId);
  }

  /**
   * Get all segment IDs containing events for a stream (O(1) lookup).
   * @returns Array of segment IDs (may be empty if stream doesn't exist)
   */
  getStreamSegments(streamId: string): bigint[] {
    return this.streamMap.getSegments(streamId);
  }

  /**
   * Check if a stream exists (O(1) lookup).
   */
  hasStream(streamId: string): boolean {
    return this.streamMap.hasStream(streamId);
  }

  /**
   * Get all stream IDs.
   */
  getAllStreamIds(): string[] {
    return this.streamMap.getAllStreamIds();
  }

  /**
   * Get the StreamMap for direct access.
   */
  getStreamMap(): StreamMap {
    return this.streamMap;
  }

  // ============================================================
  // SegmentIndexFile Accessors
  // ============================================================

  /**
   * Get the index file for a segment (cached with LRU eviction).
   */
  async getSegmentIndexFile(segmentId: bigint): Promise<SegmentIndexFile> {
    this.ensureInitialized();

    // Check cache
    let indexFile = this.indexFileCache.get(segmentId);
    if (indexFile) {
      return indexFile;
    }

    // Load or rebuild
    const segmentInfo = this.segments.get(segmentId);
    if (!segmentInfo) {
      throw new Error(`Segment ${segmentId} not found`);
    }

    indexFile = await this.loadOrRebuildIndexFile(segmentId, segmentInfo.path);
    this.indexFileCache.set(segmentId, indexFile);
    return indexFile;
  }

  /**
   * Get segment info by ID.
   */
  getSegment(segmentId: bigint): SegmentInfo | undefined {
    return this.segments.get(segmentId);
  }

  /**
   * Get segment path from ID.
   */
  private getSegmentPath(segmentId: bigint): string {
    const paddedId = segmentId.toString().padStart(8, '0');
    return `${this.config.dataDir}/segment-${paddedId}.log`;
  }

  /**
   * Ensure the manager is initialized.
   */
  private ensureInitialized(): void {
    if (!this.initialized) {
      throw new Error('SegmentManager not initialized. Call initialize() first.');
    }
  }
}
