/**
 * In-memory index for fast event lookups.
 *
 * This index is segment-aligned, meaning it can be efficiently
 * evicted per-segment for LRU caching purposes.
 *
 * The index is NOT persisted - it's rebuilt from segment files on startup.
 *
 * @example
 * ```ts
 * const index = new SegmentIndex();
 *
 * // Build index from events
 * index.addBatch(segmentId, offset, events);
 *
 * // Find events by stream
 * const entries = index.findByStream('user-123');
 *
 * // Find event by global position
 * const location = index.findByPosition(42n);
 *
 * // Evict segment from index (for LRU)
 * index.evictSegment(segmentId);
 * ```
 */

import type { StoredEvent } from './stored-event';
import type { SegmentReader } from './segment-reader';

/**
 * Index entry for stream-based lookups.
 */
export interface StreamEntry {
  /** Segment containing this event */
  segmentId: bigint;
  /** File offset of the batch containing this event */
  batchOffset: number;
  /** Stream-local revision */
  revision: number;
  /** Global position */
  globalPosition: bigint;
}

/**
 * Index entry for position-based lookups.
 */
export interface LocationEntry {
  /** Segment containing this event */
  segmentId: bigint;
  /** File offset of the batch containing this event */
  batchOffset: number;
  /** Stream ID of the event */
  streamId: string;
}

/**
 * In-memory index for fast event lookups.
 *
 * Not thread-safe - synchronize access externally if needed.
 */
export class SegmentIndex {
  /** Maps streamId -> array of entries (sorted by revision) */
  private streamIndex = new Map<string, StreamEntry[]>();

  /** Maps globalPosition -> location */
  private positionIndex = new Map<bigint, LocationEntry>();

  /** Tracks which segments have entries in the index */
  private indexedSegments = new Set<bigint>();

  /**
   * Add events from a batch to the index.
   *
   * @param segmentId - Segment the batch belongs to
   * @param batchOffset - File offset where the batch starts
   * @param events - Events in the batch
   */
  addBatch(segmentId: bigint, batchOffset: number, events: StoredEvent[]): void {
    this.indexedSegments.add(segmentId);

    for (const event of events) {
      // Add to stream index
      let streamEntries = this.streamIndex.get(event.streamId);
      if (!streamEntries) {
        streamEntries = [];
        this.streamIndex.set(event.streamId, streamEntries);
      }

      streamEntries.push({
        segmentId,
        batchOffset,
        revision: event.revision,
        globalPosition: event.globalPosition,
      });

      // Add to position index
      this.positionIndex.set(event.globalPosition, {
        segmentId,
        batchOffset,
        streamId: event.streamId,
      });
    }
  }

  /**
   * Find index entries for a stream.
   *
   * @param streamId - Stream to look up
   * @param options - Query options
   * @returns Array of index entries (sorted by revision)
   */
  findByStream(
    streamId: string,
    options: {
      fromRevision?: number | undefined;
      toRevision?: number | undefined;
      direction?: 'forward' | 'backward' | undefined;
    } = {}
  ): StreamEntry[] {
    const entries = this.streamIndex.get(streamId);
    if (!entries || entries.length === 0) {
      return [];
    }

    // Sort by revision (should already be sorted, but ensure it)
    const sorted = [...entries].sort((a, b) => a.revision - b.revision);

    // Filter by revision range
    let filtered = sorted;

    if (options.fromRevision !== undefined) {
      filtered = filtered.filter((e) => e.revision >= options.fromRevision!);
    }

    if (options.toRevision !== undefined) {
      filtered = filtered.filter((e) => e.revision <= options.toRevision!);
    }

    // Reverse if backwards
    if (options.direction === 'backward') {
      filtered.reverse();
    }

    return filtered;
  }

  /**
   * Find an event by its global position.
   *
   * @param position - Global position to look up
   * @returns Location entry or undefined if not found
   */
  findByPosition(position: bigint): LocationEntry | undefined {
    return this.positionIndex.get(position);
  }

  /**
   * Get the latest revision for a stream.
   *
   * @param streamId - Stream to query
   * @returns Latest revision, or -1 if stream doesn't exist
   */
  getStreamRevision(streamId: string): number {
    const entries = this.streamIndex.get(streamId);
    if (!entries || entries.length === 0) {
      return -1;
    }

    // Find max revision
    return Math.max(...entries.map((e) => e.revision));
  }

  /**
   * Check if a stream exists in the index.
   *
   * @param streamId - Stream to check
   * @returns true if stream has at least one event indexed
   */
  hasStream(streamId: string): boolean {
    const entries = this.streamIndex.get(streamId);
    return entries !== undefined && entries.length > 0;
  }

  /**
   * Get all indexed stream IDs.
   *
   * @returns Array of stream IDs
   */
  getStreamIds(): string[] {
    return Array.from(this.streamIndex.keys());
  }

  /**
   * Get the count of events for a stream.
   *
   * @param streamId - Stream to query
   * @returns Number of events indexed for this stream
   */
  getStreamEventCount(streamId: string): number {
    return this.streamIndex.get(streamId)?.length ?? 0;
  }

  /**
   * Evict all entries for a specific segment.
   *
   * Used for LRU cache management - removes all index entries
   * that point to the evicted segment.
   *
   * @param segmentId - Segment to evict
   */
  evictSegment(segmentId: bigint): void {
    if (!this.indexedSegments.has(segmentId)) {
      return; // Nothing to evict
    }

    // Remove from stream index
    for (const [streamId, entries] of this.streamIndex) {
      const filtered = entries.filter((e) => e.segmentId !== segmentId);
      if (filtered.length === 0) {
        this.streamIndex.delete(streamId);
      } else {
        this.streamIndex.set(streamId, filtered);
      }
    }

    // Remove from position index
    for (const [position, entry] of this.positionIndex) {
      if (entry.segmentId === segmentId) {
        this.positionIndex.delete(position);
      }
    }

    this.indexedSegments.delete(segmentId);
  }

  /**
   * Check if a segment is indexed.
   *
   * @param segmentId - Segment to check
   * @returns true if any entries from this segment are in the index
   */
  isSegmentIndexed(segmentId: bigint): boolean {
    return this.indexedSegments.has(segmentId);
  }

  /**
   * Rebuild index from a segment file.
   *
   * Reads all batches and adds them to the index.
   *
   * @param reader - Segment reader to use
   * @param path - Path to segment file
   * @param segmentId - ID of the segment being indexed
   */
  async rebuildFromSegment(
    reader: SegmentReader,
    path: string,
    segmentId: bigint
  ): Promise<{ batchCount: number; eventCount: number }> {
    // Evict any existing entries for this segment first
    this.evictSegment(segmentId);

    let batchCount = 0;
    let eventCount = 0;
    let offset = 32; // Start after header (SEGMENT_HEADER_SIZE)

    const fileSize = await reader.getFileSize(path);

    while (offset < fileSize) {
      try {
        const { events, nextOffset } = await reader.readBatchWithMetadata(path, offset);

        this.addBatch(segmentId, offset, events);
        batchCount++;
        eventCount += events.length;

        offset = nextOffset;
      } catch {
        // Stop on first error (likely end of valid data)
        break;
      }
    }

    return { batchCount, eventCount };
  }

  /**
   * Clear all index data.
   */
  clear(): void {
    this.streamIndex.clear();
    this.positionIndex.clear();
    this.indexedSegments.clear();
  }

  /**
   * Get statistics about the index.
   */
  getStats(): {
    streamCount: number;
    totalEntries: number;
    segmentCount: number;
  } {
    let totalEntries = 0;
    for (const entries of this.streamIndex.values()) {
      totalEntries += entries.length;
    }

    return {
      streamCount: this.streamIndex.size,
      totalEntries,
      segmentCount: this.indexedSegments.size,
    };
  }
}
