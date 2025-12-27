/**
 * Metadata for a stream stored in the StreamMap.
 */
export interface StreamMetadata {
  /** Current revision (latest event revision for concurrency checks) */
  latestRevision: number;
  /** Segment IDs containing events for this stream */
  segments: bigint[];
}

/**
 * Serialized form for JSON persistence.
 */
export interface StreamMapJSON {
  streams: Array<{
    streamId: string;
    latestRevision: number;
    segments: string[]; // bigint serialized as string
  }>;
}

/**
 * In-memory global index mapping streams to their locations.
 *
 * Provides O(1) lookups for:
 * - Stream revision (for optimistic concurrency checks)
 * - Which segments contain a stream's events (for efficient reads)
 *
 * This is ephemeral - rebuilt on startup from .idx files.
 * Memory usage: ~100 bytes per unique stream.
 *
 * @example
 * ```ts
 * const map = new StreamMap();
 *
 * // On write
 * map.updateStream('user-123', 0, 1n);
 * map.updateStream('user-123', 1, 1n);
 * map.updateStream('user-123', 2, 2n); // Stream spans segments 1 and 2
 *
 * // On read
 * const revision = map.getRevision('user-123'); // 2
 * const segments = map.getSegments('user-123'); // [1n, 2n]
 * ```
 */
export class StreamMap {
  private streams = new Map<string, StreamMetadata>();

  // ============================================================
  // Lookups (O(1))
  // ============================================================

  /**
   * Get the latest revision for a stream.
   * @returns The latest revision, or -1 if stream doesn't exist
   */
  getRevision(streamId: string): number {
    return this.streams.get(streamId)?.latestRevision ?? -1;
  }

  /**
   * Get all segment IDs containing events for a stream.
   * @returns Array of segment IDs (may be empty if stream doesn't exist)
   */
  getSegments(streamId: string): bigint[] {
    return this.streams.get(streamId)?.segments ?? [];
  }

  /**
   * Check if a stream exists.
   */
  hasStream(streamId: string): boolean {
    return this.streams.has(streamId);
  }

  // ============================================================
  // Updates (called on write)
  // ============================================================

  /**
   * Update stream metadata after writing events.
   *
   * If revision is higher than current, updates latestRevision.
   * If segmentId is new for this stream, adds it to segments list.
   *
   * @param streamId - The stream identifier
   * @param revision - The revision of the last event written
   * @param segmentId - The segment where the events were written
   */
  updateStream(streamId: string, revision: number, segmentId: bigint): void {
    const existing = this.streams.get(streamId);

    if (!existing) {
      // New stream
      this.streams.set(streamId, {
        latestRevision: revision,
        segments: [segmentId],
      });
      return;
    }

    // Update revision if higher
    if (revision > existing.latestRevision) {
      existing.latestRevision = revision;
    }

    // Add segment if not already present
    if (!existing.segments.includes(segmentId)) {
      existing.segments.push(segmentId);
    }
  }

  // ============================================================
  // Startup / Management
  // ============================================================

  /**
   * Clear all data from the map.
   */
  clear(): void {
    this.streams.clear();
  }

  /**
   * Get the number of unique streams.
   */
  getStreamCount(): number {
    return this.streams.size;
  }

  /**
   * Get all stream IDs (sorted for consistency).
   */
  getAllStreamIds(): string[] {
    return Array.from(this.streams.keys()).sort();
  }

  /**
   * Get statistics about the map.
   */
  getStats(): { streamCount: number; totalSegmentRefs: number } {
    let totalSegmentRefs = 0;
    for (const meta of this.streams.values()) {
      totalSegmentRefs += meta.segments.length;
    }
    return {
      streamCount: this.streams.size,
      totalSegmentRefs,
    };
  }

  // ============================================================
  // Segment Eviction (for segment cleanup)
  // ============================================================

  /**
   * Remove a segment from all streams.
   * Called when a segment is deleted (e.g., compaction).
   *
   * Streams that only had events in this segment are removed entirely.
   */
  evictSegment(segmentId: bigint): void {
    const streamsToRemove: string[] = [];

    for (const [streamId, meta] of this.streams) {
      const index = meta.segments.indexOf(segmentId);
      if (index !== -1) {
        meta.segments.splice(index, 1);

        // If stream has no more segments, mark for removal
        if (meta.segments.length === 0) {
          streamsToRemove.push(streamId);
        }
      }
    }

    // Remove streams with no segments
    for (const streamId of streamsToRemove) {
      this.streams.delete(streamId);
    }
  }

  // ============================================================
  // Serialization (for potential persistence later)
  // ============================================================

  /**
   * Serialize to JSON-compatible object.
   */
  toJSON(): StreamMapJSON {
    const streams: StreamMapJSON['streams'] = [];

    for (const [streamId, meta] of this.streams) {
      streams.push({
        streamId,
        latestRevision: meta.latestRevision,
        segments: meta.segments.map((s) => s.toString()),
      });
    }

    return { streams };
  }

  /**
   * Deserialize from JSON.
   */
  static fromJSON(data: StreamMapJSON): StreamMap {
    const map = new StreamMap();

    for (const entry of data.streams) {
      map.streams.set(entry.streamId, {
        latestRevision: entry.latestRevision,
        segments: entry.segments.map((s) => BigInt(s)),
      });
    }

    return map;
  }
}
