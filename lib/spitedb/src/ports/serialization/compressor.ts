/**
 * Abstract compression interface for compressing/decompressing data.
 *
 * This abstraction allows us to:
 * - Use zstd in production (ZstdCompressor)
 * - Use no-op compression in tests for easier debugging
 * - Swap compression algorithms without changing code
 *
 * @example
 * ```ts
 * const compressor = new ZstdCompressor();
 *
 * const compressed = compressor.compress(data, 3); // level 3
 * const decompressed = compressor.decompress(compressed);
 * ```
 */
export interface Compressor {
  /**
   * Compress data.
   * @param data - Raw bytes to compress
   * @param level - Optional compression level (implementation-specific)
   * @returns Compressed bytes
   */
  compress(data: Uint8Array, level?: number): Uint8Array;

  /**
   * Decompress data.
   * @param data - Compressed bytes
   * @returns Original uncompressed bytes
   */
  decompress(data: Uint8Array): Uint8Array;
}
