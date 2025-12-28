import type { Compressor } from '../../ports/serialization/compressor';

/**
 * Production compressor implementation using Zstandard.
 *
 * Zstd provides excellent compression ratios and very fast
 * decompression, making it ideal for event store data.
 *
 * Uses Bun's built-in zstd support for optimal performance.
 *
 * @example
 * ```ts
 * const compressor = new ZstdCompressor();
 *
 * const compressed = compressor.compress(data, 3);
 * const original = compressor.decompress(compressed);
 * ```
 */
export class ZstdCompressor implements Compressor {
  /**
   * Default compression level.
   * Level 3 provides a good balance of compression ratio and speed.
   */
  private static readonly DEFAULT_LEVEL = 3;

  compress(data: Uint8Array, level: number = ZstdCompressor.DEFAULT_LEVEL): Uint8Array {
    return Bun.zstdCompressSync(data, { level });
  }

  decompress(data: Uint8Array): Uint8Array {
    return Bun.zstdDecompressSync(data);
  }
}
