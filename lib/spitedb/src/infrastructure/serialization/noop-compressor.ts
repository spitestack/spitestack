import type { Compressor } from '../../ports/serialization/compressor';

/**
 * No-op compressor for maximum throughput.
 *
 * Use this for benchmarks or when storage size is less important than speed.
 */
export class NoopCompressor implements Compressor {
  compress(data: Uint8Array): Uint8Array {
    return data;
  }

  decompress(data: Uint8Array): Uint8Array {
    return data;
  }
}
