/**
 * CRC32 implementation for data integrity checksums.
 *
 * Uses the IEEE polynomial (0xEDB88320), the same as used by:
 * - Ethernet
 * - gzip/zlib
 * - PNG
 * - Most file systems
 *
 * @example
 * ```ts
 * const data = new TextEncoder().encode('hello world');
 * const checksum = crc32(data);
 * // checksum = 0x0D4A1185
 * ```
 */

/**
 * Pre-computed lookup table for fast CRC32 calculation.
 * Table-driven approach is ~8x faster than bit-by-bit calculation.
 */
const CRC32_TABLE = new Uint32Array(256);

// Initialize lookup table using IEEE polynomial
const POLYNOMIAL = 0xedb88320;
for (let i = 0; i < 256; i++) {
  let crc = i;
  for (let j = 0; j < 8; j++) {
    crc = crc & 1 ? (crc >>> 1) ^ POLYNOMIAL : crc >>> 1;
  }
  CRC32_TABLE[i] = crc;
}

/**
 * Calculate CRC32 checksum for a byte array.
 *
 * @param data - Input bytes to calculate checksum for
 * @param initial - Optional initial CRC value (default: 0xFFFFFFFF)
 * @returns 32-bit CRC value
 */
export function crc32(data: Uint8Array, initial: number = 0xffffffff): number {
  let crc = initial;

  for (let i = 0; i < data.length; i++) {
    const tableIndex = (crc ^ data[i]!) & 0xff;
    crc = CRC32_TABLE[tableIndex]! ^ (crc >>> 8);
  }

  // XOR with 0xFFFFFFFF for final value (standard CRC32 finalization)
  return (crc ^ 0xffffffff) >>> 0;
}

/**
 * Calculate CRC32 incrementally by combining multiple data chunks.
 *
 * @example
 * ```ts
 * const calc = new CRC32Calculator();
 * calc.update(chunk1);
 * calc.update(chunk2);
 * const checksum = calc.finalize();
 * ```
 */
export class CRC32Calculator {
  private crc: number = 0xffffffff;

  /**
   * Add more data to the running CRC calculation.
   */
  update(data: Uint8Array): void {
    for (let i = 0; i < data.length; i++) {
      const tableIndex = (this.crc ^ data[i]!) & 0xff;
      this.crc = CRC32_TABLE[tableIndex]! ^ (this.crc >>> 8);
    }
  }

  /**
   * Get the final CRC32 value.
   * After calling finalize(), the calculator is reset for reuse.
   */
  finalize(): number {
    const result = (this.crc ^ 0xffffffff) >>> 0;
    this.crc = 0xffffffff; // Reset for reuse
    return result;
  }

  /**
   * Reset the calculator to initial state.
   */
  reset(): void {
    this.crc = 0xffffffff;
  }
}
