/**
 * Deterministic Pseudo-Random Number Generator for DST.
 *
 * Uses xorshift128+ algorithm for fast, high-quality randomness.
 * Same seed always produces the same sequence, enabling
 * reproducible test failures.
 *
 * @example
 * ```ts
 * const random = new SeededRandom(12345);
 * console.log(random.next());     // Always 0.7297...
 * console.log(random.int(1, 10)); // Always 8
 *
 * // On failure, log seed for reproduction
 * console.log(`SEED=${random.getSeed()}`);
 * ```
 */

/**
 * Generate a random seed from system entropy.
 */
function generateSeed(): number {
  return Math.floor(Math.random() * 0x7fffffff);
}

/**
 * Deterministic PRNG using xorshift128+ algorithm.
 */
export class SeededRandom {
  private s0: bigint;
  private s1: bigint;
  private readonly seed: number;

  /**
   * Create a new seeded random generator.
   *
   * @param seed - Optional seed. If not provided, generates random seed and logs it.
   */
  constructor(seed?: number) {
    this.seed = seed ?? generateSeed();

    // Initialize state from seed using splitmix64
    let state = BigInt(this.seed);
    state = (state ^ (state >> 30n)) * 0xbf58476d1ce4e5b9n;
    state = (state ^ (state >> 27n)) * 0x94d049bb133111ebn;
    this.s0 = state ^ (state >> 31n);

    state = BigInt(this.seed + 1);
    state = (state ^ (state >> 30n)) * 0xbf58476d1ce4e5b9n;
    state = (state ^ (state >> 27n)) * 0x94d049bb133111ebn;
    this.s1 = state ^ (state >> 31n);

    // Log seed for reproduction (useful when no explicit seed provided)
    if (seed === undefined && typeof process !== 'undefined') {
      console.log(`[SeededRandom] Generated seed: ${this.seed}`);
    }
  }

  /**
   * Get the seed used to initialize this generator.
   */
  getSeed(): number {
    return this.seed;
  }

  /**
   * Generate next random number in [0, 1).
   */
  next(): number {
    // xorshift128+
    let s1 = this.s0;
    const s0 = this.s1;
    this.s0 = s0;
    s1 ^= s1 << 23n;
    s1 ^= s1 >> 17n;
    s1 ^= s0;
    s1 ^= s0 >> 26n;
    this.s1 = s1;

    // Convert to [0, 1) float
    const result = (this.s0 + this.s1) & 0xffffffffffffffffn;
    return Number(result & 0xfffffffffffffn) / 0x10000000000000;
  }

  /**
   * Generate random integer in [min, max] (inclusive).
   */
  int(min: number, max: number): number {
    return Math.floor(this.next() * (max - min + 1)) + min;
  }

  /**
   * Generate random boolean with given probability of true.
   *
   * @param probability - Probability of returning true (default: 0.5)
   */
  bool(probability: number = 0.5): boolean {
    return this.next() < probability;
  }

  /**
   * Pick a random element from an array.
   */
  choice<T>(arr: readonly T[]): T {
    if (arr.length === 0) {
      throw new Error('Cannot choose from empty array');
    }
    return arr[this.int(0, arr.length - 1)]!;
  }

  /**
   * Pick N random elements from an array (without replacement).
   */
  sample<T>(arr: readonly T[], count: number): T[] {
    if (count > arr.length) {
      throw new Error(`Cannot sample ${count} from array of length ${arr.length}`);
    }

    const copy = [...arr];
    const result: T[] = [];

    for (let i = 0; i < count; i++) {
      const idx = this.int(0, copy.length - 1);
      result.push(copy[idx]!);
      copy.splice(idx, 1);
    }

    return result;
  }

  /**
   * Shuffle an array in-place using Fisher-Yates.
   */
  shuffle<T>(arr: T[]): T[] {
    for (let i = arr.length - 1; i > 0; i--) {
      const j = this.int(0, i);
      [arr[i], arr[j]] = [arr[j]!, arr[i]!];
    }
    return arr;
  }

  /**
   * Generate exponentially distributed random number.
   * Useful for realistic delay simulation.
   *
   * @param mean - Mean of the distribution
   */
  exponential(mean: number): number {
    return -mean * Math.log(1 - this.next());
  }

  /**
   * Generate normally distributed random number using Box-Muller.
   *
   * @param mean - Mean of the distribution
   * @param stddev - Standard deviation
   */
  normal(mean: number = 0, stddev: number = 1): number {
    const u1 = this.next();
    const u2 = this.next();
    const z0 = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
    return z0 * stddev + mean;
  }

  /**
   * Create a child random generator with deterministic sub-seed.
   * Useful for isolating randomness in sub-components.
   */
  fork(): SeededRandom {
    return new SeededRandom(this.int(0, 0x7fffffff));
  }

  /**
   * Generate a random string of given length.
   *
   * @param length - Length of string
   * @param charset - Characters to use (default: alphanumeric)
   */
  string(
    length: number,
    charset: string = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  ): string {
    let result = '';
    for (let i = 0; i < length; i++) {
      result += charset[this.int(0, charset.length - 1)];
    }
    return result;
  }

  /**
   * Generate a random UUID v4 (deterministic based on seed).
   */
  uuid(): string {
    const hex = '0123456789abcdef';
    let uuid = '';

    for (let i = 0; i < 36; i++) {
      if (i === 8 || i === 13 || i === 18 || i === 23) {
        uuid += '-';
      } else if (i === 14) {
        uuid += '4'; // Version 4
      } else if (i === 19) {
        uuid += hex[this.int(8, 11)]; // Variant bits
      } else {
        uuid += hex[this.int(0, 15)];
      }
    }

    return uuid;
  }
}

/**
 * Get seed from environment variable or generate new one.
 */
export function getSeedFromEnv(): number {
  const envSeed = process.env['SEED'];
  if (envSeed) {
    const parsed = parseInt(envSeed, 10);
    if (!isNaN(parsed)) {
      return parsed;
    }
  }
  return generateSeed();
}

/**
 * Helper to wrap test assertions with seed reporting on failure.
 */
export async function withSeedReporting<T>(
  seed: number,
  testName: string,
  fn: () => Promise<T>
): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    console.error(`\n[DST] Test failed: ${testName}`);
    console.error(`[DST] Seed: ${seed}`);
    console.error(`[DST] To reproduce: SEED=${seed} bun test\n`);
    throw error;
  }
}
