/**
 * Simple LRU (Least Recently Used) cache implementation.
 *
 * @example
 * ```ts
 * const cache = new LRUCache<string, number>(3);
 *
 * cache.set('a', 1);
 * cache.set('b', 2);
 * cache.set('c', 3);
 * cache.set('d', 4); // 'a' is evicted
 *
 * cache.get('b'); // returns 2, marks 'b' as recently used
 * cache.set('e', 5); // 'c' is evicted (least recently used)
 * ```
 */
export class LRUCache<K, V> {
  private cache = new Map<K, V>();
  private readonly maxSize: number;

  constructor(maxSize: number) {
    if (maxSize < 1) {
      throw new Error('LRUCache maxSize must be at least 1');
    }
    this.maxSize = maxSize;
  }

  /**
   * Get a value from the cache.
   * Marks the key as recently used.
   *
   * @param key - Key to look up
   * @returns Value or undefined if not found
   */
  get(key: K): V | undefined {
    if (!this.cache.has(key)) {
      return undefined;
    }

    // Move to end (most recently used)
    const value = this.cache.get(key)!;
    this.cache.delete(key);
    this.cache.set(key, value);
    return value;
  }

  /**
   * Set a value in the cache.
   * May evict the least recently used entry if at capacity.
   *
   * @param key - Key to set
   * @param value - Value to store
   */
  set(key: K, value: V): void {
    // If key exists, remove it first (will re-add at end)
    if (this.cache.has(key)) {
      this.cache.delete(key);
    }

    // Check if we need to evict
    if (this.cache.size >= this.maxSize) {
      // Delete the first (oldest) entry
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey !== undefined) {
        this.cache.delete(oldestKey);
      }
    }

    this.cache.set(key, value);
  }

  /**
   * Check if a key exists in the cache.
   * Does NOT update access order.
   */
  has(key: K): boolean {
    return this.cache.has(key);
  }

  /**
   * Delete a key from the cache.
   *
   * @returns true if key was found and deleted
   */
  delete(key: K): boolean {
    return this.cache.delete(key);
  }

  /**
   * Clear all entries from the cache.
   */
  clear(): void {
    this.cache.clear();
  }

  /**
   * Get current number of entries in cache.
   */
  get size(): number {
    return this.cache.size;
  }

  /**
   * Get all keys in cache (oldest to newest).
   */
  keys(): IterableIterator<K> {
    return this.cache.keys();
  }

  /**
   * Get all values in cache (oldest to newest).
   */
  values(): IterableIterator<V> {
    return this.cache.values();
  }

  /**
   * Get all entries in cache (oldest to newest).
   */
  entries(): IterableIterator<[K, V]> {
    return this.cache.entries();
  }
}
