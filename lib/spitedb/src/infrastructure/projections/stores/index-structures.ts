/**
 * Index structures for efficient projection queries.
 *
 * Two index types:
 * - EqualityIndex: Fast lookups by field value (hash-based)
 * - SortedIndex: Range queries by sorted key (binary search)
 *
 * @example
 * ```ts
 * // Equality index for user status lookups
 * const statusIndex = new EqualityIndex('status');
 * statusIndex.add('user-1', 'active');
 * statusIndex.add('user-2', 'inactive');
 * statusIndex.add('user-3', 'active');
 *
 * const activeUsers = statusIndex.find('active');
 * // Set { 'user-1', 'user-3' }
 *
 * // Sorted index for time-series range queries
 * const dateIndex = new SortedIndex();
 * dateIndex.add('2024-01-01', 100);
 * dateIndex.add('2024-01-02', 150);
 * dateIndex.add('2024-01-03', 200);
 *
 * const range = dateIndex.queryRange('2024-01-01', '2024-01-02');
 * // Map { '2024-01-01' => 100, '2024-01-02' => 150 }
 * ```
 */

/**
 * Equality index for fast lookups by field value.
 *
 * Maps field values to sets of primary keys.
 * Supports O(1) lookups by value.
 */
export class EqualityIndex {
  /** Field name this index is on */
  readonly fieldName: string;

  /** Index: value -> Set<primaryKey> */
  private index = new Map<unknown, Set<string>>();

  /** Reverse index: primaryKey -> value (for efficient updates) */
  private reverse = new Map<string, unknown>();

  constructor(fieldName: string) {
    this.fieldName = fieldName;
  }

  /**
   * Add an entry to the index.
   *
   * @param primaryKey - Row primary key
   * @param fieldValue - Value of the indexed field
   */
  add(primaryKey: string, fieldValue: unknown): void {
    // Get or create set for this value
    let keys = this.index.get(fieldValue);
    if (!keys) {
      keys = new Set();
      this.index.set(fieldValue, keys);
    }
    keys.add(primaryKey);

    // Track in reverse index
    this.reverse.set(primaryKey, fieldValue);
  }

  /**
   * Remove an entry from the index.
   *
   * @param primaryKey - Row primary key
   * @param fieldValue - Value of the indexed field
   */
  remove(primaryKey: string, fieldValue: unknown): void {
    const keys = this.index.get(fieldValue);
    if (keys) {
      keys.delete(primaryKey);
      if (keys.size === 0) {
        this.index.delete(fieldValue);
      }
    }

    this.reverse.delete(primaryKey);
  }

  /**
   * Update an entry (remove old value, add new value).
   *
   * @param primaryKey - Row primary key
   * @param oldValue - Previous value
   * @param newValue - New value
   */
  update(primaryKey: string, oldValue: unknown, newValue: unknown): void {
    if (oldValue === newValue) {
      return;
    }

    this.remove(primaryKey, oldValue);
    this.add(primaryKey, newValue);
  }

  /**
   * Update an entry using the reverse index (when old value is unknown).
   *
   * @param primaryKey - Row primary key
   * @param newValue - New value
   */
  updateFromKey(primaryKey: string, newValue: unknown): void {
    const oldValue = this.reverse.get(primaryKey);
    if (oldValue !== undefined) {
      this.remove(primaryKey, oldValue);
    }
    this.add(primaryKey, newValue);
  }

  /**
   * Find all primary keys with the given field value.
   *
   * @param value - Field value to look up
   * @returns Set of primary keys (empty if none found)
   */
  find(value: unknown): Set<string> {
    return this.index.get(value) ?? new Set();
  }

  /**
   * Check if a value exists in the index.
   */
  has(value: unknown): boolean {
    const keys = this.index.get(value);
    return keys !== undefined && keys.size > 0;
  }

  /**
   * Get all distinct values in the index.
   */
  getValues(): unknown[] {
    return Array.from(this.index.keys());
  }

  /**
   * Get the count of entries for a value.
   */
  countFor(value: unknown): number {
    return this.index.get(value)?.size ?? 0;
  }

  /**
   * Get total entry count.
   */
  get size(): number {
    return this.reverse.size;
  }

  /**
   * Clear the index.
   */
  clear(): void {
    this.index.clear();
    this.reverse.clear();
  }

  /**
   * Get memory usage estimate in bytes.
   */
  getMemoryUsage(): number {
    let bytes = 0;

    // Estimate map overhead per entry: ~50 bytes
    bytes += this.index.size * 50;
    bytes += this.reverse.size * 50;

    // Estimate string sizes
    for (const key of this.reverse.keys()) {
      bytes += key.length * 2; // UTF-16
    }

    // Estimate set overhead per key: ~30 bytes
    for (const keys of this.index.values()) {
      bytes += keys.size * 30;
    }

    return bytes;
  }

  /**
   * Serialize the index for checkpointing.
   */
  toJSON(): { entries: Array<[unknown, string[]]> } {
    const entries: Array<[unknown, string[]]> = [];
    for (const [value, keys] of this.index) {
      entries.push([value, Array.from(keys)]);
    }
    return { entries };
  }

  /**
   * Restore from serialized data.
   */
  static fromJSON(fieldName: string, data: { entries: Array<[unknown, string[]]> }): EqualityIndex {
    const index = new EqualityIndex(fieldName);
    for (const [value, keys] of data.entries) {
      for (const key of keys) {
        index.add(key, value);
      }
    }
    return index;
  }
}

/**
 * Sorted index for range queries.
 *
 * Maintains keys in sorted order for efficient range queries.
 * Uses a sorted array with binary search.
 */
export class SortedIndex<TValue = unknown> {
  /** Sorted array of [key, value] pairs */
  private entries: Array<[string, TValue]> = [];

  /** Map for O(1) key existence check */
  private keyToIndex = new Map<string, number>();

  /** Whether the array needs re-sorting */
  private dirty = false;

  /**
   * Add or update a key-value pair.
   *
   * @param key - Sortable key (e.g., date string "2024-01-01")
   * @param value - Value to store
   */
  set(key: string, value: TValue): void {
    const existingIndex = this.keyToIndex.get(key);

    if (existingIndex !== undefined) {
      // Update existing
      this.entries[existingIndex]![1] = value;
    } else {
      // Add new
      this.entries.push([key, value]);
      this.keyToIndex.set(key, this.entries.length - 1);
      this.dirty = true;
    }
  }

  /**
   * Get value for a key.
   *
   * @param key - Key to look up
   * @returns Value or undefined if not found
   */
  get(key: string): TValue | undefined {
    this.ensureSorted();
    const index = this.binarySearchExact(key);
    return index >= 0 ? this.entries[index]![1] : undefined;
  }

  /**
   * Check if a key exists.
   */
  has(key: string): boolean {
    return this.keyToIndex.has(key);
  }

  /**
   * Delete a key.
   *
   * @param key - Key to delete
   * @returns true if key existed
   */
  delete(key: string): boolean {
    const index = this.keyToIndex.get(key);
    if (index === undefined) {
      return false;
    }

    // Remove from array (mark as deleted for now, compaction later)
    this.entries.splice(index, 1);

    // Rebuild index (simple approach, could optimize)
    this.rebuildKeyToIndex();

    return true;
  }

  /**
   * Query a range of keys (inclusive bounds).
   *
   * @param start - Start key (inclusive), undefined for unbounded
   * @param end - End key (inclusive), undefined for unbounded
   * @returns Map of key -> value for matching entries
   */
  queryRange(start?: string, end?: string): Map<string, TValue> {
    this.ensureSorted();

    const result = new Map<string, TValue>();

    // Find start position
    let startIndex = 0;
    if (start !== undefined) {
      startIndex = this.binarySearchLeft(start);
    }

    // Iterate until end
    for (let i = startIndex; i < this.entries.length; i++) {
      const entry = this.entries[i]!;
      const key = entry[0];

      // Check end bound
      if (end !== undefined && key > end) {
        break;
      }

      result.set(key, entry[1]);
    }

    return result;
  }

  /**
   * Get all entries in sorted order.
   */
  getAll(): Map<string, TValue> {
    this.ensureSorted();
    return new Map(this.entries);
  }

  /**
   * Get total entry count.
   */
  get size(): number {
    return this.entries.length;
  }

  /**
   * Clear the index.
   */
  clear(): void {
    this.entries = [];
    this.keyToIndex.clear();
    this.dirty = false;
  }

  /**
   * Get memory usage estimate in bytes.
   */
  getMemoryUsage(): number {
    let bytes = 0;

    // Array overhead
    bytes += this.entries.length * 30;

    // String keys
    for (const [key] of this.entries) {
      bytes += key.length * 2;
    }

    // Map overhead
    bytes += this.keyToIndex.size * 50;

    return bytes;
  }

  /**
   * Serialize for checkpointing.
   */
  toJSON(): { entries: Array<[string, TValue]> } {
    this.ensureSorted();
    return { entries: [...this.entries] };
  }

  /**
   * Restore from serialized data.
   */
  static fromJSON<T>(data: { entries: Array<[string, T]> }): SortedIndex<T> {
    const index = new SortedIndex<T>();
    for (const [key, value] of data.entries) {
      index.set(key, value);
    }
    return index;
  }

  /**
   * Ensure entries are sorted.
   */
  private ensureSorted(): void {
    if (!this.dirty) {
      return;
    }

    this.entries.sort((a, b) => a[0].localeCompare(b[0]));
    this.rebuildKeyToIndex();
    this.dirty = false;
  }

  /**
   * Rebuild the key-to-index map.
   */
  private rebuildKeyToIndex(): void {
    this.keyToIndex.clear();
    for (let i = 0; i < this.entries.length; i++) {
      this.keyToIndex.set(this.entries[i]![0], i);
    }
  }

  /**
   * Binary search for exact key match.
   * Returns index or -1 if not found.
   */
  private binarySearchExact(key: string): number {
    let left = 0;
    let right = this.entries.length - 1;

    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      const cmp = this.entries[mid]![0].localeCompare(key);

      if (cmp === 0) {
        return mid;
      } else if (cmp < 0) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }

    return -1;
  }

  /**
   * Binary search for leftmost position >= key.
   */
  private binarySearchLeft(key: string): number {
    let left = 0;
    let right = this.entries.length;

    while (left < right) {
      const mid = Math.floor((left + right) / 2);
      if (this.entries[mid]![0].localeCompare(key) < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    return left;
  }
}

/**
 * Collection of indexes for a denormalized view.
 *
 * Manages multiple equality and sorted indexes together.
 */
export class IndexCollection {
  private readonly equalityIndexes = new Map<string, EqualityIndex>();
  private readonly sortedIndexes = new Map<string, SortedIndex<unknown>>();

  /**
   * Add an equality index.
   */
  addEqualityIndex(fieldName: string): EqualityIndex {
    const index = new EqualityIndex(fieldName);
    this.equalityIndexes.set(fieldName, index);
    return index;
  }

  /**
   * Add a sorted index.
   */
  addSortedIndex(fieldName: string): SortedIndex<unknown> {
    const index = new SortedIndex<unknown>();
    this.sortedIndexes.set(fieldName, index);
    return index;
  }

  /**
   * Get an equality index.
   */
  getEqualityIndex(fieldName: string): EqualityIndex | undefined {
    return this.equalityIndexes.get(fieldName);
  }

  /**
   * Get a sorted index.
   */
  getSortedIndex(fieldName: string): SortedIndex<unknown> | undefined {
    return this.sortedIndexes.get(fieldName);
  }

  /**
   * Index a row (add to all indexes).
   *
   * @param primaryKey - Row primary key
   * @param row - Row data
   */
  indexRow(primaryKey: string, row: Record<string, unknown>): void {
    for (const [fieldName, index] of this.equalityIndexes) {
      const value = row[fieldName];
      if (value !== undefined) {
        index.updateFromKey(primaryKey, value);
      }
    }

    for (const [fieldName, index] of this.sortedIndexes) {
      const value = row[fieldName];
      if (value !== undefined) {
        index.set(primaryKey, value);
      }
    }
  }

  /**
   * Remove a row from all indexes.
   *
   * @param primaryKey - Row primary key
   * @param row - Row data (needed to know what values to remove)
   */
  removeRow(primaryKey: string, row: Record<string, unknown>): void {
    for (const [fieldName, index] of this.equalityIndexes) {
      const value = row[fieldName];
      if (value !== undefined) {
        index.remove(primaryKey, value);
      }
    }

    for (const [, index] of this.sortedIndexes) {
      index.delete(primaryKey);
    }
  }

  /**
   * Clear all indexes.
   */
  clear(): void {
    for (const index of this.equalityIndexes.values()) {
      index.clear();
    }
    for (const index of this.sortedIndexes.values()) {
      index.clear();
    }
  }

  /**
   * Get total memory usage.
   */
  getMemoryUsage(): number {
    let bytes = 0;
    for (const index of this.equalityIndexes.values()) {
      bytes += index.getMemoryUsage();
    }
    for (const index of this.sortedIndexes.values()) {
      bytes += index.getMemoryUsage();
    }
    return bytes;
  }
}
