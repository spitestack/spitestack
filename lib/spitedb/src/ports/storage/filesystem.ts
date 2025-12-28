/**
 * File opening mode for filesystem operations.
 */
export type OpenMode = 'read' | 'write' | 'append' | 'readwrite';

/**
 * File statistics returned by stat operations.
 */
export interface FileStat {
  /** Size of the file in bytes */
  size: number;
  /** Whether this is a regular file */
  isFile: boolean;
  /** Whether this is a directory */
  isDirectory: boolean;
  /** Last modification time */
  mtime: Date;
}

/**
 * Handle to an open file.
 */
export interface FileHandle {
  /** File descriptor number */
  readonly fd: number;
}

/**
 * Abstract filesystem interface for dependency injection.
 *
 * This abstraction allows us to:
 * - Use real filesystem in production (BunFileSystem)
 * - Use simulated filesystem in tests (SimulatedFileSystem)
 * - Inject faults for crash/corruption testing
 *
 * @example
 * ```ts
 * // Production
 * const fs = new BunFileSystem();
 *
 * // Testing
 * const fs = new SimulatedFileSystem(clock);
 * fs.injectFault({ syncFails: true });
 * ```
 */
export interface FileSystem {
  // === File Operations ===

  /**
   * Open a file for reading or writing.
   * @throws {Error} if file doesn't exist and mode is 'read'
   */
  open(path: string, mode: OpenMode): Promise<FileHandle>;

  /**
   * Close an open file handle.
   */
  close(handle: FileHandle): Promise<void>;

  // === Read/Write ===

  /**
   * Read bytes from a file at a specific offset.
   * @param handle - Open file handle
   * @param offset - Byte offset to start reading from
   * @param length - Number of bytes to read
   * @returns Uint8Array containing the bytes read (may be shorter than length)
   */
  read(handle: FileHandle, offset: number, length: number): Promise<Uint8Array>;

  /**
   * Write bytes to a file.
   * @param handle - Open file handle
   * @param data - Bytes to write
   * @param offset - Optional byte offset (append if not specified)
   * @returns Number of bytes written
   */
  write(handle: FileHandle, data: Uint8Array, offset?: number): Promise<number>;

  // === Durability ===

  /**
   * Flush file data to disk (fsync).
   * This ensures data durability - after sync returns successfully,
   * the data is guaranteed to survive a crash.
   */
  sync(handle: FileHandle): Promise<void>;

  // === Fast Read Operations ===

  /**
   * Read an entire file into memory.
   * Uses Bun.file().bytes() internally for optimal performance.
   * @param path - Path to the file
   * @returns File contents as Uint8Array
   */
  readFile(path: string): Promise<Uint8Array>;

  /**
   * Read a slice of a file.
   * Efficient for reading specific portions without loading the whole file.
   * @param path - Path to the file
   * @param start - Start byte offset (inclusive)
   * @param end - End byte offset (exclusive)
   * @returns Slice of file contents as Uint8Array
   */
  readFileSlice(path: string, start: number, end: number): Promise<Uint8Array>;

  // === File Management ===

  /**
   * Get file or directory statistics.
   * @throws {Error} if path doesn't exist
   */
  stat(path: string): Promise<FileStat>;

  /**
   * Check if a file or directory exists.
   */
  exists(path: string): Promise<boolean>;

  /**
   * Truncate a file to the specified length.
   */
  truncate(handle: FileHandle, length: number): Promise<void>;

  /**
   * Atomically rename a file.
   * This is used for safe writes - write to temp file, fsync, then rename.
   */
  rename(from: string, to: string): Promise<void>;

  /**
   * Delete a file.
   */
  unlink(path: string): Promise<void>;

  // === Directory Operations ===

  /**
   * Create a directory.
   * @param path - Directory path to create
   * @param options.recursive - If true, create parent directories as needed
   */
  mkdir(path: string, options?: { recursive?: boolean }): Promise<void>;

  /**
   * List entries in a directory.
   * @returns Array of entry names (not full paths)
   */
  readdir(path: string): Promise<string[]>;

  /**
   * Remove a directory.
   * @param path - Directory path to remove
   * @param options.recursive - If true, remove contents recursively
   */
  rmdir(path: string, options?: { recursive?: boolean }): Promise<void>;

  // === Memory Mapping ===

  /**
   * Memory-map a file for efficient random access reads.
   * Returns a Uint8Array backed by the mapped file.
   *
   * This is optimal for index files that need frequent random access
   * since the OS manages caching and only loads pages on demand.
   *
   * Falls back to readFile() if mmap is not supported.
   *
   * @param path - Path to the file to map
   * @returns Uint8Array view of the file contents
   * @throws {Error} if file doesn't exist
   */
  mmap(path: string): Promise<Uint8Array>;

  // === File Locking ===

  /**
   * Acquire a lock on an open file.
   *
   * Used to prevent multiple processes from corrupting data by ensuring
   * exclusive access to critical files (like the data directory lock file).
   *
   * - 'exclusive': Only one process can hold this lock. Others will block or fail.
   * - 'shared': Multiple processes can hold shared locks, but no exclusive locks.
   *
   * The lock is automatically released when the file handle is closed.
   *
   * @param handle - Open file handle to lock
   * @param mode - Lock mode: 'exclusive' for writers, 'shared' for readers
   * @throws {Error} if lock cannot be acquired (e.g., another process holds it)
   */
  flock(handle: FileHandle, mode: 'shared' | 'exclusive'): Promise<void>;

  /**
   * Release a lock on an open file.
   *
   * Note: Locks are automatically released when the file handle is closed,
   * so explicit unlock is only needed if you want to release early.
   *
   * @param handle - Open file handle to unlock
   */
  funlock(handle: FileHandle): Promise<void>;
}
