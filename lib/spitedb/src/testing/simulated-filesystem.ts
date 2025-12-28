import type { FileSystem, FileHandle, OpenMode, FileStat } from '../ports/storage/filesystem';
import type { Clock } from '../ports/time/clock';

/**
 * Internal state for an open file handle.
 */
interface FileState {
  /** Path to the file */
  path: string;
  /** Current read/write position */
  position: number;
  /** Writes that haven't been synced yet */
  unflushed: Array<{ offset: number; data: Uint8Array }>;
  /** Open mode */
  mode: OpenMode;
  /** Current lock held by this handle */
  lock?: 'shared' | 'exclusive';
}

/**
 * Lock state for a file.
 */
interface LockState {
  /** File descriptors holding shared locks */
  sharedLocks: Set<number>;
  /** File descriptor holding exclusive lock (or null) */
  exclusiveLock: number | null;
}

/**
 * Configuration for fault injection during tests.
 */
export interface FaultConfig {
  /** If true, sync operations will throw an error */
  syncFails?: boolean;
  /** Delay sync operations by this many milliseconds */
  syncDelayMs?: number;
  /** If true, simulate partial writes (corruption scenario) */
  partialWrite?: boolean;
  /** If true, read operations will throw an error */
  readFails?: boolean;
  /** If true, write operations will throw an error */
  writeFails?: boolean;
}

/**
 * Simulated filesystem for deterministic testing.
 *
 * This implementation provides:
 * - In-memory file storage
 * - Fault injection (sync failures, partial writes, etc.)
 * - Crash simulation (lose unflushed writes)
 * - Inspection APIs for test assertions
 *
 * @example
 * ```ts
 * const clock = new SimulatedClock();
 * const fs = new SimulatedFileSystem(clock);
 *
 * // Write some data
 * const handle = await fs.open('/data/test.log', 'write');
 * await fs.write(handle, new TextEncoder().encode('hello'));
 *
 * // Simulate crash before sync
 * fs.crash();
 *
 * // Data is lost!
 * expect(fs.getFileContent('/data/test.log')).toBeUndefined();
 * ```
 */
export class SimulatedFileSystem implements FileSystem {
  /** Durable file contents (after sync) */
  private files = new Map<string, Uint8Array>();

  /** Directory set */
  private directories = new Set<string>(['/']);

  /** Open file handles */
  private handles = new Map<number, FileState>();

  /** File locks by path */
  private locks = new Map<string, LockState>();

  /** Next file descriptor number */
  private nextFd = 1;

  /** Current fault configuration */
  private faults: FaultConfig = {};

  /**
   * Create a simulated filesystem.
   * @param clock - Optional clock for simulating sync delays
   */
  constructor(private readonly clock?: Clock) {}

  // ============================================================
  // Fault Injection API (for tests)
  // ============================================================

  /**
   * Inject faults into filesystem operations.
   * @param fault - Fault configuration to merge with current config
   */
  injectFault(fault: Partial<FaultConfig>): void {
    this.faults = { ...this.faults, ...fault };
  }

  /**
   * Clear all fault configurations.
   */
  clearFaults(): void {
    this.faults = {};
  }

  /**
   * Simulate a crash - lose all unflushed writes.
   * This mimics what happens when power is lost before sync completes.
   */
  crash(): void {
    // Lose all unflushed writes
    for (const state of this.handles.values()) {
      state.unflushed = [];
    }
    // Close all handles (and release all locks)
    this.handles.clear();
    this.locks.clear();
  }

  // ============================================================
  // Inspection API (for test assertions)
  // ============================================================

  /**
   * Get the durable content of a file.
   * Only returns data that has been synced.
   */
  getFileContent(path: string): Uint8Array | undefined {
    return this.files.get(path);
  }

  /**
   * Get file content as a string.
   * Convenience method for text file assertions.
   */
  getFileContentAsString(path: string): string | undefined {
    const content = this.files.get(path);
    return content ? new TextDecoder().decode(content) : undefined;
  }

  /**
   * Set file content directly (for test setup).
   */
  setFileContent(path: string, content: Uint8Array): void {
    this.files.set(path, content);
  }

  /**
   * Get all file paths in the filesystem.
   */
  getAllFilePaths(): string[] {
    return Array.from(this.files.keys());
  }

  // ============================================================
  // FileSystem Implementation
  // ============================================================

  async open(path: string, mode: OpenMode): Promise<FileHandle> {
    const fd = this.nextFd++;

    if (mode === 'write' || mode === 'append') {
      // Create file if it doesn't exist
      if (!this.files.has(path)) {
        this.files.set(path, new Uint8Array(0));
      }
      // Write mode truncates the file
      if (mode === 'write') {
        this.files.set(path, new Uint8Array(0));
      }
    } else if (mode === 'read' && !this.files.has(path)) {
      throw new Error(`ENOENT: no such file: ${path}`);
    } else if (mode === 'readwrite' && !this.files.has(path)) {
      throw new Error(`ENOENT: no such file: ${path}`);
    }

    const position = mode === 'append' ? (this.files.get(path)?.length ?? 0) : 0;

    this.handles.set(fd, {
      path,
      position,
      unflushed: [],
      mode,
    });

    return { fd };
  }

  async close(handle: FileHandle): Promise<void> {
    const state = this.handles.get(handle.fd);
    if (!state) {
      return;
    }

    // Release any locks held by this handle
    if (state.lock) {
      await this.funlock(handle);
    }

    // Note: We don't automatically sync on close - this matches real behavior
    // where data can be lost if close happens without sync
    this.handles.delete(handle.fd);
  }

  async read(handle: FileHandle, offset: number, length: number): Promise<Uint8Array> {
    if (this.faults.readFails) {
      throw new Error('Simulated read failure');
    }

    const state = this.handles.get(handle.fd);
    if (!state) {
      throw new Error('Invalid file handle');
    }

    // Read from durable storage (not unflushed writes)
    const content = this.files.get(state.path) ?? new Uint8Array(0);

    // Handle reads past end of file
    if (offset >= content.length) {
      return new Uint8Array(0);
    }

    const end = Math.min(offset + length, content.length);
    return content.slice(offset, end);
  }

  async write(handle: FileHandle, data: Uint8Array, offset?: number): Promise<number> {
    if (this.faults.writeFails) {
      throw new Error('Simulated write failure');
    }

    const state = this.handles.get(handle.fd);
    if (!state) {
      throw new Error('Invalid file handle');
    }

    if (state.mode === 'read') {
      throw new Error('Cannot write to file opened in read mode');
    }

    const writeOffset = offset ?? state.position;

    // Stage write (not yet durable until sync)
    state.unflushed.push({
      offset: writeOffset,
      data: new Uint8Array(data), // Copy to avoid external mutation
    });

    state.position = writeOffset + data.length;
    return data.length;
  }

  async sync(handle: FileHandle): Promise<void> {
    if (this.faults.syncFails) {
      throw new Error('Simulated sync failure');
    }

    if (this.faults.syncDelayMs && this.clock) {
      await this.clock.sleep(this.faults.syncDelayMs);
    }

    const state = this.handles.get(handle.fd);
    if (!state) {
      throw new Error('Invalid file handle');
    }

    // Apply unflushed writes to durable storage
    let content = this.files.get(state.path) ?? new Uint8Array(0);

    for (const write of state.unflushed) {
      if (this.faults.partialWrite) {
        // Simulate partial write (corruption scenario)
        // Only write half the data
        const partialData = write.data.slice(0, Math.floor(write.data.length / 2));
        content = this.applyWrite(content, write.offset, partialData);
        this.faults.partialWrite = false; // Only affect one write
        break;
      }
      content = this.applyWrite(content, write.offset, write.data);
    }

    this.files.set(state.path, content);
    state.unflushed = [];
  }

  async readFile(path: string): Promise<Uint8Array> {
    if (this.faults.readFails) {
      throw new Error('Simulated read failure');
    }

    const content = this.files.get(path);
    if (!content) {
      throw new Error(`ENOENT: no such file: ${path}`);
    }

    return content;
  }

  async readFileSlice(path: string, start: number, end: number): Promise<Uint8Array> {
    if (this.faults.readFails) {
      throw new Error('Simulated read failure');
    }

    const content = this.files.get(path);
    if (!content) {
      throw new Error(`ENOENT: no such file: ${path}`);
    }

    return content.slice(start, end);
  }

  async stat(path: string): Promise<FileStat> {
    if (this.directories.has(path)) {
      return {
        size: 0,
        isFile: false,
        isDirectory: true,
        mtime: new Date(),
      };
    }

    const content = this.files.get(path);
    if (!content) {
      throw new Error(`ENOENT: no such file or directory: ${path}`);
    }

    return {
      size: content.length,
      isFile: true,
      isDirectory: false,
      mtime: new Date(),
    };
  }

  async exists(path: string): Promise<boolean> {
    return this.files.has(path) || this.directories.has(path);
  }

  async truncate(handle: FileHandle, length: number): Promise<void> {
    const state = this.handles.get(handle.fd);
    if (!state) {
      throw new Error('Invalid file handle');
    }

    const content = this.files.get(state.path) ?? new Uint8Array(0);

    if (length < content.length) {
      this.files.set(state.path, content.slice(0, length));
    } else if (length > content.length) {
      // Extend with zeros
      const extended = new Uint8Array(length);
      extended.set(content);
      this.files.set(state.path, extended);
    }
  }

  async rename(from: string, to: string): Promise<void> {
    const content = this.files.get(from);
    if (!content) {
      throw new Error(`ENOENT: no such file: ${from}`);
    }

    this.files.set(to, content);
    this.files.delete(from);
  }

  async unlink(path: string): Promise<void> {
    if (!this.files.has(path)) {
      throw new Error(`ENOENT: no such file: ${path}`);
    }
    this.files.delete(path);
  }

  async mkdir(path: string, options?: { recursive?: boolean }): Promise<void> {
    if (options?.recursive) {
      const parts = path.split('/').filter(Boolean);
      let current = '';
      for (const part of parts) {
        current += '/' + part;
        this.directories.add(current);
      }
    } else {
      // Check parent exists
      const parent = path.substring(0, path.lastIndexOf('/')) || '/';
      if (!this.directories.has(parent)) {
        throw new Error(`ENOENT: no such directory: ${parent}`);
      }
      this.directories.add(path);
    }
  }

  async readdir(path: string): Promise<string[]> {
    if (!this.directories.has(path)) {
      throw new Error(`ENOENT: no such directory: ${path}`);
    }

    const entries = new Set<string>();
    const prefix = path.endsWith('/') ? path : path + '/';

    // Find files in this directory
    for (const filePath of this.files.keys()) {
      if (filePath.startsWith(prefix)) {
        const relative = filePath.slice(prefix.length);
        const name = relative.split('/')[0];
        if (name) {
          entries.add(name);
        }
      }
    }

    // Find subdirectories
    for (const dirPath of this.directories) {
      if (dirPath.startsWith(prefix) && dirPath !== path) {
        const relative = dirPath.slice(prefix.length);
        const name = relative.split('/')[0];
        if (name) {
          entries.add(name);
        }
      }
    }

    return Array.from(entries).sort();
  }

  async rmdir(path: string, options?: { recursive?: boolean }): Promise<void> {
    if (!this.directories.has(path)) {
      throw new Error(`ENOENT: no such directory: ${path}`);
    }

    const prefix = path.endsWith('/') ? path : path + '/';

    if (options?.recursive) {
      // Delete all files in directory
      for (const filePath of this.files.keys()) {
        if (filePath.startsWith(prefix)) {
          this.files.delete(filePath);
        }
      }

      // Delete all subdirectories
      for (const dirPath of this.directories) {
        if (dirPath.startsWith(prefix)) {
          this.directories.delete(dirPath);
        }
      }
    } else {
      // Check if directory is empty
      for (const filePath of this.files.keys()) {
        if (filePath.startsWith(prefix)) {
          throw new Error(`ENOTEMPTY: directory not empty: ${path}`);
        }
      }
      for (const dirPath of this.directories) {
        if (dirPath.startsWith(prefix) && dirPath !== path) {
          throw new Error(`ENOTEMPTY: directory not empty: ${path}`);
        }
      }
    }

    this.directories.delete(path);
  }

  async mmap(path: string): Promise<Uint8Array> {
    // In simulation, mmap is equivalent to readFile since we're in-memory anyway
    // Returns a copy to simulate real mmap behavior where the view is read-only
    if (this.faults.readFails) {
      throw new Error('Simulated read failure');
    }

    const content = this.files.get(path);
    if (!content) {
      throw new Error(`ENOENT: no such file: ${path}`);
    }

    // Return a copy to avoid mutation issues in tests
    return new Uint8Array(content);
  }

  async flock(handle: FileHandle, mode: 'shared' | 'exclusive'): Promise<void> {
    const state = this.handles.get(handle.fd);
    if (!state) {
      throw new Error('Invalid file handle');
    }

    const path = state.path;

    // Get or create lock state for this path
    let lockState = this.locks.get(path);
    if (!lockState) {
      lockState = { sharedLocks: new Set(), exclusiveLock: null };
      this.locks.set(path, lockState);
    }

    // Release any existing lock this handle might hold
    if (state.lock) {
      await this.funlock(handle);
      // Re-fetch lock state as funlock may have cleaned it up
      lockState = this.locks.get(path);
      if (!lockState) {
        lockState = { sharedLocks: new Set(), exclusiveLock: null };
        this.locks.set(path, lockState);
      }
    }

    if (mode === 'exclusive') {
      // Exclusive lock requires no other locks exist
      if (lockState.exclusiveLock !== null) {
        throw new Error(
          `Failed to acquire exclusive lock on ${path}: ` +
            `another process holds an exclusive lock`
        );
      }
      if (lockState.sharedLocks.size > 0) {
        throw new Error(
          `Failed to acquire exclusive lock on ${path}: ` +
            `${lockState.sharedLocks.size} shared lock(s) are held`
        );
      }
      lockState.exclusiveLock = handle.fd;
    } else {
      // Shared lock requires no exclusive lock
      if (lockState.exclusiveLock !== null) {
        throw new Error(
          `Failed to acquire shared lock on ${path}: ` +
            `another process holds an exclusive lock`
        );
      }
      lockState.sharedLocks.add(handle.fd);
    }

    state.lock = mode;
  }

  async funlock(handle: FileHandle): Promise<void> {
    const state = this.handles.get(handle.fd);
    if (!state) {
      throw new Error('Invalid file handle');
    }

    if (!state.lock) {
      return; // No lock to release
    }

    const lockState = this.locks.get(state.path);
    if (lockState) {
      if (state.lock === 'exclusive') {
        if (lockState.exclusiveLock === handle.fd) {
          lockState.exclusiveLock = null;
        }
      } else {
        lockState.sharedLocks.delete(handle.fd);
      }

      // Clean up empty lock state
      if (lockState.exclusiveLock === null && lockState.sharedLocks.size === 0) {
        this.locks.delete(state.path);
      }
    }

    delete state.lock;
  }

  /**
   * Apply a write to file content, extending if necessary.
   */
  private applyWrite(content: Uint8Array, offset: number, data: Uint8Array): Uint8Array {
    const newLength = Math.max(content.length, offset + data.length);
    const newContent = new Uint8Array(newLength);
    newContent.set(content);
    newContent.set(data, offset);
    return newContent;
  }
}
