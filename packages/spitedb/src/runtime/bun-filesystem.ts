import {
  openSync,
  closeSync,
  readSync,
  writeSync,
  statSync,
  existsSync,
  ftruncateSync,
  renameSync,
  unlinkSync,
  mkdirSync,
  readdirSync,
  rmdirSync,
} from 'node:fs';
import { open as openAsync } from 'node:fs/promises';
import type { FileSystem, FileHandle, OpenMode, FileStat } from '../interfaces/filesystem';

// Map to store async file handles for sync() operation
const asyncHandles = new Map<number, import('node:fs/promises').FileHandle>();

/**
 * Production filesystem implementation using hybrid Bun/Node.js APIs.
 *
 * Uses a hybrid approach optimized for our async-only architecture:
 * - Writes: node:fs/promises for async fsync (non-blocking durability)
 * - Reads: Bun.file().bytes() for optimal read performance
 * - Other ops: sync APIs wrapped in async (fast in Bun)
 *
 * @example
 * ```ts
 * const fs = new BunFileSystem();
 * const handle = await fs.open('/data/events.log', 'append');
 * await fs.write(handle, data);
 * await fs.sync(handle);  // Async fsync for durability
 * await fs.close(handle);
 *
 * // Fast reads using Bun native API
 * const content = await fs.readFile('/data/segment.log');
 * ```
 */
export class BunFileSystem implements FileSystem {
  async open(path: string, mode: OpenMode): Promise<FileHandle> {
    const flags = this.modeToFlags(mode);
    const fd = openSync(path, flags);

    // Also open an async handle for sync() operations
    // This allows truly async fsync without blocking the event loop
    if (mode !== 'read') {
      const asyncHandle = await openAsync(path, flags);
      asyncHandles.set(fd, asyncHandle);
    }

    return { fd };
  }

  async close(handle: FileHandle): Promise<void> {
    // Close async handle if it exists
    const asyncHandle = asyncHandles.get(handle.fd);
    if (asyncHandle) {
      await asyncHandle.close();
      asyncHandles.delete(handle.fd);
    }
    closeSync(handle.fd);
  }

  async read(handle: FileHandle, offset: number, length: number): Promise<Uint8Array> {
    const buffer = new Uint8Array(length);
    const bytesRead = readSync(handle.fd, buffer, 0, length, offset);
    return buffer.subarray(0, bytesRead);
  }

  async write(handle: FileHandle, data: Uint8Array, offset?: number): Promise<number> {
    return writeSync(handle.fd, data, 0, data.length, offset ?? null);
  }

  async sync(handle: FileHandle): Promise<void> {
    // Use async fsync for non-blocking durability
    const asyncHandle = asyncHandles.get(handle.fd);
    if (asyncHandle) {
      await asyncHandle.sync();
    } else {
      // Fallback to sync version for read-only handles (shouldn't happen)
      const { fsyncSync } = await import('node:fs');
      fsyncSync(handle.fd);
    }
  }

  async readFile(path: string): Promise<Uint8Array> {
    // Use Bun's native file API for optimal read performance
    return Bun.file(path).bytes();
  }

  async readFileSlice(path: string, start: number, end: number): Promise<Uint8Array> {
    // Use Bun's native slice for efficient partial reads
    const file = Bun.file(path);
    const blob = file.slice(start, end);
    const arrayBuffer = await blob.arrayBuffer();
    return new Uint8Array(arrayBuffer);
  }

  async stat(path: string): Promise<FileStat> {
    const stats = statSync(path);
    return {
      size: stats.size,
      isFile: stats.isFile(),
      isDirectory: stats.isDirectory(),
      mtime: stats.mtime,
    };
  }

  async exists(path: string): Promise<boolean> {
    return existsSync(path);
  }

  async truncate(handle: FileHandle, length: number): Promise<void> {
    ftruncateSync(handle.fd, length);
  }

  async rename(from: string, to: string): Promise<void> {
    renameSync(from, to);
  }

  async unlink(path: string): Promise<void> {
    unlinkSync(path);
  }

  async mkdir(path: string, options?: { recursive?: boolean }): Promise<void> {
    mkdirSync(path, { recursive: options?.recursive ?? false });
  }

  async readdir(path: string): Promise<string[]> {
    return readdirSync(path);
  }

  async rmdir(path: string, options?: { recursive?: boolean }): Promise<void> {
    rmdirSync(path, { recursive: options?.recursive ?? false });
  }

  async mmap(path: string): Promise<Uint8Array> {
    try {
      // Bun.mmap returns a Uint8Array backed by a memory-mapped file
      // The OS handles paging and caching automatically
      return Bun.mmap(path);
    } catch {
      // Fallback for environments where mmap fails (e.g., Windows edge cases)
      return await Bun.file(path).bytes();
    }
  }

  /**
   * Convert OpenMode to Node.js file flags.
   */
  private modeToFlags(mode: OpenMode): string {
    switch (mode) {
      case 'read':
        return 'r';
      case 'write':
        return 'w';
      case 'append':
        return 'a';
      case 'readwrite':
        return 'r+';
    }
  }
}
