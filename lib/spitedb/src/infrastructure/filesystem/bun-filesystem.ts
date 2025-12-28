import {
  statSync,
  existsSync,
  renameSync,
  unlinkSync,
  mkdirSync,
  readdirSync,
  rmdirSync,
} from 'node:fs';
import { open as openAsync, type FileHandle as NodeFileHandle } from 'node:fs/promises';
import type { FileSystem, FileHandle, OpenMode, FileStat } from '../../ports/storage/filesystem';
import { flock as flockFFI, LOCK_SH, LOCK_EX, LOCK_UN } from '../resource-limits';

/**
 * Map storing async FileHandles by their file descriptor number.
 *
 * IMPORTANT: We use a single FileHandle for both write() and sync() operations.
 * This ensures that fsync flushes the writes made through the same handle.
 * Using separate handles (even for the same file) would cause data loss because
 * fsync only flushes writes made through its own file descriptor.
 */
const handles = new Map<number, NodeFileHandle>();

/**
 * Production filesystem implementation using hybrid Bun/Node.js APIs.
 *
 * Architecture:
 * - Write path: Uses `node:fs/promises` with a single FileHandle per open file.
 *   This ensures write() and sync() operate on the same file descriptor,
 *   guaranteeing that fsync actually flushes our writes.
 * - Read path: Uses Bun.file().bytes() for optimal read performance.
 *   Reads don't need fd consistency since they're independent operations.
 *
 * This design is safe for server use - all I/O operations are async and
 * won't block the event loop.
 *
 * @example
 * ```ts
 * const fs = new BunFileSystem();
 * const handle = await fs.open('/data/events.log', 'write');
 * await fs.write(handle, data);
 * await fs.sync(handle);  // Flushes the write above - same fd!
 * await fs.close(handle);
 *
 * // Fast reads using Bun native API
 * const content = await fs.readFile('/data/segment.log');
 * ```
 */
export class BunFileSystem implements FileSystem {
  async open(path: string, mode: OpenMode): Promise<FileHandle> {
    const flags = this.modeToFlags(mode);
    const nodeHandle = await openAsync(path, flags);
    const fd = nodeHandle.fd;

    handles.set(fd, nodeHandle);
    return { fd };
  }

  async close(handle: FileHandle): Promise<void> {
    const nodeHandle = handles.get(handle.fd);
    if (nodeHandle) {
      await nodeHandle.close();
      handles.delete(handle.fd);
    }
  }

  async read(handle: FileHandle, offset: number, length: number): Promise<Uint8Array> {
    const nodeHandle = handles.get(handle.fd);
    if (!nodeHandle) {
      throw new Error(`Invalid file handle: fd=${handle.fd}`);
    }

    const buffer = new Uint8Array(length);
    const result = await nodeHandle.read(buffer, 0, length, offset);
    return buffer.subarray(0, result.bytesRead);
  }

  async write(handle: FileHandle, data: Uint8Array, offset?: number): Promise<number> {
    const nodeHandle = handles.get(handle.fd);
    if (!nodeHandle) {
      throw new Error(`Invalid file handle: fd=${handle.fd}`);
    }

    const result = await nodeHandle.write(data, 0, data.length, offset ?? null);
    return result.bytesWritten;
  }

  async sync(handle: FileHandle): Promise<void> {
    const nodeHandle = handles.get(handle.fd);
    if (!nodeHandle) {
      throw new Error(`Invalid file handle: fd=${handle.fd}`);
    }

    // This syncs the SAME handle that received the writes - correct!
    await nodeHandle.sync();
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
    const nodeHandle = handles.get(handle.fd);
    if (!nodeHandle) {
      throw new Error(`Invalid file handle: fd=${handle.fd}`);
    }

    await nodeHandle.truncate(length);
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

  async flock(handle: FileHandle, mode: 'shared' | 'exclusive'): Promise<void> {
    const operation = mode === 'exclusive' ? LOCK_EX : LOCK_SH;
    const success = flockFFI(handle.fd, operation);

    if (!success) {
      throw new Error(
        `Failed to acquire ${mode} lock on file descriptor ${handle.fd}. ` +
          `Another process may be holding an incompatible lock.`
      );
    }
  }

  async funlock(handle: FileHandle): Promise<void> {
    const success = flockFFI(handle.fd, LOCK_UN);

    if (!success) {
      throw new Error(`Failed to release lock on file descriptor ${handle.fd}`);
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
