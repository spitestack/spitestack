/**
 * Write all bytes to a file, handling short writes.
 *
 * The underlying fs.write() may return fewer bytes than requested
 * (a "short write"). This helper retries until all bytes are written
 * or an error occurs.
 *
 * @example
 * ```ts
 * import { writeAllBytes } from './support/write-all-bytes';
 *
 * const data = new Uint8Array([1, 2, 3, 4]);
 * await writeAllBytes(fs, handle, data, 0);
 * ```
 */

import type { FileSystem, FileHandle } from '../../../ports/storage/filesystem';

/**
 * Error thrown when a write cannot complete after retry.
 */
export class ShortWriteError extends Error {
  constructor(
    public readonly bytesRequested: number,
    public readonly bytesWritten: number
  ) {
    super(
      `Short write: only ${bytesWritten}/${bytesRequested} bytes written`
    );
    this.name = 'ShortWriteError';
  }
}

/**
 * Write all bytes to a file, handling short writes.
 *
 * This function ensures all data is written by retrying if the underlying
 * write returns fewer bytes than expected. If a write returns 0 bytes
 * (indicating inability to make progress), it throws a ShortWriteError.
 *
 * @param fs - Filesystem implementation
 * @param handle - Open file handle
 * @param data - Data to write
 * @param offset - Byte offset in the file (undefined for append mode)
 * @throws ShortWriteError if unable to write all bytes
 */
export async function writeAllBytes(
  fs: FileSystem,
  handle: FileHandle,
  data: Uint8Array,
  offset?: number
): Promise<void> {
  let written = 0;

  while (written < data.length) {
    const remaining = data.subarray(written);
    // For append mode (offset undefined), we don't specify an offset
    // For seek mode, we advance the offset by bytes already written
    const writeOffset = offset !== undefined ? offset + written : undefined;
    const bytesWritten = await fs.write(handle, remaining, writeOffset);

    if (bytesWritten === 0) {
      // Cannot make progress - throw error
      throw new ShortWriteError(data.length, written);
    }

    written += bytesWritten;
  }
}
