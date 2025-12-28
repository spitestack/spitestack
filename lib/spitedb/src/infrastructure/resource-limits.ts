/**
 * System resource limits management using FFI.
 *
 * Automatically increases file descriptor limits on startup to ensure
 * spitedb can handle many open segment files without user configuration.
 *
 * @example
 * ```ts
 * const result = increaseFileDescriptorLimit();
 * if (!result.success) {
 *   console.warn(result.warning);
 * }
 * // Now we have more file descriptors available
 * ```
 */

import { dlopen, FFIType, ptr, suffix } from 'bun:ffi';

/**
 * Resource limit constants.
 * RLIMIT_NOFILE is the limit on number of open file descriptors.
 */
const RLIMIT_NOFILE = process.platform === 'darwin' ? 8 : 7;

/**
 * flock() operation constants.
 */
export const LOCK_SH = 1; // Shared lock (read)
export const LOCK_EX = 2; // Exclusive lock (write)
export const LOCK_NB = 4; // Non-blocking (OR with SH or EX)
export const LOCK_UN = 8; // Unlock

/**
 * Result of attempting to increase file descriptor limit.
 */
export interface ResourceLimitResult {
  /** Whether the limit was successfully increased */
  success: boolean;
  /** Previous soft limit */
  previousLimit: number;
  /** New soft limit (same as previous if unchanged) */
  newLimit: number;
  /** Hard limit (maximum we can set without root) */
  hardLimit: number;
  /** Warning message if limit is still low */
  warning?: string;
}

/**
 * rlimit structure for getrlimit/setrlimit syscalls.
 * Contains soft limit (rlim_cur) and hard limit (rlim_max).
 */
interface RLimit {
  soft: bigint;
  hard: bigint;
}

// Load libc for system calls.
// Bun 1.3+ expects a string path, so we resolve a platform-appropriate name.
let libc: ReturnType<typeof dlopen> | null = null;

function getLibcCandidates(): string[] {
  if (process.platform === 'darwin') {
    return ['libc.dylib'];
  }
  if (process.platform === 'linux') {
    return ['libc.so.6', 'libc.so'];
  }
  return [`libc.${suffix}`];
}

function getLibc() {
  if (!libc) {
    try {
      for (const candidate of getLibcCandidates()) {
        try {
          libc = dlopen(candidate, {
            getrlimit: {
              args: [FFIType.i32, FFIType.ptr],
              returns: FFIType.i32,
            },
            setrlimit: {
              args: [FFIType.i32, FFIType.ptr],
              returns: FFIType.i32,
            },
            flock: {
              args: [FFIType.i32, FFIType.i32],
              returns: FFIType.i32,
            },
          });
          break;
        } catch {
          // Try next candidate.
        }
      }
    } catch (e) {
      // FFI not available (e.g., in test environment)
      return null;
    }
  }
  return libc;
}

// Type for FFI function calls
type FFIFunction = (...args: unknown[]) => unknown;

/**
 * Get current resource limits for file descriptors.
 */
function getResourceLimits(): RLimit | null {
  const lib = getLibc();
  if (!lib) return null;

  const getrlimitFn = lib.symbols['getrlimit'] as FFIFunction | undefined;
  if (!getrlimitFn) return null;

  // rlimit struct: two 64-bit values (soft, hard)
  // On both macOS and Linux x64, rlim_t is 64-bit
  const buffer = new BigUint64Array(2);
  const result = getrlimitFn(RLIMIT_NOFILE, ptr(buffer)) as number;

  if (result !== 0) {
    return null;
  }

  return {
    soft: buffer[0]!,
    hard: buffer[1]!,
  };
}

/**
 * Set resource limits for file descriptors.
 */
function setResourceLimits(soft: bigint, hard: bigint): boolean {
  const lib = getLibc();
  if (!lib) return false;

  const setrlimitFn = lib.symbols['setrlimit'] as FFIFunction | undefined;
  if (!setrlimitFn) return false;

  const buffer = new BigUint64Array([soft, hard]);
  const result = setrlimitFn(RLIMIT_NOFILE, ptr(buffer)) as number;

  return result === 0;
}

/**
 * Acquire or release a file lock using flock(2).
 *
 * @param fd - File descriptor to lock/unlock
 * @param operation - LOCK_SH, LOCK_EX, LOCK_UN, optionally OR'd with LOCK_NB
 * @returns true if successful, false if would block (with LOCK_NB) or error
 */
export function flock(fd: number, operation: number): boolean {
  const lib = getLibc();
  if (!lib) {
    throw new Error('FFI not available for flock');
  }

  const flockFn = lib.symbols['flock'] as FFIFunction | undefined;
  if (!flockFn) {
    throw new Error('flock symbol not available');
  }

  const result = flockFn(fd, operation) as number;
  return result === 0;
}

/**
 * Target file descriptor limit.
 * 65536 is a common production value used by databases.
 */
const TARGET_FD_LIMIT = 65536n;

/**
 * Minimum recommended file descriptor limit.
 * Below this, we warn the user.
 */
const MIN_RECOMMENDED_FD_LIMIT = 1024n;

/**
 * Increase the file descriptor limit to allow more open files.
 *
 * This is called automatically when opening an EventStore to ensure
 * spitedb can handle many segment files without manual configuration.
 *
 * The function:
 * 1. Gets current soft and hard limits
 * 2. Raises soft limit toward hard limit (capped at TARGET_FD_LIMIT)
 * 3. Returns result with warning if hard limit is too low
 *
 * @returns Result indicating success and any warnings
 */
export function increaseFileDescriptorLimit(): ResourceLimitResult {
  const limits = getResourceLimits();

  // If we can't get limits (FFI unavailable), return gracefully
  if (!limits) {
    return {
      success: false,
      previousLimit: 0,
      newLimit: 0,
      hardLimit: 0,
      warning: 'Could not read file descriptor limits (FFI unavailable)',
    };
  }

  const previousLimit = Number(limits.soft);
  const hardLimit = Number(limits.hard);

  // Determine target: min of hard limit and our target
  const target = limits.hard < TARGET_FD_LIMIT ? limits.hard : TARGET_FD_LIMIT;

  // If already at or above target, nothing to do
  if (limits.soft >= target) {
    return {
      success: true,
      previousLimit,
      newLimit: previousLimit,
      hardLimit,
    };
  }

  // Try to increase soft limit
  const success = setResourceLimits(target, limits.hard);

  if (success) {
    const newLimit = Number(target);
    const result: ResourceLimitResult = {
      success: true,
      previousLimit,
      newLimit,
      hardLimit,
    };

    // Warn if hard limit is low
    if (limits.hard < MIN_RECOMMENDED_FD_LIMIT) {
      result.warning =
        `File descriptor hard limit is ${hardLimit}. For large deployments, ` +
        `increase it with: sudo launchctl limit maxfiles 65536 65536 (macOS) ` +
        `or edit /etc/security/limits.conf (Linux)`;
    }

    return result;
  }

  // Failed to increase
  return {
    success: false,
    previousLimit,
    newLimit: previousLimit,
    hardLimit,
    warning:
      `Could not increase file descriptor limit from ${previousLimit} to ${Number(target)}. ` +
      `You may need to increase the hard limit manually.`,
  };
}

/**
 * Get current file descriptor limits without modifying them.
 * Useful for diagnostics and logging.
 */
export function getFileDescriptorLimits(): { soft: number; hard: number } | null {
  const limits = getResourceLimits();
  if (!limits) return null;

  return {
    soft: Number(limits.soft),
    hard: Number(limits.hard),
  };
}
