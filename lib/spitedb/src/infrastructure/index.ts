// Filesystem
export { BunFileSystem } from './filesystem';

// Time
export { BunClock } from './time';

// Serialization
export {
  MsgpackSerializer,
  FastEventSerializer,
  BinaryEventBatchSerializer,
  ZstdCompressor,
  NoopCompressor,
} from './serialization';

// Storage
export {
  // CRC32
  crc32,
  CRC32Calculator,
  // Segment format
  SEGMENT_MAGIC,
  SEGMENT_VERSION,
  SEGMENT_HEADER_SIZE,
  BATCH_MAGIC,
  BATCH_HEADER_SIZE,
  // Segment components
  SegmentWriter,
  SegmentReader,
  SegmentIndex,
  SegmentManager,
  // Storage errors
  InvalidSegmentHeaderError,
  InvalidBatchError,
  BatchChecksumError,
  // Types
  type SegmentHeader,
  type BatchHeader,
  type BatchWriteResult,
  type ValidationResult,
  type StreamEntry,
  type LocationEntry,
  type SegmentManagerConfig,
  type SegmentInfo,
  type RecoveryResult,
} from './storage';

// Re-export StoredEvent from domain for convenience
export type { StoredEvent } from '../domain/events/stored-event';

// Projections
export {
  DefaultProjectionRegistry,
  AggregatorStore,
  type AggregatorStoreConfig,
  DenormalizedViewStore,
  type DenormalizedViewStoreConfig,
  DiskKeyValueStore,
  type DiskKeyValueStoreConfig,
  EqualityIndex,
  SortedIndex,
  IndexCollection,
} from './projections';

// Resource limits
export {
  increaseFileDescriptorLimit,
  getFileDescriptorLimits,
  type ResourceLimitResult,
  LOCK_SH,
  LOCK_EX,
  LOCK_NB,
  LOCK_UN,
  flock,
} from './resource-limits';
