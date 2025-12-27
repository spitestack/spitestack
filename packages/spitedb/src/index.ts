// Domain - Value Objects
export {
  StreamId,
  GlobalPosition,
  Revision,
  TenantId,
  BatchId,
  CommandId,
} from './domain/value-objects';

// Domain - Errors
export {
  InvalidStreamIdError,
  InvalidPositionError,
  ConcurrencyError,
} from './domain/errors';

// Interfaces
export type {
  OpenMode,
  FileStat,
  FileHandle,
  FileSystem,
  Timer,
  Clock,
  Serializer,
  Compressor,
} from './interfaces';

// Runtime - Production Implementations
export {
  BunFileSystem,
  BunClock,
  MsgpackSerializer,
  ZstdCompressor,
} from './runtime';

// Storage Layer
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
  type StoredEvent,
  type BatchWriteResult,
  type ValidationResult,
  type StreamEntry,
  type LocationEntry,
  type SegmentManagerConfig,
  type SegmentInfo,
  type RecoveryResult,
} from './storage';

// EventStore - Main API
export {
  EventStore,
  type EventStoreConfig,
  type InputEvent,
  type AppendOptions,
  type AppendResult,
  type ReadStreamOptions,
  type ReadGlobalOptions,
} from './event-store';
