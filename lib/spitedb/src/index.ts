// ============================================================
// SpiteDB - Primary Public API
// ============================================================

export { SpiteDB, type SpiteDBOptions } from './spitedb';

// SpiteDB Errors
export {
  SpiteDBError,
  SpiteDBNotOpenError,
  ProjectionsNotStartedError,
  ProjectionBackpressureError,
  ProjectionBackpressureTimeoutError,
} from './errors';

// ============================================================
// Types commonly used with SpiteDB
// ============================================================

// Event types
export type {
  InputEvent,
  AppendOptions,
  AppendResult,
  StreamAppend,
  BatchAppendResult,
  ReadStreamOptions,
  ReadGlobalOptions,
} from './application/event-store';

export type { StoredEvent } from './domain/events/stored-event';

// Projection types for registration
export type {
  ProjectionRegistration,
  ProjectionRuntimeOptions,
} from './ports/projections';

export type { ProjectionCoordinatorStatus } from './application/projections';

// ============================================================
// Errors (thrown by SpiteDB methods)
// ============================================================

// Domain errors
export { ConcurrencyError } from './domain/errors';

// Projection errors
export {
  ProjectionNotFoundError,
  ProjectionCatchUpTimeoutError,
} from './errors';

// ============================================================
// Advanced / Internal APIs
// ============================================================
// These are exported for advanced users, testing, and custom configurations.
// Most users should only need SpiteDB above.

// Domain - Value Objects
export {
  StreamId,
  GlobalPosition,
  Revision,
  TenantId,
  BatchId,
  CommandId,
} from './domain/value-objects';

// Domain - Other Errors
export {
  InvalidStreamIdError,
  InvalidPositionError,
} from './domain/errors';

// Ports - Interfaces (for DST and custom implementations)
export type {
  OpenMode,
  FileStat,
  FileHandle,
  FileSystem,
} from './ports/storage';

export type {
  Timer,
  Clock,
} from './ports/time';

export type {
  Serializer,
  Compressor,
} from './ports/serialization';

// Runtime - Production Implementations
export {
  BunFileSystem,
  BunClock,
  MsgpackSerializer,
  FastEventSerializer,
  BinaryEventBatchSerializer,
  ZstdCompressor,
  NoopCompressor,
} from './infrastructure';

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
  type BatchWriteResult,
  type ValidationResult,
  type StreamEntry,
  type LocationEntry,
  type SegmentManagerConfig,
  type SegmentInfo,
  type RecoveryResult,
  type ReadBatchProfileSample,
  type ReadBatchProfiler,
} from './infrastructure/storage';

// EventStore - for direct access
export { EventStore, type EventStoreConfig } from './application/event-store';

// Projections - Extended exports for advanced usage
export {
  // Interfaces (from ports)
  type ProjectionKind,
  type AccessPattern,
  type ProjectionMetadata,
  type Projection,
  type QueryFilter,
  type TimeRange,
  type ProjectionStoreConfig,
  type ProjectionStore,
  type ProjectionStoreFactory,
  type ResolvedRegistration,
  type ProjectionRegistry,
} from './ports/projections';

export {
  // Default implementation
  DefaultProjectionRegistry,
  // Stores
  AggregatorStore,
  type AggregatorStoreConfig,
  DenormalizedViewStore,
  type DenormalizedViewStoreConfig,
  DiskKeyValueStore,
  type DiskKeyValueStoreConfig,
  EqualityIndex,
  SortedIndex,
  IndexCollection,
} from './infrastructure/projections';

export {
  // Runtime
  CheckpointManager,
  type CheckpointManagerConfig,
  type Checkpoint,
  CHECKPOINT_MAGIC,
  CHECKPOINT_VERSION,
  CHECKPOINT_HEADER_SIZE,
  ProjectionRunner,
  type ProjectionRunnerConfig,
  type ProjectionRunnerStatus,
  ProjectionCoordinator,
  type ProjectionCoordinatorConfig,
} from './application/projections';

// Projection Errors (excluding already exported errors)
export {
  ProjectionError,
  ProjectionBuildError,
  ProjectionAlreadyRegisteredError,
  ProjectionDisabledError,
  ProjectionCoordinatorError,
  CheckpointWriteError,
  CheckpointLoadError,
  CheckpointCorruptionError,
  CheckpointVersionError,
} from './errors';
