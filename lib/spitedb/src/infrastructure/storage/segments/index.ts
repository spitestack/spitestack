export {
  encodeSegmentHeader,
  decodeSegmentHeader,
  isValidSegmentHeader,
  SEGMENT_MAGIC,
  SEGMENT_VERSION,
  SEGMENT_HEADER_SIZE,
  InvalidSegmentHeaderError,
  type SegmentHeader,
} from './segment-header';

export {
  SegmentWriter,
  type BatchWriteResult,
} from './segment-writer';

export {
  SegmentReader,
  type ValidationResult,
  type ReadBatchProfileSample,
  type ReadBatchProfiler,
} from './segment-reader';

export {
  SegmentIndex,
  type StreamEntry,
  type LocationEntry,
} from './segment-index';

export {
  SegmentIndexFile,
  IndexCorruptedError,
  INDEX_MAGIC,
  INDEX_VERSION,
  INDEX_HEADER_SIZE,
  INDEX_ENTRY_SIZE,
  type IndexEntry,
  type IndexHeader,
} from './segment-index-file';

export {
  SegmentManager,
  type SegmentManagerConfig,
  type SegmentInfo,
  type RecoveryResult,
} from './segment-manager';
