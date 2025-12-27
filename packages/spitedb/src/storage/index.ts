export { crc32, CRC32Calculator } from './crc32';

export {
  encodeSegmentHeader,
  decodeSegmentHeader,
  isValidSegmentHeader,
  InvalidSegmentHeaderError,
  SEGMENT_MAGIC,
  SEGMENT_VERSION,
  SEGMENT_HEADER_SIZE,
  type SegmentHeader,
} from './segment-header';

export {
  encodeBatchRecord,
  decodeBatchHeader,
  extractBatchPayload,
  getBatchRecordSize,
  isValidBatchMagic,
  InvalidBatchError,
  BatchChecksumError,
  BATCH_MAGIC,
  BATCH_HEADER_SIZE,
  type BatchHeader,
} from './batch-record';

export { type StoredEvent } from './stored-event';

export { SegmentWriter, type BatchWriteResult } from './segment-writer';

export { SegmentReader, type ValidationResult } from './segment-reader';

export { SegmentIndex, type StreamEntry, type LocationEntry } from './segment-index';

export { StreamMap, type StreamMetadata, type StreamMapJSON } from './stream-map';

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

export { LRUCache } from './lru-cache';

export {
  SegmentManager,
  type SegmentManagerConfig,
  type SegmentInfo,
  type RecoveryResult,
} from './segment-manager';
