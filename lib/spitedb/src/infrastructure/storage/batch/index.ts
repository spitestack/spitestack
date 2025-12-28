export {
  encodeBatchRecord,
  decodeBatchHeader,
  extractBatchPayload,
  isValidBatchMagic,
  BATCH_MAGIC,
  BATCH_HEADER_SIZE,
  InvalidBatchError,
  BatchChecksumError,
  type BatchHeader,
} from './batch-record';
