import { describe, test, expect } from 'bun:test';
import {
  encodeBatchRecord,
  decodeBatchHeader,
  extractBatchPayload,
  getBatchRecordSize,
  isValidBatchMagic,
  InvalidBatchError,
  BatchChecksumError,
  BATCH_MAGIC,
  BATCH_HEADER_SIZE,
} from '../../../../../src/infrastructure/storage/batch/batch-record';
import { crc32 } from '../../../../../src/infrastructure/storage/support/crc32';

describe('BatchRecord', () => {
  describe('encodeBatchRecord', () => {
    test('should encode batch with correct header size', () => {
      const payload = new Uint8Array([1, 2, 3, 4, 5]);
      const record = encodeBatchRecord(1n, 10, payload, 100);

      expect(record.length).toBe(BATCH_HEADER_SIZE + payload.length);
    });

    test('should encode magic number correctly', () => {
      const payload = new Uint8Array([1, 2, 3]);
      const record = encodeBatchRecord(1n, 1, payload, 10);
      const view = new DataView(record.buffer);

      expect(view.getUint32(0, false)).toBe(BATCH_MAGIC);
    });

    test('should encode payload length correctly', () => {
      const payload = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
      const record = encodeBatchRecord(1n, 1, payload, 50);
      const view = new DataView(record.buffer);

      expect(view.getUint32(8, false)).toBe(10); // compressed length
      expect(view.getUint32(12, false)).toBe(50); // uncompressed length
    });

    test('should encode batch ID correctly', () => {
      const payload = new Uint8Array([1]);
      const record = encodeBatchRecord(123456789012345n, 1, payload, 1);
      const view = new DataView(record.buffer);

      expect(view.getBigUint64(16, false)).toBe(123456789012345n);
    });

    test('should encode event count correctly', () => {
      const payload = new Uint8Array([1]);
      const record = encodeBatchRecord(1n, 42, payload, 1);
      const view = new DataView(record.buffer);

      expect(view.getUint32(24, false)).toBe(42);
    });

    test('should include payload after header', () => {
      const payload = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
      const record = encodeBatchRecord(1n, 1, payload, 4);

      expect(record.subarray(BATCH_HEADER_SIZE)).toEqual(payload);
    });

    test('should compute valid checksum', () => {
      const payload = new Uint8Array([1, 2, 3, 4, 5]);
      const record = encodeBatchRecord(1n, 1, payload, 100);
      const view = new DataView(record.buffer);

      const storedChecksum = view.getUint32(4, false);
      const checksumData = record.subarray(8);
      const computedChecksum = crc32(checksumData);

      expect(storedChecksum).toBe(computedChecksum);
    });
  });

  describe('decodeBatchHeader', () => {
    test('should decode encoded header correctly', () => {
      const payload = new Uint8Array([1, 2, 3, 4, 5]);
      const record = encodeBatchRecord(999n, 25, payload, 100);

      const header = decodeBatchHeader(record);

      expect(header.magic).toBe(BATCH_MAGIC);
      expect(header.compressedLength).toBe(5);
      expect(header.uncompressedLength).toBe(100);
      expect(header.batchId).toBe(999n);
      expect(header.eventCount).toBe(25);
    });

    test('should throw on header too short', () => {
      const data = new Uint8Array(16);

      expect(() => decodeBatchHeader(data)).toThrow(InvalidBatchError);
      expect(() => decodeBatchHeader(data)).toThrow('Header too short');
    });

    test('should throw on invalid magic number', () => {
      const data = new Uint8Array(BATCH_HEADER_SIZE);
      const view = new DataView(data.buffer);
      view.setUint32(0, 0xdeadbeef, false);

      expect(() => decodeBatchHeader(data)).toThrow(InvalidBatchError);
      expect(() => decodeBatchHeader(data)).toThrow('Invalid magic number');
    });

    test('should throw on checksum mismatch', () => {
      const payload = new Uint8Array([1, 2, 3, 4, 5]);
      const record = encodeBatchRecord(1n, 1, payload, 5);

      // Corrupt the payload
      record[BATCH_HEADER_SIZE] = 0xff;

      expect(() => decodeBatchHeader(record)).toThrow(BatchChecksumError);
      expect(() => decodeBatchHeader(record)).toThrow('Checksum mismatch');
    });

    test('should skip checksum validation when requested', () => {
      const payload = new Uint8Array([1, 2, 3, 4, 5]);
      const record = encodeBatchRecord(1n, 1, payload, 5);

      // Corrupt the payload
      record[BATCH_HEADER_SIZE] = 0xff;

      // Should not throw with validateChecksum=false
      const header = decodeBatchHeader(record, false);
      expect(header.batchId).toBe(1n);
    });

    test('should throw on incomplete batch when validating checksum', () => {
      const payload = new Uint8Array([1, 2, 3, 4, 5]);
      const record = encodeBatchRecord(1n, 1, payload, 5);

      // Truncate the record
      const truncated = record.subarray(0, BATCH_HEADER_SIZE + 2);

      expect(() => decodeBatchHeader(truncated)).toThrow(InvalidBatchError);
      expect(() => decodeBatchHeader(truncated)).toThrow('Incomplete batch');
    });
  });

  describe('extractBatchPayload', () => {
    test('should extract payload correctly', () => {
      const payload = new Uint8Array([0xca, 0xfe, 0xba, 0xbe]);
      const record = encodeBatchRecord(1n, 1, payload, 4);

      const extracted = extractBatchPayload(record);

      expect(extracted).toEqual(payload);
    });

    test('should use provided header', () => {
      const payload = new Uint8Array([1, 2, 3]);
      const record = encodeBatchRecord(1n, 1, payload, 3);
      const header = decodeBatchHeader(record);

      const extracted = extractBatchPayload(record, header);

      expect(extracted).toEqual(payload);
    });
  });

  describe('getBatchRecordSize', () => {
    test('should calculate correct size', () => {
      expect(getBatchRecordSize(0)).toBe(BATCH_HEADER_SIZE);
      expect(getBatchRecordSize(100)).toBe(BATCH_HEADER_SIZE + 100);
      expect(getBatchRecordSize(1000)).toBe(BATCH_HEADER_SIZE + 1000);
    });
  });

  describe('isValidBatchMagic', () => {
    test('should return true for valid magic', () => {
      const record = encodeBatchRecord(1n, 1, new Uint8Array([1]), 1);
      expect(isValidBatchMagic(record)).toBe(true);
    });

    test('should return false for invalid magic', () => {
      const data = new Uint8Array(10);
      expect(isValidBatchMagic(data)).toBe(false);
    });

    test('should return false for data too short', () => {
      expect(isValidBatchMagic(new Uint8Array(3))).toBe(false);
    });
  });

  describe('roundtrip', () => {
    test('should preserve all values through encode/decode cycle', () => {
      const testCases = [
        { batchId: 0n, eventCount: 0, payload: new Uint8Array(0), uncompressedLength: 0 },
        { batchId: 1n, eventCount: 1, payload: new Uint8Array([1]), uncompressedLength: 1 },
        {
          batchId: 0xffffffffffffffffn,
          eventCount: 1000000,
          payload: new Uint8Array(1000).fill(0xaa),
          uncompressedLength: 5000,
        },
      ];

      for (const tc of testCases) {
        const record = encodeBatchRecord(tc.batchId, tc.eventCount, tc.payload, tc.uncompressedLength);
        const header = decodeBatchHeader(record);
        const extractedPayload = extractBatchPayload(record, header);

        expect(header.batchId).toBe(tc.batchId);
        expect(header.eventCount).toBe(tc.eventCount);
        expect(header.compressedLength).toBe(tc.payload.length);
        expect(header.uncompressedLength).toBe(tc.uncompressedLength);
        expect(extractedPayload).toEqual(tc.payload);
      }
    });
  });
});
