import { describe, test, expect } from 'bun:test';
import {
  encodeSegmentHeader,
  decodeSegmentHeader,
  isValidSegmentHeader,
  InvalidSegmentHeaderError,
  SEGMENT_MAGIC,
  SEGMENT_VERSION,
  SEGMENT_HEADER_SIZE,
} from '../../../../../src/infrastructure/storage/segments/segment-header';

describe('SegmentHeader', () => {
  describe('encodeSegmentHeader', () => {
    test('should encode header to correct size', () => {
      const header = encodeSegmentHeader({
        flags: 0,
        segmentId: 1n,
        basePosition: 0,
      });
      expect(header.length).toBe(SEGMENT_HEADER_SIZE);
    });

    test('should encode magic number correctly', () => {
      const header = encodeSegmentHeader({
        flags: 0,
        segmentId: 1n,
        basePosition: 0,
      });
      const view = new DataView(header.buffer);
      expect(view.getUint32(0, false)).toBe(SEGMENT_MAGIC);
    });

    test('should encode version correctly', () => {
      const header = encodeSegmentHeader({
        flags: 0,
        segmentId: 1n,
        basePosition: 0,
      });
      expect(header[4]).toBe(SEGMENT_VERSION);
    });

    test('should encode segment ID correctly', () => {
      const header = encodeSegmentHeader({
        flags: 0,
        segmentId: 12345678901234567890n,
        basePosition: 0,
      });
      const view = new DataView(header.buffer);
      expect(view.getBigUint64(8, false)).toBe(12345678901234567890n);
    });

    test('should encode base position correctly', () => {
      const header = encodeSegmentHeader({
        flags: 0,
        segmentId: 1n,
        basePosition: 9876543210,
      });
      const view = new DataView(header.buffer);
      expect(view.getBigUint64(16, false)).toBe(9876543210n);
    });

    test('should encode flags correctly', () => {
      const header = encodeSegmentHeader({
        flags: 0x42,
        segmentId: 1n,
        basePosition: 0,
      });
      expect(header[5]).toBe(0x42);
    });
  });

  describe('decodeSegmentHeader', () => {
    test('should decode encoded header correctly', () => {
      const original = {
        flags: 0x12,
        segmentId: 123456789n,
        basePosition: 987654321,
      };

      const encoded = encodeSegmentHeader(original);
      const decoded = decodeSegmentHeader(encoded);

      expect(decoded.magic).toBe(SEGMENT_MAGIC);
      expect(decoded.version).toBe(SEGMENT_VERSION);
      expect(decoded.flags).toBe(original.flags);
      expect(decoded.segmentId).toBe(original.segmentId);
      expect(decoded.basePosition).toBe(original.basePosition);
    });

    test('should throw on header too short', () => {
      const data = new Uint8Array(16);
      expect(() => decodeSegmentHeader(data)).toThrow(InvalidSegmentHeaderError);
      expect(() => decodeSegmentHeader(data)).toThrow('Header too short');
    });

    test('should throw on invalid magic number', () => {
      const data = new Uint8Array(SEGMENT_HEADER_SIZE);
      const view = new DataView(data.buffer);
      view.setUint32(0, 0xdeadbeef, false); // Wrong magic

      expect(() => decodeSegmentHeader(data)).toThrow(InvalidSegmentHeaderError);
      expect(() => decodeSegmentHeader(data)).toThrow('Invalid magic number');
    });

    test('should throw on unsupported version', () => {
      const data = encodeSegmentHeader({
        flags: 0,
        segmentId: 1n,
        basePosition: 0,
      });
      data[4] = 99; // Wrong version

      expect(() => decodeSegmentHeader(data)).toThrow(InvalidSegmentHeaderError);
      expect(() => decodeSegmentHeader(data)).toThrow('Unsupported version');
    });

    test('should handle data with extra bytes', () => {
      const header = encodeSegmentHeader({
        flags: 0,
        segmentId: 1n,
        basePosition: 0,
      });

      // Add extra bytes
      const extended = new Uint8Array(100);
      extended.set(header);

      const decoded = decodeSegmentHeader(extended);
      expect(decoded.magic).toBe(SEGMENT_MAGIC);
    });
  });

  describe('isValidSegmentHeader', () => {
    test('should return true for valid header', () => {
      const header = encodeSegmentHeader({
        flags: 0,
        segmentId: 1n,
        basePosition: 0,
      });
      expect(isValidSegmentHeader(header)).toBe(true);
    });

    test('should return false for data too short', () => {
      expect(isValidSegmentHeader(new Uint8Array(16))).toBe(false);
    });

    test('should return false for invalid magic', () => {
      const data = new Uint8Array(SEGMENT_HEADER_SIZE);
      const view = new DataView(data.buffer);
      view.setUint32(0, 0xdeadbeef, false);
      expect(isValidSegmentHeader(data)).toBe(false);
    });

    test('should return false for invalid version', () => {
      const header = encodeSegmentHeader({
        flags: 0,
        segmentId: 1n,
        basePosition: 0,
      });
      header[4] = 99;
      expect(isValidSegmentHeader(header)).toBe(false);
    });
  });

  describe('roundtrip', () => {
    test('should preserve all values through encode/decode cycle', () => {
      const testCases = [
        { flags: 0, segmentId: 0n, basePosition: 0 },
        { flags: 255, segmentId: 1n, basePosition: 1 },
        { flags: 0x55, segmentId: 0xffffffffffffffffn, basePosition: 9876543210 },
        { flags: 0xaa, segmentId: 1234567890123456789n, basePosition: 987654321098765 },
      ];

      for (const original of testCases) {
        const encoded = encodeSegmentHeader(original);
        const decoded = decodeSegmentHeader(encoded);

        expect(decoded.flags).toBe(original.flags);
        expect(decoded.segmentId).toBe(original.segmentId);
        expect(decoded.basePosition).toBe(original.basePosition);
      }
    });
  });
});
