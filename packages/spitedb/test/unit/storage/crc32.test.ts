import { describe, test, expect } from 'bun:test';
import { crc32, CRC32Calculator } from '../../../src/storage/crc32';

describe('CRC32', () => {
  describe('crc32 function', () => {
    test('should calculate correct checksum for empty data', () => {
      const result = crc32(new Uint8Array(0));
      expect(result).toBe(0x00000000);
    });

    test('should calculate correct checksum for "hello world"', () => {
      const data = new TextEncoder().encode('hello world');
      const result = crc32(data);
      // Known CRC32 value for "hello world"
      expect(result).toBe(0x0d4a1185);
    });

    test('should calculate correct checksum for "123456789"', () => {
      // Standard test vector - CRC32 of "123456789" should be 0xCBF43926
      const data = new TextEncoder().encode('123456789');
      const result = crc32(data);
      expect(result).toBe(0xcbf43926);
    });

    test('should return consistent results for same input', () => {
      const data = new TextEncoder().encode('test data');
      const result1 = crc32(data);
      const result2 = crc32(data);
      expect(result1).toBe(result2);
    });

    test('should return different results for different input', () => {
      const data1 = new TextEncoder().encode('test data 1');
      const data2 = new TextEncoder().encode('test data 2');
      expect(crc32(data1)).not.toBe(crc32(data2));
    });

    test('should handle binary data with null bytes', () => {
      const data = new Uint8Array([0x00, 0x00, 0x00, 0x00]);
      const result = crc32(data);
      expect(result).toBe(0x2144df1c);
    });

    test('should handle single byte', () => {
      const data = new Uint8Array([0x61]); // 'a'
      const result = crc32(data);
      expect(result).toBe(0xe8b7be43);
    });
  });

  describe('CRC32Calculator', () => {
    test('should calculate same result as crc32 function', () => {
      const data = new TextEncoder().encode('hello world');

      const calc = new CRC32Calculator();
      calc.update(data);
      const calcResult = calc.finalize();

      expect(calcResult).toBe(crc32(data));
    });

    test('should calculate correct checksum across multiple updates', () => {
      const fullData = new TextEncoder().encode('hello world');
      const part1 = new TextEncoder().encode('hello');
      const part2 = new TextEncoder().encode(' world');

      const calc = new CRC32Calculator();
      calc.update(part1);
      calc.update(part2);
      const result = calc.finalize();

      expect(result).toBe(crc32(fullData));
    });

    test('should reset after finalize', () => {
      const data1 = new TextEncoder().encode('first');
      const data2 = new TextEncoder().encode('second');

      const calc = new CRC32Calculator();

      calc.update(data1);
      const result1 = calc.finalize();

      calc.update(data2);
      const result2 = calc.finalize();

      expect(result1).toBe(crc32(data1));
      expect(result2).toBe(crc32(data2));
    });

    test('should allow manual reset', () => {
      const calc = new CRC32Calculator();
      calc.update(new TextEncoder().encode('some data'));
      calc.reset();
      calc.update(new TextEncoder().encode('123456789'));

      expect(calc.finalize()).toBe(0xcbf43926);
    });

    test('should handle many small updates', () => {
      const fullData = new TextEncoder().encode('abcdefghij');

      const calc = new CRC32Calculator();
      for (const char of 'abcdefghij') {
        calc.update(new TextEncoder().encode(char));
      }

      expect(calc.finalize()).toBe(crc32(fullData));
    });
  });
});
