/**
 * Abstract serialization interface for encoding/decoding data.
 *
 * This abstraction allows us to:
 * - Use msgpack in production (MsgpackSerializer)
 * - Use JSON in tests for easier debugging
 * - Swap serialization formats without changing code
 *
 * @example
 * ```ts
 * const serializer = new MsgpackSerializer();
 *
 * const encoded = serializer.encode({ type: 'UserCreated', data: {...} });
 * const decoded = serializer.decode<Event>(encoded);
 * ```
 */
export interface Serializer {
  /**
   * Encode a value to binary format.
   * @param value - Value to encode (must be serializable)
   * @returns Encoded bytes
   */
  encode<T>(value: T): Uint8Array;

  /**
   * Decode binary data back to a value.
   * @param data - Encoded bytes
   * @returns Decoded value
   */
  decode<T>(data: Uint8Array): T;
}
