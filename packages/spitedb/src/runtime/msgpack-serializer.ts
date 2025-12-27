import { pack, unpack } from 'msgpackr';
import type { Serializer } from '../interfaces/serializer';

/**
 * Production serializer implementation using MessagePack.
 *
 * MessagePack is a binary serialization format that is:
 * - Faster than JSON
 * - More compact than JSON
 * - Supports binary data natively
 * - Widely supported across languages
 *
 * @example
 * ```ts
 * const serializer = new MsgpackSerializer();
 *
 * const event = { type: 'UserCreated', data: { name: 'Alice' } };
 * const bytes = serializer.encode(event);
 * const decoded = serializer.decode<typeof event>(bytes);
 * ```
 */
export class MsgpackSerializer implements Serializer {
  encode<T>(value: T): Uint8Array {
    return pack(value);
  }

  decode<T>(data: Uint8Array): T {
    return unpack(data) as T;
  }
}
