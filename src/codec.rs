//! # Event Batch Encoding and Decoding
//!
//! This module provides the codec for encoding events into batches and decoding
//! them back. All batches are compressed with Zstd (level 1) and encrypted with
//! AES-256-GCM.
//!
//! ## Batch Format
//!
//! Raw format (before compression/encryption):
//! ```text
//! [event1_data][event2_data][event3_data]...
//! ```
//!
//! Stored format:
//! ```text
//! AES-GCM(Zstd([event1_data][event2_data]...))
//! ```
//!
//! All metadata (stream_id, global_pos, stream_rev, timestamp) lives in the
//! `event_index` and `batches` tables, not in the blob. This enables efficient
//! queries and keeps the storage format simple.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::crypto::{BatchCryptor, AES_GCM_NONCE_SIZE};
use crate::error::Result;
use crate::types::EventData;

// =============================================================================
// Encoding
// =============================================================================

/// Encodes events into a compressed and encrypted batch blob.
///
/// # Returns
///
/// A tuple of:
/// - The encrypted blob data
/// - The nonce used for encryption (must be stored with the batch)
/// - A vec of (byte_offset, byte_len) for each event in the uncompressed batch
///
/// # Arguments
///
/// * `events` - Events to encode
/// * `batch_id` - Unique batch identifier (for key derivation)
/// * `cryptor` - The cryptor for compression and encryption
pub fn encode_batch(
    events: &[EventData],
    batch_id: i64,
    cryptor: &BatchCryptor,
) -> Result<(Vec<u8>, [u8; AES_GCM_NONCE_SIZE], Vec<(usize, usize)>)> {
    // Concatenate raw payloads
    let mut data = Vec::new();
    let mut offsets = Vec::new();

    for event in events {
        let start_offset = data.len();
        data.extend_from_slice(&event.data);
        let len = event.data.len();
        offsets.push((start_offset, len));
    }

    // Compress and encrypt
    let (ciphertext, nonce) = cryptor.seal(&data, batch_id)?;

    Ok((ciphertext, nonce, offsets))
}

// =============================================================================
// Decoding
// =============================================================================

/// Decrypts and decompresses batch data, then extracts a single event's bytes.
///
/// # Arguments
///
/// * `batch_data` - The encrypted batch blob from the database
/// * `nonce` - The nonce used during encryption
/// * `batch_id` - Batch identifier (for key derivation)
/// * `byte_offset` - Byte offset of this event within the decrypted batch
/// * `byte_len` - Byte length of this event
/// * `cryptor` - The cryptor for decryption and decompression
///
/// # Returns
///
/// The raw event data bytes.
pub fn decode_event_data(
    batch_data: &[u8],
    nonce: &[u8; AES_GCM_NONCE_SIZE],
    batch_id: i64,
    byte_offset: usize,
    byte_len: usize,
    cryptor: &BatchCryptor,
) -> Result<Vec<u8>> {
    let plaintext = cryptor.open(batch_data, nonce, batch_id)?;
    Ok(plaintext[byte_offset..byte_offset + byte_len].to_vec())
}

/// Decrypts and decompresses an entire batch.
///
/// Use this when you need all events from a batch, to avoid repeated decryption.
///
/// # Arguments
///
/// * `batch_data` - The encrypted batch blob from the database
/// * `nonce` - The nonce used during encryption
/// * `batch_id` - Batch identifier (for key derivation)
/// * `cryptor` - The cryptor for decryption and decompression
///
/// # Returns
///
/// The decrypted and decompressed batch data.
pub fn decode_batch(
    batch_data: &[u8],
    nonce: &[u8; AES_GCM_NONCE_SIZE],
    batch_id: i64,
    cryptor: &BatchCryptor,
) -> Result<Vec<u8>> {
    cryptor.open(batch_data, nonce, batch_id)
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Computes a checksum for batch data.
///
/// Uses XXH3-64 for consistency with stream hashing. XXH3 is extremely fast
/// and provides good distribution for integrity checking.
///
/// Note: Checksum is computed on the encrypted ciphertext, not the plaintext.
pub fn compute_checksum(data: &[u8]) -> Vec<u8> {
    let hash = xxhash_rust::xxh3::xxh3_64(data);
    hash.to_le_bytes().to_vec()
}

/// Returns the current time in milliseconds since Unix epoch.
pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::EnvKeyProvider;

    fn test_cryptor() -> BatchCryptor {
        let key = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
        ];
        BatchCryptor::new(EnvKeyProvider::from_key(key))
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let cryptor = test_cryptor();
        let batch_id = 1i64;

        let events = vec![
            EventData::new(b"event 1 data".to_vec()),
            EventData::new(b"event 2 data".to_vec()),
        ];

        let (blob, nonce, offsets) = encode_batch(&events, batch_id, &cryptor).unwrap();

        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets[0], (0, 12)); // "event 1 data" = 12 bytes
        assert_eq!(offsets[1], (12, 12)); // "event 2 data" = 12 bytes

        // Decode first event
        let data1 = decode_event_data(&blob, &nonce, batch_id, offsets[0].0, offsets[0].1, &cryptor).unwrap();
        assert_eq!(data1, b"event 1 data");

        // Decode second event
        let data2 = decode_event_data(&blob, &nonce, batch_id, offsets[1].0, offsets[1].1, &cryptor).unwrap();
        assert_eq!(data2, b"event 2 data");
    }

    #[test]
    fn test_encode_single_event() {
        let cryptor = test_cryptor();
        let batch_id = 2i64;

        let events = vec![EventData::new(b"hello".to_vec())];

        let (blob, nonce, offsets) = encode_batch(&events, batch_id, &cryptor).unwrap();

        // The encrypted blob will be larger than "hello" due to compression overhead and auth tag
        assert!(!blob.is_empty());
        assert_eq!(offsets, vec![(0, 5)]);

        // Verify we can decode it
        let decoded = decode_event_data(&blob, &nonce, batch_id, 0, 5, &cryptor).unwrap();
        assert_eq!(decoded, b"hello");
    }

    #[test]
    fn test_encode_empty_events() {
        let cryptor = test_cryptor();
        let batch_id = 3i64;

        let events: Vec<EventData> = vec![];

        let (blob, nonce, offsets) = encode_batch(&events, batch_id, &cryptor).unwrap();

        assert!(offsets.is_empty());

        // Decrypt and verify empty
        let decoded = decode_batch(&blob, &nonce, batch_id, &cryptor).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_checksum_deterministic() {
        let data = b"test data for checksum";
        let checksum1 = compute_checksum(data);
        let checksum2 = compute_checksum(data);
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_checksum_different_data() {
        let checksum1 = compute_checksum(b"data1");
        let checksum2 = compute_checksum(b"data2");
        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_decode_batch_all_events() {
        let cryptor = test_cryptor();
        let batch_id = 4i64;

        let events = vec![
            EventData::new(b"first".to_vec()),
            EventData::new(b"second".to_vec()),
            EventData::new(b"third".to_vec()),
        ];

        let (blob, nonce, offsets) = encode_batch(&events, batch_id, &cryptor).unwrap();

        // Decode entire batch once
        let plaintext = decode_batch(&blob, &nonce, batch_id, &cryptor).unwrap();

        // Extract each event from the plaintext
        for (i, (offset, len)) in offsets.iter().enumerate() {
            let event_data = &plaintext[*offset..*offset + *len];
            assert_eq!(event_data, events[i].data.as_slice());
        }
    }
}
