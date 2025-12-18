//! # Cryptographic Primitives for Batch Encryption
//!
//! This module provides compression and encryption for event batches. All batches
//! are compressed with Zstd (level 1) and encrypted with AES-256-GCM.
//!
//! ## Key Management
//!
//! Keys are managed through the [`KeyProvider`] trait, which supports:
//! - Environment variable-based keys ([`EnvKeyProvider`])
//! - Future: AWS KMS, HashiCorp Vault, etc.
//!
//! Per-batch keys are derived from the master key using HKDF-SHA256, ensuring
//! unique keys for each batch even if nonces were to collide.
//!
//! ## Security Properties
//!
//! - **Confidentiality**: AES-256-GCM encryption
//! - **Integrity**: GCM authentication tag
//! - **Key isolation**: Per-batch keys via HKDF
//! - **Nonce uniqueness**: Random 96-bit nonces

use std::env;

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use hkdf::Hkdf;
use rand::{RngCore, SeedableRng};
use rand::rngs::StdRng;
use sha2::Sha256;

use crate::error::{Error, Result};

// =============================================================================
// Constants
// =============================================================================

/// Codec identifier for Zstd compression (level 1).
pub const CODEC_ZSTD_L1: i32 = 1;

/// Cipher identifier for AES-256-GCM.
pub const CIPHER_AES256GCM: i32 = 1;

/// AES-256 key size in bytes.
pub const AES256_KEY_SIZE: usize = 32;

/// AES-GCM nonce size in bytes (96 bits).
pub const AES_GCM_NONCE_SIZE: usize = 12;

/// Zstd compression level (1 = fastest).
pub const ZSTD_COMPRESSION_LEVEL: i32 = 1;

/// Environment variable name for the master encryption key.
pub const MASTER_KEY_ENV_VAR: &str = "SPITEDB_MASTER_KEY";

// =============================================================================
// Key Provider Trait
// =============================================================================

/// Trait for providing encryption keys.
///
/// This abstraction allows swapping between different key sources:
/// - Environment variable-based keys (current implementation)
/// - AWS KMS
/// - HashiCorp Vault
/// - Azure Key Vault
/// - etc.
pub trait KeyProvider: Send + Sync {
    /// Returns the master key bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be retrieved.
    fn get_master_key(&self) -> Result<[u8; AES256_KEY_SIZE]>;

    /// Derives a per-batch key from the master key using HKDF.
    ///
    /// # Arguments
    ///
    /// * `batch_id` - Unique identifier for the batch (used as HKDF info)
    /// * `nonce` - The nonce that will be used for encryption (included in derivation)
    ///
    /// # Returns
    ///
    /// A 32-byte derived key unique to this batch.
    fn derive_batch_key(
        &self,
        batch_id: i64,
        nonce: &[u8; AES_GCM_NONCE_SIZE],
    ) -> Result<[u8; AES256_KEY_SIZE]>;
}

// =============================================================================
// Environment Variable Key Provider
// =============================================================================

/// Key provider that reads the master key from an environment variable.
///
/// The master key must be provided as a 64-character hex string (32 bytes)
/// in the `SPITEDB_MASTER_KEY` environment variable.
///
/// # Example
///
/// ```bash
/// export SPITEDB_MASTER_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
/// ```
pub struct EnvKeyProvider {
    /// Cached master key (hex-decoded from env var).
    master_key: [u8; AES256_KEY_SIZE],
}

impl EnvKeyProvider {
    /// Creates a new EnvKeyProvider by reading `SPITEDB_MASTER_KEY`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Environment variable is not set
    /// - Value is not valid hex
    /// - Decoded length is not 32 bytes
    pub fn from_env() -> Result<Self> {
        let hex_key = env::var(MASTER_KEY_ENV_VAR).map_err(|_| {
            Error::KeyProvider(format!(
                "{} environment variable not set",
                MASTER_KEY_ENV_VAR
            ))
        })?;

        let key_bytes = hex_decode(&hex_key).map_err(|e| {
            Error::KeyProvider(format!("invalid hex in {}: {}", MASTER_KEY_ENV_VAR, e))
        })?;

        if key_bytes.len() != AES256_KEY_SIZE {
            return Err(Error::KeyProvider(format!(
                "{} must be {} hex characters (got {})",
                MASTER_KEY_ENV_VAR,
                AES256_KEY_SIZE * 2,
                hex_key.len()
            )));
        }

        let mut master_key = [0u8; AES256_KEY_SIZE];
        master_key.copy_from_slice(&key_bytes);

        Ok(Self { master_key })
    }

    /// Creates a provider with a specific key (for testing).
    pub fn from_key(key: [u8; AES256_KEY_SIZE]) -> Self {
        Self { master_key: key }
    }
}

impl KeyProvider for EnvKeyProvider {
    fn get_master_key(&self) -> Result<[u8; AES256_KEY_SIZE]> {
        Ok(self.master_key)
    }

    fn derive_batch_key(
        &self,
        batch_id: i64,
        nonce: &[u8; AES_GCM_NONCE_SIZE],
    ) -> Result<[u8; AES256_KEY_SIZE]> {
        // HKDF-SHA256 key derivation:
        // - salt: first 16 bytes of master key (deterministic but unique per master key)
        // - ikm: master key
        // - info: batch_id || nonce (ensures unique key per batch)
        let salt = &self.master_key[..16];

        let mut info = Vec::with_capacity(8 + AES_GCM_NONCE_SIZE);
        info.extend_from_slice(&batch_id.to_le_bytes());
        info.extend_from_slice(nonce);

        let hk = Hkdf::<Sha256>::new(Some(salt), &self.master_key);
        let mut output = [0u8; AES256_KEY_SIZE];
        hk.expand(&info, &mut output)
            .map_err(|_| Error::KeyProvider("HKDF expand failed".into()))?;

        Ok(output)
    }
}

// =============================================================================
// Batch Cryptor
// =============================================================================

/// Handles compression and encryption of batch data.
///
/// # Design Decision: Always Both
///
/// This type always compresses AND encrypts together. There's no option
/// to do just one. This eliminates the "footgun" of accidentally storing
/// unencrypted data when encryption was intended.
pub struct BatchCryptor {
    key_provider: Box<dyn KeyProvider>,
}

impl BatchCryptor {
    /// Creates a new BatchCryptor with the given key provider.
    pub fn new(key_provider: impl KeyProvider + 'static) -> Self {
        Self {
            key_provider: Box::new(key_provider),
        }
    }

    /// Creates a BatchCryptor from the environment variable.
    ///
    /// # Errors
    ///
    /// Returns an error if `SPITEDB_MASTER_KEY` is not set or invalid.
    pub fn from_env() -> Result<Self> {
        Ok(Self::new(EnvKeyProvider::from_env()?))
    }

    /// Creates a new BatchCryptor with the same key.
    ///
    /// This is useful when you need to share the cryptor across threads,
    /// since the underlying key provider can't be cloned directly.
    pub fn clone_with_same_key(&self) -> Self {
        // Get the master key from the current provider
        let key = self.key_provider.get_master_key()
            .expect("key provider should always return key after initialization");
        Self::new(EnvKeyProvider::from_key(key))
    }

    /// Compresses and encrypts batch data.
    ///
    /// # Arguments
    ///
    /// * `plaintext` - Raw batch data (concatenated event payloads)
    /// * `batch_id` - Unique batch identifier (for key derivation)
    ///
    /// # Returns
    ///
    /// A tuple of:
    /// - Encrypted ciphertext
    /// - Nonce used for encryption (must be stored with batch)
    ///
    /// # Process
    ///
    /// 1. Compress plaintext with Zstd level 1
    /// 2. Generate random 12-byte nonce
    /// 3. Derive per-batch key from master key using HKDF
    /// 4. Encrypt compressed data with AES-256-GCM
    pub fn seal(
        &self,
        plaintext: &[u8],
        batch_id: i64,
    ) -> Result<(Vec<u8>, [u8; AES_GCM_NONCE_SIZE])> {
        // Step 1: Compress with Zstd level 1
        let compressed = zstd::encode_all(plaintext, ZSTD_COMPRESSION_LEVEL)
            .map_err(|e| Error::Compression(e.to_string()))?;

        // Step 2: Generate random nonce
        let nonce = generate_nonce();

        // Step 3: Derive per-batch key
        let key = self.key_provider.derive_batch_key(batch_id, &nonce)?;

        // Step 4: Encrypt with AES-256-GCM
        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|e| Error::Encryption(format!("failed to create cipher: {}", e)))?;

        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce), compressed.as_ref())
            .map_err(|e| Error::Encryption(format!("encryption failed: {}", e)))?;

        Ok((ciphertext, nonce))
    }

    /// Decrypts and decompresses batch data.
    ///
    /// # Arguments
    ///
    /// * `ciphertext` - Encrypted batch data
    /// * `nonce` - Nonce used during encryption
    /// * `batch_id` - Batch identifier (for key derivation)
    ///
    /// # Returns
    ///
    /// Decrypted and decompressed plaintext.
    ///
    /// # Process
    ///
    /// 1. Derive per-batch key from master key using HKDF
    /// 2. Decrypt with AES-256-GCM (also verifies integrity)
    /// 3. Decompress with Zstd
    pub fn open(
        &self,
        ciphertext: &[u8],
        nonce: &[u8; AES_GCM_NONCE_SIZE],
        batch_id: i64,
    ) -> Result<Vec<u8>> {
        // Step 1: Derive per-batch key
        let key = self.key_provider.derive_batch_key(batch_id, nonce)?;

        // Step 2: Decrypt with AES-256-GCM
        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|e| Error::Encryption(format!("failed to create cipher: {}", e)))?;

        let compressed = cipher
            .decrypt(Nonce::from_slice(nonce), ciphertext)
            .map_err(|e| Error::Encryption(format!("decryption failed: {}", e)))?;

        // Step 3: Decompress with Zstd
        let plaintext = zstd::decode_all(compressed.as_slice())
            .map_err(|e| Error::Compression(e.to_string()))?;

        Ok(plaintext)
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Generates a cryptographically secure random nonce.
fn generate_nonce() -> [u8; AES_GCM_NONCE_SIZE] {
    let mut rng = StdRng::from_entropy();
    let mut nonce = [0u8; AES_GCM_NONCE_SIZE];
    rng.fill_bytes(&mut nonce);
    nonce
}

/// Decodes a hex string into bytes.
fn hex_decode(hex: &str) -> std::result::Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err("hex string must have even length".into());
    }

    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|e| format!("invalid hex at position {}: {}", i, e))
        })
        .collect()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
        ]
    }

    #[test]
    fn test_seal_open_roundtrip() {
        let cryptor = BatchCryptor::new(EnvKeyProvider::from_key(test_key()));
        let plaintext = b"Hello, world! This is test data for compression and encryption.";
        let batch_id = 12345i64;

        let (ciphertext, nonce) = cryptor.seal(plaintext, batch_id).unwrap();
        let decrypted = cryptor.open(&ciphertext, &nonce, batch_id).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_seal_open_empty_data() {
        let cryptor = BatchCryptor::new(EnvKeyProvider::from_key(test_key()));
        let plaintext = b"";
        let batch_id = 1i64;

        let (ciphertext, nonce) = cryptor.seal(plaintext, batch_id).unwrap();
        let decrypted = cryptor.open(&ciphertext, &nonce, batch_id).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_seal_open_large_data() {
        let cryptor = BatchCryptor::new(EnvKeyProvider::from_key(test_key()));
        let plaintext: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let batch_id = 999i64;

        let (ciphertext, nonce) = cryptor.seal(&plaintext, batch_id).unwrap();

        // Compressed+encrypted should be smaller than original for repetitive data
        assert!(ciphertext.len() < plaintext.len());

        let decrypted = cryptor.open(&ciphertext, &nonce, batch_id).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_different_batch_ids_produce_different_ciphertexts() {
        let cryptor = BatchCryptor::new(EnvKeyProvider::from_key(test_key()));
        let plaintext = b"same data";

        let (ciphertext1, _) = cryptor.seal(plaintext, 1).unwrap();
        let (ciphertext2, _) = cryptor.seal(plaintext, 2).unwrap();

        // Different batch IDs should produce different ciphertexts
        // (due to different nonces and different derived keys)
        assert_ne!(ciphertext1, ciphertext2);
    }

    #[test]
    fn test_wrong_batch_id_fails_decryption() {
        let cryptor = BatchCryptor::new(EnvKeyProvider::from_key(test_key()));
        let plaintext = b"secret data";
        let batch_id = 100i64;

        let (ciphertext, nonce) = cryptor.seal(plaintext, batch_id).unwrap();

        // Try to decrypt with wrong batch_id - should fail authentication
        let result = cryptor.open(&ciphertext, &nonce, 999);
        assert!(result.is_err());
    }

    #[test]
    fn test_tampered_ciphertext_fails() {
        let cryptor = BatchCryptor::new(EnvKeyProvider::from_key(test_key()));
        let plaintext = b"important data";
        let batch_id = 42i64;

        let (mut ciphertext, nonce) = cryptor.seal(plaintext, batch_id).unwrap();

        // Tamper with the ciphertext
        if !ciphertext.is_empty() {
            ciphertext[0] ^= 0xff;
        }

        // Decryption should fail due to authentication
        let result = cryptor.open(&ciphertext, &nonce, batch_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_hex_decode_valid() {
        assert_eq!(hex_decode("00").unwrap(), vec![0x00]);
        assert_eq!(hex_decode("ff").unwrap(), vec![0xff]);
        assert_eq!(hex_decode("0102").unwrap(), vec![0x01, 0x02]);
        assert_eq!(hex_decode("deadbeef").unwrap(), vec![0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn test_hex_decode_invalid() {
        assert!(hex_decode("0").is_err()); // Odd length
        assert!(hex_decode("gg").is_err()); // Invalid hex chars
    }

    #[test]
    fn test_derive_batch_key_deterministic() {
        let provider = EnvKeyProvider::from_key(test_key());
        let nonce = [0u8; 12];
        let batch_id = 123i64;

        let key1 = provider.derive_batch_key(batch_id, &nonce).unwrap();
        let key2 = provider.derive_batch_key(batch_id, &nonce).unwrap();

        assert_eq!(key1, key2);
    }

    #[test]
    fn test_derive_batch_key_different_for_different_inputs() {
        let provider = EnvKeyProvider::from_key(test_key());
        let nonce = [0u8; 12];

        let key1 = provider.derive_batch_key(1, &nonce).unwrap();
        let key2 = provider.derive_batch_key(2, &nonce).unwrap();

        assert_ne!(key1, key2);
    }
}
