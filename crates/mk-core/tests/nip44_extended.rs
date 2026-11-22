//! `vectors/nip44/extended-length.json` replayed through the pinned rust-nostr fork's public
//! `nip44::v2` API: the `00 00 || u32be(len)` extended plaintext-length prefix that spec
//! `transports/nostr.md` requires every NIP-44 layer and every signer to handle.

#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    clippy::tests_outside_test_module,
    clippy::shadow_reuse,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::too_many_lines,
    reason = "test code"
)]

use mk_core as _;

use cgka_engine as _;
use cgka_traits as _;
use chacha20poly1305 as _;
use getrandom as _;
use hkdf as _;
use openmls as _;
use openmls_rust_crypto as _;
use openmls_traits as _;
use proptest as _;
use serde as _;
use storage_sqlite as _;
use thiserror as _;
use tokio as _;
use transport_nostr_peeler as _;
use url as _;
use zeroize as _;

use std::path::Path;

use base64::Engine as _;
use nostr::nips::nip44::v2::{ConversationKey, decrypt_to_bytes, encrypt_to_bytes_with_nonce};
use sha2::Digest as _;

const VECTORS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../vectors");
const EXTENDED_PREFIX_THRESHOLD: usize = 65_536;

fn hex_bytes(value: &str) -> Vec<u8> {
    hex::decode(value).expect("hex")
}

/// NIP-44 `calcPaddedLen`: 32 below or equal to 32, else the next power-of-two
/// boundary with a chunk of that boundary / 8 once it passes 256.
const fn calc_padded_len(len: usize) -> usize {
    if len <= 32 {
        return 32;
    }
    let next_power = 1usize << (len - 1).bit_width();
    let chunk = if next_power <= 256 {
        32
    } else {
        next_power / 8
    };
    chunk * ((len - 1) / chunk + 1)
}

#[test]
fn extended_length_prefix_vectors() {
    let bytes =
        std::fs::read(Path::new(VECTORS).join("nip44/extended-length.json")).expect("vector file");
    let vectors: serde_json::Value = serde_json::from_slice(&bytes).expect("vector json");
    let conversation_key =
        ConversationKey::from_slice(&hex_bytes(vectors["conversation_key"].as_str().unwrap()))
            .expect("conversation key");
    let nonce: [u8; 32] = hex_bytes(vectors["nonce"].as_str().unwrap())
        .try_into()
        .expect("32-byte nonce");

    for case in vectors["cases"].as_array().unwrap() {
        let len = usize::try_from(case["len"].as_u64().unwrap()).unwrap();
        let plaintext = vec![b'a'; len];
        assert_eq!(
            sha2::Sha256::digest(&plaintext)[..],
            hex_bytes(case["plaintext_sha256"].as_str().unwrap())[..],
            "plaintext hash mismatch at len {len}"
        );

        let payload = encrypt_to_bytes_with_nonce(&conversation_key, &plaintext, nonce)
            .expect("encrypt_to_bytes_with_nonce");
        let encoded = base64::engine::general_purpose::STANDARD.encode(&payload);
        assert_eq!(
            sha2::Sha256::digest(encoded.as_bytes())[..],
            hex_bytes(case["payload_sha256"].as_str().unwrap())[..],
            "payload hash mismatch at len {len}"
        );

        // payload = version(1) | nonce(32) | prefix | padded plaintext | mac(32);
        // the extended prefix costs 4 bytes more than the legacy u16 one.
        let prefix_len = if len >= EXTENDED_PREFIX_THRESHOLD {
            6
        } else {
            2
        };
        assert_eq!(
            payload.len(),
            1 + 32 + prefix_len + calc_padded_len(len) + 32,
            "payload layout at len {len}"
        );
        assert_eq!(
            decrypt_to_bytes(&conversation_key, &payload).expect("decrypt_to_bytes"),
            plaintext
        );
    }
}
