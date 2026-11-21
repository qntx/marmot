#![no_main]

use libfuzzer_sys::fuzz_target;
use mk_core::media::{MediaReference, decrypt_file};
use sha2::Digest as _;

// Attachment decryption: sha256(ciphertext) -> AEAD-open -> sha256(plaintext).
// Every input must return a typed error or plaintext whose hash matches —
// never panic.
//
// Layout: `[secret 32B][ct hash 32B][pt hash 32B][nonce 12B]` then UTF-8
// `media_type\nfilename\n` and the remaining bytes are the ciphertext.
fuzz_target!(|data: &[u8]| {
    if data.len() < 32 + 32 + 32 + 12 {
        return;
    }
    let mut secret = [0u8; 32];
    secret.copy_from_slice(&data[0..32]);
    let mut ciphertext_sha256 = [0u8; 32];
    ciphertext_sha256.copy_from_slice(&data[32..64]);
    let mut plaintext_sha256 = [0u8; 32];
    plaintext_sha256.copy_from_slice(&data[64..96]);
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&data[96..108]);
    let rest = &data[108..];
    let first = rest.iter().position(|b| *b == b'\n').unwrap_or(rest.len());
    let (type_bytes, tail) = rest.split_at(first.min(rest.len()));
    let tail = tail.get(1..).unwrap_or_default();
    let second = tail.iter().position(|b| *b == b'\n').unwrap_or(tail.len());
    let (name_bytes, ciphertext) = tail.split_at(second.min(tail.len()));
    let ciphertext = ciphertext.get(1..).unwrap_or_default();
    let media_type = String::from_utf8_lossy(type_bytes).into_owned();
    let filename = String::from_utf8_lossy(name_bytes).into_owned();
    if filename.is_empty() || filename.len() > 255 || filename.contains('\0') {
        return;
    }
    let reference = MediaReference {
        locators: Vec::new(),
        ciphertext_sha256,
        plaintext_sha256,
        nonce,
        media_type,
        filename,
        dim: None,
        thumbhash: None,
    };
    if let Ok(plaintext) = decrypt_file(&secret, &reference, ciphertext)
        && sha2::Sha256::digest(&plaintext)[..] != reference.plaintext_sha256
    {
        panic!("decrypt returned plaintext failing its declared hash");
    }
});
