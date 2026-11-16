//! Encrypted-media pure-layer tests: the recorded `v2-crypto.json` key
//! schedule/encryption KATs, the byte-for-byte MDK `imeta-v{1,2}.json`
//! parser fixtures, spec edge cases not in any fixture, and destination
//! classification.

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
use tokio as _;

use base64 as _;
use cgka_engine as _;
use cgka_traits as _;
use chacha20poly1305 as _;
use getrandom as _;
use hkdf as _;
use nostr as _;
use openmls as _;
use openmls_rust_crypto as _;
use openmls_traits as _;
use proptest as _;
use serde as _;
use sha2 as _;
use storage_sqlite as _;
use thiserror as _;
use transport_nostr_peeler as _;
use url as _;
use zeroize as _;

use std::net::IpAddr;
use std::path::Path;

use mk_core::DestinationPolicy;
use mk_core::media::{
    AttachmentRejection, BlobEndpoint, Destination, EncryptedMediaPolicy, Locator, MediaReference,
    check_resolved_address, parse_imeta,
};

#[cfg(feature = "testing")]
use mk_core::media;
#[cfg(feature = "testing")]
use sha2::Digest as _;

const VECTORS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../vectors");

fn hex_bytes(value: &str) -> Vec<u8> {
    hex::decode(value).expect("hex")
}

fn hex32(value: &str) -> [u8; 32] {
    hex_bytes(value).try_into().expect("32 bytes")
}

fn load(path: &str) -> serde_json::Value {
    let bytes = std::fs::read(Path::new(VECTORS).join(path)).expect("vector file");
    serde_json::from_slice(&bytes).expect("vector json")
}

#[cfg(feature = "testing")]
mod crypto_kats {
    use super::*;

    #[test]
    fn key_schedule_cases_match() {
        let vectors = load("encrypted-media/v2-crypto.json");
        for case in vectors["key_schedule"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let secret = hex32(case["media_secret_hex"].as_str().unwrap());
            let plaintext_sha256 = hex32(case["plaintext_sha256"].as_str().unwrap());
            let media_type = case["media_type"].as_str().unwrap();
            let filename = case["filename"].as_str().unwrap();

            let key = media::derive_file_key(&secret, &plaintext_sha256, media_type, filename);
            assert_eq!(
                key.as_bytes(),
                &hex32(case["file_key_hex"].as_str().unwrap()),
                "{name}: file key"
            );
            assert_eq!(
                media::media_aad(&plaintext_sha256, media_type, filename),
                hex_bytes(case["aad_hex"].as_str().unwrap()),
                "{name}: aad"
            );
        }
    }

    #[test]
    fn encryption_cases_match_and_decrypt() {
        let vectors = load("encrypted-media/v2-crypto.json");
        for case in vectors["encryption"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let secret = hex32(case["media_secret_hex"].as_str().unwrap());
            let plaintext_sha256 = hex32(case["plaintext_sha256"].as_str().unwrap());
            let media_type = case["media_type"].as_str().unwrap();
            let filename = case["filename"].as_str().unwrap();
            let plaintext = hex_bytes(case["plaintext_hex"].as_str().unwrap());
            let nonce: [u8; 12] = hex_bytes(case["nonce_hex"].as_str().unwrap())
                .try_into()
                .unwrap();
            let ciphertext = hex_bytes(case["ciphertext_hex"].as_str().unwrap());
            let ciphertext_sha256 = hex32(case["ciphertext_sha256"].as_str().unwrap());

            let key = media::derive_file_key(&secret, &plaintext_sha256, media_type, filename);
            let aad = media::media_aad(&plaintext_sha256, media_type, filename);
            assert_eq!(
                media::encrypt_file(&key, &nonce, &aad, &plaintext).unwrap(),
                ciphertext,
                "{name}: ciphertext"
            );
            let digest = sha2::Sha256::digest(&ciphertext);
            assert_eq!(&digest[..], &ciphertext_sha256, "{name}: ciphertext hash");

            let reference = MediaReference {
                locators: vec![Locator {
                    kind: "blossom-v1".to_owned(),
                    value: format!("https://media.example/{}", hex::encode(ciphertext_sha256)),
                }],
                ciphertext_sha256,
                plaintext_sha256,
                nonce,
                media_type: media_type.to_owned(),
                filename: filename.to_owned(),
                dim: None,
                thumbhash: None,
            };
            assert_eq!(
                media::decrypt_file(&secret, &reference, &ciphertext).unwrap(),
                plaintext,
                "{name}: decrypt"
            );

            // One flipped ciphertext byte → ciphertext hash mismatch.
            let mut tampered = ciphertext.clone();
            tampered[0] ^= 0x01;
            assert_eq!(
                media::decrypt_file(&secret, &reference, &tampered),
                Err(media::MediaError::CiphertextHashMismatch),
                "{name}: tampered"
            );

            // Correct hash, wrong filename → AEAD fails (AAD mismatch).
            let mut mismatched = reference.clone();
            mismatched.filename = format!("{}x", reference.filename);
            assert_eq!(
                media::decrypt_file(&secret, &mismatched, &ciphertext),
                Err(media::MediaError::DecryptionFailed),
                "{name}: wrong filename decrypts"
            );
        }
    }
}

mod imeta_vectors {
    use super::*;

    fn expected_reference(case: &serde_json::Value) -> MediaReference {
        let expected = &case["expected"];
        MediaReference {
            locators: expected["locators"]
                .as_array()
                .unwrap()
                .iter()
                .map(|locator| Locator {
                    kind: locator["kind"].as_str().unwrap().to_owned(),
                    value: locator["value"].as_str().unwrap().to_owned(),
                })
                .collect(),
            ciphertext_sha256: hex32(expected["ciphertext_sha256"].as_str().unwrap()),
            plaintext_sha256: hex32(expected["plaintext_sha256"].as_str().unwrap()),
            nonce: hex_bytes(expected["nonce_hex"].as_str().unwrap())
                .try_into()
                .unwrap(),
            media_type: expected["media_type"].as_str().unwrap().to_owned(),
            filename: expected["file_name"].as_str().unwrap().to_owned(),
            dim: expected["dim"].as_str().map(str::to_owned),
            thumbhash: expected["thumbhash"].as_str().map(str::to_owned),
        }
    }

    fn tag_of(case: &serde_json::Value) -> Vec<String> {
        case["tag"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| field.as_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn v2_valid_cases_parse_and_round_trip() {
        let vectors = load("mdk/encrypted-media/imeta-v2.json");
        for case in vectors["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let tag = tag_of(case);
            if case["valid"].as_bool().unwrap() {
                let reference = parse_imeta(&tag).unwrap_or_else(|_| panic!("{name}"));
                assert_eq!(reference, expected_reference(case), "{name}");
                assert_eq!(reference.to_imeta(), tag, "{name}: wire round-trip");
            } else {
                let expected = case["rejection_kind"].as_str().unwrap();
                let Err(kind) = parse_imeta(&tag) else {
                    panic!("{name}: accepted a {expected} case");
                };
                assert_eq!(kind.as_str(), expected, "{name}");
            }
        }
    }

    #[test]
    fn v1_cases_are_all_rejected() {
        let vectors = load("mdk/encrypted-media/imeta-v1.json");
        for case in vectors["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            assert!(
                parse_imeta(&tag_of(case)).is_err(),
                "{name}: v1 accepted as v2"
            );
        }
    }
}

mod imeta_spec_edges {
    use super::*;

    const HASH: &str = "abababababababababababababababababababababababababababababababab";
    const HASH2: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
    const NONCE: &str = "efefefefefefefefefefefef";

    fn base() -> Vec<String> {
        vec![
            "imeta".to_owned(),
            "v encrypted-media-v2".to_owned(),
            format!("locator blossom-v1 https://media.example/{HASH}.bin"),
            format!("ciphertext_sha256 {HASH}"),
            format!("plaintext_sha256 {HASH2}"),
            format!("nonce {NONCE}"),
            "m image/png".to_owned(),
            "filename a.png".to_owned(),
        ]
    }

    fn rejected(tag: &[String]) -> AttachmentRejection {
        parse_imeta(tag).expect_err("expected rejection")
    }

    #[test]
    fn blurhash_is_rejected() {
        let mut tag = base();
        tag.push("blurhash LEHV6nWB2yk8pyo0adR*.7kCMdnj".to_owned());
        assert_eq!(rejected(&tag), AttachmentRejection::MalformedField);
    }

    #[test]
    fn nonce_lengths_are_strict() {
        for nonce in ["efefefefefefefefefefef", "efefefefefefefefefefefefef"] {
            let mut tag = base();
            tag[5] = format!("nonce {nonce}");
            assert_eq!(
                rejected(&tag),
                AttachmentRejection::MalformedField,
                "{nonce}"
            );
        }
    }

    #[test]
    fn hash_hex_accepts_any_case() {
        // Spec + MDK `hex::decode`: the hash/nonce fields are case-insensitive;
        // `to_imeta` re-emits canonical lowercase.
        let mut tag = base();
        tag[3] = format!("ciphertext_sha256 {}", HASH.to_uppercase());
        tag[4] = format!("plaintext_sha256 {}", HASH2.to_uppercase());
        tag[5] = format!("nonce {}", NONCE.to_uppercase());
        let reference = parse_imeta(&tag).expect("uppercase hex parses");
        let reemitted = reference.to_imeta();
        assert_eq!(reemitted[3], format!("ciphertext_sha256 {HASH}"));
        assert_eq!(reemitted[4], format!("plaintext_sha256 {HASH2}"));
        assert_eq!(reemitted[5], format!("nonce {NONCE}"));

        // Wrong length or non-hex still rejects.
        for (index, value) in [
            (3usize, "abab".to_owned()),
            (4, "g".repeat(64)),
            (5, "ab".repeat(13)),
        ] {
            let mut bad = base();
            let field = bad[index].split(' ').next().expect("field").to_owned();
            bad[index] = format!("{field} {value}");
            assert_eq!(rejected(&bad), AttachmentRejection::MalformedField);
        }
    }

    #[test]
    fn blossom_locator_must_be_http() {
        let mut tag = base();
        tag[2] = format!("locator blossom-v1 ftp://media.example/{HASH}.bin");
        assert_eq!(rejected(&tag), AttachmentRejection::MalformedField);
    }

    #[test]
    fn empty_locator_kind_is_rejected() {
        let mut tag = base();
        tag[2] = "locator  https://media.example/x".to_owned();
        assert_eq!(rejected(&tag), AttachmentRejection::MalformedField);
    }

    #[test]
    fn duplicate_dim_is_rejected() {
        let mut tag = base();
        tag.push("dim 1x1".to_owned());
        tag.push("dim 2x2".to_owned());
        assert_eq!(rejected(&tag), AttachmentRejection::DuplicateField);
    }

    #[test]
    fn filename_nul_and_length_bounds() {
        for filename in [String::from("a\u{0}b"), "x".repeat(256)] {
            let mut tag = base();
            tag[7] = format!("filename {filename}");
            assert_eq!(rejected(&tag), AttachmentRejection::MalformedField);
        }
        let mut tag = base();
        tag[7] = format!("filename {}", "x".repeat(255));
        assert!(parse_imeta(&tag).is_ok());
    }

    #[test]
    fn out_of_policy_locator_kind_still_parses() {
        let mut tag = base();
        tag[2] = "locator ipfs-v0 https://gateway.example/ipfs/x".to_owned();
        assert_eq!(parse_imeta(&tag).unwrap().locators[0].kind, "ipfs-v0");
    }

    #[test]
    fn loopback_locator_still_parses() {
        let mut tag = base();
        tag[2] = format!("locator blossom-v1 http://127.0.0.1:7777/{HASH}");
        let reference = parse_imeta(&tag).unwrap();
        assert_eq!(
            reference.locators[0].destination(),
            Some(Destination::Loopback)
        );
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let mut tag = base();
        tag.push("vendor custom value".to_owned());
        tag.push("x".to_owned());
        assert!(parse_imeta(&tag).is_ok());
    }
}

mod destinations {
    use super::*;

    #[test]
    fn endpoint_urls_and_classification() {
        let endpoint = BlobEndpoint {
            locator_kind: "blossom-v1".to_owned(),
            base_url: "https://media.example/".to_owned(),
        };
        assert_eq!(endpoint.upload_url(), "https://media.example/upload");
        assert_eq!(
            endpoint.fetch_url(&[0xab; 32]),
            format!("https://media.example/{}", "ab".repeat(32))
        );
        assert_eq!(endpoint.destination(), Some(Destination::DomainName));

        let loopback = BlobEndpoint {
            locator_kind: "blossom-v1".to_owned(),
            base_url: "http://127.0.0.1:7777".to_owned(),
        };
        assert_eq!(loopback.destination(), Some(Destination::Loopback));
        assert_eq!(
            loopback
                .destination()
                .and_then(|d| d.permitted(DestinationPolicy {
                    allow_loopback: false
                })),
            Some(false)
        );
        assert_eq!(
            loopback
                .destination()
                .and_then(|d| d.permitted(DestinationPolicy {
                    allow_loopback: true
                })),
            Some(true)
        );

        let private = Locator {
            kind: "blossom-v1".to_owned(),
            value: "https://10.0.0.4/blob".to_owned(),
        };
        assert_eq!(private.destination(), Some(Destination::NonPublicAddress));
        assert_eq!(
            private
                .destination()
                .and_then(|d| d.permitted(DestinationPolicy {
                    allow_loopback: true
                })),
            Some(false)
        );

        let public = Locator {
            kind: "blossom-v1".to_owned(),
            value: "https://93.184.216.34/blob".to_owned(),
        };
        assert_eq!(public.destination(), Some(Destination::PublicAddress));

        let non_http = Locator {
            kind: "x".to_owned(),
            value: "ipfs://cid".to_owned(),
        };
        assert_eq!(non_http.destination(), None);
    }

    #[test]
    fn resolved_addresses_follow_the_policy() {
        let strict = DestinationPolicy {
            allow_loopback: false,
        };
        let permissive = DestinationPolicy {
            allow_loopback: true,
        };
        assert!(check_resolved_address("8.8.8.8".parse::<IpAddr>().unwrap(), strict).is_ok());
        assert!(check_resolved_address("127.0.0.1".parse().unwrap(), strict).is_err());
        assert!(check_resolved_address("127.0.0.1".parse().unwrap(), permissive).is_ok());
        assert!(check_resolved_address("192.168.1.1".parse().unwrap(), permissive).is_err());
    }

    #[test]
    fn policy_round_trip_through_component() {
        let policy = EncryptedMediaPolicy::blossom(&["https://media.example/".to_owned()]);
        let component = policy.to_component().unwrap();
        assert_eq!(component.component_id, 0x800b);
        let decoded = EncryptedMediaPolicy::from_component(&component.data).unwrap();
        assert_eq!(decoded.allowed_locator_kinds, vec!["blossom-v1".to_owned()]);
        assert_eq!(decoded.endpoints.len(), 1);
        assert!(decoded.allows_locator_kind("blossom-v1"));
        assert!(!decoded.allows_locator_kind("ipfs-v0"));
    }
}
