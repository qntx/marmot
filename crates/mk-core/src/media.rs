//! Encrypted media v2 (`0x800b`): strict `imeta` parsing, the HKDF file-key
//! schedule, ChaCha20-Poly1305 file encryption, group policy and destination
//! safety.
//!
//! This module is pure: no session state, no storage, no I/O. The session
//! layer binds it to epochs and durable rows.
//!
//! Wire rules follow `features/encrypted-media.md` "Message Shape" +
//! "Validation" exactly as MDK's shared parser implements them
//! (`marmot-app/src/media/mod.rs::parse_media_attachment` on the pinned rev),
//! narrowed to v2 only: the `v` field is judged before every other field so
//! rejection categories are field-order independent; a present `blurhash` is
//! `malformed_field`; every other unknown field is ignored (the same choice
//! MDK makes); single-occurrence fields reject on a second occurrence;
//! out-of-policy locator kinds and unsafe destinations are never a parse-time
//! validity condition — a locator this client cannot or must not fetch is
//! unfetchable, not invalid (spec "Validation": media is authenticated by its
//! hashes + AEAD independent of the locator).
//!
//! Hash and nonce fields accept any hex case (spec "Validation": "not a
//! 32-byte hex SHA-256 value" / "24 hex characters"; MDK `hex::decode`).
//! [`MediaReference::to_imeta`] emits canonical lowercase, so the exact
//! inverse is `parse_imeta(r.to_imeta()) == Ok(r)` — the fuzzer asserts it.

use std::net::IpAddr;

use cgka_traits::app_components::{
    BLOSSOM_LOCATOR_KIND_V1, ENCRYPTED_MEDIA_FORMAT_V2, EncryptedMediaPolicyV2,
    GROUP_ENCRYPTED_MEDIA_V2_COMPONENT_ID, canonicalize_marmot_media_type,
    decode_encrypted_media_policy_v2, encode_encrypted_media_policy_v2, is_loopback_host,
    is_loopback_ip, is_public_ip, reject_non_public_ip,
};
use chacha20poly1305::ChaCha20Poly1305;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use hkdf::Hkdf;
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::{Host, Url};
use zeroize::Zeroizing;

use crate::session::ComponentUpdate;
use crate::session::DestinationPolicy;

/// The `imeta` tag name carrying one media reference.
pub const IMETA_TAG: &str = "imeta";

/// One way to fetch an attachment: a `locator <kind> <value>` field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Locator {
    /// The locator scheme (`blossom-v1`, or an out-of-policy kind this client
    /// keeps but cannot fetch).
    pub kind: String,
    /// The locator value (a URL).
    pub value: String,
}

impl Locator {
    /// The destination-safety class of the locator's host literal. `None`
    /// when the value is not an `http`/`https` URL — the host never resolves
    /// such locators.
    #[must_use]
    pub fn destination(&self) -> Option<Destination> {
        classify_url(&self.value)
    }
}

/// A validated `encrypted-media-v2` attachment reference.
///
/// Hashes and the nonce are stored as bytes; the wire representation is
/// always lowercase hex (see [`parse_imeta`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaReference {
    /// Locators in tag order.
    pub locators: Vec<Locator>,
    /// SHA-256 of the ciphertext blob.
    pub ciphertext_sha256: [u8; 32],
    /// SHA-256 of the plaintext.
    pub plaintext_sha256: [u8; 32],
    /// The 12-byte AEAD nonce.
    pub nonce: [u8; 12],
    /// The canonical Marmot media type (byte-equal to
    /// `canonicalize_marmot_media_type` output).
    pub media_type: String,
    /// The filename, preserved exactly (1..=255 UTF-8 bytes, no NUL).
    pub filename: String,
    /// Optional `WxH` dimensions, verbatim.
    pub dim: Option<String>,
    /// Optional thumbhash, verbatim.
    pub thumbhash: Option<String>,
}

impl MediaReference {
    /// The `imeta` tag for this reference: `v`, locators, hashes, nonce,
    /// `m`, `filename`, then `dim`/`thumbhash` when present — including
    /// `Some("")` emitted as a present-empty value (`"dim "`). Canonical
    /// lowercase hex: `parse_imeta(reference.to_imeta()) == Ok(reference)`.
    #[must_use]
    pub fn to_imeta(&self) -> Vec<String> {
        let mut tag = Vec::with_capacity(7 + self.locators.len() + 2);
        tag.push(IMETA_TAG.to_owned());
        tag.push(format!("v {ENCRYPTED_MEDIA_FORMAT_V2}"));
        for locator in &self.locators {
            tag.push(format!("locator {} {}", locator.kind, locator.value));
        }
        tag.push(format!(
            "ciphertext_sha256 {}",
            hex::encode(self.ciphertext_sha256)
        ));
        tag.push(format!(
            "plaintext_sha256 {}",
            hex::encode(self.plaintext_sha256)
        ));
        tag.push(format!("nonce {}", hex::encode(self.nonce)));
        tag.push(format!("m {}", self.media_type));
        tag.push(format!("filename {}", self.filename));
        if let Some(dim) = &self.dim {
            tag.push(format!("dim {dim}"));
        }
        if let Some(thumbhash) = &self.thumbhash {
            tag.push(format!("thumbhash {thumbhash}"));
        }
        tag
    }
}

/// The stable category of one rejected `imeta` attachment. `as_str` names are
/// MDK's `MediaAttachmentRejectionKind` vocabulary, shared with the recorded
/// fixtures' `rejection_kind` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Error)]
#[non_exhaustive]
pub enum AttachmentRejection {
    /// Not a decodable `imeta` tag: wrong first element, a known field with
    /// no value, or a `locator` without its `kind value` pair.
    #[error("invalid_structure")]
    InvalidStructure,
    /// The `v` field is absent or names a format this client does not
    /// implement (only `encrypted-media-v2` is implemented).
    #[error("unsupported_format")]
    UnsupportedFormat,
    /// A required field (`locator`, `ciphertext_sha256`, `plaintext_sha256`,
    /// `nonce`, `m`, `filename`) is absent or empty.
    #[error("missing_field")]
    MissingField,
    /// A single-occurrence field appears more than once.
    #[error("duplicate_field")]
    DuplicateField,
    /// A present field has an invalid value: non-hex or wrong-length hash or
    /// nonce, an unparseable locator URL, a Blossom locator that is not
    /// http/https, a non-canonical media type, an invalid filename, or a
    /// forbidden `blurhash`.
    #[error("malformed_field")]
    MalformedField,
}

impl AttachmentRejection {
    /// The stable `snake_case` label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidStructure => "invalid_structure",
            Self::UnsupportedFormat => "unsupported_format",
            Self::MissingField => "missing_field",
            Self::DuplicateField => "duplicate_field",
            Self::MalformedField => "malformed_field",
        }
    }
}

/// Parse one `imeta` tag into a validated [`MediaReference`].
///
/// Field split is at the FIRST space; a value may contain spaces or be empty.
/// The `v` pre-pass runs before everything so `unsupported_format` and
/// duplicate-`v` verdicts never depend on field order.
///
/// # Errors
/// [`AttachmentRejection`], attachment-local by contract: a rejected tag
/// never invalidates its siblings or the carrying message.
pub fn parse_imeta(tag: &[String]) -> Result<MediaReference, AttachmentRejection> {
    if tag.first().map(String::as_str) != Some(IMETA_TAG) {
        return Err(AttachmentRejection::InvalidStructure);
    }
    let mut version = None;
    for field in tag.iter().skip(1) {
        if field == "v" {
            return Err(AttachmentRejection::InvalidStructure);
        }
        if let Some(value) = field.strip_prefix("v ") {
            if version.is_some() {
                return Err(AttachmentRejection::DuplicateField);
            }
            if value != ENCRYPTED_MEDIA_FORMAT_V2 {
                return Err(AttachmentRejection::UnsupportedFormat);
            }
            version = Some(());
        }
    }
    if version.is_none() {
        return Err(AttachmentRejection::UnsupportedFormat);
    }

    let mut locators = Vec::new();
    let mut ciphertext_sha256 = None;
    let mut plaintext_sha256 = None;
    let mut nonce_hex = None;
    let mut media_type = None;
    let mut filename = None;
    let mut dim = None;
    let mut thumbhash = None;
    let set_once = |slot: &mut Option<String>, value: &str| {
        if slot.is_some() {
            return Err(AttachmentRejection::DuplicateField);
        }
        *slot = Some(value.to_owned());
        Ok(())
    };
    for field in tag.iter().skip(1) {
        if field == "blurhash" || field.starts_with("blurhash ") {
            return Err(AttachmentRejection::MalformedField);
        }
        if let Some(rest) = field.strip_prefix("locator ") {
            let (kind, value) = rest
                .split_once(' ')
                .ok_or(AttachmentRejection::InvalidStructure)?;
            locators.push(Locator {
                kind: kind.to_owned(),
                value: value.to_owned(),
            });
            continue;
        }
        let Some((key, value)) = field.split_once(' ') else {
            if matches!(
                field.as_str(),
                "locator"
                    | "ciphertext_sha256"
                    | "plaintext_sha256"
                    | "nonce"
                    | "m"
                    | "filename"
                    | "dim"
                    | "thumbhash"
            ) {
                return Err(AttachmentRejection::InvalidStructure);
            }
            continue;
        };
        match key {
            "ciphertext_sha256" => set_once(&mut ciphertext_sha256, value)?,
            "plaintext_sha256" => set_once(&mut plaintext_sha256, value)?,
            "nonce" => set_once(&mut nonce_hex, value)?,
            "m" => set_once(&mut media_type, value)?,
            "filename" => set_once(&mut filename, value)?,
            "dim" => set_once(&mut dim, value)?,
            "thumbhash" => set_once(&mut thumbhash, value)?,
            _ => {}
        }
    }
    let required = |value: Option<String>| {
        value
            .filter(|value| !value.is_empty())
            .ok_or(AttachmentRejection::MissingField)
    };
    let reference = MediaReference {
        locators,
        ciphertext_sha256: hash32(&required(ciphertext_sha256)?)?,
        plaintext_sha256: hash32(&required(plaintext_sha256)?)?,
        nonce: nonce12(&required(nonce_hex)?)?,
        media_type: required(media_type)?,
        filename: required(filename)?,
        dim,
        thumbhash,
    };
    validate_reference(&reference)?;
    Ok(reference)
}

/// One-way structural validation shared by [`parse_imeta`]. Rejection kinds
/// mirror MDK's `MediaAttachmentReference::validate` for the v2 arm; locator
/// kinds and destinations are deliberately absent from validity.
fn validate_reference(reference: &MediaReference) -> Result<(), AttachmentRejection> {
    if reference.locators.is_empty() {
        return Err(AttachmentRejection::MissingField);
    }
    for locator in &reference.locators {
        if locator.kind.trim().is_empty() || locator.value.trim().is_empty() {
            return Err(AttachmentRejection::MalformedField);
        }
        let Ok(url) = Url::parse(&locator.value) else {
            return Err(AttachmentRejection::MalformedField);
        };
        if locator.kind == BLOSSOM_LOCATOR_KIND_V1 && !matches!(url.scheme(), "http" | "https") {
            return Err(AttachmentRejection::MalformedField);
        }
    }
    let filename = &reference.filename;
    if filename.is_empty() || filename.len() > 255 || filename.contains('\0') {
        return Err(AttachmentRejection::MalformedField);
    }
    match canonicalize_marmot_media_type(&reference.media_type) {
        Ok(canonical) if canonical == reference.media_type => {}
        _ => return Err(AttachmentRejection::MalformedField),
    }
    Ok(())
}

/// Decode exactly 64 hex digits (any case) into 32 bytes.
fn hash32(value: &str) -> Result<[u8; 32], AttachmentRejection> {
    exact_hex(value)
}

/// Decode exactly 24 hex digits (any case) into 12 bytes.
fn nonce12(value: &str) -> Result<[u8; 12], AttachmentRejection> {
    exact_hex(value)
}

fn exact_hex<const N: usize>(value: &str) -> Result<[u8; N], AttachmentRejection> {
    if value.len() != N * 2 {
        return Err(AttachmentRejection::MalformedField);
    }
    let decoded = hex::decode(value).map_err(|_| AttachmentRejection::MalformedField)?;
    decoded
        .try_into()
        .map_err(|_| AttachmentRejection::MalformedField)
}

/// Every media error the pure layer and the session's media methods produce.
/// Carries no payload: filenames, URLs and hashes never appear here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum MediaError {
    /// The media type is not canonicalizable to a Marmot media type.
    #[error("invalid media type")]
    InvalidMediaType,
    /// The filename violates the profile (empty, over 255 bytes, or NUL).
    #[error("invalid filename")]
    InvalidFilename,
    /// The ciphertext blob's SHA-256 does not match the reference.
    #[error("ciphertext hash mismatch")]
    CiphertextHashMismatch,
    /// AEAD open failed (wrong key material, nonce, AAD, or truncated
    /// ciphertext).
    #[error("decryption failed")]
    DecryptionFailed,
    /// The decrypted plaintext's SHA-256 does not match the reference.
    #[error("plaintext hash mismatch")]
    PlaintextHashMismatch,
    /// The locator's kind is not in the group's allowed locator kinds.
    #[error("locator kind not allowed by the group policy")]
    LocatorNotAllowedByPolicy,
    /// The locator is structurally invalid (empty kind/value or not a URL,
    /// or a Blossom locator that is not http/https).
    #[error("invalid locator")]
    InvalidLocator,
    /// The `0x800b` component bytes or a caller-supplied policy failed the
    /// spec codec's validation.
    #[error("invalid encrypted-media policy")]
    InvalidPolicy,
    /// The plaintext exceeds ChaCha20-Poly1305's per-message ceiling — the
    /// only failure `encrypt_file` can produce.
    #[error("payload too large")]
    PayloadTooLarge,
}

/// The per-file key: HKDF-SHA256 expand-only over the epoch's 32-byte media
/// secret. `Debug` is redacted; the bytes are exposed only to test builds.
#[derive(Clone)]
pub struct FileKey(Zeroizing<[u8; 32]>);

impl std::fmt::Debug for FileKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FileKey([REDACTED])")
    }
}

#[cfg(feature = "testing")]
impl FileKey {
    /// The raw 32-byte key, for known-answer tests only.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// The HKDF `info` for one file: `"encrypted-media-v2" 0x00 hash 0x00
/// media_type 0x00 filename 0x00 "key"`.
#[must_use]
pub fn media_key_info(plaintext_sha256: &[u8; 32], media_type: &str, filename: &str) -> Vec<u8> {
    let mut info = Vec::with_capacity(
        ENCRYPTED_MEDIA_FORMAT_V2.len() + 1 + 32 + 1 + media_type.len() + 1 + filename.len() + 4,
    );
    info.extend_from_slice(ENCRYPTED_MEDIA_FORMAT_V2.as_bytes());
    info.push(0);
    info.extend_from_slice(plaintext_sha256);
    info.push(0);
    info.extend_from_slice(media_type.as_bytes());
    info.push(0);
    info.extend_from_slice(filename.as_bytes());
    info.push(0);
    info.extend_from_slice(b"key");
    info
}

/// The AEAD associated data for one file: `"encrypted-media-v2" 0x00 hash
/// 0x00 media_type 0x00 filename`.
#[must_use]
pub fn media_aad(plaintext_sha256: &[u8; 32], media_type: &str, filename: &str) -> Vec<u8> {
    let mut aad =
        Vec::with_capacity(ENCRYPTED_MEDIA_FORMAT_V2.len() + 1 + 32 + 1 + media_type.len() + 1);
    aad.extend_from_slice(ENCRYPTED_MEDIA_FORMAT_V2.as_bytes());
    aad.push(0);
    aad.extend_from_slice(plaintext_sha256);
    aad.push(0);
    aad.extend_from_slice(media_type.as_bytes());
    aad.push(0);
    aad.extend_from_slice(filename.as_bytes());
    aad
}

/// Derive one file's key from the epoch's 32-byte media secret (HKDF-SHA256,
/// expand-only, `from_prk`).
#[must_use]
pub fn derive_file_key(
    media_secret: &[u8; 32],
    plaintext_sha256: &[u8; 32],
    media_type: &str,
    filename: &str,
) -> FileKey {
    // `from_prk` rejects only PRKs shorter than SHA-256's 32-byte output;
    // `media_secret` is exactly 32 bytes, so this never fails.
    let Ok(hkdf) = Hkdf::<Sha256>::from_prk(media_secret) else {
        return FileKey(Zeroizing::new([0u8; 32]));
    };
    let mut key = Zeroizing::new([0u8; 32]);
    // A 32-byte output can never exceed HKDF-SHA256's length ceiling.
    hkdf.expand(
        &media_key_info(plaintext_sha256, media_type, filename),
        &mut *key,
    )
    .unwrap_or(());
    FileKey(key)
}

/// Encrypt `plaintext` under `key`/`nonce` with `aad` (ChaCha20-Poly1305).
/// Output is `plaintext.len() + 16` bytes (ciphertext + tag).
///
/// # Errors
/// [`MediaError::PayloadTooLarge`] — the only failure this primitive can
/// produce (the cipher's per-message ceiling).
pub fn encrypt_file(
    key: &FileKey,
    nonce: &[u8; 12],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, MediaError> {
    let cipher = ChaCha20Poly1305::new((&*key.0).into());
    cipher
        .encrypt(
            nonce.into(),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| MediaError::PayloadTooLarge)
}

/// Decrypt one attachment: verify `sha256(ciphertext)` against the reference,
/// AEAD-open with the reference's AAD, then verify `sha256(plaintext)`.
///
/// # Errors
/// [`MediaError::CiphertextHashMismatch`], [`MediaError::DecryptionFailed`]
/// (which also covers a wrong filename or media type — the AAD no longer
/// matches) and [`MediaError::PlaintextHashMismatch`].
pub fn decrypt_file(
    media_secret: &[u8; 32],
    reference: &MediaReference,
    ciphertext: &[u8],
) -> Result<Vec<u8>, MediaError> {
    let digest = Sha256::digest(ciphertext);
    if digest[..] != reference.ciphertext_sha256 {
        return Err(MediaError::CiphertextHashMismatch);
    }
    let key = derive_file_key(
        media_secret,
        &reference.plaintext_sha256,
        &reference.media_type,
        &reference.filename,
    );
    let aad = media_aad(
        &reference.plaintext_sha256,
        &reference.media_type,
        &reference.filename,
    );
    let cipher = ChaCha20Poly1305::new((&*key.0).into());
    let plaintext = cipher
        .decrypt(
            (&reference.nonce).into(),
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| MediaError::DecryptionFailed)?;
    if Sha256::digest(&plaintext)[..] != reference.plaintext_sha256 {
        return Err(MediaError::PlaintextHashMismatch);
    }
    Ok(plaintext)
}

/// Validate a caller-supplied media type for [`crate::Session`]'s seal path.
///
/// Canonicalizes it (parameters stripped, whitespace trimmed, `image/jpg`
/// aliased) — the canonical form is what feeds key derivation, AAD and the
/// wire `m` field.
///
/// # Errors
/// [`MediaError::InvalidMediaType`].
pub fn canonical_media_type(media_type: &str) -> Result<String, MediaError> {
    canonicalize_marmot_media_type(media_type).map_err(|_| MediaError::InvalidMediaType)
}

/// Validate a caller-supplied filename for the v2 profile.
///
/// 1..=255 UTF-8 bytes, no NUL.
///
/// # Errors
/// [`MediaError::InvalidFilename`].
pub fn validate_filename(filename: &str) -> Result<(), MediaError> {
    if filename.is_empty() || filename.len() > 255 || filename.contains('\0') {
        return Err(MediaError::InvalidFilename);
    }
    Ok(())
}

/// Validate an outbound locator.
///
/// Non-empty kind and value, URL-parseable, and `blossom-v1` locators
/// restricted to http/https. Locator kinds are not checked here — that is
/// the sender policy rule (`send_media`).
///
/// # Errors
/// [`MediaError::InvalidLocator`].
pub fn validate_locator(locator: &Locator) -> Result<(), MediaError> {
    if locator.kind.trim().is_empty() || locator.value.trim().is_empty() {
        return Err(MediaError::InvalidLocator);
    }
    let url = Url::parse(&locator.value).map_err(|_| MediaError::InvalidLocator)?;
    if locator.kind == BLOSSOM_LOCATOR_KIND_V1 && !matches!(url.scheme(), "http" | "https") {
        return Err(MediaError::InvalidLocator);
    }
    Ok(())
}

/// One default blob-store endpoint of the group's media policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobEndpoint {
    /// The locator kind this endpoint serves (`blossom-v1`).
    pub locator_kind: String,
    /// The server's base URL (kept verbatim; `server_root` trims).
    pub base_url: String,
}

impl BlobEndpoint {
    /// The base URL with all trailing `/` removed.
    fn server_root(&self) -> &str {
        self.base_url.trim_end_matches('/')
    }

    /// The BUD-02 upload URL (`server_root + "/upload"`).
    #[must_use]
    pub fn upload_url(&self) -> String {
        format!("{}/upload", self.server_root())
    }

    /// The fallback fetch URL for one ciphertext hash
    /// (`server_root + "/" + lowercase hex`), per spec "Locator Kinds".
    #[must_use]
    pub fn fetch_url(&self, ciphertext_sha256: &[u8; 32]) -> String {
        format!("{}/{}", self.server_root(), hex::encode(ciphertext_sha256))
    }

    /// The destination-safety class of the base URL's host literal. `None`
    /// when it is not an `http`/`https` URL.
    #[must_use]
    pub fn destination(&self) -> Option<Destination> {
        classify_url(&self.base_url)
    }
}

/// The group's `encrypted-media-v2` policy (`0x800b` component), decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedMediaPolicy {
    /// Locator kinds a sender may emit and receivers may fetch.
    pub allowed_locator_kinds: Vec<String>,
    /// Default blob endpoints, in policy order (fallback priority).
    pub endpoints: Vec<BlobEndpoint>,
}

impl EncryptedMediaPolicy {
    /// A `blossom-v1`-only policy over `endpoints` — the shape wn's default
    /// group carries.
    #[must_use]
    pub fn blossom(endpoints: &[String]) -> Self {
        Self {
            allowed_locator_kinds: vec![BLOSSOM_LOCATOR_KIND_V1.to_owned()],
            endpoints: endpoints
                .iter()
                .map(|base_url| BlobEndpoint {
                    locator_kind: BLOSSOM_LOCATOR_KIND_V1.to_owned(),
                    base_url: base_url.clone(),
                })
                .collect(),
        }
    }

    /// Decode the `0x800b` component bytes via the spec codec (its
    /// validation is the spec's).
    ///
    /// # Errors
    /// [`MediaError::InvalidPolicy`] for malformed or off-spec bytes.
    pub fn from_component(bytes: &[u8]) -> Result<Self, MediaError> {
        let decoded =
            decode_encrypted_media_policy_v2(bytes).map_err(|_| MediaError::InvalidPolicy)?;
        Ok(Self {
            allowed_locator_kinds: decoded.allowed_locator_kinds,
            endpoints: decoded
                .default_blob_endpoints
                .iter()
                .map(|endpoint| BlobEndpoint {
                    locator_kind: endpoint.locator_kind.clone(),
                    base_url: endpoint.base_url.clone(),
                })
                .collect(),
        })
    }

    /// Encode this policy as a `0x800b` [`ComponentUpdate`]. The codec's
    /// validation is the spec's — an empty `allowed_locator_kinds` or an
    /// endpoint whose kind is not allowed fails here.
    ///
    /// # Errors
    /// [`MediaError::InvalidPolicy`].
    pub fn to_component(&self) -> Result<ComponentUpdate, MediaError> {
        let policy = EncryptedMediaPolicyV2::new(
            ENCRYPTED_MEDIA_FORMAT_V2.to_owned(),
            self.allowed_locator_kinds.clone(),
            self.endpoints.iter().map(|endpoint| {
                cgka_traits::app_components::BlobStoreEndpointV2 {
                    locator_kind: endpoint.locator_kind.clone(),
                    base_url: endpoint.base_url.clone(),
                }
            }),
        )
        .map_err(|_| MediaError::InvalidPolicy)?;
        let data =
            encode_encrypted_media_policy_v2(&policy).map_err(|_| MediaError::InvalidPolicy)?;
        Ok(ComponentUpdate {
            component_id: GROUP_ENCRYPTED_MEDIA_V2_COMPONENT_ID,
            data,
        })
    }

    /// Whether `kind` is an allowed locator kind.
    #[must_use]
    pub fn allows_locator_kind(&self, kind: &str) -> bool {
        self.allowed_locator_kinds
            .iter()
            .any(|allowed| allowed == kind)
    }
}

/// The destination-safety class of one URL host.
///
/// Spec `implementation-model.md` "Network destination safety". IP literals
/// are classified directly; a domain name cannot be classified until the
/// host resolves it — [`Self::permitted`] returns `None` for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Destination {
    /// A public IP literal.
    PublicAddress,
    /// A DNS name; the host must classify the resolved addresses before
    /// connecting.
    DomainName,
    /// `localhost`/`*.localhost` or a loopback IP literal — permitted only
    /// under [`DestinationPolicy::allow_loopback`].
    Loopback,
    /// A non-public, non-loopback literal (private, link-local, special-use).
    /// Never permitted.
    NonPublicAddress,
}

impl Destination {
    /// Whether this class may be contacted under `policy`. `None` for
    /// [`Self::DomainName`] — the decision waits for resolution
    /// ([`check_resolved_address`]).
    #[must_use]
    pub const fn permitted(self, policy: DestinationPolicy) -> Option<bool> {
        match self {
            Self::PublicAddress => Some(true),
            Self::DomainName => None,
            Self::Loopback => Some(policy.allow_loopback),
            Self::NonPublicAddress => Some(false),
        }
    }
}

/// Classify the host literal of an `http`/`https` URL. `None` when the value
/// is not an http(s) URL or carries no host.
fn classify_url(raw: &str) -> Option<Destination> {
    let url = Url::parse(raw).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    match url.host()? {
        Host::Ipv4(addr) => Some(classify_ip(IpAddr::V4(addr))),
        Host::Ipv6(addr) => Some(classify_ip(IpAddr::V6(addr))),
        Host::Domain(domain) => Some(if is_loopback_host(Host::Domain(domain)) {
            Destination::Loopback
        } else {
            Destination::DomainName
        }),
    }
}

fn classify_ip(addr: IpAddr) -> Destination {
    if is_public_ip(addr) {
        Destination::PublicAddress
    } else if is_loopback_ip(addr) {
        Destination::Loopback
    } else {
        Destination::NonPublicAddress
    }
}

/// A resolved IP address refused by [`check_resolved_address`]. Carries no
/// address.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
#[error("destination refused by local safety policy")]
pub struct DestinationRefused;

/// Post-resolution SSRF check: a host that resolved `addr` may connect only
/// if the address is public, or loopback under `allow_loopback`.
///
/// # Errors
/// [`DestinationRefused`] for every non-public or disallowed address.
pub fn check_resolved_address(
    addr: IpAddr,
    policy: DestinationPolicy,
) -> Result<(), DestinationRefused> {
    reject_non_public_ip(addr, policy.allow_loopback).map_err(|_| DestinationRefused)
}
