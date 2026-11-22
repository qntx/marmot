//! `serde(with = ...)` adapters implementing the wire scalar rules: byte
//! arrays as lowercase hex, `Vec<u8>` as lowercase hex, `u64` as decimal
//! strings. Custom errors are the fixed `"hex"`/`"number"` labels the
//! decode layer maps to `invalid_request` reasons.

use serde::{Deserialize, Deserializer, Serializer};

/// Fixed label emitted for any hex-string violation.
const HEX: &str = "hex";
/// Fixed label emitted for any number violation.
const NUMBER: &str = "number";

/// `[u8; N]` as a lowercase hex string.
pub(crate) mod hex_str {
    use super::{Deserialize, Deserializer, HEX, Serializer};

    /// Serialize bytes as lowercase hex.
    pub(crate) fn serialize<S: Serializer, const N: usize>(
        bytes: &[u8; N],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(bytes))
    }

    /// Decode a lowercase hex string of exactly `N` bytes.
    pub(crate) fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        deserializer: D,
    ) -> Result<[u8; N], D::Error> {
        let text = String::deserialize(deserializer)?;
        let decoded = hex::decode(&text).map_err(|_| serde::de::Error::custom(HEX))?;
        decoded
            .try_into()
            .map_err(|_| serde::de::Error::custom(HEX))
    }
}

/// `Vec<[u8; N]>` as a list of lowercase hex strings.
pub(crate) mod hex_str_seq {
    use super::{Deserialize, Deserializer, HEX, Serializer};

    /// Serialize each byte array as lowercase hex.
    pub(crate) fn serialize<S: Serializer, const N: usize>(
        ids: &[[u8; N]],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(ids.iter().map(hex::encode))
    }

    /// Decode a list of lowercase hex strings of exactly `N` bytes each.
    pub(crate) fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        deserializer: D,
    ) -> Result<Vec<[u8; N]>, D::Error> {
        let texts = Vec::<String>::deserialize(deserializer)?;
        texts
            .into_iter()
            .map(|text| {
                let decoded = hex::decode(&text).map_err(|_| serde::de::Error::custom(HEX))?;
                decoded
                    .try_into()
                    .map_err(|_| serde::de::Error::custom(HEX))
            })
            .collect()
    }
}

/// `Vec<u8>` as a lowercase hex string of any length.
pub(crate) mod hex_bytes {
    use super::{Deserialize, Deserializer, HEX, Serializer};

    /// Serialize bytes as lowercase hex.
    pub(crate) fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(bytes))
    }

    /// Decode a lowercase hex string of any length.
    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(deserializer)?;
        hex::decode(&text).map_err(|_| serde::de::Error::custom(HEX))
    }
}

/// `Vec<Vec<u8>>` as a list of lowercase hex strings of any length.
pub(crate) mod hex_bytes_seq {
    use super::{Deserialize, Deserializer, HEX, Serializer};

    /// Serialize each byte string as lowercase hex.
    pub(crate) fn serialize<S: Serializer>(
        items: &[Vec<u8>],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(items.iter().map(hex::encode))
    }

    /// Decode a list of lowercase hex strings of any length.
    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Vec<u8>>, D::Error> {
        let texts = Vec::<String>::deserialize(deserializer)?;
        texts
            .into_iter()
            .map(|text| hex::decode(&text).map_err(|_| serde::de::Error::custom(HEX)))
            .collect()
    }
}

/// `Option<Vec<u8>>` as a nullable lowercase hex string.
pub(crate) mod opt_hex_bytes {
    use super::{Deserialize, Deserializer, HEX, Serializer};

    /// Serialize `Some` as lowercase hex, `None` as `null`.
    // serde's `with` protocol passes `&Option<T>`; the signature is fixed.
    #[allow(clippy::ref_option, reason = "serde `with` mandates `&Option<T>`")]
    pub(crate) fn serialize<S: Serializer>(
        bytes: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match bytes {
            Some(bytes) => serializer.serialize_str(&hex::encode(bytes)),
            None => serializer.serialize_none(),
        }
    }

    /// Decode `null` to `None` or a hex string of any length to `Some`.
    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        let text = Option::<String>::deserialize(deserializer)?;
        text.map_or_else(
            || Ok(None),
            |text| {
                hex::decode(&text)
                    .map(Some)
                    .map_err(|_| serde::de::Error::custom(HEX))
            },
        )
    }
}

/// `Option<[u8; 32]>` as a nullable lowercase hex string.
pub(crate) mod opt_hex32 {
    use super::{Deserialize, Deserializer, HEX, Serializer};

    /// Serialize `Some` as lowercase hex, `None` as `null`.
    // serde's `with` protocol passes `&Option<T>`; the signature is fixed.
    #[allow(clippy::ref_option, reason = "serde `with` mandates `&Option<T>`")]
    pub(crate) fn serialize<S: Serializer>(
        bytes: &Option<[u8; 32]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match bytes {
            Some(bytes) => serializer.serialize_str(&hex::encode(bytes)),
            None => serializer.serialize_none(),
        }
    }

    /// Decode `null` to `None` or a 32-byte hex string to `Some`.
    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<[u8; 32]>, D::Error> {
        let text = Option::<String>::deserialize(deserializer)?;
        match text {
            Some(text) => {
                let decoded = hex::decode(&text).map_err(|_| serde::de::Error::custom(HEX))?;
                let array: [u8; 32] = decoded
                    .try_into()
                    .map_err(|_| serde::de::Error::custom(HEX))?;
                Ok(Some(array))
            }
            None => Ok(None),
        }
    }
}

/// `u64` as a decimal string.
pub(crate) mod u64_str {
    use super::{Deserialize, Deserializer, NUMBER, Serializer};

    /// Serialize a `u64` as its decimal string.
    // serde's `with` protocol passes `&u64`; the signature is fixed.
    #[allow(
        clippy::trivially_copy_pass_by_ref,
        reason = "serde `with` mandates `&T`"
    )]
    pub(crate) fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    /// Decode a decimal string into a `u64`.
    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse::<u64>()
            .map_err(|_| serde::de::Error::custom(NUMBER))
    }
}

/// `Option<u64>` as a nullable decimal string.
pub(crate) mod opt_u64_str {
    use super::{Deserialize, Deserializer, NUMBER, Serializer};

    /// Serialize `Some` as a decimal string, `None` as `null`.
    // serde's `with` protocol passes `&Option<T>`; the signature is fixed.
    #[allow(clippy::ref_option, reason = "serde `with` mandates `&Option<T>`")]
    pub(crate) fn serialize<S: Serializer>(
        value: &Option<u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => serializer.serialize_str(&value.to_string()),
            None => serializer.serialize_none(),
        }
    }

    /// Decode `null` to `None` or a decimal string to `Some`.
    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u64>, D::Error> {
        let text = Option::<String>::deserialize(deserializer)?;
        text.map_or_else(
            || Ok(None),
            |text| {
                text.parse::<u64>()
                    .map(Some)
                    .map_err(|_| serde::de::Error::custom(NUMBER))
            },
        )
    }
}
