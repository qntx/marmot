//! Typed open-time errors for the KV store.

use cgka_traits::storage::StorageError;
use thiserror::Error;

/// Failure to open a [`crate::KvStorageProvider`] over a set of host rows.
///
/// Carries no row contents: the only payload is the integer schema version the
/// rows advertise.
#[derive(Debug, Error)]
#[non_exhaustive]
#[allow(
    variant_size_differences,
    reason = "error payloads are small; boxing them would obscure the API"
)]
pub enum OpenError {
    /// The rows were written by a newer (or older, unsupported) schema version
    /// than this build understands. There is no migration path: refuse to
    /// open rather than guess at foreign rows.
    #[error("unsupported kv schema version {found}")]
    UnsupportedSchemaVersion {
        /// The version found in the rows.
        found: u32,
    },
    /// Rows exist but carry no schema-version row, so their format cannot be
    /// established.
    #[error("kv rows present without a schema-version row")]
    MissingSchemaVersion,
    /// A meta row that must decode cleanly did not.
    #[error("malformed kv meta row")]
    MalformedMeta {
        /// Which meta row failed to decode.
        row: MetaRow,
    },
    /// The store lock is poisoned by a panic while held.
    #[error("kv store lock poisoned")]
    LockPoisoned,
}

/// Which [`crate::keys::Table::Meta`] row a [`OpenError::MalformedMeta`]
/// names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetaRow {
    /// The schema-version row.
    SchemaVersion,
    /// The monotonic sequence counter row.
    Sequence,
    /// The pending-reference high-water row.
    PendingHighWater,
}

impl From<OpenError> for StorageError {
    fn from(error: OpenError) -> Self {
        Self::Backend(error.to_string())
    }
}
