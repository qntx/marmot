//! The durable-rows codec: the byte format [`crate::Host::load`] returns and
//! hosts persist.
//!
//! Layout: `[version u8 = 1][count u32 BE]` then per row
//! `[key len u32 BE][key][value len u32 BE][value]`. The decoder is strict —
//! truncation, trailing bytes and unknown versions are typed errors, and no
//! error echoes payload bytes.

/// Current rows wire format version.
const ROWS_VERSION: u8 = 1;

/// Every row needs at least two length prefixes.
const ROW_HEADER_LEN: usize = 8;

/// Why [`decode_rows`] refused a byte string. Never carries row bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RowsError {
    /// A declared field extends past the end of the input.
    #[error("truncated rows")]
    Truncated,
    /// Extra bytes follow the last declared row.
    #[error("trailing bytes after rows")]
    TrailingBytes,
    /// A version byte this decoder does not understand.
    #[error("unsupported rows version {found}")]
    UnsupportedVersion {
        /// The version byte found.
        found: u8,
    },
}

/// Encode `rows` in the versioned binary format [`crate::Host::load`]
/// returns.
#[must_use]
pub fn encode_rows(rows: &[Row]) -> Vec<u8> {
    let count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
    let mut out = Vec::new();
    out.push(ROWS_VERSION);
    out.extend_from_slice(&count.to_be_bytes());
    for (key, value) in rows {
        out.extend_from_slice(&u32::try_from(key.len()).unwrap_or(u32::MAX).to_be_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&u32::try_from(value.len()).unwrap_or(u32::MAX).to_be_bytes());
        out.extend_from_slice(value);
    }
    out
}

/// One decoded key/value row.
pub type Row = (Vec<u8>, Vec<u8>);

/// Decode `bytes` produced by [`encode_rows`].
///
/// # Errors
/// [`RowsError`] for truncation, trailing bytes or an unknown version byte.
/// Pre-allocation is bounded by the remaining input length, never by the
/// declared count.
pub fn decode_rows(bytes: &[u8]) -> Result<Vec<Row>, RowsError> {
    let mut cursor = Cursor { bytes, pos: 0 };
    let version = cursor.take(1)?;
    if version.first().copied() != Some(ROWS_VERSION) {
        return Err(RowsError::UnsupportedVersion {
            found: version.first().copied().unwrap_or(0),
        });
    }
    let count = cursor.take_u32()?;
    let mut rows = Vec::with_capacity(usize::min(
        usize::try_from(count).unwrap_or(usize::MAX),
        bytes.len() / ROW_HEADER_LEN,
    ));
    for _ in 0..count {
        let key_len = usize::try_from(cursor.take_u32()?).unwrap_or(usize::MAX);
        let key = cursor.take(key_len)?.to_vec();
        let value_len = usize::try_from(cursor.take_u32()?).unwrap_or(usize::MAX);
        let value = cursor.take(value_len)?.to_vec();
        rows.push((key, value));
    }
    if cursor.pos != bytes.len() {
        return Err(RowsError::TrailingBytes);
    }
    Ok(rows)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8], RowsError> {
        let end = self.pos.checked_add(len).ok_or(RowsError::Truncated)?;
        let slice = self.bytes.get(self.pos..end).ok_or(RowsError::Truncated)?;
        self.pos = end;
        Ok(slice)
    }

    fn take_u32(&mut self) -> Result<u32, RowsError> {
        let raw: [u8; 4] = self.take(4)?.try_into().map_err(|_| RowsError::Truncated)?;
        Ok(u32::from_be_bytes(raw))
    }
}
