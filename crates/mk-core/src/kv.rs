//! The host KV port: the smallest transactional boundary a host can implement
//! on `IndexedDB`, MMKV, or an embedded database.
//!
//! The host owns persistence. At open it loads every durable row and hands the
//! full set to the store; after that the store is only ever written to the
//! host, one [`KvBatch`] per committed unit (a bare write outside any
//! transaction, or the outermost `with_transaction`/`with_group_savepoint`
//! scope). [`KvBatch::seq`] is a session-scoped counter starting at 1 and
//! increasing by exactly 1 per batch.
//!
//! Host contract:
//!
//! - **Atomicity.** Apply all ops of a batch or none. Never apply a batch
//!   partially.
//! - **Ordering.** Apply batches strictly in `seq` order. The store calls
//!   `commit` while holding its write lock, so batches always arrive in order;
//!   a host must not reorder or skip them. A host may coalesce consecutive
//!   batches into one durable write.
//! - **Acknowledgement.** The session-side durability gate (a later milestone)
//!   lets a host echo seqs back as they become durable. Acks are cumulative:
//!   acknowledging `n` means every batch `<= n` is durable. A host must never
//!   acknowledge a batch it has not made durable — `commit` returning means the
//!   batch was *accepted*, not necessarily persisted; an asynchronous host
//!   (`IndexedDB`) must hold a synchronous WAL or equivalent acknowledgement
//!   path, or the layer above must treat the durable frontier conservatively.
//! - **No mutation.** The host never writes rows on its own; the row set it
//!   loads must be exactly the committed history of the store that produced
//!   it.
//! - **No reentrancy.** `commit` must not call back into the store or session.
//! - **Failure.** If a batch cannot be made durable the session is over: drop
//!   it and reopen from what is durable. There is no resend — a later batch's
//!   state may depend on the lost one.

use std::fmt;

/// Session-scoped commit sequence number attached to every [`KvBatch`].
///
/// Starts at 1 for each opened store and increases by exactly 1 per committed
/// batch. It is not persisted; durability is established per session by the
/// ack protocol described in the module docs.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitSeq(u64);

impl CommitSeq {
    /// The raw sequence number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Wrap a raw counter; only the store constructs seqs.
    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

impl fmt::Display for CommitSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Debug for CommitSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CommitSeq({})", self.0)
    }
}

/// One row mutation inside a [`KvBatch`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvOp {
    /// Insert or replace `key` with `value`.
    Put {
        /// Row key produced by the store's key schema.
        key: Vec<u8>,
        /// Serialized row value.
        value: Vec<u8>,
    },
    /// Remove `key`. Deleting an absent key is a no-op for the host.
    Delete {
        /// Row key produced by the store's key schema.
        key: Vec<u8>,
    },
}

/// The atomic unit of durable state: one committed unit's mutations plus its
/// session sequence number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvBatch {
    /// Monotonic sequence number, `+1` per batch.
    pub seq: CommitSeq,
    /// The mutations, in application order.
    pub ops: Vec<KvOp>,
}

/// Current batch wire format version.
const BATCH_CODEC_VERSION: u8 = 1;

impl KvBatch {
    /// Encode this batch in the versioned binary wire format.
    ///
    /// Layout: `[version u8 = 1][seq u64 BE][op count u32 BE]` then per op
    /// `[tag u8][key len u32 BE][key][value len u32 BE][value]` where `tag` is
    /// 0 for put and 1 for delete (delete ops carry no value length/value).
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(BATCH_CODEC_VERSION);
        out.extend_from_slice(&self.seq.0.to_be_bytes());
        out.extend_from_slice(
            &u32::try_from(self.ops.len())
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        for op in &self.ops {
            match op {
                KvOp::Put { key, value } => {
                    out.push(0);
                    out.extend_from_slice(
                        &u32::try_from(key.len()).unwrap_or(u32::MAX).to_be_bytes(),
                    );
                    out.extend_from_slice(key);
                    out.extend_from_slice(
                        &u32::try_from(value.len()).unwrap_or(u32::MAX).to_be_bytes(),
                    );
                    out.extend_from_slice(value);
                }
                KvOp::Delete { key } => {
                    out.push(1);
                    out.extend_from_slice(
                        &u32::try_from(key.len()).unwrap_or(u32::MAX).to_be_bytes(),
                    );
                    out.extend_from_slice(key);
                }
            }
        }
        out
    }

    /// Decode a batch produced by [`KvBatch::encode`].
    ///
    /// Rejects anything but version 1, truncated input, trailing bytes,
    /// unknown op tags, declared lengths that overflow the remaining input,
    /// and batches with zero ops (the store never emits one, so accepting one
    /// would only hide a corrupt stream).
    ///
    /// # Errors
    ///
    /// [`BatchCodecError`] describing the rejection class; the error never
    /// echoes input bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, BatchCodecError> {
        let mut cursor = Cursor { bytes, pos: 0 };
        let version = cursor.u8()?;
        if version != BATCH_CODEC_VERSION {
            return Err(BatchCodecError::UnsupportedVersion { found: version });
        }
        let seq = CommitSeq(cursor.u64()?);
        let op_count = cursor.u32()?;
        if op_count == 0 {
            return Err(BatchCodecError::Empty);
        }
        let mut ops = Vec::with_capacity(op_count as usize);
        for _ in 0..op_count {
            let tag = cursor.u8()?;
            let key_len = cursor.u32()? as usize;
            let key = cursor.take(key_len)?.to_vec();
            let op = match tag {
                0 => {
                    let value_len = cursor.u32()? as usize;
                    let value = cursor.take(value_len)?.to_vec();
                    KvOp::Put { key, value }
                }
                1 => KvOp::Delete { key },
                _ => return Err(BatchCodecError::UnknownOpTag { tag }),
            };
            ops.push(op);
        }
        if cursor.pos != bytes.len() {
            return Err(BatchCodecError::TrailingBytes);
        }
        Ok(Self { seq, ops })
    }
}

/// What a host implements to persist committed units.
///
/// See the module-level contract. `commit` runs while the store's write lock
/// is held — that is what guarantees batches arrive strictly in `seq` order —
/// so implementations must be quick, must not block on unrelated work, and
/// must never call back into the store or session.
pub trait KvHost: Send + Sync + fmt::Debug {
    /// Accept one committed batch. Returning means the batch was accepted for
    /// persistence per the host's durability contract; it does not by itself
    /// prove the bytes reached durable storage (see module docs).
    fn commit(&self, batch: KvBatch);
}

/// Failure decoding a [`KvBatch`]. Payload bytes are never included.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BatchCodecError {
    /// Truncated input: a declared field extends past the end.
    #[error("truncated kv batch")]
    Truncated,
    /// Extra bytes follow the last declared op.
    #[error("trailing bytes after kv batch")]
    TrailingBytes,
    /// A version byte this decoder does not understand.
    #[error("unsupported kv batch version {found}")]
    UnsupportedVersion {
        /// The version byte found.
        found: u8,
    },
    /// The op count declared zero operations.
    #[error("kv batch with no ops")]
    Empty,
    /// An op tag other than put/delete.
    #[error("unknown kv batch op tag {tag}")]
    UnknownOpTag {
        /// The tag byte found.
        tag: u8,
    },
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], BatchCodecError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(BatchCodecError::Truncated)?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(BatchCodecError::Truncated)?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, BatchCodecError> {
        let byte = *self.take(1)?.first().ok_or(BatchCodecError::Truncated)?;
        Ok(byte)
    }

    fn u32(&mut self) -> Result<u32, BatchCodecError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| BatchCodecError::Truncated)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, BatchCodecError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| BatchCodecError::Truncated)?;
        Ok(u64::from_be_bytes(bytes))
    }
}
