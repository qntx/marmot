# mk-core

Marmot kit core: a pure-Rust persistent key-value storage provider for the MDK
engine, plus the session API built on it.

The crate exposes `KvStorageProvider`, an implementation of the MDK
`cgka_traits::storage::StorageProvider` aggregate and the OpenMLS storage
trait whose observable behavior matches MDK's `storage-sqlite`. The host
persists an ordered sequence of atomic `KvBatch` commits through the `KvHost`
trait; reopening replays the durable rows.

See the crate-level rustdoc for the host contract, the key schema, and the
memory-growth inventory.
