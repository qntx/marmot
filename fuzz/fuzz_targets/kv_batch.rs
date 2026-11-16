#![no_main]

use libfuzzer_sys::fuzz_target;
use mk_core::KvBatch;

fuzz_target!(|data: &[u8]| {
    // The durable batch codec: decode must never panic, and a successful
    // decode must re-encode to the exact input bytes (canonical encoding).
    if let Ok(batch) = KvBatch::decode(data) {
        assert_eq!(
            batch.encode(),
            data,
            "KvBatch re-encode must be identity on success"
        );
    }
});
