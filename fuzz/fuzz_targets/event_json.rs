#![no_main]

use libfuzzer_sys::fuzz_target;
use mk_core::Event;

fuzz_target!(|data: &str| {
    // The NIP-01 event JSON decoder we own: on success the canonical
    // re-encode must re-parse to an equal event, and `verify` must not panic.
    if let Ok(event) = Event::from_json(data) {
        let wire = event.to_json();
        let reparsed = Event::from_json(&wire);
        assert!(reparsed.is_ok(), "re-encoded JSON must re-parse");
        assert_eq!(reparsed.ok().as_ref(), Some(&event), "re-parse must equal");
        let _ = event.verify();
    }
});
