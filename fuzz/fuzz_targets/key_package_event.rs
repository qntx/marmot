#![no_main]

use libfuzzer_sys::fuzz_target;
use mk_core::Event;
use mk_core::key_package::key_package_info;

fuzz_target!(|data: &str| {
    // KeyPackage publication parsing: arbitrary events must classify or
    // reject, never panic.
    if let Ok(event) = Event::from_json(data) {
        let _ = key_package_info(&event);
    }
});
