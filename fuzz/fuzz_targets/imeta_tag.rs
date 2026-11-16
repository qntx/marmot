#![no_main]

use libfuzzer_sys::fuzz_target;
use mk_core::media::parse_imeta;

// The `imeta` parser sees fully attacker-controlled strings: every field is
// text a remote member wrote. Derive the tag as NUL-separated fields; the
// parser must classify or reject, never panic, and accept its own output.
fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let tag: Vec<String> = std::iter::once("imeta".to_owned())
        .chain(text.split('\0').map(str::to_owned))
        .collect();
    let Ok(reference) = parse_imeta(&tag) else {
        return;
    };
    // Round-trip: a reference that parsed must re-serialize into a tag that
    // parses to the same reference (exact inverse).
    let round_trip = parse_imeta(&reference.to_imeta());
    assert_eq!(round_trip.as_ref(), Ok(&reference));
});
