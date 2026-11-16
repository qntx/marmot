#![no_main]

use libfuzzer_sys::fuzz_target;
use mk_core::agent_stream::{self, AgentStreamPolicy};

// Agent-stream tag anchors and the `0x8006` policy codec are parsed from
// remote-controlled bytes. Derive tags as newline-separated `name\tvalue`
// pairs (a line without a tab is a bare-name tag); every parser must
// classify or reject, never panic, and the policy codec must round-trip
// what it accepts.
fuzz_target!(|data: &[u8]| {
    if let Some(policy) = AgentStreamPolicy::from_component(data)
        && let Ok(component) = policy.to_component()
    {
        let round_trip = AgentStreamPolicy::from_component(&component.data);
        assert_eq!(round_trip, Some(policy));
    }

    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let tags: Vec<Vec<String>> = text
        .lines()
        .map(|line| line.split('\t').map(str::to_owned).collect())
        .collect();
    let _ = agent_stream::parse_stream_start(&tags);
    let _ = agent_stream::parse_stream_final(&tags);
});
