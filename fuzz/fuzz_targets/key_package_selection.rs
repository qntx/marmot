#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use mk_bridge::protocol::UnsignedEvent;
use mk_core::key_package::select_key_packages;
use mk_core::testing::LocalSigner;
use mk_core::{AccountSigner, Event};
use tokio::runtime::Runtime;

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    })
}

fuzz_target!(|data: &[u8]| {
    // The D1 selection over one account's fetched kind-30443 publications
    // and kind-5 deletion requests: arbitrary event sets and clocks must
    // select, never panic. Framing: `{"now": <u64>, "events": [<unsigned
    // events>]}` — every unsigned event is re-signed by one fixed account.
    let Ok(input) = serde_json::from_slice::<serde_json::Value>(data) else {
        return;
    };
    let Some(raw) = input.get("events") else {
        return;
    };
    let Ok(unsigned) = serde_json::from_value::<Vec<UnsignedEvent>>(raw.clone()) else {
        return;
    };
    let now = input
        .get("now")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let signer = LocalSigner::from_secret([0x42; 32]).expect("fixed secret");
    let events: Vec<Event> = unsigned
        .into_iter()
        .filter_map(|unsigned| {
            runtime()
                .block_on(signer.sign_event(unsigned.into()))
                .ok()
        })
        .collect();
    let _ = select_key_packages(signer.account(), &events, now);
});
