#![no_main]

use libfuzzer_sys::fuzz_target;
use mk_bridge::protocol::{Config, OpenResponse, PROTOCOL_VERSION, Time};
use mk_bridge::{Bridge, Host, HostError};
use mk_core::testing::LocalSigner;

/// In-memory host whose `load` serves one fixed byte string — the fuzz
/// input exercises the durable-rows decoder through it.
struct BytesHost(Vec<u8>);

impl Host for BytesHost {
    fn load(&mut self) -> Result<Vec<u8>, HostError> {
        Ok(self.0.clone())
    }

    fn commit(&mut self, _batch: &[u8]) -> Result<(), HostError> {
        Ok(())
    }
}

fuzz_target!(|data: &[u8]| {
    // The input as an open config.
    let mut bridge = Bridge::new(BytesHost(Vec::new()));
    let response = bridge.open(data);
    let decoded: OpenResponse =
        serde_json::from_slice(&response).expect("open response must decode");
    let _ = decoded;

    // The input as durable rows returned by `Host::load` under a valid
    // config: exercises the rows codec on every open.
    let signer = LocalSigner::from_secret([0x42; 32]).expect("fixed secret");
    let config = serde_json::to_vec(&Config {
        protocol_version: PROTOCOL_VERSION,
        account: signer.account().0,
        now: Time {
            unix_ms: 1_700_000_000_000,
            monotonic_ms: 0,
        },
        allow_loopback: true,
        client_name: None,
    })
    .expect("config encodes");
    let mut bridge = Bridge::new(BytesHost(data.to_vec()));
    let response = bridge.open(&config);
    let decoded: OpenResponse =
        serde_json::from_slice(&response).expect("open response must decode");
    let _ = decoded;
});
