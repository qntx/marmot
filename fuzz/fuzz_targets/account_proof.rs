#![no_main]

use libfuzzer_sys::fuzz_target;
use mk_core::Event;
use mk_core::testing::ProofRequest;

fuzz_target!(|data: &str| {
    // The account-identity-proof validation `provide_account_proof` runs:
    // arbitrary signed events against one fixed request must classify or
    // reject, never panic.
    let request = ProofRequest::fixed();
    if let Ok(event) = Event::from_json(data) {
        let _ = request.check(&event);
    }
});
