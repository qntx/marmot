#![no_main]

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use libfuzzer_sys::fuzz_target;
use mk_core::testing::{LocalSigner, ManualClock, RecordingHost};
use mk_core::{DestinationPolicy, Opened, Session, SessionConfig};

/// Poll an async read path with a bounded spin: resolves immediately for
/// the synchronous `LocalSigner`; gives up rather than hanging a run.
fn bounded_block_on<F: Future>(future: F) -> Option<F::Output> {
    struct Nop;
    impl Wake for Nop {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Waker::from(Arc::new(Nop));
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    for _ in 0..10_000 {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return Some(value);
        }
        std::thread::yield_now();
    }
    None
}

// Arbitrary durable rows -> Session::open -> every read path that decodes
// session-owned rows (group snapshots, outbox, pacing). Errors are fine;
// panics are not.
fuzz_target!(|data: &[u8]| {
    let Ok(rows) = mk_bridge::decode_rows(data) else {
        return;
    };
    let signer: Arc<dyn mk_core::AccountSigner> = Arc::new(LocalSigner::new());
    let clock: Arc<dyn mk_core::Clock> = Arc::new(ManualClock::new());
    let host: Arc<dyn mk_core::KvHost> = Arc::new(RecordingHost::new());
    let opened = Session::open(SessionConfig {
        signer,
        host,
        rows,
        clock,
        destination_policy: DestinationPolicy {
            allow_loopback: true,
        },
        client_name: None,
    });
    let Ok(Opened::Ready(session)) = opened else {
        return;
    };
    let mut session = *session;
    if let Ok(groups) = session.groups() {
        for group in &groups {
            let _ = session.group(&group.id);
        }
    }
    let _ = session.take_outbound();
    let _ = bounded_block_on(session.tick());
});
