//! `mk-wasm` — the wasm-bindgen shell over [`mk_bridge`], packaged for
//! JavaScript hosts as `@qntx/marmot-wasm`.
//!
//! One exported class, [`MarmotBridge`], mirrors the bridge contract
//! exactly: `open`/`call`/`drain` are bytes-in/bytes-out over the same JSON
//! envelopes, and `close` releases the session. The constructor takes the
//! host's `load`/`commit` functions; a throw out of either is a
//! [`HostError`], which poisons the bridge by contract.
//!
//! A panic hook installed at module init flips a static flag: after a trap
//! escapes to JavaScript every method on every instance answers the encoded
//! `{"type":"error","error":{"kind":"poisoned"}}` envelope instead of
//! running again. The crate feature `test-panic` adds a `triggerPanic`
//! export used to exercise that path; it is never enabled in shipped
//! builds and the size check asserts its absence.
//!
//! On `wasm32-unknown-unknown` the nostr fork's `universal-time` facade
//! needs a time provider defined in the final artifact; `WebTime` supplies
//! it over `web-time`. It is the only ambient clock read in the
//! artifact — `mk-bridge` itself takes time through the host.
#![allow(unsafe_code, reason = "wasm-bindgen emits unsafe extern glue")]

#[cfg(target_family = "wasm")]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(not(target_family = "wasm"))]
use universal_time as _;
#[cfg(not(target_family = "wasm"))]
use web_time as _;

use js_sys::{Function, Uint8Array};
use mk_bridge::{Bridge, BridgeError, Host, HostError, encode_error};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

/// Set by the panic hook; after a trap every entry point answers `poisoned`.
static PANICKED: AtomicBool = AtomicBool::new(false);

/// Install the panic hook once, at module init, before any `MarmotBridge`
/// exists. `panic = "abort"` still runs the hook before the trap escapes.
#[wasm_bindgen(start)]
pub fn install_panic_hook() {
    std::panic::set_hook(Box::new(|_| {
        PANICKED.store(true, Ordering::SeqCst);
    }));
}

/// `web-time`-backed provider for the `universal-time` facade the nostr
/// fork calls on wasm32 (`Timestamp::now`, `Instant::now`). The facade's
/// API only exists on wasm targets, so the whole block is cfg'd out on
/// hosts to keep workspace builds compiling.
#[cfg(target_family = "wasm")]
#[derive(Debug)]
struct WebTime;

#[cfg(target_family = "wasm")]
impl universal_time::WallClock for WebTime {
    fn system_time(&self) -> universal_time::SystemTime {
        let since_epoch = web_time::SystemTime::now()
            .duration_since(web_time::UNIX_EPOCH)
            .unwrap_or_default();
        universal_time::SystemTime::from_unix_duration(since_epoch)
    }
}

#[cfg(target_family = "wasm")]
impl universal_time::MonotonicClock for WebTime {
    fn instant(&self) -> universal_time::Instant {
        // `universal_time::Instant` ticks are opaque; anchor them at first
        // use so `performance.now()` deltas stay monotonic.
        static BOOT: OnceLock<web_time::Instant> = OnceLock::new();
        let ticks =
            web_time::Instant::now().duration_since(*BOOT.get_or_init(web_time::Instant::now));
        universal_time::Instant::from_ticks(ticks)
    }
}

#[cfg(target_family = "wasm")]
universal_time::define_time_provider!(WebTime);

/// `Host` over two injected JavaScript functions: `load() -> Uint8Array`
/// and `commit(batch: Uint8Array)`. A throw, or a `load` answer that is not
/// a `Uint8Array`, maps to [`HostError`] — poisoning the bridge.
#[derive(Debug)]
struct JsHost {
    load: Function,
    commit: Function,
}

impl Host for JsHost {
    fn load(&mut self) -> Result<Vec<u8>, HostError> {
        let value = self
            .load
            .call0(&JsValue::NULL)
            .map_err(|_| HostError::new())?;
        let array = value.dyn_ref::<Uint8Array>().ok_or_else(HostError::new)?;
        Ok(array.to_vec())
    }

    fn commit(&mut self, batch: &[u8]) -> Result<(), HostError> {
        let array = Uint8Array::from(batch);
        self.commit
            .call1(&JsValue::NULL, &array)
            .map(|_| ())
            .map_err(|_| HostError::new())
    }
}

/// One bridge session: bytes in, bytes out, exactly the `mk-bridge`
/// envelopes. Construct it with the host's `load`/`commit` functions.
#[derive(Debug)]
#[wasm_bindgen]
pub struct MarmotBridge {
    inner: Bridge<JsHost>,
}

#[wasm_bindgen]
impl MarmotBridge {
    /// Create a bridge over the host `load`/`commit` functions.
    #[must_use]
    #[wasm_bindgen(constructor)]
    pub fn new(load: Function, commit: Function) -> Self {
        Self {
            inner: Bridge::new(JsHost { load, commit }),
        }
    }

    /// Open the session; returns the encoded `OpenResponse` (an `error`
    /// envelope once the module has trapped).
    #[must_use]
    pub fn open(&mut self, config: &[u8]) -> Vec<u8> {
        if PANICKED.load(Ordering::SeqCst) {
            return encode_error(&BridgeError::Poisoned);
        }
        self.inner.open(config)
    }

    /// Dispatch one call envelope; returns the encoded `Response`.
    #[must_use]
    pub fn call(&mut self, request: &[u8]) -> Vec<u8> {
        if PANICKED.load(Ordering::SeqCst) {
            return encode_error(&BridgeError::Poisoned);
        }
        self.inner.call(request)
    }

    /// Drain queued session events; returns the encoded `DrainResponse`.
    #[must_use]
    pub fn drain(&mut self) -> Vec<u8> {
        if PANICKED.load(Ordering::SeqCst) {
            return encode_error(&BridgeError::Poisoned);
        }
        self.inner.drain()
    }

    /// Release the session. A no-op once the module has trapped.
    pub fn close(&mut self) {
        if PANICKED.load(Ordering::SeqCst) {
            return;
        }
        self.inner.close();
    }
}

/// Feature `test-panic` only: panic inside a wasm call so tests can prove
/// the module answers `poisoned` afterwards. Never shipped.
///
/// # Panics
/// Always — that is the point of the export.
#[cfg(feature = "test-panic")]
#[allow(clippy::panic, reason = "the export exists to panic")]
#[wasm_bindgen(js_name = triggerPanic)]
pub fn trigger_panic() {
    panic!("mk-wasm test-panic export");
}
