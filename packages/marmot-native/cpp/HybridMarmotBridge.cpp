#include "HybridMarmotBridge.hpp"

#include <stdexcept>

namespace margelo::nitro::marmotnative {

namespace {

using InputCall = void (*)(MkBridge*, const uint8_t*, size_t, MkBytes*);

// One blocking mk_bridge_* call on a Nitro pool thread. The `MkBytes` answer is Rust-owned:
// wrap it without a copy and let the ArrayBuffer's deleter hand it back to mk_bytes_free
// exactly once.
std::shared_ptr<ArrayBuffer> runCall(MkBridge* handle, InputCall call, const std::shared_ptr<ArrayBuffer>& input) {
  MkBytes out{};
  call(handle, input->data(), input->size(), &out);
  return ArrayBuffer::wrap(out.ptr, out.len, [out]() { mk_bytes_free(out); });
}

std::shared_ptr<ArrayBuffer> runDrain(MkBridge* handle) {
  MkBytes out{};
  mk_bridge_drain(handle, &out);
  return ArrayBuffer::wrap(out.ptr, out.len, [out]() { mk_bytes_free(out); });
}

} // namespace

std::shared_ptr<Promise<std::shared_ptr<ArrayBuffer>>> HybridMarmotBridge::open(const std::shared_ptr<ArrayBuffer>& config) {
  // Open is once-per-bridge: a second call's `load` would race the first task's `rows` clear
  // on the engine thread (or read a cleared snapshot). Reject before going async so it can
  // never reach mk_bridge_open.
  if (_opened.exchange(true)) {
    return Promise<std::shared_ptr<ArrayBuffer>>::rejected(
        std::make_exception_ptr(std::runtime_error("marmot: open may run once per bridge")));
  }
  // JS ArrayBuffers may only be read on the JS thread — copy the input before going async.
  // The async task captures `self` so the bridge (and the mk-ffi handle) outlives the
  // in-flight call even if JS drops its last reference.
  auto input = ArrayBuffer::copy(config);
  auto self = shared_cast<HybridMarmotBridge>();
  return Promise<std::shared_ptr<ArrayBuffer>>::async([self, input = std::move(input)]() {
    auto result = runCall(self->_handle, mk_bridge_open, input);
    // `load` is done once the mk_bridge_open that triggered it returns (the ABI contract) —
    // drop the snapshot; it can be tens of MB.
    self->_ctx->rows.clear();
    self->_ctx->rows.shrink_to_fit();
    return result;
  });
}

std::shared_ptr<Promise<std::shared_ptr<ArrayBuffer>>> HybridMarmotBridge::call(const std::shared_ptr<ArrayBuffer>& request) {
  auto input = ArrayBuffer::copy(request);
  auto self = shared_cast<HybridMarmotBridge>();
  return Promise<std::shared_ptr<ArrayBuffer>>::async(
      [self, input = std::move(input)]() { return runCall(self->_handle, mk_bridge_call, input); });
}

std::shared_ptr<Promise<std::shared_ptr<ArrayBuffer>>> HybridMarmotBridge::drain() {
  auto self = shared_cast<HybridMarmotBridge>();
  return Promise<std::shared_ptr<ArrayBuffer>>::async([self]() { return runDrain(self->_handle); });
}

std::shared_ptr<Promise<void>> HybridMarmotBridge::close() {
  auto self = shared_cast<HybridMarmotBridge>();
  // mk_bridge_close blocks until the engine thread ran Bridge::close and exited — that is
  // exactly why it runs on the pool, not the JS thread.
  return Promise<void>::async([self]() { mk_bridge_close(self->_handle); });
}

} // namespace margelo::nitro::marmotnative
