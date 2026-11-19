#include "HybridMarmotNative.hpp"
#include "HybridMarmotBridge.hpp"

#include <stdexcept>

namespace margelo::nitro::marmotnative {

namespace {

HostContext& unbox(void* rawCtx) {
  return **static_cast<std::shared_ptr<HostContext>*>(rawCtx);
}

// `load` (engine thread): the bridge copies the blob synchronously; it must stay valid only
// until the mk_bridge_open that triggered it returns — `open` clears it right after.
int32_t hostLoad(void* rawCtx, const uint8_t** ptr, size_t* len) noexcept {
  try {
    const auto& rows = unbox(rawCtx).rows;
    *ptr = rows.data();
    *len = rows.size();
    return 0;
  } catch (...) {
    return 1;
  }
}

// `commit` (engine thread): one batch as a notification, strictly in seq order and before the
// ABI call that produced it returns. The JS function arrives as an AsyncJSCallback: invoking it
// posts through the runtime's CallInvokerDispatcher (`runAsync`, FIFO) — the same queue that
// delivers Promise resolutions — so commits reach JS in seq order and before the call's
// promise resolves. Every failure (copy, callback) returns nonzero and poisons the bridge.
int32_t hostCommit(void* rawCtx, const uint8_t* ptr, size_t len) noexcept {
  try {
    unbox(rawCtx).commit(ArrayBuffer::copy(ptr, len));
    return 0;
  } catch (...) {
    return 1;
  }
}

// Runs exactly once on the engine thread after the bridge is dropped (also on a detached
// free): drop the vtable's shared_ptr. The HybridMarmotBridge member keeps the context alive
// until the hybrid itself is gone.
void hostRelease(void* rawCtx) noexcept {
  delete static_cast<std::shared_ptr<HostContext>*>(rawCtx);
}

} // namespace

std::shared_ptr<HybridMarmotBridgeSpec>
HybridMarmotNative::create(const std::shared_ptr<ArrayBuffer>& rows,
                           const std::function<void(const std::shared_ptr<ArrayBuffer>& batch)>& commit) {
  // Runs on the JS thread: copy the snapshot now — a JS-backed ArrayBuffer must never be
  // dereferenced from the engine thread.
  auto ctx = std::make_shared<HostContext>();
  if (rows->size() > 0) {
    ctx->rows.assign(rows->data(), rows->data() + rows->size());
  }
  ctx->commit = commit;

  auto* box = new std::shared_ptr<HostContext>(ctx);
  const MkHostVtable vtable{
      .load = hostLoad,
      .commit = hostCommit,
      .release = hostRelease,
  };
  MkBridge* handle = mk_bridge_new(&vtable, box);
  if (handle == nullptr) {
    // `release` never runs when mk_bridge_new fails — free the box here.
    delete box;
    throw std::runtime_error("mk_bridge_new failed");
  }
  return std::make_shared<HybridMarmotBridge>(handle, std::move(ctx));
}

} // namespace margelo::nitro::marmotnative
