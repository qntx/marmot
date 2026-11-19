#pragma once

#include "HybridMarmotNativeSpec.hpp"

namespace margelo::nitro::marmotnative {

class HybridMarmotNative final : public HybridMarmotNativeSpec {
public:
  // HybridObject is a virtual base, so the most-derived class must pass TAG itself.
  HybridMarmotNative() : HybridObject(TAG) {}

  std::shared_ptr<HybridMarmotBridgeSpec>
  create(const std::shared_ptr<ArrayBuffer>& rows,
         const std::function<void(const std::shared_ptr<ArrayBuffer>& batch)>& commit) override;
};

} // namespace margelo::nitro::marmotnative
