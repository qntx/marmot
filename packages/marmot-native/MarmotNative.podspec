require "json"

package = JSON.parse(File.read(File.join(__dir__, "package.json")))

Pod::Spec.new do |s|
  s.name         = "MarmotNative"
  s.version      = package["version"]
  s.summary      = package["description"]
  s.homepage     = package["homepage"] || package.dig("repository", "url") || "https://github.com/qntx/marmot"
  s.license      = package["license"]
  s.authors      = package["author"]

  s.platforms    = { :ios => min_ios_version_supported }
  s.source       = { :git => "https://github.com/qntx/marmot.git", :tag => "v#{s.version}" }

  s.source_files = [
    # C++ HybridObject implementation plus the marmot_ffi C header (the symbols
    # themselves come from the vendored xcframework below).
    "cpp/**/*.{h,hpp,c,cpp}",
    "include/**/*.h",
  ]

  # Prebuilt mk-ffi cdylib (arm64 device + arm64 simulator), built by
  # `bun run build:native` at the repository root.
  s.public_header_files = "include/marmot_ffi.h"

  s.vendored_frameworks = "ios/MarmotFfi.xcframework"

  load 'nitrogen/generated/ios/MarmotNative+autolinking.rb'
  add_nitrogen_files(s)

  s.dependency 'React-jsi'
  s.dependency 'React-callinvoker'
  install_modules_dependencies(s)
end
