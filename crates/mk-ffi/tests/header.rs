//! `include/marmot_ffi.h` is generated and committed: regenerating it
//! with `cbindgen.toml` must reproduce it byte for byte.

#![allow(
    clippy::tests_outside_test_module,
    unused_crate_dependencies,
    reason = "every integration-test binary links the full dev-dependency set"
)]

use std::path::Path;

/// Regenerating the header from `cbindgen.toml` reproduces the
/// committed file exactly; the shipped surface stays reviewed.
#[test]
#[cfg_attr(miri, ignore = "cbindgen reads source files; Miri isolation blocks fs")]
fn committed_header_matches_cbindgen_output() {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let config =
        cbindgen::Config::from_file(crate_dir.join("cbindgen.toml")).expect("parse cbindgen.toml");
    let bindings = cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(config)
        .generate()
        .expect("generate bindings");
    let mut generated = Vec::new();
    bindings.write(&mut generated);
    let committed = std::fs::read(crate_dir.join("include/marmot_ffi.h"))
        .expect("read committed header — regenerate per cbindgen.toml");
    assert_eq!(
        generated, committed,
        "include/marmot_ffi.h is stale — regenerate per cbindgen.toml"
    );
}
