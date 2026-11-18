import type { HybridObject } from "react-native-nitro-modules";

/**
 * One mk-ffi bridge behind the C ABI: `open`/`call`/`drain` map 1:1 onto `mk_bridge_*` and run on
 * Nitro's shared thread pool, so the blocking engine-thread calls never park the JS thread. The
 * engine thread delivers every `commit` before the ABI call that produced it returns, so JS sees
 * commits in seq order ahead of each call's promise.
 */
export interface MarmotBridge extends HybridObject<{ ios: "c++"; android: "c++" }> {
  open(config: ArrayBuffer): Promise<ArrayBuffer>;
  call(request: ArrayBuffer): Promise<ArrayBuffer>;
  drain(): Promise<ArrayBuffer>;
  close(): Promise<void>;
}

/**
 * The autolinked entry point: `create` takes the durable `rows` snapshot up front (the mk-ffi host
 * `load` serves it once, inside the first `open`) and the `commit` notification the engine thread
 * fires per committed batch.
 */
export interface MarmotNative extends HybridObject<{ ios: "c++"; android: "c++" }> {
  create(rows: ArrayBuffer, commit: (batch: ArrayBuffer) => void): MarmotBridge;
}
