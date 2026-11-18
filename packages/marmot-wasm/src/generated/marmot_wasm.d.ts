/* tslint:disable */
/* eslint-disable */

/**
 * One bridge session: bytes in, bytes out, exactly the `mk-bridge`
 * envelopes. Construct it with the host's `load`/`commit` functions.
 */
export class MarmotBridge {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Dispatch one call envelope; returns the encoded `Response`.
     */
    call(request: Uint8Array | number[]): Uint8Array;
    /**
     * Release the session. A no-op once the module has trapped.
     */
    close(): void;
    /**
     * Drain queued session events; returns the encoded `DrainResponse`.
     */
    drain(): Uint8Array;
    /**
     * Create a bridge over the host `load`/`commit` functions.
     */
    constructor(load: Function, commit: Function);
    /**
     * Open the session; returns the encoded `OpenResponse` (an `error`
     * envelope once the module has trapped).
     */
    open(config: Uint8Array | number[]): Uint8Array;
}

/**
 * Install the panic hook once, at module init, before any `MarmotBridge`
 * exists. `panic = "abort"` still runs the hook before the trap escapes.
 */
export function install_panic_hook(): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_marmotbridge_free: (a: number, b: number) => void;
    readonly install_panic_hook: () => void;
    readonly marmotbridge_call: (a: number, b: number, c: number) => [number, number];
    readonly marmotbridge_close: (a: number) => void;
    readonly marmotbridge_drain: (a: number) => [number, number];
    readonly marmotbridge_new: (a: any, b: any) => number;
    readonly marmotbridge_open: (a: number, b: number, c: number) => [number, number];
    readonly rustsecp256k1_v0_10_0_context_create: (a: number) => number;
    readonly rustsecp256k1_v0_10_0_context_destroy: (a: number) => void;
    readonly rustsecp256k1_v0_10_0_default_error_callback_fn: (a: number, b: number) => void;
    readonly rustsecp256k1_v0_10_0_default_illegal_callback_fn: (a: number, b: number) => void;
    readonly wasm_bindgen_84586dc4a6b3820d___convert__closures_____invoke_______true_: (a: number, b: number) => void;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
