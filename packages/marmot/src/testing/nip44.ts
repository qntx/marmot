/**
 * Host `Signer` NIP-44 conformance: spec `transports/nostr.md` requires every NIP-44 layer a client
 * uses — including the signer's own `nip44Encrypt`/`nip44Decrypt` — to carry plaintexts of at least
 * 65,536 bytes under the `00 00 || u32be(len)` extended length prefix. `checkSignerNip44` exercises
 * both directions against a fixed conformance peer: a signer that throws, truncates or emits the
 * legacy 2-byte prefix fails with `Nip44ConformanceError`.
 */

import { getPublicKey } from "@qntx/nostr";
import { calcPaddedLen, decryptFromPubkey, encryptToPubkey } from "@qntx/nostr/nips/nip44";

import type { Signer } from "../backend/engine.ts";

/** Plaintext sizes the conformance check exercises — both above the legacy u16 limit. */
export const NIP44_EXTENDED_SIZES = [65_536, 131_073] as const;

/** A signer that failed a `checkSignerNip44` step; `step` and `size` name the failing leg. */
export class Nip44ConformanceError extends Error {
  readonly step: string;
  readonly size: number;
  constructor(step: string, size: number, cause?: unknown) {
    super(`signer ${step} failed for a ${size}-byte plaintext (NIP-44 extended length)`, {
      cause,
    });
    this.name = "Nip44ConformanceError";
    this.step = step;
    this.size = size;
  }
}

/** The fixed conformance peer: an arbitrary valid secret; only the keypair matters. */
const PEER_SECRET = new Uint8Array(32).fill(0x44);
const PEER_PUBKEY = getPublicKey(PEER_SECRET);

/**
 * Run the host `Signer` through the NIP-44 extended-length round trip in both directions for every
 * `NIP44_EXTENDED_SIZES` plaintext: (1) `signer.nip44Encrypt` output must decrypt under
 * `@qntx/nostr` to the exact plaintext and carry the extended prefix — its decoded byte length
 * equals `1 + 32 + 6 + calcPaddedLen(len) + 32`; (2) a payload produced by `@qntx/nostr` must
 * decrypt under `signer.nip44Decrypt` to the same plaintext.
 */
export async function checkSignerNip44(signer: Signer): Promise<void> {
  for (const size of NIP44_EXTENDED_SIZES) {
    // oxlint-disable-next-line no-await-in-loop -- sequential so the reported step and size stay deterministic
    await checkSize(signer, size);
  }
}

async function checkSize(signer: Signer, size: number): Promise<void> {
  const plaintext = "a".repeat(size);
  let ciphertext: string;
  try {
    ciphertext = await signer.nip44Encrypt(PEER_PUBKEY, plaintext);
  } catch (error) {
    throw new Nip44ConformanceError("nip44Encrypt", size, error);
  }
  const decoded = base64Length(ciphertext);
  const expected = 1 + 32 + 6 + calcPaddedLen(size) + 32;
  if (decoded !== expected) {
    throw new Nip44ConformanceError("nip44Encrypt", size);
  }
  let roundTrip: string;
  try {
    roundTrip = decryptFromPubkey(ciphertext, PEER_SECRET, signer.pubkey, {
      maxPayloadChars: ciphertext.length,
    });
  } catch (error) {
    throw new Nip44ConformanceError("nip44Encrypt", size, error);
  }
  if (roundTrip !== plaintext) {
    throw new Nip44ConformanceError("nip44Encrypt", size);
  }
  const inbound = encryptToPubkey(plaintext, PEER_SECRET, signer.pubkey);
  let recovered: string;
  try {
    recovered = await signer.nip44Decrypt(PEER_PUBKEY, inbound);
  } catch (error) {
    throw new Nip44ConformanceError("nip44Decrypt", size, error);
  }
  if (recovered !== plaintext) {
    throw new Nip44ConformanceError("nip44Decrypt", size);
  }
}

function base64Length(payload: string): number {
  return atob(payload).length;
}
