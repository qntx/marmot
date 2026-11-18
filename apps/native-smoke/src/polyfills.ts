/**
 * Hermes has no WebCrypto; nostr-tools and the test cipher need
 * `crypto.getRandomValues`. expo-crypto provides a synchronous native
 * implementation — install it only where the global is missing.
 */

import { getRandomValues } from "expo-crypto";

type CryptoLike = { getRandomValues?: (array: Uint8Array) => Uint8Array };

const cryptoGlobal = (globalThis as { crypto?: CryptoLike }).crypto;
if (cryptoGlobal?.getRandomValues === undefined) {
  (globalThis as { crypto?: CryptoLike }).crypto = {
    ...cryptoGlobal,
    getRandomValues: (array: Uint8Array) => getRandomValues(array),
  };
}
