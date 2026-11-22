import { describe, expect, test } from "vite-plus/test";

import { checkSignerNip44 } from "@qntx/marmot/testing";

import { testSigner } from "../signer.ts";

describe("checkSignerNip44", () => {
  test("the nostr-tools test signer passes NIP-44 extended-length conformance", async () => {
    await expect(checkSignerNip44(testSigner())).resolves.toBeUndefined();
  });
});
