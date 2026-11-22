import { expect, test } from "bun:test";

import { checkSignerNip44 } from "@qntx/marmot/testing";

import { testSigner } from "./signer.ts";

test("the nostr-tools signer passes NIP-44 extended-length conformance", async () => {
  await checkSignerNip44(testSigner());
  expect(true).toBe(true);
});
