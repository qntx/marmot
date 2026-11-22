import { describe, expect, test } from "vite-plus/test";

import { safeRelayUrl } from "../src/transport/url.ts";

describe("safeRelayUrl — accepted without loopback", () => {
  const ok = [
    "wss://relay.example",
    "wss://relay.example/",
    "wss://relay.example:443",
    "wss://relay.example:8443",
    "wss://relay.example/path",
    "wss://relay.example/path?x=1",
    "wss://relay.example.", // trailing-dot FQDN
    "wss://xn--nxasmq6b.example", // punycode label
    "wss://localhost.evil.example", // a public domain, not the loopback name
    "wss://8.8.8.8",
    "wss://1.1.1.1",
    "wss://[2606:4700:4700::1111]",
    "wss://[2a01:4f8::1]:4443",
    "wss://[::ffff:8.8.8.8]", // v4-mapped public
  ];

  test.each(ok)("%s", (url) => {
    expect(safeRelayUrl(url, false)).toBe(true);
  });
});

describe("safeRelayUrl — refused without loopback", () => {
  const bad = [
    "",
    "wss://",
    "not a url",
    "relay.example",
    "//relay.example",
    "ftp://relay.example",
    "https://relay.example",
    "http://relay.example",
    "wss://relay.example#frag", // fragments refused
    "wss://user@relay.example", // userinfo refused
    "wss://user:pass@relay.example",
    "wss://rel ay.example", // space is not printable-ascii ≥ 0x21
    "wss://rälay.example", // non-ASCII refused
    "wss://relay.example:0",
    "wss://relay.example:65536",
    "wss://relay.example:80x",
    "wss://relay.example:", // empty port
    "wss://[::1", // unclosed bracket
    "wss://[zz::1]", // invalid hex group
    "wss://[::1]x", // garbage after bracket
    // ws only ever reaches loopback hosts
    "ws://relay.example",
    "ws://8.8.8.8",
    "ws://localhost.evil.example",
    "ws://[2606:4700:4700::1111]",
    "ws://[::ffff:8.8.8.8]",
    // loopback (verdict "loopback" — the flag rescues these)
    "wss://localhost",
    "wss://LOCALHOST",
    "wss://localhost.",
    "wss://foo.localhost",
    "ws://localhost",
    "ws://localhost:8080",
    "wss://127.0.0.1",
    "wss://127.1.2.3",
    "wss://127.1", // 2-part IPv4 is still 127.0.0.1
    "wss://2130706433", // decimal DWORD = 127.0.0.1
    "wss://0x7f000001", // hex DWORD = 127.0.0.1
    "wss://0177.0.0.1", // octal = 127.0.0.1
    "wss://[::1]",
    "ws://[::1]",
    // non-public IPv4 (verdict "refused" — the flag does not rescue)
    "wss://0.0.0.0",
    "wss://10.0.0.1",
    "wss://10.255.255.255",
    "wss://100.64.0.1", // CGNAT
    "wss://100.127.255.255",
    "wss://169.254.1.1", // link-local
    "wss://169.254.169.254", // cloud metadata
    "wss://172.16.0.1",
    "wss://172.31.255.254",
    "wss://192.168.0.1",
    "wss://192.168.255.255",
    "wss://192.0.0.1",
    "wss://192.0.2.1", // TEST-NET-1
    "wss://192.88.99.1",
    "wss://198.18.0.1",
    "wss://198.19.255.255",
    "wss://198.51.100.1", // TEST-NET-2
    "wss://203.0.113.1", // TEST-NET-3
    "wss://224.0.0.1", // multicast
    "wss://255.255.255.255",
    // non-public IPv6
    "wss://[::]", // unspecified
    "wss://[ff02::1]", // multicast
    "wss://[fc00::1]", // ULA
    "wss://[fd12:3456::1]",
    "wss://[fe80::1]", // link-local
    "wss://[fec0::1]", // site-local
    "wss://[2001:db8::1]", // documentation
    "wss://[2001::1]", // Teredo block start
    "wss://[2002::1]", // 6to4
    "wss://[3fff::1]", // documentation v6
    "wss://[64:ff9b::8.8.8.8]", // NAT64 — outside 2000::/3
    "wss://[::ffff:127.0.0.1]", // v4-mapped loopback is refused, not loopback
    "wss://[::ffff:10.0.0.1]",
    "wss://[::ffff:169.254.169.254]",
    "wss://[::ffff:192.168.0.1]",
  ];

  test.each(bad)("%s", (url) => {
    expect(safeRelayUrl(url, false)).toBe(false);
  });
});

describe("safeRelayUrl — allowLoopback", () => {
  test("rescues loopback hosts only", () => {
    for (const url of [
      "ws://localhost",
      "ws://localhost:8080",
      "wss://localhost",
      "wss://foo.localhost",
      "ws://127.0.0.1",
      "ws://127.0.0.1:7777",
      "wss://127.0.0.1",
      "ws://127.1",
      "ws://2130706433",
      "ws://[::1]",
      "wss://[::1]",
      "wss://relay.example.com",
    ]) {
      expect(safeRelayUrl(url, true)).toBe(true);
    }
  });

  test("ws still refuses public hosts and every refused verdict stays refused", () => {
    for (const url of [
      "ws://relay.example",
      "ws://8.8.8.8",
      "wss://10.0.0.1",
      "ws://10.0.0.1",
      "wss://192.168.1.1",
      "ws://[::ffff:127.0.0.1]",
      "wss://[::ffff:10.0.0.1]",
      "wss://169.254.169.254",
      "wss://[fd00::1]",
      "wss://[fe80::1]",
    ]) {
      expect(safeRelayUrl(url, true)).toBe(false);
    }
  });

  test("the flag never rescues malformed URLs", () => {
    for (const url of ["", "wss://", "ftp://127.0.0.1", "wss://localhost:0", "wss://user@a"]) {
      expect(safeRelayUrl(url, true)).toBe(false);
    }
  });
});

describe("safeRelayUrl — size limits", () => {
  test("urls longer than 512 bytes refuse", () => {
    const long = `wss://${"a".repeat(500)}.example`;
    expect(safeRelayUrl(long, false)).toBe(false);
  });
});
