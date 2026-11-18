/**
 * `safeRelayUrl` — the destination-safety (SSRF) gate every relay URL passes before any connection.
 * Hand-written parser: `URL` shims differ across Hermes, Node and browsers, and this is a security
 * check. The URL is never rewritten; callers compare relay strings exactly.
 *
 * Host classification mirrors MDK `cgka_traits::app_components::host_safety` exactly: the IPv4
 * non-public set, the IPv6 special-purpose rules, IPv4-mapped IPv6 folded into the IPv4 rules, and
 * `localhost`/`*.localhost` (one trailing dot stripped) as loopback domains.
 */

const MAX_URL_BYTES = 512;
const MAX_DOMAIN_LENGTH = 253;
const MAX_LABEL_LENGTH = 63;
const MAX_PORT = 65535;

/** UTF-8 byte length: the ASCII gate runs first, so this only counts code units. */
function withinSizeLimit(url: string): boolean {
  return url.length <= MAX_URL_BYTES;
}

function isPrintableAscii(url: string): boolean {
  for (let i = 0; i < url.length; i += 1) {
    const code = url.codePointAt(i) ?? 0;
    if (code < 0x21 || code > 0x7e) {
      return false;
    }
  }
  return true;
}

/** One IPv4 part: decimal, `0x` hex, or leading-`0` octal — the WHATWG grammar. */
function parseIpv4Part(part: string): number | undefined {
  if (part.startsWith("0x") || part.startsWith("0X")) {
    const digits = part.slice(2);
    if (digits === "" || !/^[0-9a-fA-F]+$/.test(digits)) {
      return undefined;
    }
    return Number.parseInt(digits, 16);
  }
  if (part.length > 1 && part.startsWith("0")) {
    if (!/^[0-7]+$/.test(part)) {
      return undefined;
    }
    return Number.parseInt(part, 8);
  }
  if (!/^[0-9]+$/.test(part)) {
    return undefined;
  }
  return Math.trunc(Number(part));
}

/** The WHATWG IPv4 parser: 1–4 parts, the last filling the remaining bytes. */
function parseIpv4(host: string): [number, number, number, number] | undefined {
  const parts = host.split(".");
  if (parts.length > 4) {
    return undefined;
  }
  const numbers: number[] = [];
  for (const part of parts) {
    const n = part === "" ? undefined : parseIpv4Part(part);
    if (n === undefined) {
      return undefined;
    }
    numbers.push(n);
  }
  const count = numbers.length;
  for (const [i, n] of numbers.entries()) {
    const bound = i === count - 1 ? 256 ** (5 - count) : 256;
    if (n >= bound) {
      return undefined;
    }
  }
  let address = numbers[count - 1] ?? 0;
  for (let i = 0; i < count - 1; i += 1) {
    address += (numbers[i] ?? 0) * 256 ** (3 - i);
  }
  const b0 = Math.floor(address / 16777216);
  const b1 = Math.floor((address % 16777216) / 65536);
  const b2 = Math.floor((address % 65536) / 256);
  return [b0, b1, b2, address % 256];
}

/**
 * Whether `host` ends in a number per WHATWG: the last dot-label is all decimal digits or
 * `0x`-prefixed hex. Such hosts must parse as IPv4 or fail outright.
 */
function endsInNumber(host: string): boolean {
  const last = host.slice(host.lastIndexOf(".") + 1);
  return /^[0-9]+$/.test(last) || /^0[xX][0-9a-fA-F]+$/.test(last);
}

/** One IPv6 group or the strict dotted-IPv4 tail (returns two segments for the tail). */
function parseIpv6Groups(text: string): number[] | undefined {
  if (text === "") {
    return [];
  }
  const groups = text.split(":");
  const out: number[] = [];
  for (const [i, group] of groups.entries()) {
    if (group === "") {
      return undefined;
    }
    if (i === groups.length - 1 && group.includes(".")) {
      const v4 = group.split(".");
      if (v4.length !== 4) {
        return undefined;
      }
      let pair = 0;
      for (const part of v4) {
        if (!/^[0-9]{1,3}$/.test(part)) {
          return undefined;
        }
        const n = Math.trunc(Number(part));
        if (n > 255) {
          return undefined;
        }
        pair = pair * 256 + n;
      }
      out.push(Math.floor(pair / 65536), pair % 65536);
    } else {
      if (!/^[0-9a-fA-F]{1,4}$/.test(group)) {
        return undefined;
      }
      out.push(Number.parseInt(group, 16));
    }
  }
  return out;
}

/** Full RFC 4291 text form: `::` compression once, optional embedded dotted IPv4 tail. */
function parseIpv6(text: string): number[] | undefined {
  if (text.includes("%")) {
    return undefined;
  }
  const halves = text.split("::");
  if (halves.length > 2) {
    return undefined;
  }
  if (halves.length === 1) {
    const groups = parseIpv6Groups(halves[0] ?? "");
    return groups !== undefined && groups.length === 8 ? groups : undefined;
  }
  const left = parseIpv6Groups(halves[0] ?? "");
  const right = parseIpv6Groups(halves[1] ?? "");
  if (left === undefined || right === undefined) {
    return undefined;
  }
  const zeros = 8 - left.length - right.length;
  if (zeros < 1) {
    return undefined;
  }
  const out = [...left];
  for (let i = 0; i < zeros; i += 1) {
    out.push(0);
  }
  return [...out, ...right];
}

/** `is_public_ipv4` from host_safety.rs: the canonical non-public set. */
function isPublicIpv4(octets: readonly [number, number, number, number]): boolean {
  const [a, b] = octets;
  if (a === 0 || a === 10 || a === 127 || a >= 224) {
    return false;
  }
  if (a === 100 && b >= 64 && b <= 127) {
    return false;
  }
  if (a === 169 && b === 254) {
    return false;
  }
  if (a === 172 && b >= 16 && b <= 31) {
    return false;
  }
  if (a === 192 && b === 168) {
    return false;
  }
  if (a === 192 && b === 0 && (octets[2] === 0 || octets[2] === 2)) {
    return false;
  }
  if (a === 192 && b === 88 && octets[2] === 99) {
    return false;
  }
  if (a === 198 && (b === 18 || b === 19)) {
    return false;
  }
  if (a === 198 && b === 51 && octets[2] === 100) {
    return false;
  }
  return !(a === 203 && b === 0 && octets[2] === 113);
}

/** `is_public_ipv6` from host_safety.rs: mapped IPv4 folds into the IPv4 rules. */
function isPublicIpv6(segments: ReadonlyArray<number>): boolean {
  const mapped =
    segments[0] === 0 &&
    segments[1] === 0 &&
    segments[2] === 0 &&
    segments[3] === 0 &&
    segments[4] === 0 &&
    segments[5] === 0xffff;
  if (mapped) {
    const s6 = segments[6] ?? 0;
    const s7 = segments[7] ?? 0;
    return isPublicIpv4([Math.floor(s6 / 256), s6 % 256, Math.floor(s7 / 256), s7 % 256]);
  }
  const first = segments[0] ?? 0;
  const second = segments[1] ?? 0;
  // Loopback (::1) and unspecified (::) — `to_ipv4_mapped` already handled above.
  if (segments.every((s) => s === 0) || isLoopbackIpv6(segments)) {
    return false;
  }
  if (first >= 0xff00) {
    return false;
  }
  // ULA fc00::/7 and link-local fe80::/10.
  if ((first >= 0xfc00 && first <= 0xfdff) || (first >= 0xfe80 && first <= 0xfebf)) {
    return false;
  }
  if (first === 0x2001 && second <= 0x01ff) {
    return false;
  }
  if (first === 0x2002) {
    return false;
  }
  if (first === 0x2001 && second === 0x0db8) {
    return false;
  }
  if (first === 0x3fff && second <= 0x0fff) {
    return false;
  }
  // Global unicast 2000::/3.
  return first >= 0x2000 && first <= 0x3fff;
}

/** `::1` only — mapped IPv4 loopback is deliberately not loopback, mirroring MDK. */
function isLoopbackIpv6(segments: ReadonlyArray<number>): boolean {
  return segments.every((s, i) => (i < 7 ? s === 0 : s === 1));
}

/** `localhost` or `*.localhost`, one trailing dot stripped, case-insensitive. */
function isLoopbackDomain(host: string): boolean {
  const lowered = host.toLowerCase();
  return lowered === "localhost" || lowered.endsWith(".localhost");
}

/** LDH labels: a-z, 0-9, `-` (case-insensitive); labels 1–63 chars, total ≤ 253. */
function isValidDomain(host: string): boolean {
  if (host === "" || host.length > MAX_DOMAIN_LENGTH) {
    return false;
  }
  for (const label of host.split(".")) {
    if (label.length === 0 || label.length > MAX_LABEL_LENGTH) {
      return false;
    }
    if (!/^[a-zA-Z0-9-]+$/.test(label)) {
      return false;
    }
  }
  return true;
}

type HostVerdict = "public" | "loopback" | "refused";

function classifyDomain(host: string): HostVerdict {
  if (!isValidDomain(host)) {
    return "refused";
  }
  return isLoopbackDomain(host) ? "loopback" : "public";
}

function classifyHost(host: string, bracketed: boolean): HostVerdict {
  if (bracketed) {
    const segments = parseIpv6(host);
    if (segments === undefined) {
      return "refused";
    }
    if (isLoopbackIpv6(segments)) {
      return "loopback";
    }
    return isPublicIpv6(segments) ? "public" : "refused";
  }
  // One trailing dot is stripped for both the IPv4 check and the domain checks.
  const unrooted = host.endsWith(".") ? host.slice(0, -1) : host;
  if (endsInNumber(unrooted)) {
    const octets = parseIpv4(unrooted);
    if (octets === undefined) {
      return "refused";
    }
    if (octets[0] === 127) {
      return "loopback";
    }
    return isPublicIpv4(octets) ? "public" : "refused";
  }
  return classifyDomain(unrooted);
}

function parsePort(text: string): boolean {
  if (!/^[0-9]+$/.test(text)) {
    return false;
  }
  const port = Math.trunc(Number(text));
  return port >= 1 && port <= MAX_PORT;
}

/**
 * Whether `url` is a dialable relay endpoint. `wss` needs a public host (or a loopback host when
 * `allowLoopback`); `ws` additionally requires a loopback host even with `allowLoopback`.
 * Everything else refuses.
 */
export function safeRelayUrl(url: string, allowLoopback: boolean): boolean {
  if (!withinSizeLimit(url) || !isPrintableAscii(url)) {
    return false;
  }
  if (url.includes("#")) {
    return false;
  }
  const schemeEnd = url.indexOf(":");
  if (schemeEnd <= 0) {
    return false;
  }
  const scheme = url.slice(0, schemeEnd).toLowerCase();
  if (scheme !== "wss" && scheme !== "ws") {
    return false;
  }
  const after = url.slice(schemeEnd + 1);
  if (!after.startsWith("//")) {
    return false;
  }
  const rest = after.slice(2);
  let authorityEnd = rest.length;
  for (const mark of ["/", "?"]) {
    const at = rest.indexOf(mark);
    if (at !== -1 && at < authorityEnd) {
      authorityEnd = at;
    }
  }
  const authority = rest.slice(0, authorityEnd);
  if (authority.includes("@")) {
    return false;
  }
  let host: string;
  let bracketed = false;
  let portText: string | undefined;
  if (authority.startsWith("[")) {
    const close = authority.indexOf("]");
    if (close === -1) {
      return false;
    }
    bracketed = true;
    host = authority.slice(1, close);
    const tail = authority.slice(close + 1);
    if (tail !== "") {
      if (!tail.startsWith(":")) {
        return false;
      }
      portText = tail.slice(1);
    }
  } else {
    const colon = authority.indexOf(":");
    if (colon === -1) {
      host = authority;
    } else {
      host = authority.slice(0, colon);
      portText = authority.slice(colon + 1);
    }
  }
  if (host === "" || host.includes("%")) {
    return false;
  }
  if (portText !== undefined && !parsePort(portText)) {
    return false;
  }
  const verdict = classifyHost(host, bracketed);
  if (verdict === "public") {
    return scheme === "wss";
  }
  if (verdict === "loopback") {
    return allowLoopback;
  }
  return false;
}
