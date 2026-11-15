# Security Policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| < 1.0   | No        |

This library is pre-release; no version receives security support yet.

## Reporting a vulnerability

Report security issues privately via GitHub Security Advisories on
[qntx/marmot](https://github.com/qntx/marmot/security/advisories/new), or
email `security@qntx.org` if that channel is unavailable.

Please include:

- Affected version and runtime (Node/Bun/browser/React Native)
- Minimal reproduction
- Impact assessment (e.g. key-material exposure, group-state forgery,
  plaintext leak)

Do not open public issues for unfixed vulnerabilities.

## Threat model (summary)

This SDK implements the Marmot protocol — MLS group messaging over Nostr.
The security-critical protocol engine is upstream MDK, pinned by git rev in
`upstream.json` and never patched in place; defects are reported upstream
with a reproduction. On our side: no secret leaves the engine backend except
as a write to the injected, host-encrypted KV store; diagnostics carry no
account ids, group ids, message ids, relay URLs, pubkeys, payloads, or key
material; and every URL taken from group state passes destination-safety
(SSRF) checks before use.
