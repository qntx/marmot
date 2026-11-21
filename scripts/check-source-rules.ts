/// <reference types="node" />
/**
 * Source-level safety rules the compiler cannot express. Reports each violation as `path:line:
 * rule` on stderr and exits nonzero when any rule fires.
 *
 * (a) `unsafe` forms appear under crates/<crate>/src only in crates/mk-ffi/src/abi.rs. (b) In
 * abi.rs every `unsafe {`/`unsafe impl` has a `// SAFETY:` in the contiguous comment block directly
 * above it. (c) No logging/printing in crates/mk-{core,bridge,wasm,ffi}/src outside cfg(test)
 * modules and crates/mk-core/src/testing.rs. (d) No `console.` in packages/<pkg>/src, excluding
 * packages/marmot-wasm/src/generated/. (e) No native logging calls in the marmot-native C++ trees.
 */

import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join, sep } from "node:path";

export type Finding = {
  path: string;
  line: number;
  rule: string;
};

const UNSAFE_ALLOW_PATH = join("crates", "mk-ffi", "src", "abi.rs");
const RUST_LOG_CRATES = new Set(["mk-core", "mk-bridge", "mk-wasm", "mk-ffi"]);
const RUST_LOG_EXEMPT = join("crates", "mk-core", "src", "testing.rs");
const CONSOLE_EXEMPT_PREFIX = join("packages", "marmot-wasm", "src", "generated") + sep;
const NATIVE_DIRS = [
  join("packages", "marmot-native", "cpp"),
  join("packages", "marmot-native", "android", "src", "main", "cpp"),
];

const UNSAFE_FORM =
  /\bunsafe\s*\{|\bunsafe\s+fn\b|\bunsafe\s+impl\b|\bunsafe\s+extern\b|#\s*\[\s*unsafe\s*\(/;
const UNSAFE_COMMENTED = /\bunsafe\s*\{|\bunsafe\s+impl\b/;
const RUST_LOG =
  /\b(?:tracing|log)::|\bstd::io::(?:stdout|stderr)\b|(?<![A-Za-z0-9_])(?:eprintln|eprint|println|print|dbg)!/;
const CONSOLE_CALL = /\bconsole\s*\./;
const NATIVE_LOG =
  /(?<![A-Za-z0-9_])(?:printf|fprintf)\s*\(|\bstd::cout\b|\bstd::cerr\b|__android_log|(?<![A-Za-z0-9_])(?:NSLog|os_log)\s*\(/;

/**
 * Blank out comments and string/char literal contents (spaces, same length), so rules match only
 * real code and brace counting stays exact. Handles line comments, nested block comments,
 * `"…"`/`b"…"`/`r#"…"#` strings, `'x'` char literals and template literals.
 */
export function sanitizedLines(source: string): string[] {
  const lines = source.split("\n");
  const out: string[] = [];
  let blockDepth = 0;
  for (const line of lines) {
    const chars = line.split("");
    let i = 0;
    while (i < chars.length) {
      const ch = chars[i] ?? "";
      const next = chars[i + 1] ?? "";
      if (blockDepth > 0) {
        if (ch === "/" && next === "*") {
          blockDepth += 1;
          chars[i] = " ";
          chars[i + 1] = " ";
          i += 2;
        } else if (ch === "*" && next === "/") {
          blockDepth -= 1;
          chars[i] = " ";
          chars[i + 1] = " ";
          i += 2;
        } else {
          chars[i] = " ";
          i += 1;
        }
        continue;
      }
      if (ch === "/" && next === "/") {
        for (let j = i; j < chars.length; j += 1) {
          chars[j] = " ";
        }
        break;
      }
      if (ch === "/" && next === "*") {
        blockDepth = 1;
        chars[i] = " ";
        chars[i + 1] = " ";
        i += 2;
        continue;
      }
      // String openers: r"…", r#"…"#, b"…" and plain "…". Raw strings close on
      // `"` + their hash run; plain strings close on an unescaped `"`.
      const raw = /^(?:b?r(#*)"|b?")/.exec(line.slice(i));
      if (raw !== null) {
        const [marker] = raw;
        const isRaw = marker.startsWith("r") || marker.startsWith("br");
        let end = chars.length;
        if (isRaw) {
          const close = `"${raw[1] ?? ""}`;
          const found = line.indexOf(close, i + marker.length);
          end = found === -1 ? chars.length : found + close.length;
        } else {
          for (let j = i + marker.length; j < chars.length; j += 1) {
            if (chars[j] === "\\") {
              j += 1;
            } else if (chars[j] === '"') {
              end = j + 1;
              break;
            }
          }
        }
        for (let j = i; j < end; j += 1) {
          chars[j] = " ";
        }
        i = end;
        continue;
      }
      if (ch === "`") {
        let end = chars.length;
        for (let j = i + 1; j < chars.length; j += 1) {
          if (chars[j] === "\\") {
            j += 1;
          } else if (chars[j] === "`") {
            end = j + 1;
            break;
          }
        }
        for (let j = i; j < end; j += 1) {
          chars[j] = " ";
        }
        i = end;
        continue;
      }
      if (ch === "'") {
        // Char literal: 'x', '\n', '\u{…}' — otherwise a lifetime, leave alone.
        const lit = /^'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]+\}|.)|[^'\\])'/.exec(line.slice(i));
        if (lit !== null) {
          for (let j = i; j < i + lit[0].length; j += 1) {
            chars[j] = " ";
          }
          i += lit[0].length;
          continue;
        }
      }
      i += 1;
    }
    out.push(chars.join(""));
  }
  return out;
}

/** Line indexes covered by `#[cfg(test)]` items (attr line through the item's end). */
function cfgTestLines(lines: string[]): Set<number> {
  const covered = new Set<number>();
  const depthBefore: number[] = [];
  let depth = 0;
  for (const line of lines) {
    depthBefore.push(depth);
    for (const ch of line) {
      depth += ch === "{" ? 1 : ch === "}" ? -1 : 0;
    }
  }
  for (let i = 0; i < lines.length; i += 1) {
    if (!/#\s*\[\s*cfg\(\s*test\s*\)\s*\]/.test(lines[i] ?? "")) {
      continue;
    }
    const attrDepth = depthBefore[i] ?? 0;
    covered.add(i);
    for (let j = i + 1; j < lines.length; j += 1) {
      covered.add(j);
      const line = lines[j] ?? "";
      const brace = line.indexOf("{");
      const semi = line.indexOf(";");
      if (semi !== -1 && (brace === -1 || semi < brace)) {
        break;
      }
      if (brace !== -1) {
        // Exempt through the line that closes the item's block.
        let end = j;
        for (let k = j + 1; k < lines.length; k += 1) {
          end = k;
          if ((depthBefore[k] ?? 0) + countBraces(lines[k] ?? "") <= attrDepth) {
            break;
          }
        }
        for (let k = j + 1; k <= end; k += 1) {
          covered.add(k);
        }
        break;
      }
    }
  }
  return covered;
}

function countBraces(line: string): number {
  let depth = 0;
  for (const ch of line) {
    depth += ch === "{" ? 1 : ch === "}" ? -1 : 0;
  }
  return depth;
}

/**
 * Rule (a)+(b): unsafe forms are allowed only in `abi.rs`, where each `unsafe {` / `unsafe impl`
 * needs a `// SAFETY:` in the contiguous comment block directly above.
 */
export function unsafeFindings(path: string, source: string): Finding[] {
  const segments = path.split(sep);
  if (segments.at(0) !== "crates" || segments.at(2) !== "src" || !path.endsWith(".rs")) {
    return [];
  }
  const lines = source.split("\n");
  const code = sanitizedLines(source);
  if (path !== UNSAFE_ALLOW_PATH) {
    const findings: Finding[] = [];
    for (const [i, line] of code.entries()) {
      if (UNSAFE_FORM.test(line)) {
        findings.push({ path, line: i + 1, rule: "unsafe outside crates/mk-ffi/src/abi.rs" });
      }
    }
    return findings;
  }
  const findings: Finding[] = [];
  for (const [i, line] of code.entries()) {
    if (!UNSAFE_COMMENTED.test(line)) {
      continue;
    }
    let documented = false;
    for (let j = i - 1; j >= 0 && /^\s*\/\//.test(lines[j] ?? ""); j -= 1) {
      if ((lines[j] ?? "").includes("SAFETY:")) {
        documented = true;
      }
    }
    if (!documented) {
      findings.push({ path, line: i + 1, rule: "unsafe without preceding // SAFETY:" });
    }
  }
  return findings;
}

/**
 * Rule (c): no logging or stdout/stderr printing in the mk-* library crates, outside `#[cfg(test)]`
 * items and `mk-core/src/testing.rs`.
 */
export function rustLogFindings(path: string, source: string): Finding[] {
  const segments = path.split(sep);
  const crateDir = segments.at(-3) ?? "";
  if (
    segments.at(-4) !== "crates" ||
    !RUST_LOG_CRATES.has(crateDir) ||
    segments.at(-2) !== "src" ||
    !path.endsWith(".rs") ||
    path === RUST_LOG_EXEMPT
  ) {
    return [];
  }
  const code = sanitizedLines(source);
  const exempt = cfgTestLines(code);
  const findings: Finding[] = [];
  for (const [i, line] of code.entries()) {
    if (!exempt.has(i) && RUST_LOG.test(line)) {
      findings.push({ path, line: i + 1, rule: "log/print in library source" });
    }
  }
  return findings;
}

/** Rule (d): no `console.` under packages/<pkg>/src, except the generated wasm glue. */
export function tsConsoleFindings(path: string, source: string): Finding[] {
  const segments = path.split(sep);
  if (
    segments.at(0) !== "packages" ||
    segments.at(2) !== "src" ||
    !path.endsWith(".ts") ||
    path.startsWith(CONSOLE_EXEMPT_PREFIX)
  ) {
    return [];
  }
  const code = sanitizedLines(source);
  const findings: Finding[] = [];
  for (const [i, line] of code.entries()) {
    if (CONSOLE_CALL.test(line)) {
      findings.push({ path, line: i + 1, rule: "console.* in package source" });
    }
  }
  return findings;
}

/** Rule (e): no native logging calls in the marmot-native C++ trees. */
export function nativeLogFindings(path: string, source: string): Finding[] {
  if (!NATIVE_DIRS.some((dir) => path.startsWith(dir + sep))) {
    return [];
  }
  const code = sanitizedLines(source);
  const findings: Finding[] = [];
  for (const [i, line] of code.entries()) {
    if (NATIVE_LOG.test(line)) {
      findings.push({ path, line: i + 1, rule: "native log call" });
    }
  }
  return findings;
}

/** Apply every rule whose path gate matches. */
export function fileFindings(path: string, source: string): Finding[] {
  return [
    ...unsafeFindings(path, source),
    ...rustLogFindings(path, source),
    ...tsConsoleFindings(path, source),
    ...nativeLogFindings(path, source),
  ];
}

function* walk(dir: string): Generator<string> {
  if (!existsSync(dir)) {
    return;
  }
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      yield* walk(path);
    } else if (entry.isFile()) {
      yield path;
    }
  }
}

function scanFiles(): string[] {
  const files: string[] = [];
  const crates = join("crates");
  if (existsSync(crates)) {
    for (const entry of readdirSync(crates, { withFileTypes: true })) {
      if (entry.isDirectory()) {
        for (const file of walk(join(crates, entry.name, "src"))) {
          if (file.endsWith(".rs")) {
            files.push(file);
          }
        }
      }
    }
  }
  const packages = join("packages");
  if (existsSync(packages)) {
    for (const entry of readdirSync(packages, { withFileTypes: true })) {
      if (entry.isDirectory()) {
        for (const file of walk(join(packages, entry.name, "src"))) {
          if (file.endsWith(".ts")) {
            files.push(file);
          }
        }
      }
    }
  }
  for (const dir of NATIVE_DIRS) {
    files.push(...walk(dir));
  }
  return files;
}

if (import.meta.main) {
  const findings: Finding[] = [];
  for (const file of scanFiles()) {
    findings.push(...fileFindings(file, readFileSync(file, "utf8")));
  }
  for (const finding of findings) {
    console.error(`${finding.path}:${finding.line}: ${finding.rule}`);
  }
  if (findings.length > 0) {
    process.exitCode = 1;
  }
}
