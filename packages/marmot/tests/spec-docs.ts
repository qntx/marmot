import { readFileSync } from "node:fs";
import { join } from "node:path";

// Recorded spec copies, byte-identical to the pinned upstream rev — the
// constants under src/core/ are tested against these, not the live checkout.
const SPEC_DIR = join(import.meta.dirname, "..", "..", "..", "vectors", "spec", "foundation");

export function specDoc(name: string): string {
  return readFileSync(join(SPEC_DIR, name), "utf8");
}

function sectionBody(doc: string, section: string): string {
  const start = doc.indexOf(`## ${section}`);
  if (start === -1) {
    throw new Error(`section not found: ${section}`);
  }
  const rest = doc.slice(start);
  const end = rest.indexOf("\n## ");
  return end === -1 ? rest : rest.slice(0, end);
}

/** Data rows of the markdown table under a `##` section, cells trimmed of backticks/quotes. */
export function tableRows(doc: string, section: string): string[][] {
  return sectionBody(doc, section)
    .split("\n")
    .filter((line) => line.startsWith("|"))
    .slice(2)
    .map((line) =>
      line
        .split("|")
        .slice(1, -1)
        .map((cell) => cell.trim().replaceAll("`", "").replaceAll('"', "")),
    );
}

/** Backticked names of the `- \`name\`: ...`bullet list under a`##` section. */
export function bulletNames(doc: string, section: string): string[] {
  return sectionBody(doc, section)
    .split("\n")
    .filter((line) => line.startsWith("- `"))
    .map((line) => /^- `([^`]+)`/.exec(line)?.[1] ?? "");
}
