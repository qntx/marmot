/**
 * Run parameters reach the app two ways:
 * - iOS: `xcrun simctl openurl` raises a per-open user consent alert that headless runs cannot
 *   dismiss, so the driver writes `Documents/run.json` into the app container before
 *   `simctl launch` and the app reads it here.
 * - Android: `am start -d marmotsmoke://run?...` is headless-safe — the deep link arrives via
 *   `Linking.getInitialURL` (cold) or the `url` event (warm).
 * Both normalize into the same `RunParams`.
 */

import { Linking } from "react-native";
import { File, Paths } from "expo-file-system";

export type RunParams = {
  mode: string;
  run: string;
  db: string;
  key: string | undefined;
  secret: string | undefined;
  messages: number;
  /** Perf mode only. */
  case: string;
  reps: number;
  members: number;
  windows: number[];
  timeLimitSec: number;
  source: string | undefined;
};

function normalize(fields: Map<string, string>): RunParams | undefined {
  const mode = fields.get("mode");
  if (mode === undefined) {
    return undefined;
  }
  return {
    mode,
    run: fields.get("run") ?? "run",
    db: fields.get("db") ?? "marmot",
    key: fields.get("key"),
    secret: fields.get("secret"),
    messages: Number(fields.get("messages") ?? "500"),
    case: fields.get("case") ?? "",
    reps: Number(fields.get("reps") ?? "1"),
    members: Number(fields.get("members") ?? "0"),
    windows: (fields.get("windows") ?? "")
      .split(",")
      .map((w) => Number(w))
      .filter((w) => Number.isFinite(w) && w > 0),
    timeLimitSec: Number(fields.get("timeLimitSec") ?? "0"),
    source: fields.get("source"),
  };
}

export function parseRunUrl(url: string): RunParams | undefined {
  const query = url.split("?")[1] ?? "";
  const fields = new Map<string, string>();
  for (const pair of query.split("&")) {
    const eq = pair.indexOf("=");
    if (eq > 0) {
      fields.set(pair.slice(0, eq), decodeURIComponent(pair.slice(eq + 1)));
    }
  }
  return normalize(fields);
}

/** File first (iOS driver handoff), then the deep link (Android driver / manual). */
export async function readRunParams(): Promise<RunParams | undefined> {
  try {
    const file = new File(Paths.document, "run.json");
    if (file.exists) {
      const raw = (await file.json()) as Record<string, unknown>;
      return normalize(new Map(Object.entries(raw).map(([k, v]) => [k, String(v)])));
    }
  } catch {
    // No handoff file — fall through to the deep link.
  }
  const url = await Linking.getInitialURL();
  return url == null ? undefined : parseRunUrl(url);
}
