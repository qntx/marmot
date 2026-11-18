/**
 * Deep-link / run-file driven smoke runner (see params.ts for the two handoff paths). Each mode
 * posts one machine-readable result `{mode, ok, error?, timings?, platform, osVersion, model}`;
 * kill's result is process death by contract.
 */

import { useEffect, useRef, useState } from "react";
import { Linking, Platform, StyleSheet, Text, View } from "react-native";

import { post } from "./collector.ts";
import { parseRunUrl, readRunParams } from "./params.ts";
import type { RunParams } from "./params.ts";
import { runFlowMode, runKillMode, runVerifyMode } from "./modes.ts";

const deviceInfo = (): { platform: string; osVersion: string; model: string } => ({
  platform: Platform.OS,
  osVersion: String(Platform.Version),
  model: String(
    (Platform.constants as Record<string, unknown>)["Model"] ??
      (Platform.constants as Record<string, unknown>)["model"] ??
      (Platform.constants as Record<string, unknown>)["interfaceIdiom"] ??
      "unknown",
  ),
});

export function App() {
  const [status, setStatus] = useState("idle");
  const started = useRef(false);

  useEffect(() => {
    const run = async (params: RunParams | undefined): Promise<void> => {
      if (params === undefined || started.current) {
        return;
      }
      started.current = true;
      setStatus(`running ${params.mode}`);
      const base = { mode: params.mode, run: params.run, ...deviceInfo() };
      try {
        const extra =
          params.mode === "flow"
            ? await runFlowMode(params)
            : params.mode === "verify"
              ? await runVerifyMode(params)
              : params.mode === "kill"
                ? await runKillMode(params)
                : {};
        await post(`/${params.run}/result`, { ...base, ok: true, ...extra });
        setStatus(`${params.mode} ok`);
      } catch (error) {
        try {
          await post(`/${params.run}/result`, { ...base, ok: false, error: String(error) });
        } catch {
          // The collector being unreachable is itself a driver-visible failure.
        }
        setStatus(`${params.mode} failed: ${String(error)}`);
      }
    };
    void readRunParams().then(run);
    const subscription = Linking.addEventListener("url", ({ url }) =>
      void run(parseRunUrl(url ?? "")),
    );
    return () => subscription.remove();
  }, []);

  return (
    <View style={styles.container}>
      <Text style={styles.title}>marmot native smoke</Text>
      <Text style={styles.status}>{status}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, alignItems: "center", justifyContent: "center", backgroundColor: "#fff" },
  title: { fontSize: 18, marginBottom: 8 },
  status: { fontSize: 13, color: "#444" },
});
