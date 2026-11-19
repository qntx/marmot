// Cleartext HTTP to the host-side collector only: a network-security-config
// permitting cleartext to 127.0.0.1/localhost (reached via `adb reverse`),
// applied through android:networkSecurityConfig on the application element.
const { withAndroidManifest, withDangerousMod } = require("expo/config-plugins");
const { mkdirSync, writeFileSync } = require("node:fs");
const { join } = require("node:path");

const NETWORK_SECURITY_CONFIG = `<?xml version="1.0" encoding="utf-8"?>
<network-security-config>
  <domain-config cleartextTrafficPermitted="true">
    <domain includeSubdomains="false">localhost</domain>
    <domain includeSubdomains="false">127.0.0.1</domain>
  </domain-config>
</network-security-config>
`;

function withLocalhostCleartext(config) {
  config = withDangerousMod(config, [
    "android",
    async (mod) => {
      const resXml = join(
        mod.modRequest.platformProjectRoot,
        "app/src/main/res/xml",
      );
      mkdirSync(resXml, { recursive: true });
      writeFileSync(
        join(resXml, "network_security_config.xml"),
        NETWORK_SECURITY_CONFIG,
      );
      return mod;
    },
  ]);
  return withAndroidManifest(config, (mod) => {
    mod.modResults.manifest.application[0].$["android:networkSecurityConfig"] =
      "@xml/network_security_config";
    return mod;
  });
}

module.exports = withLocalhostCleartext;
