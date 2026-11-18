const path = require("path");
const { getDefaultConfig } = require("expo/metro-config");

const config = getDefaultConfig(__dirname);

// @qntx/marmot and @qntx/marmot-native are file: deps copied into node_modules
// by scripts/sync-local.ts; Metro must watch their real paths under the repo
// root as well for resolution and bundle inputs.
config.watchFolders = [path.resolve(__dirname, "../..")];

// MARMOT_SMOKE_NO_NATIVE=1 (app-size delta builds only): the package is out of
// autolinking via react-native.config.js, and its imports resolve to the stub.
if (process.env.MARMOT_SMOKE_NO_NATIVE === "1") {
  const stub = path.resolve(__dirname, "src/native-stub.ts");
  const previous = config.resolver.resolveRequest;
  config.resolver.resolveRequest = (context, moduleName, platform) => {
    if (moduleName === "@qntx/marmot-native") {
      return { type: "sourceFile", filePath: stub };
    }
    return typeof previous === "function"
      ? previous(context, moduleName, platform)
      : context.resolveRequest(context, moduleName, platform);
  };
}

module.exports = config;
