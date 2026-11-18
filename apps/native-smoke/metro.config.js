const path = require("path");
const { getDefaultConfig } = require("expo/metro-config");

const config = getDefaultConfig(__dirname);

// @qntx/marmot and @qntx/marmot-native are file: deps symlinked to the repo
// root; Metro must watch their real paths to resolve and bundle them.
config.watchFolders = [path.resolve(__dirname, "../..")];

module.exports = config;
