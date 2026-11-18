// MARMOT_SMOKE_NO_NATIVE=1 strips @qntx/marmot-native out of the app for the
// app-size delta measurement: autolinking skips the package and metro.config.js
// resolves its imports to src/native-stub.ts. Never set for a real smoke run.
module.exports =
  process.env.MARMOT_SMOKE_NO_NATIVE === "1"
    ? {
        dependencies: {
          "@qntx/marmot-native": { platforms: { ios: null, android: null } },
        },
      }
    : {};
