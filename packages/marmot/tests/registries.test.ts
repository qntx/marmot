import { describe, expect, test } from "vite-plus/test";

import {
  ALPN_IDENTIFIERS,
  APP_COMPONENTS,
  EVENT_KINDS,
  EXPORTERS,
  MARMOT_EXTENSION_TYPES,
  MLS_EXTENSION_DRAFT_IDS,
} from "../src/core/registries.ts";
import { specDoc, tableRows } from "./spec-docs.ts";

const REGISTRIES = specDoc("registries.md");

const hex = (id: number): string => `0x${id.toString(16).padStart(4, "0")}`;

describe("registries mirror the recorded spec", () => {
  test("app component ids", () => {
    const spec = tableRows(REGISTRIES, "App component ids").map(([id, name]) => `${id}|${name}`);
    const code = Object.values(APP_COMPONENTS).map((entry) => `${hex(entry.id)}|${entry.name}`);
    expect(code.toSorted()).toStrictEqual(spec.toSorted());
  });

  test("upstream MLS extension draft ids", () => {
    const spec = tableRows(REGISTRIES, "Upstream MLS extension draft ids").map(
      ([ns, id, name]) => `${ns}|${id}|${name}`,
    );
    const code = Object.values(MLS_EXTENSION_DRAFT_IDS).map(
      (entry) => `${entry.namespace}|${hex(entry.id)}|${entry.name}`,
    );
    expect(code.toSorted()).toStrictEqual(spec.toSorted());
  });

  test("marmot custom extension types", () => {
    const spec = tableRows(REGISTRIES, "Marmot custom MLS extension types").map(
      ([id, name]) => `${id}|${name}`,
    );
    const code = Object.values(MARMOT_EXTENSION_TYPES).map(
      (entry) => `${hex(entry.id)}|${entry.name}`,
    );
    expect(code.toSorted()).toStrictEqual(spec.toSorted());
  });

  test("nostr event kinds", () => {
    const spec = tableRows(REGISTRIES, "Nostr event kinds used by Marmot").map(
      ([kind, name, layer]) => `${kind}|${name}|${layer}`,
    );
    const code = Object.values(EVENT_KINDS).map(
      (entry) => `${entry.kind}|${entry.name}|${entry.layer}`,
    );
    expect(code.toSorted()).toStrictEqual(spec.toSorted());
  });

  test("alpn identifiers", () => {
    const spec = tableRows(REGISTRIES, "ALPN and protocol identifiers").map(
      ([id, use]) => `${id}|${use}`,
    );
    const code = Object.values(ALPN_IDENTIFIERS).map((entry) => `${entry.id}|${entry.use}`);
    expect(code.toSorted()).toStrictEqual(spec.toSorted());
  });

  test("exporter labels", () => {
    const spec = tableRows(REGISTRIES, "Exporter labels").map(
      ([label, context, length]) => `${label}|${context}|${length}`,
    );
    const code = Object.values(EXPORTERS).map(
      (entry) => `${entry.label}|${entry.context}|${entry.length}`,
    );
    expect(code.toSorted()).toStrictEqual(spec.toSorted());
  });

  test("withdrawn and superseded ids are never active", () => {
    const retired = [...Object.values(APP_COMPONENTS), ...Object.values(MARMOT_EXTENSION_TYPES)]
      .filter((entry) => entry.status !== "active")
      .map((entry) => entry.id);
    expect(retired.toSorted((a, b) => a - b)).toStrictEqual(
      [0x800a, 0xf2f1].toSorted((a, b) => a - b),
    );
  });
});
