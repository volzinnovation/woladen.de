import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeLiveApiBaseUrl,
  queryGermanLiveApiBaseUrl,
  queryLiveApiBaseUrl,
  resolveGermanLiveApiBaseUrl,
  resolveLiveApiBaseUrl,
} from "./live-api.mjs";

test("normalizeLiveApiBaseUrl trims and strips trailing slashes", () => {
  assert.equal(normalizeLiveApiBaseUrl(" http://127.0.0.1:8001/ "), "http://127.0.0.1:8001");
  assert.equal(normalizeLiveApiBaseUrl("not-a-url"), "");
});

test("queryLiveApiBaseUrl reads the explicit local override", () => {
  assert.equal(
    queryLiveApiBaseUrl("http://127.0.0.1:4173/?station=abc&liveApiBaseUrl=http://127.0.0.1:8001"),
    "http://127.0.0.1:8001",
  );
});

test("queryGermanLiveApiBaseUrl reads the explicit German backend override", () => {
  assert.equal(
    queryGermanLiveApiBaseUrl("http://127.0.0.1:4173/?deLiveApiBaseUrl=http://127.0.0.1:8002"),
    "http://127.0.0.1:8002",
  );
});

test("query live API helpers ignore production-host overrides", () => {
  assert.equal(
    queryLiveApiBaseUrl("https://woladen.de/?liveApiBaseUrl=https://attacker.test"),
    "",
  );
  assert.equal(
    queryGermanLiveApiBaseUrl("https://www.woladen.de/?deLiveApiBaseUrl=https://attacker.test"),
    "",
  );
});

test("resolveLiveApiBaseUrl prefers query override over configured and host defaults", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "https://live-eu.woladen.de",
      locationHref: "http://127.0.0.1:4173/?liveApiBaseUrl=http://127.0.0.1:8001",
      locationHostname: "127.0.0.1",
    }),
    "http://127.0.0.1:8001",
  );
});

test("resolveLiveApiBaseUrl ignores query override on production hosts", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://woladen.de/?liveApiBaseUrl=https://attacker.test",
      locationHostname: "woladen.de",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveLiveApiBaseUrl keeps configured values when production query override is ignored", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "https://configured.example.test/api/",
      locationHref: "https://woladen.de/?liveApiBaseUrl=https://attacker.test",
      locationHostname: "woladen.de",
    }),
    "https://configured.example.test/api",
  );
});

test("resolveLiveApiBaseUrl keeps the live-eu production default for localhost without override", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "http://127.0.0.1:4173/",
      locationHostname: "127.0.0.1",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveLiveApiBaseUrl allows local query override without explicit hostname", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "http://localhost:4173/?liveApiBaseUrl=http://127.0.0.1:8001",
    }),
    "http://127.0.0.1:8001",
  );
});

test("resolveLiveApiBaseUrl maps the public site to live-eu", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://woladen.de/",
      locationHostname: "woladen.de",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveGermanLiveApiBaseUrl maps German live data to live-eu", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://woladen.de/",
      locationHostname: "woladen.de",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveGermanLiveApiBaseUrl ignores German-specific query override on production hosts", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://www.woladen.de/?deLiveApiBaseUrl=https://attacker.test",
      locationHostname: "www.woladen.de",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveGermanLiveApiBaseUrl ignores primary query fallback on production hosts", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://woladen.de/?liveApiBaseUrl=https://attacker.test",
      locationHostname: "woladen.de",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveGermanLiveApiBaseUrl falls back to the primary configured backend", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://example.test/",
      locationHostname: "example.test",
      primaryBaseUrl: "http://127.0.0.1:8010",
    }),
    "http://127.0.0.1:8010",
  );
});

test("resolveGermanLiveApiBaseUrl keeps local single-backend override behavior", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "http://127.0.0.1:4173/?liveApiBaseUrl=http://127.0.0.1:8001",
      locationHostname: "127.0.0.1",
    }),
    "http://127.0.0.1:8001",
  );
});

test("resolveGermanLiveApiBaseUrl prefers the German-specific override", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "http://127.0.0.1:4173/?liveApiBaseUrl=http://127.0.0.1:8001&deLiveApiBaseUrl=http://127.0.0.1:8002",
      locationHostname: "127.0.0.1",
    }),
    "http://127.0.0.1:8002",
  );
});
