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

test("resolveGermanLiveApiBaseUrl maps German live data to live.woladen.de", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://woladen.de/",
      locationHostname: "woladen.de",
    }),
    "https://live.woladen.de",
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
