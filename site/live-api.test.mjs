import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeLiveApiBaseUrl,
  queryLiveApiBaseUrl,
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

test("resolveLiveApiBaseUrl prefers query override over configured and host defaults", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "https://live.woladen.de",
      locationHref: "http://127.0.0.1:4173/?liveApiBaseUrl=http://127.0.0.1:8001",
      locationHostname: "127.0.0.1",
    }),
    "http://127.0.0.1:8001",
  );
});

test("resolveLiveApiBaseUrl keeps the existing production default for localhost without override", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "http://127.0.0.1:4173/",
      locationHostname: "127.0.0.1",
    }),
    "https://live.woladen.de",
  );
});
