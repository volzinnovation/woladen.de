import assert from "node:assert/strict";
import test from "node:test";

import {
  createManagementDataSource,
  normalizeManagementApiBaseUrl,
} from "./management-api.mjs";

function response(payload, status = 200) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async json() {
      return payload;
    },
  };
}

test("management API base URL normalization removes trailing slashes", () => {
  assert.equal(
    normalizeManagementApiBaseUrl(" https://live-eu.woladen.de/v1/management/// "),
    "https://live-eu.woladen.de/v1/management",
  );
});

test("live management source queries PostgreSQL dashboard contracts", async () => {
  const requests = [];
  const source = createManagementDataSource({
    apiBaseUrl: "https://live-eu.woladen.de/v1/management/",
    staticFallbackEnabled: false,
    fetchImpl: async (url, options) => {
      requests.push({ url, options });
      if (url.endsWith("/dashboard/index")) {
        return response({ source: "postgresql", available_dates: ["2026-07-15"] });
      }
      return response({ source: "postgresql", summary_series: [{ snapshot_date: "2026-07-15" }] });
    },
  });

  const index = await source.loadIndex();
  const loaded = await source.loadSnapshot("2026-07-15");
  assert.equal(index.source, "postgresql");
  assert.equal(loaded.source, "postgresql");
  assert.equal(loaded.trends.summary_series.length, 1);
  assert.match(requests[1].url, /dashboard\?archive_date=2026-07-15/);
  assert.equal(requests[0].options.credentials, "include");
  assert.equal(requests[0].options.headers.Authorization, undefined);
});

test("unavailable live endpoint falls back to the explicit static cache", async () => {
  const requests = [];
  const source = createManagementDataSource({
    apiBaseUrl: "/api/management",
    staticFallbackEnabled: true,
    fetchImpl: async (url) => {
      requests.push(url);
      if (url === "/api/management/dashboard/index") {
        return response({}, 503);
      }
      if (url.endsWith("index.json")) {
        return response({ available_dates: ["2026-07-14"] });
      }
      if (url.endsWith("trends.json")) {
        return response({ summary_series: [] });
      }
      return response({ snapshot_date: "2026-07-14" });
    },
  });

  const index = await source.loadIndex();
  const loaded = await source.loadSnapshot("2026-07-14");
  assert.deepEqual(index.available_dates, ["2026-07-14"]);
  assert.equal(loaded.source, "static-cache");
  assert.equal(requests.includes("./data/management/index.json"), true);
});
