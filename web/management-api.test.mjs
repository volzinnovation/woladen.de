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
    fetchImpl: async (url, options) => {
      requests.push({ url, options });
      if (url.endsWith("/dashboard/index")) {
        return response({ source: "postgresql", available_dates: ["2026-07-15"] });
      }
      if (url.endsWith("/countries")) {
        return response({ countries: [{ country_code: "DE" }] });
      }
      if (url.includes("/provider-health?")) {
        return response({ rows: [{ provider_uid: "provider" }] });
      }
      if (url.includes("/profile?")) {
        return response({ rows: [{ local_hour: 12 }] });
      }
      if (url.includes("/report?")) {
        return response({ rows: [{ country_code: "DE" }] });
      }
      return response({ source: "postgresql", summary_series: [{ snapshot_date: "2026-07-15" }] });
    },
  });

  const index = await source.loadIndex();
  const countries = await source.loadCountries();
  const overview = await source.loadCountryOverview("2026-07-15", {
    startDate: "2026-07-02",
    endDate: "2026-07-15",
  });
  const loaded = await source.loadSnapshot("2026-07-15", {
    countryCode: "DE",
    providerUid: "chargecloud",
    trendDays: 28,
  });
  const report = await source.loadReport({
    startDate: "2026-06-18",
    endDate: "2026-07-15",
    countryCode: "DE",
    providerUid: "chargecloud",
  });
  const health = await source.loadProviderHealth({
    startDate: "2026-06-18",
    endDate: "2026-07-15",
    countryCode: "DE",
    providerUid: "chargecloud",
  });
  const profile = await source.loadProfile({
    startDate: "2026-06-18",
    endDate: "2026-07-15",
    countryCode: "DE",
    providerUid: "chargecloud",
  });
  assert.equal(index.source, "postgresql");
  assert.equal(countries.countries[0].country_code, "DE");
  assert.equal(overview.rows[0].country_code, "DE");
  assert.match(requests[2].url, /start_date=2026-07-02/);
  assert.match(requests[2].url, /end_date=2026-07-15/);
  assert.equal(loaded.source, "live-eu");
  assert.equal(loaded.trends.summary_series.length, 1);
  assert.equal(report.rows[0].country_code, "DE");
  assert.equal(health.rows[0].provider_uid, "provider");
  assert.equal(profile.rows[0].local_hour, 12);
  assert.match(requests[3].url, /dashboard\?archive_date=2026-07-15/);
  assert.match(requests[3].url, /country_code=DE/);
  assert.match(requests[3].url, /provider_uid=chargecloud/);
  assert.match(requests[3].url, /trend_days=28/);
  assert.match(requests[4].url, /provider_uid=chargecloud/);
  assert.match(requests[6].url, /profile\?/);
  assert.equal(requests[0].options.credentials, "include");
  assert.equal(requests[0].options.headers.Authorization, undefined);
});

test("live reports load every API page", async () => {
  const offsets = [];
  const source = createManagementDataSource({
    apiBaseUrl: "https://live-eu.woladen.de/v1/management",
    fetchImpl: async (url) => {
      const parsed = new URL(url);
      const offset = Number(parsed.searchParams.get("offset"));
      offsets.push(offset);
      if (offset === 0) {
        return response({
          rows: Array.from({ length: 1000 }, (_, index) => ({
            operator_name: `Operator ${index}`,
            total_rows: 1002,
          })),
        });
      }
      return response({
        rows: [
          { operator_name: "Operator 1000", total_rows: 1002 },
          { operator_name: "Operator 1001", total_rows: 1002 },
        ],
      });
    },
  });

  const report = await source.loadReport({
    startDate: "2026-07-24",
    endDate: "2026-07-24",
    countryCode: "DE",
    groupBy: "operator",
  });

  assert.deepEqual(offsets, [0, 1000]);
  assert.equal(report.rows.length, 1002);
  assert.equal(report.rows.at(-1).operator_name, "Operator 1001");
});
