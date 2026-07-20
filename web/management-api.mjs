const DEFAULT_STATIC_INDEX_PATH = "./data/management/index.json";
const DEFAULT_STATIC_TRENDS_PATH = "./data/management/trends.json";

export function normalizeManagementApiBaseUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function staticSnapshotPath(dateText) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(dateText || ""));
  if (!match) {
    throw new Error("Ungültiges Auswertungsdatum.");
  }
  return `./data/management/days/${match[1]}/${match[2]}/${match[3]}/snapshot.json`;
}

function staticTrendsForWindow(trends, endDate, trendDays) {
  const end = new Date(`${endDate}T00:00:00Z`);
  const days = Number(trendDays);
  if (Number.isNaN(end.getTime()) || !Number.isInteger(days) || days < 1) {
    return trends;
  }
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - days + 1);
  const startDate = start.toISOString().slice(0, 10);
  return {
    ...trends,
    summary_series: (trends?.summary_series || []).filter(
      (row) => row?.snapshot_date >= startDate && row?.snapshot_date <= endDate,
    ),
  };
}

async function fetchJson(fetchImpl, url) {
  const response = await fetchImpl(url, {
    cache: "no-store",
    credentials: "include",
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${url}`);
  }
  return await response.json();
}

export function createManagementDataSource({
  apiBaseUrl = "",
  staticFallbackEnabled = true,
  fetchImpl = globalThis.fetch,
  staticIndexPath = DEFAULT_STATIC_INDEX_PATH,
  staticTrendsPath = DEFAULT_STATIC_TRENDS_PATH,
} = {}) {
  const normalizedApiBaseUrl = normalizeManagementApiBaseUrl(apiBaseUrl);
  let source = normalizedApiBaseUrl ? "postgresql" : "static-cache";
  let staticTrends = null;
  let staticIndex = null;

  async function loadStaticIndex() {
    source = "static-cache";
    staticIndex = staticIndex || (await fetchJson(fetchImpl, staticIndexPath));
    return staticIndex;
  }

  async function loadIndex() {
    if (source === "postgresql") {
      try {
        return await fetchJson(fetchImpl, `${normalizedApiBaseUrl}/dashboard/index`);
      } catch (error) {
        if (!staticFallbackEnabled) {
          throw error;
        }
      }
    }
    return await loadStaticIndex();
  }

  async function loadCountries() {
    if (source === "postgresql") {
      try {
        return await fetchJson(fetchImpl, `${normalizedApiBaseUrl}/countries`);
      } catch (error) {
        if (!staticFallbackEnabled) {
          throw error;
        }
      }
    }
    const index = await loadStaticIndex();
    const dates = Array.isArray(index.available_dates) ? index.available_dates : [];
    return {
      schema_version: "management-static-country-fallback-v1",
      countries: dates.length
        ? [
            {
              country_code: "DE",
              first_date: dates[0],
              last_date: dates.at(-1),
              observed_days: dates.length,
            },
          ]
        : [],
    };
  }

  async function loadCountryOverview(
    dateText,
    { startDate = dateText, endDate = dateText } = {},
  ) {
    if (source === "postgresql") {
      try {
        const query = new URLSearchParams({
          start_date: startDate,
          end_date: endDate,
          group_by: "country",
          limit: "100",
          offset: "0",
        });
        return await fetchJson(fetchImpl, `${normalizedApiBaseUrl}/report?${query.toString()}`);
      } catch (error) {
        if (!staticFallbackEnabled) {
          throw error;
        }
        source = "static-cache";
      }
    }
    const snapshot = await fetchJson(fetchImpl, staticSnapshotPath(dateText));
    return {
      schema_version: "management-static-country-overview-v1",
      start_date: dateText,
      end_date: dateText,
      rows: [
        {
          country_code: "DE",
          ...(snapshot.summary || {}),
          station_count:
            snapshot.summary?.station_count ??
            snapshot.summary?.daily_afir_stations_observed ??
            snapshot.summary?.afir_stations_observed,
          observed_evses:
            snapshot.summary?.observed_evses ??
            snapshot.summary?.daily_afir_stations_observed ??
            snapshot.summary?.afir_stations_observed,
        },
      ],
    };
  }

  async function loadSnapshot(
    dateText,
    { countryCode = "", providerUid = "", trendDays = 90 } = {},
  ) {
    if (source === "postgresql") {
      try {
        const query = new URLSearchParams({
          archive_date: dateText,
          station_limit: "10",
          trend_days: String(trendDays),
        });
        if (countryCode) {
          query.set("country_code", countryCode);
        }
        if (providerUid) {
          query.set("provider_uid", providerUid);
        }
        const snapshot = await fetchJson(
          fetchImpl,
          `${normalizedApiBaseUrl}/dashboard?${query.toString()}`,
        );
        return {
          snapshot,
          trends: { summary_series: snapshot.summary_series || [] },
          source,
        };
      } catch (error) {
        if (!staticFallbackEnabled) {
          throw error;
        }
        source = "static-cache";
      }
    }
    if (providerUid) {
      throw new Error("Für Anbieteransichten ist keine statische Ersatzauswertung verfügbar.");
    }
    if (countryCode && countryCode !== "DE") {
      throw new Error(`Für ${countryCode} ist keine statische Ersatzauswertung verfügbar.`);
    }
    const [snapshot, trends] = await Promise.all([
      fetchJson(fetchImpl, staticSnapshotPath(dateText)),
      staticTrends
        ? Promise.resolve(staticTrends)
        : fetchJson(fetchImpl, staticTrendsPath),
    ]);
    staticTrends = trends;
    return {
      snapshot,
      trends: staticTrendsForWindow(trends, dateText, trendDays),
      source,
    };
  }

  async function loadReport({
    startDate,
    endDate,
    countryCode = "",
    providerUid = "",
    groupBy = "provider",
  }) {
    if (source !== "postgresql") {
      return {
        schema_version: "management-static-report-unavailable-v1",
        start_date: startDate,
        end_date: endDate,
        group_by: groupBy,
        rows: [],
      };
    }
    const query = new URLSearchParams({
      start_date: startDate,
      end_date: endDate,
      group_by: groupBy,
      limit: "1000",
      offset: "0",
    });
    if (countryCode) {
      query.set("country_code", countryCode);
    }
    if (providerUid) {
      query.set("provider_uid", providerUid);
    }
    return await fetchJson(fetchImpl, `${normalizedApiBaseUrl}/report?${query.toString()}`);
  }

  async function loadProviderHealth({
    startDate,
    endDate,
    countryCode = "",
    providerUid = "",
  }) {
    if (source !== "postgresql") {
      return {
        schema_version: "management-static-provider-health-unavailable-v1",
        start_date: startDate,
        end_date: endDate,
        rows: [],
      };
    }
    const query = new URLSearchParams({
      start_date: startDate,
      end_date: endDate,
      limit: "1000",
      offset: "0",
    });
    if (countryCode) {
      query.set("country_code", countryCode);
    }
    if (providerUid) {
      query.set("provider_uid", providerUid);
    }
    return await fetchJson(fetchImpl, `${normalizedApiBaseUrl}/provider-health?${query.toString()}`);
  }

  async function loadProfile({
    startDate,
    endDate,
    countryCode = "",
    providerUid = "",
    groupBy = "hour",
  }) {
    if (source !== "postgresql") {
      return {
        schema_version: "management-static-profile-unavailable-v1",
        start_date: startDate,
        end_date: endDate,
        group_by: groupBy,
        rows: [],
      };
    }
    const query = new URLSearchParams({
      start_date: startDate,
      end_date: endDate,
      group_by: groupBy,
    });
    if (countryCode) {
      query.set("country_code", countryCode);
    }
    if (providerUid) {
      query.set("provider_uid", providerUid);
    }
    return await fetchJson(fetchImpl, `${normalizedApiBaseUrl}/profile?${query.toString()}`);
  }

  return {
    loadCountries,
    loadCountryOverview,
    loadIndex,
    loadSnapshot,
    loadReport,
    loadProviderHealth,
    loadProfile,
    currentSource: () => source,
  };
}
