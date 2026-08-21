export const LIVE_MANAGEMENT_API_BASE_URL =
  "https://live-eu.woladen.de/v1/management";

export function normalizeManagementApiBaseUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "");
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

async function fetchPaginatedRows(fetchImpl, baseUrl, query, pageSize = 1000) {
  const rows = [];
  let firstPayload = null;
  let offset = 0;
  while (true) {
    query.set("limit", String(pageSize));
    query.set("offset", String(offset));
    const payload = await fetchJson(fetchImpl, `${baseUrl}?${query.toString()}`);
    firstPayload ||= payload;
    const pageRows = Array.isArray(payload?.rows) ? payload.rows : [];
    rows.push(...pageRows);
    const totalRows = Number(
      payload?.total_rows ?? firstPayload?.total_rows ?? firstPayload?.rows?.[0]?.total_rows ?? 0,
    );
    if (
      pageRows.length < pageSize ||
      (Number.isFinite(totalRows) && totalRows > 0 && rows.length >= totalRows)
    ) {
      break;
    }
    offset += pageSize;
  }
  return { ...(firstPayload || {}), rows };
}

export function createManagementDataSource({
  apiBaseUrl = LIVE_MANAGEMENT_API_BASE_URL,
  fetchImpl = globalThis.fetch,
} = {}) {
  const baseUrl = normalizeManagementApiBaseUrl(apiBaseUrl) || LIVE_MANAGEMENT_API_BASE_URL;

  const loadIndex = () => fetchJson(fetchImpl, `${baseUrl}/dashboard/index`);
  const loadCountries = () => fetchJson(fetchImpl, `${baseUrl}/countries`);

  async function loadCountryOverview(
    dateText,
    { startDate = dateText, endDate = dateText } = {},
  ) {
    const query = new URLSearchParams({
      start_date: startDate,
      end_date: endDate,
      group_by: "country",
      limit: "100",
      offset: "0",
    });
    return await fetchJson(fetchImpl, `${baseUrl}/report?${query.toString()}`);
  }

  async function loadSnapshot(
    dateText,
    { countryCode = "", providerUid = "", trendDays = 90 } = {},
  ) {
    const query = new URLSearchParams({
      archive_date: dateText,
      station_limit: "10",
      trend_days: String(trendDays),
    });
    if (countryCode) query.set("country_code", countryCode);
    if (providerUid) query.set("provider_uid", providerUid);
    const snapshot = await fetchJson(fetchImpl, `${baseUrl}/dashboard?${query.toString()}`);
    return {
      snapshot,
      trends: { summary_series: snapshot.summary_series || [] },
      source: "live-eu",
    };
  }

  async function loadReport({
    startDate,
    endDate,
    countryCode = "",
    providerUid = "",
    groupBy = "provider",
  }) {
    const query = new URLSearchParams({
      start_date: startDate,
      end_date: endDate,
      group_by: groupBy,
    });
    if (countryCode) query.set("country_code", countryCode);
    if (providerUid) query.set("provider_uid", providerUid);
    return await fetchPaginatedRows(fetchImpl, `${baseUrl}/report`, query);
  }

  async function loadProviderHealth({
    startDate,
    endDate,
    countryCode = "",
    providerUid = "",
  }) {
    const query = new URLSearchParams({
      start_date: startDate,
      end_date: endDate,
      limit: "1000",
      offset: "0",
    });
    if (countryCode) query.set("country_code", countryCode);
    if (providerUid) query.set("provider_uid", providerUid);
    return await fetchJson(fetchImpl, `${baseUrl}/provider-health?${query.toString()}`);
  }

  async function loadProfile({
    startDate,
    endDate,
    countryCode = "",
    providerUid = "",
    groupBy = "hour",
  }) {
    const query = new URLSearchParams({
      start_date: startDate,
      end_date: endDate,
      group_by: groupBy,
    });
    if (countryCode) query.set("country_code", countryCode);
    if (providerUid) query.set("provider_uid", providerUid);
    return await fetchJson(fetchImpl, `${baseUrl}/profile?${query.toString()}`);
  }

  return {
    loadCountries,
    loadCountryOverview,
    loadIndex,
    loadSnapshot,
    loadReport,
    loadProviderHealth,
    loadProfile,
    currentSource: () => "live-eu",
  };
}
