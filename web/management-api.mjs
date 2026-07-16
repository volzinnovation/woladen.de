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

  async function loadStaticIndex() {
    source = "static-cache";
    return await fetchJson(fetchImpl, staticIndexPath);
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

  async function loadSnapshot(dateText) {
    if (source === "postgresql") {
      try {
        const query = new URLSearchParams({
          archive_date: dateText,
          station_limit: "10",
          trend_days: "90",
        });
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
    const [snapshot, trends] = await Promise.all([
      fetchJson(fetchImpl, staticSnapshotPath(dateText)),
      staticTrends
        ? Promise.resolve(staticTrends)
        : fetchJson(fetchImpl, staticTrendsPath),
    ]);
    staticTrends = trends;
    return { snapshot, trends, source };
  }

  return {
    loadIndex,
    loadSnapshot,
    currentSource: () => source,
  };
}
