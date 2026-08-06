const STATION_DETAIL_SCRIPT_PATTERN =
  /<script\b[^>]*\bid=["']station-detail-data["'][^>]*>([\s\S]*?)<\/script>/i;

export const QUERY_STATION_PAGE_PATH = "./station.html";

export function queryStationPagePath(stationId = "") {
  const normalized = String(stationId || "").trim();
  return normalized
    ? `${QUERY_STATION_PAGE_PATH}?station=${encodeURIComponent(normalized)}`
    : QUERY_STATION_PAGE_PATH;
}

export function staticStationPagePath(stationId) {
  const normalized = String(stationId || "").trim();
  const separatorIndex = normalized.indexOf(":");
  if (separatorIndex > 0 && /^[A-Z]{2}$/i.test(normalized.slice(0, separatorIndex))) {
    const namespace = normalized.slice(0, separatorIndex);
    const localId = normalized.slice(separatorIndex + 1);
    // Generated filenames contain an encoded local ID (for example
    // `provider%3Astations`). The URL must encode that literal `%` once more.
    const encodedFilename = encodeURIComponent(encodeURIComponent(localId));
    return `./station/${encodeURIComponent(namespace)}/${encodedFilename}.html`;
  }
  return `./station/${encodeURIComponent(encodeURIComponent(normalized))}.html`;
}

export function shouldPreferStaticStationDetail(
  stationId,
  { hasBundledFeature = false } = {},
) {
  // Station detail pages are now a single query-driven route. The catalog API
  // must be attempted first because there is no per-station HTML file to use
  // as a static-first fallback.
  return false;
}

export function parseEmbeddedStationDetailPayload(htmlText) {
  const match = STATION_DETAIL_SCRIPT_PATTERN.exec(String(htmlText || ""));
  if (!match) {
    throw new Error("station_detail_data_missing");
  }
  const payload = JSON.parse(match[1]);
  if (!payload || typeof payload !== "object" || !payload.station) {
    throw new Error("station_detail_data_invalid");
  }
  return payload;
}
