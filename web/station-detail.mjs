export const QUERY_STATION_PAGE_PATH = "./station.html";

export function queryStationPagePath(stationId = "") {
  const normalized = String(stationId || "").trim();
  return normalized
    ? `${QUERY_STATION_PAGE_PATH}?station=${encodeURIComponent(normalized)}`
    : QUERY_STATION_PAGE_PATH;
}
