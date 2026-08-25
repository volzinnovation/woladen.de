export const QUERY_STATION_PAGE_PATH = "./station.html";

function normalizedText(value) {
  return String(value ?? "").trim();
}

export function distinctStationOperatorName(station = {}) {
  const stationName = normalizedText(station?.station_name);
  const hasExplicitOperatorName = Object.prototype.hasOwnProperty.call(station, "operator_name");
  const operatorName = normalizedText(
    hasExplicitOperatorName ? station.operator_name : station?.operator,
  );

  return stationName && operatorName && operatorName !== stationName
    ? operatorName
    : "";
}

export function queryStationPagePath(stationId = "") {
  const normalized = String(stationId || "").trim();
  return normalized
    ? `${QUERY_STATION_PAGE_PATH}?station=${encodeURIComponent(normalized)}`
    : QUERY_STATION_PAGE_PATH;
}
