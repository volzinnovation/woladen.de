const STATION_DETAIL_SCRIPT_PATTERN =
  /<script\b[^>]*\bid=["']station-detail-data["'][^>]*>([\s\S]*?)<\/script>/i;

export function staticStationPagePath(stationId) {
  const normalized = String(stationId || "").trim();
  const separatorIndex = normalized.indexOf(":");
  if (separatorIndex > 0 && /^[A-Z]{2}$/i.test(normalized.slice(0, separatorIndex))) {
    const namespace = normalized.slice(0, separatorIndex);
    const localId = normalized.slice(separatorIndex + 1);
    const encodedFilename = encodeURIComponent(encodeURIComponent(localId));
    return `./station/${encodeURIComponent(namespace)}/${encodedFilename}.html`;
  }
  return `./station/${encodeURIComponent(encodeURIComponent(normalized))}.html`;
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
