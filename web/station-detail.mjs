const STATION_DETAIL_SCRIPT_PATTERN =
  /<script\b[^>]*\bid=["']station-detail-data["'][^>]*>([\s\S]*?)<\/script>/i;

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
  if (hasBundledFeature) {
    return false;
  }
  const normalized = String(stationId || "").trim();
  const separatorIndex = normalized.indexOf(":");
  if (separatorIndex <= 0) {
    return true;
  }
  const namespace = normalized.slice(0, separatorIndex);
  if (!/^[A-Z]{2}$/i.test(namespace)) {
    return true;
  }
  // Germany retains its static-first deep-link behavior. Other country feeds
  // can expose live/API stations before the next static bundle publication,
  // so trying the catalog API first avoids a predictable missing-page 404.
  return namespace.toUpperCase() === "DE";
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
