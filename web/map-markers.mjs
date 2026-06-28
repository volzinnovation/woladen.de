import { normalizeOutOfOrderProbabilityStatus } from "./occupancy-api.mjs";

const MARKER_COLORS = {
  gold: "#f59e0b",
  silver: "#94a3b8",
  bronze: "#b45309",
  grey: "#64748b",
};
const OUT_OF_ORDER_MARKER_LABELS = {
  mostly_broken: "Meist defekt",
  often_broken: "Oft defekt",
  sometimes_broken: "Manchmal defekt",
};

export function getStationMarkerColor(props = {}) {
  const total = Number(props?.amenities_total || 0);
  if (total > 10) return MARKER_COLORS.gold;
  if (total > 5) return MARKER_COLORS.silver;
  if (total > 0) return MARKER_COLORS.bronze;
  return MARKER_COLORS.grey;
}

export function hasLiveStationData(props = {}) {
  const total = Number(props?.live_total_evses || 0);
  const liveTimestamp = String(
    props?.live_source_observed_at ||
    props?.live_fetched_at ||
    props?.live_ingested_at ||
    "",
  ).trim();
  return Boolean(liveTimestamp) || (Number.isFinite(total) && total > 0);
}

export function stationMarkerViewModel(props = {}) {
  const hasLiveData = hasLiveStationData(props);
  const outOfOrderProbabilityStatus = normalizeOutOfOrderProbabilityStatus(
    props?.occupancy_out_of_order_probability_status,
    props?.occupancy_out_of_order_probability,
  );
  const stationName = String(props?.operator || props?.station_name || "Ladestation").trim();
  const city = String(props?.city || "").trim();
  const titleParts = [`${stationName}${city ? `, ${city}` : ""}`];
  if (outOfOrderProbabilityStatus) {
    titleParts.push(OUT_OF_ORDER_MARKER_LABELS[outOfOrderProbabilityStatus] || "Defekt-Hinweis");
  }
  const classNames = [hasLiveData ? "station-marker-live" : "station-marker-static"];
  if (outOfOrderProbabilityStatus) {
    classNames.push("station-marker-outage-risk", `station-marker-outage-risk-${outOfOrderProbabilityStatus}`);
  }
  return {
    color: getStationMarkerColor(props),
    hasLiveData,
    outOfOrderProbabilityStatus,
    state: hasLiveData ? "live" : "static",
    title: titleParts.join(" · "),
    className: classNames.join(" "),
  };
}
