export function normalizeRating(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  const rounded = Math.round(numeric);
  return rounded >= 1 && rounded <= 5 ? rounded : 0;
}

export function parseStoredRatings(raw) {
  const ratings = new Map();
  if (!raw) {
    return ratings;
  }

  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    return ratings;
  }

  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return ratings;
  }

  Object.entries(payload).forEach(([stationId, value]) => {
    const id = String(stationId || "").trim();
    const rating = normalizeRating(value);
    if (id && rating > 0) {
      ratings.set(id, rating);
    }
  });
  return ratings;
}

export function serializeStoredRatings(ratings) {
  const entries = Array.from(ratings instanceof Map ? ratings.entries() : [])
    .map(([stationId, value]) => [String(stationId || "").trim(), normalizeRating(value)])
    .filter(([stationId, rating]) => stationId && rating > 0)
    .sort(([left], [right]) => left.localeCompare(right));

  return JSON.stringify(Object.fromEntries(entries));
}

export function getUserRating(ratings, stationId) {
  const id = String(stationId || "").trim();
  if (!id || !(ratings instanceof Map)) {
    return 0;
  }
  return normalizeRating(ratings.get(id));
}

export function formatRatingValue(rating) {
  const numeric = Number(rating);
  if (!Number.isFinite(numeric) || numeric < 1 || numeric > 5) {
    return "";
  }
  return numeric.toFixed(1).replace(".", ",");
}

export function normalizeRatingSummary(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  const stationId = String(value.station_id || "").trim();
  const averageRating = Number(value.average_rating);
  const ratingCount = Math.round(Number(value.rating_count || 0));
  if (
    !stationId ||
    !Number.isFinite(averageRating) ||
    averageRating < 1 ||
    averageRating > 5 ||
    !Number.isFinite(ratingCount) ||
    ratingCount <= 0
  ) {
    return null;
  }
  return {
    station_id: stationId,
    average_rating: averageRating,
    rating_count: ratingCount,
  };
}

export function formatRatingCount(count) {
  const numeric = Math.round(Number(count || 0));
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "";
  }
  return `${numeric} ${numeric === 1 ? "Bewertung" : "Bewertungen"}`;
}
