export const SPA_RATING_TIER_GOLD = "gold";
export const SPA_RATING_TIER_SILVER = "silver";
export const SPA_RATING_TIER_BRONZE = "bronze";
export const SPA_RATING_TIER_STANDARD = "standard";

export function normalizeSpaRating(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric >= 1 && numeric <= 5
    ? numeric
    : null;
}

export function getSpaRatingTier(value) {
  const rating = normalizeSpaRating(value);
  if (rating === null) {
    return SPA_RATING_TIER_STANDARD;
  }
  if (rating > 4.5) {
    return SPA_RATING_TIER_GOLD;
  }
  if (rating > 4.25) {
    return SPA_RATING_TIER_SILVER;
  }
  if (rating > 4) {
    return SPA_RATING_TIER_BRONZE;
  }
  return SPA_RATING_TIER_STANDARD;
}
