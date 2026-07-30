import assert from "node:assert/strict";
import test from "node:test";

import {
  SPA_RATING_TIER_BRONZE,
  SPA_RATING_TIER_GOLD,
  SPA_RATING_TIER_SILVER,
  SPA_RATING_TIER_STANDARD,
  getSpaRatingTier,
  normalizeSpaRating,
} from "./spa-rating.mjs";

test("normalizes valid Google Maps averages", () => {
  assert.equal(normalizeSpaRating("4.3"), 4.3);
  assert.equal(normalizeSpaRating(5), 5);
  assert.equal(normalizeSpaRating(null), null);
  assert.equal(normalizeSpaRating(""), null);
  assert.equal(normalizeSpaRating(5.1), null);
});

test("uses strict spa medal thresholds", () => {
  assert.equal(getSpaRatingTier(4), SPA_RATING_TIER_STANDARD);
  assert.equal(getSpaRatingTier(4.01), SPA_RATING_TIER_BRONZE);
  assert.equal(getSpaRatingTier(4.25), SPA_RATING_TIER_BRONZE);
  assert.equal(getSpaRatingTier(4.26), SPA_RATING_TIER_SILVER);
  assert.equal(getSpaRatingTier(4.5), SPA_RATING_TIER_SILVER);
  assert.equal(getSpaRatingTier(4.51), SPA_RATING_TIER_GOLD);
  assert.equal(getSpaRatingTier(null), SPA_RATING_TIER_STANDARD);
});
