import test from "node:test";
import assert from "node:assert/strict";

import {
  FAVORITE_SOURCE_MIGRATION,
  addFavoriteCategory,
  favoriteCategorySuggestions,
  getFavoriteCategories,
  getFavoriteStationIds,
  migrateLegacyFavorites,
  normalizeFavoriteCategories,
  parseStoredFavoriteMetadata,
  removeFavoriteCategory,
  removeFavoriteItem,
  serializeFavoriteMetadata,
} from "./favorite-metadata.mjs";

function normalizeStationId(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  if (/^[0-9a-f]{16}$/i.test(raw)) {
    return `DE:${raw.toLowerCase()}`;
  }
  const match = raw.match(/^([a-z]{2}):(.+)$/i);
  return match ? `${match[1].toUpperCase()}:${match[2]}` : raw;
}

test("legacy favorites migrate to v2 metadata without deleting categories later", () => {
  const metadata = migrateLegacyFavorites(
    JSON.stringify(["884BD7B49EF38349", "DE:884bd7b49ef38349", "", "NL:station-1"]),
    {
      normalizeStationId,
      now: "2026-06-22T12:00:00Z",
    },
  );

  assert.deepEqual(Array.from(getFavoriteStationIds(metadata)), [
    "DE:884bd7b49ef38349",
    "NL:station-1",
  ]);
  assert.deepEqual(metadata.items.get("DE:884bd7b49ef38349"), {
    station_id: "DE:884bd7b49ef38349",
    categories: [],
    created_at: "2026-06-22T12:00:00.000Z",
    updated_at: "2026-06-22T12:00:00.000Z",
    source: FAVORITE_SOURCE_MIGRATION,
  });
});

test("stored v2 metadata ignores malformed items and normalizes categories", () => {
  const metadata = parseStoredFavoriteMetadata(
    JSON.stringify({
      version: 2,
      items: {
        one: {
          station_id: "DE:station-1",
          categories: [" Home ", "home", "Route   to   Paris", "", "x".repeat(80)],
          created_at: "bad-date",
          updated_at: "2026-06-22T12:05:00Z",
          source: "route",
        },
        bad: {
          categories: ["Work"],
        },
      },
    }),
    {
      normalizeStationId,
      now: "2026-06-22T12:00:00Z",
    },
  );

  assert.equal(metadata.items.size, 1);
  assert.deepEqual(metadata.items.get("DE:station-1").categories, [
    "Home",
    "Route to Paris",
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  ]);
  assert.equal(metadata.items.get("DE:station-1").created_at, "2026-06-22T12:00:00.000Z");
  assert.equal(metadata.items.get("DE:station-1").updated_at, "2026-06-22T12:05:00Z");
});

test("category normalization preserves existing display casing", () => {
  assert.deepEqual(
    normalizeFavoriteCategories(["home", " HOME ", "work"], ["Home", "Route to Paris"]),
    ["Home", "work"],
  );
});

test("category suggestions prefer prefix matches before substrings", () => {
  const metadata = migrateLegacyFavorites(JSON.stringify(["DE:one", "DE:two", "DE:three", "DE:four"]));
  addFavoriteCategory(metadata, "DE:one", "Home");
  addFavoriteCategory(metadata, "DE:two", "Route to Paris");
  addFavoriteCategory(metadata, "DE:three", "Workshop");
  addFavoriteCategory(metadata, "DE:four", "After work");

  assert.deepEqual(favoriteCategorySuggestions(metadata, "wo"), ["Workshop", "After work"]);
  assert.deepEqual(favoriteCategorySuggestions(metadata, "o"), [
    "After work",
    "Home",
    "Route to Paris",
    "Workshop",
  ]);
  assert.deepEqual(favoriteCategorySuggestions(metadata, "", { exclude: ["home"] }), [
    "After work",
    "Route to Paris",
    "Workshop",
  ]);
});

test("removing one category keeps the favorite item", () => {
  const metadata = migrateLegacyFavorites(JSON.stringify(["DE:one"]));
  addFavoriteCategory(metadata, "DE:one", "Home");
  addFavoriteCategory(metadata, "DE:one", "Work");
  removeFavoriteCategory(metadata, "DE:one", "Home");

  assert.equal(getFavoriteStationIds(metadata).has("DE:one"), true);
  assert.deepEqual(metadata.items.get("DE:one").categories, ["Work"]);
  assert.deepEqual(getFavoriteCategories(metadata), ["Work"]);

  removeFavoriteItem(metadata, "DE:one");
  assert.equal(getFavoriteStationIds(metadata).has("DE:one"), false);
});

test("serializing v2 metadata produces a stable storage payload", () => {
  const metadata = migrateLegacyFavorites(JSON.stringify(["DE:b", "DE:a"]), {
    now: "2026-06-22T12:00:00Z",
  });
  addFavoriteCategory(metadata, "DE:b", "Home", { now: "2026-06-22T12:05:00Z" });

  assert.deepEqual(JSON.parse(serializeFavoriteMetadata(metadata)), {
    version: 2,
    items: {
      "DE:a": {
        station_id: "DE:a",
        categories: [],
        created_at: "2026-06-22T12:00:00.000Z",
        updated_at: "2026-06-22T12:00:00.000Z",
        source: "migration",
      },
      "DE:b": {
        station_id: "DE:b",
        categories: ["Home"],
        created_at: "2026-06-22T12:00:00.000Z",
        updated_at: "2026-06-22T12:05:00.000Z",
        source: "migration",
      },
    },
  });
});
