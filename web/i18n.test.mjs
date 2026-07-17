import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const I18N_MODULE_URL = new URL("./i18n.mjs", import.meta.url);
const INDEX_HTML_URL = new URL("./index.html", import.meta.url);
const STYLES_URL = new URL("./styles.css", import.meta.url);
const DEFAULT_META = {
  title: "woladen - Smart EV Stops in Europe",
  description: "Find available chargers near great bakeries, restaurants, shops, playgrounds and cafés. Because charging time should be time well spent.",
  ogTitle: "woladen - Smart EV Stops in Europe",
  ogDescription: "Find available chargers near great bakeries, restaurants, shops, playgrounds and cafés. Because charging time should be time well spent.",
  socialAlt: "woladen preview with a Europe charger map and the slogan: The human side of charging.",
};

function readText(url) {
  return fs.readFileSync(url, "utf8");
}

function fallbackBundle() {
  const source = readText(I18N_MODULE_URL);
  const match = source.match(/const FALLBACK_BUNDLE = (\{[\s\S]*?\n\});/);
  assert.ok(match, "FALLBACK_BUNDLE should be parseable");
  return vm.runInNewContext(`(${match[1].replace(/;$/, "")})`);
}

function flattenKeys(value, prefix = "") {
  return Object.entries(value).flatMap(([key, child]) => {
    const next = prefix ? `${prefix}.${key}` : key;
    if (child && typeof child === "object" && !Array.isArray(child)) {
      return flattenKeys(child, next);
    }
    return next;
  });
}

function readBundle(language) {
  return JSON.parse(readText(new URL(`./i18n/${language}.json`, import.meta.url)));
}

test("German and Dutch app bundles cover all fallback UI keys", () => {
  const fallbackKeys = flattenKeys(fallbackBundle()).sort();
  for (const language of ["de", "nl"]) {
    const translatedKeys = new Set(flattenKeys(readBundle(language)));
    const missing = fallbackKeys.filter((key) => !translatedKeys.has(key));
    assert.deepEqual(missing, [], `${language}.json is missing fallback keys`);
  }
});

test("localized route bundles include route action labels", () => {
  const routeActionKeys = [
    "addAllFavorites",
    "addAllFavoritesWithCount",
    "favoriteCategory",
    "favoritesAdded",
    "mapFixed",
    "removeRoute",
  ];
  const bundleFiles = fs
    .readdirSync(new URL("./i18n", import.meta.url))
    .filter((filename) => filename.endsWith(".json"));
  for (const filename of bundleFiles) {
    const route = readBundle(filename.replace(/\.json$/, "")).route || {};
    const missing = routeActionKeys.filter((key) => !route[key]);
    assert.deepEqual(missing, [], `${filename} is missing route action labels`);
  }
});

test("info about intro stays factual across localized bundles", () => {
  const fallbackIntro = fallbackBundle().info.aboutIntro;
  assert.equal(fallbackIntro, "woladen knows");
  const bundleFiles = fs
    .readdirSync(new URL("./i18n", import.meta.url))
    .filter((filename) => filename.endsWith(".json"));
  for (const filename of bundleFiles) {
    const intro = readBundle(filename.replace(/\.json$/, "")).info?.aboutIntro;
    if (!intro) {
      continue;
    }
    assert.ok(intro.length <= 24, `${filename} info.aboutIntro should stay concise`);
    assert.doesNotMatch(intro, /[.!?]/, `${filename} info.aboutIntro should not include marketing sentences`);
  }
});

test("occupancy history note is bound to the translation bundle", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  const appJs = readText(new URL("./app.js", import.meta.url));
  assert.match(indexHtml, /<p class="detail-subnote compact" data-i18n="station\.typicalOccupancyNote">/);
  assert.match(indexHtml, /data-i18n-aria-label="rating\.ariaLabel"/);
  assert.doesNotMatch(appJs, /occupancy-history-value/);
});

test("occupancy history layout keeps desktop bars visible", () => {
  const styles = readText(STYLES_URL);
  assert.match(styles, /\.occupancy-history-label\s*\{[\s\S]*?grid-row:\s*2;/);
  assert.match(styles, /\.occupancy-history-track\s*\{[\s\S]*?grid-row:\s*1;[\s\S]*?height:\s*100%;/);
  const mobileBlock = styles.match(/@media \(max-width: 640px\) \{[\s\S]*?\/\* INFO VIEW \*\//)?.[0] || "";
  assert.match(mobileBlock, /\.occupancy-history-label\s*\{[\s\S]*?grid-column:\s*1;[\s\S]*?grid-row:\s*1;/);
  assert.match(mobileBlock, /\.occupancy-history-track\s*\{[\s\S]*?grid-column:\s*2;[\s\S]*?grid-row:\s*1;/);
});

test("map app install promo keeps its dismiss button clickable", () => {
  const styles = readText(STYLES_URL);
  assert.match(styles, /\.map-controls-overlay > \.app-install-promo\s*\{[\s\S]*?pointer-events:\s*auto;/);
});

test("detail view orders amenities, live status, notes, and details", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  const addressIndex = indexHtml.indexOf('id="detail-address"');
  const navigationIndex = indexHtml.indexOf('class="detail-actions"');
  const amenitiesIndex = indexHtml.indexOf('id="detail-amenities-title"');
  const liveIndex = indexHtml.indexOf('id="detail-live-section"');
  const noteIndex = indexHtml.indexOf('class="detail-note"');
  const detailsIndex = indexHtml.indexOf('id="detail-details-section"');
  assert.ok(addressIndex > -1, "address exists");
  assert.ok(navigationIndex > -1, "navigation actions exist");
  assert.ok(amenitiesIndex > -1, "amenities section exists");
  assert.ok(liveIndex > -1, "live section exists");
  assert.ok(noteIndex > -1, "note section exists");
  assert.ok(detailsIndex > -1, "details section exists");
  assert.ok(addressIndex < navigationIndex, "navigation should come after address");
  assert.ok(navigationIndex < amenitiesIndex, "navigation should come before amenities");
  assert.ok(amenitiesIndex < liveIndex, "amenities should come before live status");
  assert.ok(liveIndex < noteIndex, "personal note should come after live status");
  assert.ok(noteIndex < detailsIndex, "personal note should come before details");
});

test("favorite control sits before the detail title and favorites use map star markers", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  const appJs = readText(new URL("./app.js", import.meta.url));
  const favoriteButtonIndex = indexHtml.indexOf('id="btn-toggle-fav"');
  const detailTitleIndex = indexHtml.indexOf('id="detail-title"');
  assert.ok(favoriteButtonIndex > -1, "favorite button exists");
  assert.ok(detailTitleIndex > -1, "detail title exists");
  assert.ok(favoriteButtonIndex < detailTitleIndex, "favorite button should sit before the station name");
  assert.match(appJs, /FAVORITE_MARKER_SIZE/);
  assert.match(appJs, /function getFavoriteStationMarkerIcon\(\)/);
  assert.match(appJs, /state\.favorites\.has\(stationId\)[\s\S]*createFavoriteStationMarker/);
  assert.match(appJs, /function refreshFavoriteDependentViews\(stationIds = \[\]\)/);
  assert.match(appJs, /updateMapMarkersForStationIds\(stationIds\)/);
  assert.match(appJs, /renderDetailStationMarker\(currentDetailFeature\)/);
});

test("favorite stations use passive star markers on cards and in the info legend", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  const appJs = readText(new URL("./app.js", import.meta.url));
  const createStationCardSource = appJs.slice(
    appJs.indexOf("function createStationCard"),
    appJs.indexOf("function getListDisplayItems"),
  );
  assert.match(indexHtml, /data-i18n="info\.legendFavorite"/);
  assert.match(indexHtml, /class="favorite-station-star legend-favorite-star"/);
  assert.match(createStationCardSource, /state\.favorites\.has\(stationId\)/);
  assert.match(createStationCardSource, /favorite-station-star card-favorite-star/);
  assert.match(createStationCardSource, /aria-hidden="true">★<\/span>/);
  assert.doesNotMatch(createStationCardSource, /toggleDetailFavorite/);
});

test("app shell uses the canonical woladen brand SEO copy", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  assert.match(indexHtml, /<title>woladen - Smart EV Stops in Europe<\/title>/);
  assert.doesNotMatch(indexHtml, /data-i18n="seo\.seoName"/);
  assert.match(indexHtml, /class="brand-intro-icon"/);
  assert.match(indexHtml, /class="brand-intro-brand">woladen<\/span>/);
  assert.doesNotMatch(indexHtml, /data-i18n="seo\.primaryTagline"/);
  assert.match(indexHtml, /data-i18n="seo\.humanHook"/);
  assert.match(indexHtml, /data-i18n="seo\.timeLine"/);
  assert.match(indexHtml, /class="brand-intro-hook"[\s\S]*data-i18n="seo\.humanHook"[\s\S]*data-i18n="seo\.timeLine"/);
  const listIntro = indexHtml.match(/<div id="view-list"[\s\S]*?<\/section>/)?.[0] || "";
  const infoIntro = indexHtml.match(/<div id="view-info"[\s\S]*?<\/section>/)?.[0] || "";
  assert.doesNotMatch(listIntro, /data-i18n="seo\.productMessage"/);
  assert.doesNotMatch(infoIntro, /data-i18n="seo\.productMessage"/);
  assert.doesNotMatch(indexHtml, /charging boredom|No charging boredom|Reliable charging/i);
});

test("route planner is visible in main navigation", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  const appJs = readText(new URL("./app.js", import.meta.url));
  assert.match(indexHtml, /id="view-route"/);
  assert.match(indexHtml, /class="nav-item"[^>]*data-target="view-route"/);
  assert.match(appJs, /const VIEW_ORDER = \["view-list", "view-map", "view-route", "view-favorites", "view-settings", "view-info"\];/);
  assert.match(appJs, /const VIEW_IDS = new Set\(VIEW_ORDER\);/);
});

test("settings keep units before number fields and a single-column desktop layout", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  const styles = readText(STYLES_URL);
  assert.match(indexHtml, /class="settings-input-wrap"><span>kWh<\/span><input id="setting-battery-kwh"/);
  assert.match(indexHtml, /class="settings-input-wrap"><span>kWh\/100 km<\/span><input id="setting-consumption"/);
  assert.match(styles, /#view-settings \.settings-container\s*{[^}]*grid-template-columns: minmax\(0, 1fr\)/);
});

test("route actions sit above route results and can save visible stations as favorites", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  const appJs = readText(new URL("./app.js", import.meta.url));
  const routeFormIndex = indexHtml.indexOf('id="route-form"');
  const routeTitleIndex = indexHtml.indexOf('data-i18n="route.title"');
  const routeSummaryIndex = indexHtml.indexOf('id="route-summary"');
  const routeResultsIndex = indexHtml.indexOf('id="route-results"');
  assert.ok(routeFormIndex > -1, "route form exists");
  assert.ok(routeTitleIndex > -1 && routeTitleIndex < routeFormIndex, "route section title is above route form");
  assert.ok(routeSummaryIndex > routeFormIndex && routeSummaryIndex < routeResultsIndex, "route summary sits above route results");
  assert.match(appJs, /filterButton\.id = "btn-route-filter"/);
  assert.match(appJs, /favoriteButton\.id = "route-favorite-all"/);
  assert.match(appJs, /actions\.append\(filterButton, mapButton, favoriteButton\)/);
  assert.match(appJs, /function addRouteResultsToFavorites\(\)/);
  assert.match(appJs, /FAVORITE_SOURCE_ROUTE/);
  assert.match(appJs, /routeFavoriteCandidateFeatures\(\)/);
  assert.match(appJs, /function compactRouteFavoriteEndpointLabel\(/);
  assert.match(appJs, /route\.favoriteCategory/);
});

test("route map lock prevents standard map station loading while route is displayed", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  const appJs = readText(new URL("./app.js", import.meta.url));
  assert.match(indexHtml, /id="route-map-lock"/);
  assert.match(indexHtml, /data-i18n="route\.mapFixed"/);
  assert.match(indexHtml, /id="route-map-clear"/);
  assert.match(appJs, /function hasPinnedRouteMap\(\)/);
  assert.match(appJs, /function clearRoute\(\)/);
  assert.match(appJs, /function loadCatalogStationsFromMapCenter[\s\S]*hasPinnedRouteMap\(\)[\s\S]*return;/);
  assert.match(appJs, /function queueCatalogSearchFromMapMove[\s\S]*hasPinnedRouteMap\(\)[\s\S]*clearTimeout\(catalogMapMoveTimer\)/);
  assert.match(appJs, /function getMapMarkerFeatures\(\)[\s\S]*hasPinnedRouteMap\(\)[\s\S]*getRouteDisplayFeatures\(\)/);
});

test("fallback metadata keeps the canonical English title", () => {
  assert.deepEqual(
    JSON.parse(JSON.stringify(fallbackBundle().meta)),
    DEFAULT_META,
    "fallback metadata is stale",
  );
});

test("localized bundles translate the SEO title fields", () => {
  const expectedTitles = {
    de: "woladen - Smarte EV-Stopps in Europa",
    fr: "woladen - Arrêts de recharge intelligents en Europe",
    nl: "woladen - Slimme EV-stops in Europa",
  };
  for (const [language, expectedTitle] of Object.entries(expectedTitles)) {
    const bundle = readBundle(language);
    assert.equal(bundle.meta.title, expectedTitle, `${language}.json meta title`);
    assert.equal(bundle.meta.ogTitle, expectedTitle, `${language}.json og title`);
    assert.equal(bundle.seo.seoName, expectedTitle, `${language}.json SEO name`);
    assert.equal(bundle.seo.homeTitle, expectedTitle, `${language}.json home title`);
  }
});
