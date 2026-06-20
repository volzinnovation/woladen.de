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
  socialAlt: "woladen preview with a Europe map and the tagline: Plugs for Cars. Perks for People.",
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

test("detail view orders amenities, live status, notes, and details", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  const amenitiesIndex = indexHtml.indexOf('id="detail-amenities-title"');
  const liveIndex = indexHtml.indexOf('id="detail-live-section"');
  const noteIndex = indexHtml.indexOf('class="detail-note"');
  const detailsIndex = indexHtml.indexOf('id="detail-details-section"');
  assert.ok(amenitiesIndex > -1, "amenities section exists");
  assert.ok(liveIndex > -1, "live section exists");
  assert.ok(noteIndex > -1, "note section exists");
  assert.ok(detailsIndex > -1, "details section exists");
  assert.ok(amenitiesIndex < liveIndex, "amenities should come before live status");
  assert.ok(liveIndex < noteIndex, "personal note should come after live status");
  assert.ok(noteIndex < detailsIndex, "personal note should come before details");
});

test("app shell uses the canonical woladen brand SEO copy", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  assert.match(indexHtml, /<title>woladen - Smart EV Stops in Europe<\/title>/);
  assert.doesNotMatch(indexHtml, /data-i18n="seo\.seoName"/);
  assert.match(indexHtml, /class="brand-intro-icon"/);
  assert.match(indexHtml, /class="brand-intro-brand">woladen:<\/span>/);
  assert.match(indexHtml, /data-i18n="seo\.primaryTagline"/);
  assert.match(indexHtml, /data-i18n="seo\.humanHook"/);
  assert.match(indexHtml, /data-i18n="seo\.timeLine"/);
  assert.match(indexHtml, /class="brand-intro-hook"[\s\S]*data-i18n="seo\.humanHook"[\s\S]*data-i18n="seo\.timeLine"/);
  const listIntro = indexHtml.match(/<div id="view-list"[\s\S]*?<\/section>/)?.[0] || "";
  const infoIntro = indexHtml.match(/<div id="view-info"[\s\S]*?<\/section>/)?.[0] || "";
  assert.doesNotMatch(listIntro, /data-i18n="seo\.productMessage"/);
  assert.match(infoIntro, /data-i18n="seo\.productMessage"/);
  assert.doesNotMatch(indexHtml, /charging boredom|No charging boredom|Reliable charging/i);
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
