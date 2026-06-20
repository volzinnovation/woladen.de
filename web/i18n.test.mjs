import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const I18N_MODULE_URL = new URL("./i18n.mjs", import.meta.url);
const INDEX_HTML_URL = new URL("./index.html", import.meta.url);
const I18N_DIR_URL = new URL("./i18n/", import.meta.url);
const CANONICAL_META = {
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
  assert.match(indexHtml, /<p class="detail-subnote compact" data-i18n="station\.typicalOccupancyNote">/);
  assert.match(indexHtml, /data-i18n-aria-label="rating\.ariaLabel"/);
});

test("app shell uses the canonical woladen brand SEO copy", () => {
  const indexHtml = readText(INDEX_HTML_URL);
  assert.match(indexHtml, /<title>woladen - Smart EV Stops in Europe<\/title>/);
  assert.match(indexHtml, /data-i18n="seo\.seoName"/);
  assert.match(indexHtml, /data-i18n="seo\.primaryTagline"/);
  assert.match(indexHtml, /data-i18n="seo\.humanHook"/);
  assert.match(indexHtml, /data-i18n="seo\.timeLine"/);
  assert.doesNotMatch(indexHtml, /charging boredom|No charging boredom|Reliable charging/i);
});

test("language metadata keeps the canonical woladen brand copy", () => {
  assert.deepEqual(
    JSON.parse(JSON.stringify(fallbackBundle().meta)),
    CANONICAL_META,
    "fallback metadata is stale",
  );
  const languages = fs
    .readdirSync(I18N_DIR_URL)
    .filter((fileName) => fileName.endsWith(".json"))
    .sort();
  assert.ok(languages.length > 0, "language bundles should exist");
  for (const fileName of languages) {
    const bundle = JSON.parse(readText(new URL(fileName, I18N_DIR_URL)));
    if (bundle.meta) {
      assert.deepEqual(bundle.meta, CANONICAL_META, `${fileName} has stale metadata`);
    }
  }
});
