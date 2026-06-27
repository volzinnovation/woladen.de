# woladen SEO Implementation Plan

## Purpose

This plan defines the SEO architecture for the woladen rebrand and international rollout.
The main correction is that language and country coverage are separate dimensions:

- A user can search in Italian, Spanish, French, German, Dutch, or any other supported language.
- A country page must only exist for countries woladen currently serves.
- Therefore the URL model must support every supported language for every served country.

The interactive app can still localize dynamically, but the SEO layer must be static,
crawlable, language-specific HTML generated at build time.

## Brand Contract

The SEO implementation must preserve this hierarchy everywhere:

- Brand/app name: `woladen`
- SEO name: `woladen - Smart EV Stops in Europe`
- Slogan: `The human side of charging`
- Subtitle: `Because charging time is your time`
- Product message: `Find EV charging stops near bakeries, restaurants, shops, playgrounds, cafes, and other useful places nearby.`

Avoid falling back to generic charger-directory positioning:

- Do not lead with `EV map` as the main promise.
- Do not use or revive `Plugs for Cars. Perks for People.` or variants such as `Plugs for cars, Perks for humans`.
- Do not revive `charging boredom` copy.
- Do not describe woladen as only a list of charging stations.
- Do not imply live availability or pricing exists everywhere.

The durable positioning is: woladen helps people choose better charging stops, not just
find plugs.

## Current Coverage Source

Country SEO pages must be generated only from `data/open_static_summary.json`.
This prevents aspirational pages for countries not currently served.

Current snapshot: `2026-06-17T12:54:55+00:00`.

| Country | Code | Locations | Charging points | Fast-charging locations |
| --- | --- | ---: | ---: | ---: |
| Austria | AT | 14,661 | 38,771 | 3,435 |
| Belgium | BE | 4,219 | 12,907 | 112 |
| Switzerland | CH | 8,670 | 18,725 | 1,217 |
| Cyprus | CY | 100 | 171 | 11 |
| Czechia | CZ | 3,755 | 6,594 | 1,878 |
| Germany | DE | 72,155 | 197,527 | 16,633 |
| Denmark | DK | 3,396 | 13,533 | 503 |
| Spain | ES | 12,237 | 36,432 | 5,441 |
| Finland | FI | 3,674 | 19,430 | 1,254 |
| France | FR | 63,728 | 159,613 | 11,911 |
| Greece | GR | 3,975 | 9,250 | 718 |
| Hungary | HU | 1,346 | 2,523 | 372 |
| Lithuania | LT | 2,496 | 13,814 | 760 |
| Luxembourg | LU | 530 | 530 | 17 |
| Latvia | LV | 1,102 | 3,203 | 772 |
| Malta | MT | 184 | 184 | 32 |
| Netherlands | NL | 61,244 | 157,380 | 1,408 |
| Norway | NO | 5,175 | 32,672 | 1,924 |
| Poland | PL | 6,600 | 13,070 | 2,961 |
| Portugal | PT | 7,978 | 19,340 | 3,018 |
| Sweden | SE | 8,922 | 61,108 | 2,218 |
| Slovenia | SI | 1,191 | 3,405 | 186 |

Do not generate Italy, Croatia, or other country pages until they appear in the coverage
summary.

## SEO Architecture

Use language-first URLs:

```text
/
/en/
/de/
/fr/
/nl/

/en/coverage/
/de/abdeckung/
/fr/couverture/

/en/germany/
/de/deutschland/
/fr/allemagne/
/it/germania/

/en/germany/berlin/
/de/deutschland/berlin/

/en/germany/bakeries/
/de/deutschland/baeckereien/
```

Rules:

- The first URL segment is the content language.
- The second URL segment identifies the served country or content hub.
- Slugs should be localized where stable and short.
- Display names must be fully localized.
- `x-default` should point to `/` or `/en/`, depending on the final homepage decision.
- Legacy `/?lang=de` URLs remain functional for users, but are not the SEO target.

## Internationalization Model

The current dynamic i18n approach is fine for app interaction, but it should not be the
primary SEO implementation.

### Current Risk

The current model updates title, description, canonical, Open Graph, and page copy at
runtime from JavaScript. That creates avoidable SEO risk:

- Crawlers may see the default language before rendering.
- Social crawlers often do not execute the app in the same way as Googlebot.
- One physical URL with query parameters creates weaker canonical and hreflang signals.
- Internal links hidden behind app state or hash navigation are less reliable for crawling.
- Runtime fallbacks can silently produce English body copy on non-English pages.

### Target Model

Use build-time rendered HTML for SEO pages:

- `scripts/build_site.py` renders localized SEO pages.
- `web/i18n.mjs` remains responsible for the interactive app UI.
- SEO strings live in explicit translation bundles, not ad hoc inline strings.
- The build fails if a required SEO string is missing for a supported language.
- No localized SEO page silently falls back to English.

Recommended translation structure:

```text
web/i18n/en.json
web/i18n/de.json
...
```

Add an `seo` namespace to each language file:

```json
{
  "seo": {
    "brandName": "woladen",
    "seoName": "woladen - Smart EV Stops in Europe",
    "primaryTagline": "The human side of charging",
    "humanHook": "The human side of charging",
    "timeLine": "Because charging time is your time",
    "homeTitle": "woladen - Smart EV Stops in Europe",
    "homeDescription": "Find better EV charging stops across Europe with useful places nearby."
  }
}
```

## Page Types

### 1. Localized Homepages

Examples:

```text
/en/
/de/
/fr/
/nl/
```

English content:

- Title: `woladen - Smart EV Stops in Europe`
- H1: `woladen`
- Slogan: `The human side of charging`
- Subtitle: `Because charging time is your time`
- Body: `Find EV charging stops across Europe near bakeries, restaurants, shops, playgrounds, cafes, and other useful places nearby. Because charging time is your time.`

Homepage sections:

- Primary app/search entry
- Explore by country
- Better charging stops
- Available languages
- App install links

### 2. Localized Coverage Hubs

Examples:

```text
/en/coverage/
/de/abdeckung/
/fr/couverture/
```

English content:

- Title: `EV Charging Coverage Across Europe | woladen`
- H1: `EV Charging Coverage Across Europe`
- Slogan: `The human side of charging`
- Intro: `woladen helps you find smarter charging stops in the countries we currently serve: fast chargers for the car, useful places for the people inside it.`

Required content:

- Table of served countries
- Locations, charging points, fast-charging locations
- Internal links to every localized country page
- Data freshness note from `open_static_summary.generated_at`

### 3. Localized Country Pages

Examples:

```text
/en/germany/
/de/deutschland/
/fr/allemagne/
```

English template:

- Title: `EV Charging Stops in Germany | woladen`
- H1: `EV Charging Stops in Germany`
- Slogan: `The human side of charging`
- Intro: `Find EV charging stops across Germany with useful places nearby: restaurants, bakeries, shops, playgrounds, cafes, and other ways to make charging time yours.`

Required content:

- Country coverage stats
- Map preview
- Top charging stops ranked by amenity richness and fast-charging usefulness
- Major networks/operators
- City links where city data is good enough
- Amenity links where enough matching locations exist
- Link back to localized coverage hub

### 4. Localized City Pages

Examples:

```text
/en/germany/berlin/
/de/deutschland/berlin/
/fr/allemagne/berlin/
```

Generate only when city data quality is sufficient.

Initial threshold:

- At least 20 stations in the city.
- At least 5 stations with amenities.
- City name must be present and not a placeholder.

English template:

- Title: `EV Charging Stops in Berlin | woladen`
- H1: `EV Charging Stops in Berlin`
- Intro: `Find EV charging stops in Berlin with useful places nearby. Charge the car, then use the time for food, coffee, shopping, errands, or a break.`

### 5. Localized Country + Amenity Pages

Examples:

```text
/en/germany/bakeries/
/de/deutschland/baeckereien/
/fr/allemagne/boulangeries/
```

Generate only when enough locations match the amenity.

Initial threshold:

- At least 25 matching stations in the country.
- At least 10 fast-charging locations among the matches, where possible.

Amenity groups:

- Restaurants
- Cafes
- Bakeries
- Shops
- Playgrounds
- Parks
- Hotels
- Toilets

English template:

- Title: `EV Chargers Near Bakeries in Germany | woladen`
- H1: `EV Chargers Near Bakeries in Germany`
- Intro: `A better charging stop is not just a plug. Find EV chargers in Germany with bakeries nearby, so charging time can become coffee, breakfast, or a short break.`

### 6. Station Pages

Keep station detail pages language-neutral at first.

Do not multiply every station page by every language until crawl budget and index quality
are measured. The current station-page inventory is already large. Multiplying by every
language would create millions of URLs and could bury the higher-value country, city, and
amenity pages.

Instead:

- Keep station pages as canonical technical landing pages.
- Link to station pages from localized country/city/amenity pages.
- Localize top-stop summaries on the parent SEO pages.

## Hreflang and Canonicals

Each localized SEO page must canonicalize to itself:

```html
<link rel="canonical" href="https://woladen.de/de/deutschland/" />
```

Do not canonicalize non-English pages back to English.

Each page identity must have hreflang alternates for all generated language versions:

```html
<link rel="alternate" hreflang="en" href="https://woladen.de/en/germany/" />
<link rel="alternate" hreflang="de" href="https://woladen.de/de/deutschland/" />
<link rel="alternate" hreflang="fr" href="https://woladen.de/fr/allemagne/" />
<link rel="alternate" hreflang="x-default" href="https://woladen.de/en/germany/" />
```

Use language codes, not country-targeted codes, unless the page is intentionally targeted
to a specific language-region pair. A French page about Germany is `hreflang="fr"`, not
`fr-DE`.

The sitemap should include the same alternate sets, or the HTML head should. Pick one
source of truth and generate it consistently from the page graph.

## Structured Data

Use conservative structured data that matches page content:

- `WebSite` on localized homepages.
- `SoftwareApplication` or `MobileApplication` where app install links appear.
- `BreadcrumbList` on coverage, country, city, and amenity pages.
- `ItemList` for top charging stops on country, city, and amenity pages.

Do not add unsupported or misleading rich-result markup just because schema.org has a
type that looks relevant.

## Internal Linking

Internal links must be real `<a href="...">` links in server-rendered HTML.

Homepage:

- Link to every localized country page.
- Link to localized coverage hub.
- Link to major language homepages.

Coverage hub:

- Link to every country in the same language.
- Link to alternate languages through the language switcher.

Country page:

- Link to top cities.
- Link to amenity pages.
- Link to top station pages.
- Link to sibling country pages in the same language.

City page:

- Link to parent country page.
- Link to top station pages.
- Link to amenity combinations available in that city only if counts justify them.

## Build Implementation

Extend `scripts/build_site.py` with a static SEO renderer.

Recommended internal model:

```python
@dataclass(frozen=True)
class SeoPage:
    identity: str
    language: str
    path: str
    title: str
    description: str
    h1: str
    canonical_path: str
    alternate_paths: dict[str, str]
```

Generation steps:

1. Load supported languages from `web/i18n.mjs` or a shared JSON manifest.
2. Load SEO translations from `web/i18n/*.json`.
3. Load served countries from `data/open_static_summary.json`.
4. Load station, city, amenity, and operator aggregates from the open static SQLite bundle.
5. Generate localized homepages.
6. Generate localized coverage hubs.
7. Generate localized country pages.
8. Generate city pages above threshold.
9. Generate country + amenity pages above threshold.
10. Emit sitemap entries and hreflang groups.
11. Copy assets with root-relative URLs so localized pages do not break under nested paths.

## Asset and App Routing Notes

Localized SEO pages should use root-relative assets:

```html
<link rel="stylesheet" href="/styles.css" />
<script type="module" src="/app.js"></script>
```

The interactive app can continue to live at `/`, but it should be able to read a language
hint from path-based SEO pages if a user opens the app from `/de/deutschland/`.

Do not rely on `./styles.css` or `./app.js` in generated nested pages.

## Sitemaps

Emit separate sitemap files:

```text
sitemap.xml
sitemap-pages.xml
sitemap-seo-home.xml
sitemap-seo-coverage.xml
sitemap-seo-countries.xml
sitemap-seo-cities-1.xml
sitemap-seo-amenities-1.xml
sitemap-stations-1.xml
```

Keep station pages separate from editorial SEO pages so Search Console reporting remains
readable.

Every generated indexable page must appear in exactly one sitemap URL set.

## Rollout Phases

### Phase 1: Brand and Static Language Homepages

- Add SEO namespace to all supported language JSON files.
- Generate `/en/`, `/de/`, `/fr/`, and all other supported language homepages.
- Add self-canonicals and hreflang alternates.
- Keep `/` as x-default or English, then make that explicit.

### Phase 2: Coverage Hubs

- Generate localized coverage pages.
- Link from homepage and app info view.
- Include served country counts and data freshness.

### Phase 3: Country Pages

- Generate localized pages for all countries in `open_static_summary.json`.
- Include country stats, top stops, major operators, map preview, and internal links.
- Do not generate unsupported countries.

### Phase 4: City Pages

- Generate city pages above quality thresholds.
- Start with countries that have strong city data.
- Exclude countries with empty or placeholder-heavy city fields until data improves.

### Phase 5: Country + Amenity Pages

- Generate pages for amenity combinations above threshold.
- Prioritize restaurants, cafes, bakeries, shops, playgrounds, hotels, parks, and toilets.
- Use the woladen brand promise in intros, not generic keyword stuffing.

### Phase 6: Measurement and Expansion

- Monitor Search Console by sitemap group.
- Track indexed pages, impressions, clicks, duplicate/canonical warnings, and crawled-not-indexed rates.
- Expand thresholds only when indexed quality is healthy.
- Consider localized station pages only after country/city/amenity pages show traction.

## Validation Checklist

Before finishing an implementation:

- `node --test web/filtering.test.mjs web/location.test.mjs`
- `python3 scripts/build_site.py`
- `python3 -m http.server 4173 --directory site`
- Inspect `/en/`, `/de/`, `/en/coverage/`, one large country, one small country, one city page, and one amenity page.
- Verify translated title, meta description, H1, canonical, hreflang, and body copy.
- Verify all country pages are generated only for countries in `data/open_static_summary.json`.
- Verify no generated SEO page silently falls back to English.
- Verify nested pages load CSS, images, and scripts.
- Verify `sitemap.xml` references all sitemap groups.
- Run `rg "charging boredom|No charging boredom|Europe EV Map" web scripts site docs`.

## Sources

- Google Search Central: localized versions and hreflang
  `https://developers.google.com/search/docs/specialty/international/localized-versions`
- Google Search Central: JavaScript SEO basics
  `https://developers.google.com/search/docs/crawling-indexing/javascript/javascript-seo-basics`
- Google Search Central: canonical URL guidance
  `https://developers.google.com/search/docs/crawling-indexing/consolidate-duplicate-urls`

#Review through Gemini chatbot

This is an exceptionally robust, elite-tier SEO implementation plan. You have accurately identified and mitigated the biggest traps in international SEO—specifically the critical distinction between **content language** and **target country**, and the crawlability risks of reliance on runtime client-side JavaScript.

The strategy of using strict data-count thresholds to prevent thin-content programmatic pages is a brilliant engineering safeguard.

To elevate this plan from great to bulletproof, here are targeted refinements and improvements to address edge cases, optimize crawl performance, and ensure a seamless user experience.

---

## 1. Concrete Strategy for Root `/` and `x-default`

The plan currently leaves the root URL configuration open. Leaving this to chance can cause indexing confusion.

### The Recommendation

Since this is a static build-time architecture, handle the root path `/` at the edge layer (CDN/Server like Cloudflare Workers, Nginx, or Vercel).

* **Behavior:** The root `/` should look at the user’s browser `Accept-Language` header and execute a fast **302 (temporary) redirect** to the appropriate language folder (e.g., `/de/` or `/fr/`). If no matching language is detected, fallback redirect to `/en/`.
* **Hreflang Mapping:** Define `/en/` as your `x-default` and primary English destination.

```html
<link rel="alternate" hreflang="x-default" href="https://woladen.de/en/" />
<link rel="alternate" hreflang="en" href="https://woladen.de/en/" />
<link rel="alternate" hreflang="de" href="https://woladen.de/de/" />

```

---

## 2. Mitigation of Hreflang HTML Bloat

Decoupling language and country creates a massive URL matrix. If you have 6 languages and 22 countries, that is a baseline of 132 country landing pages. When you multiply that by dozens of cities and amenity combinations, putting every single cross-referenced `hreflang` alternate link tag into the `<head>` of every HTML page will drastically bloat page sizes. This hurts your Time to First Byte (TTFB) and wastes mobile bandwidth.

### The Improvement

Move the weight to the sitemaps. Keep self-canonical and immediate language alternatives in the HTML `<head>`, but offload the massive cross-country programmatic matrix mapping to your **XML Sitemaps**.

Update your **Sitemaps** plan phase to explicitly handle hreflang nodes:

```xml
<!-- In sitemap-seo-countries.xml -->
<url>
  <loc>https://woladen.de/de/deutschland/</loc>
  <xhtml:link rel="alternate" hreflang="en" href="https://woladen.de/en/germany/" />
  <xhtml:link rel="alternate" hreflang="fr" href="https://woladen.de/fr/allemagne/" />
  <xhtml:link rel="alternate" hreflang="it" href="https://woladen.de/it/germania/" />
  <xhtml:link rel="alternate" hreflang="x-default" href="https://woladen.de/en/germany/" />
</url>

```

---

## 3. The "Language-Neutral" Station Page UX Bridge

Keeping the millions of unique station detail pages language-neutral is completely the correct move for preserving your crawl budget. However, if a user transitions from a beautiful, fully localized French page (`/fr/allemagne/berlin/`) to an English or purely raw language-neutral station page, the sudden language break will damage conversion and increase bounce rates.

### The Improvement

Address this in the **Asset and App Routing Notes**. While the static HTML shell of the station page remains language-neutral for the Googlebot, the client-side `web/i18n.mjs` script should look for a state indicator to dynamically localize the interactive app wrapper UI elements (buttons, navigation headers, search bars).

* **The Logic:** The app script checks `sessionStorage` or the `document.referrer` path. If it contains `/fr/`, the interactive UI wrapper components instantly flip to French at runtime, even though the core static station data (address, connector names) remains technically neutral.

---

## 4. Architectural Safeguard for Amenity Mapping

When generating country + amenity pages dynamically (e.g., `/de/deutschland/baeckereien/`), localized slugs can occasionally fall out of sync or collide during build steps across multiple languages.

### The Improvement

Add a strict explicit dictionary mapping layer to your `SeoPage` dataclass structure within `scripts/build_site.py`. This ensures your slug generation doesn't rely on ad-hoc translation strings.

```python
# Suggested addition to build configuration logic
AMENITY_SLUGS = {
    "bakeries": {"en": "bakeries", "de": "baeckereien", "fr": "boulangeries", "es": "panaderias"},
    "playgrounds": {"en": "playgrounds", "de": "spielplaetze", "fr": "terrains-de-jeux"}
}

```

If an amenity passes the threshold check in `data/open_static_summary.json` but lacks a validated entry in the `AMENITY_SLUGS` matrix for a given language, **the build must fail gracefully or skip that specific language page**, rather than guessing or outputting un-vetted translation string slugs.
