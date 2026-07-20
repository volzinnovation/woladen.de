# Management PostgreSQL live-data adapter

The management page now uses the PostgreSQL analytics API owned by
`Woladen.de-analytics`. Existing static JSON remains an explicit rebuildable
Germany-only fallback for outages.

The page has three modes:

- `management.html`: a neutral index with aggregate country and provider tables;
- `management.html?country=DE`: a country report with daily station/provider
  evidence and selectable 7/28/90-day history;
- `management.html?provider=chargecloud&country=DE`: a provider report with
  provider-scoped KPIs, station evidence, trends, transport health, and a
  local-hour load/reliability profile.

## Browser configuration

`web/config.js` defaults to the rate-limited management proxy at
`https://live-eu.woladen.de/v1/management`. Deployments can override
`window.WOLADEN_MANAGEMENT_API_BASE_URL`. The adapter requests:

```text
GET /v1/management/countries
GET /v1/management/dashboard/index
GET /v1/management/dashboard?archive_date=YYYY-MM-DD&country_code=CC&station_limit=10&trend_days=7|28|90
GET /v1/management/report?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&group_by=country
GET /v1/management/report?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&group_by=provider&country_code=CC
GET /v1/management/provider-health?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&country_code=CC
GET /v1/management/dashboard?archive_date=YYYY-MM-DD&provider_uid=UID&country_code=CC&station_limit=10&trend_days=7|28|90
GET /v1/management/profile?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&group_by=hour&provider_uid=UID&country_code=CC
```

The browser uses `credentials: "include"`. The proxy is responsible for
rate-limiting, origin policy, and any customer session/entitlement decision;
never place the backend shared management token in JavaScript, HTML, a URL, or
browser storage.

When the live proxy is unavailable and static fallback is enabled, the page
uses:

```text
data/management/index.json
data/management/days/YYYY/MM/DD/snapshot.json
data/management/trends.json
```

`window.WOLADEN_MANAGEMENT_STATIC_FALLBACK_ENABLED` defaults to `true`. The
fallback contains the historical Germany page only; it does not invent
country reports for missing live data.

## Source visibility and testing

The page records the active source as
`document.documentElement.dataset.managementDataSource`, with either
`postgresql` or `static-cache`. Tests cover live success, live-to-static
fallback, URL normalization, and browser rendering for both sources.

The overview uses grouped country and provider reports rather than one
dashboard request per row. Country and provider details each load one scoped
dashboard plus a grouped provider report and provider-health query. Provider
details additionally request a bounded hourly profile. The provider profile
chart deliberately uses the latest seven days even when the surrounding report
window is 28 or 90 days; this keeps the interval query responsive while the
tables retain the selected rolling window. Large station-stat materializations
and raw/bulk report artifacts are deliberately not requested by the public page.
