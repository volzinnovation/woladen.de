# Management live-data adapter

The management page can read its dashboard contract from the private
PostgreSQL analytics API owned by `Woladen.de-analytics`. Existing static JSON
remains an explicit rebuildable fallback for outages and expensive bulk views.

## Browser configuration

Set `window.WOLADEN_MANAGEMENT_API_BASE_URL` in `web/config.js` or injected
deployment configuration to an approved same-origin endpoint ending in
`/v1/management`. The adapter requests:

```text
GET /v1/management/dashboard/index
GET /v1/management/dashboard?archive_date=YYYY-MM-DD&station_limit=10&trend_days=90
```

The browser uses `credentials: "include"`. It must authenticate through a
customer session/entitlement proxy; never place the backend shared management
token in JavaScript, HTML, a URL, or browser storage.

The default base URL is empty, so checked-in builds continue to use:

```text
data/management/index.json
data/management/days/YYYY/MM/DD/snapshot.json
data/management/trends.json
```

`window.WOLADEN_MANAGEMENT_STATIC_FALLBACK_ENABLED` defaults to `true`. Set it
to `false` only after the authenticated live path has production monitoring and
an intentional no-fallback product decision.

## Source visibility and testing

The page records the active source as
`document.documentElement.dataset.managementDataSource`, with either
`postgresql` or `static-cache`. Tests cover live success, live-to-static
fallback, URL normalization, and browser rendering for both sources.

Production activation remains human-gated on entitlement/session behavior,
security review, private API routing, rate limiting, monitoring, and rollout.
The adapter itself does not broaden data access or expose the private API.
