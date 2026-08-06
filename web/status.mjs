const DATA_PATHS = {
  openStatic: "./data/open_static_summary.json",
  buildSummary: "./data/summary.json",
  managementIndex: "./data/management/index.json",
  occupancyIndex: "./data/station-occupancy/index.json",
  // The diagnostics path is already exposed by the current Caddy policy.
  // The dedicated endpoint remains available after the next privileged
  // Caddy reload; this fallback keeps the page operational immediately.
  operational: "https://live-eu.woladen.de/commercial/v1/status?view=operational",
  liveEuHealth: "https://live-eu.woladen.de/healthz",
  germanyLive: "https://live.woladen.de/healthz",
};

const numberFormat = new Intl.NumberFormat("de-DE");
const dateTimeFormat = new Intl.DateTimeFormat("de-DE", {
  day: "2-digit",
  month: "2-digit",
  year: "numeric",
  hour: "2-digit",
  minute: "2-digit",
});
const dateFormat = new Intl.DateTimeFormat("de-DE", {
  day: "2-digit",
  month: "2-digit",
  year: "numeric",
});

function formatNumber(value) {
  const number = Number(value || 0);
  return numberFormat.format(Number.isFinite(number) ? number : 0);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (!Number.isFinite(bytes) || bytes < 0) {
    return "n/a";
  }
  if (bytes < 1000) {
    return `${formatNumber(bytes)} B`;
  }
  const units = ["kB", "MB", "GB", "TB"];
  let scaled = bytes;
  let index = -1;
  while (scaled >= 1000 && index < units.length - 1) {
    scaled /= 1000;
    index += 1;
  }
  return `${new Intl.NumberFormat("de-DE", { maximumFractionDigits: 1 }).format(scaled)} ${units[index]}`;
}

function stateLabel(value) {
  return {
    healthy: "OK",
    degraded: "Eingeschränkt",
    unavailable: "Nicht erreichbar",
    not_configured: "Nicht eingerichtet",
  }[String(value || "").trim()] || "Unbekannt";
}

function stateClass(value) {
  const state = String(value || "unknown").trim();
  return ["healthy", "degraded", "unavailable", "not_configured"].includes(state)
    ? state
    : "unknown";
}

function parseDate(value) {
  const text = String(value || "").trim();
  if (!text) {
    return null;
  }
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function formatTimestamp(value) {
  const parsed = parseDate(value);
  return parsed ? dateTimeFormat.format(parsed) : "n/a";
}

function formatDate(value) {
  const parsed = parseDate(`${value || ""}T00:00:00Z`);
  return parsed ? dateFormat.format(parsed) : "n/a";
}

function ageLabel(value) {
  const parsed = parseDate(value);
  if (!parsed) {
    return "kein Zeitstempel";
  }
  const days = Math.max(0, Math.round((Date.now() - parsed.getTime()) / 86400000));
  if (days === 0) {
    return "heute";
  }
  if (days === 1) {
    return "1 Tag alt";
  }
  return `${days} Tage alt`;
}

function setAlert(message, error = false) {
  const alert = document.getElementById("data-status-alert");
  if (!alert) {
    return;
  }
  alert.hidden = !message;
  alert.textContent = message || "";
  alert.classList.toggle("is-error", error);
}

async function fetchJson(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`);
  }
  return response.json();
}

async function loadStatusData() {
  const entries = Object.entries(DATA_PATHS);
  const results = await Promise.allSettled(entries.map(([, path]) => fetchJson(path)));
  const data = {};
  const failures = [];

  results.forEach((result, index) => {
    const [key, path] = entries[index];
    if (result.status === "fulfilled") {
      data[key] = result.value;
    } else {
      failures.push({ key, path, error: result.reason });
    }
  });

  return { data, failures };
}

function countrySum(openStatic, key) {
  return (Array.isArray(openStatic?.countries) ? openStatic.countries : []).reduce(
    (total, country) => total + Number(country?.[key] || 0),
    0,
  );
}

function renderKpis(data) {
  const openStatic = data.openStatic || {};
  const bundle = openStatic.bundle || {};
  const summary = data.buildSummary || {};
  const records = summary.records || {};
  const management = data.managementIndex || {};
  const occupancy = data.occupancyIndex || {};
  const stationIds = Array.isArray(occupancy.station_ids) ? occupancy.station_ids.length : 0;
  const cards = [
    {
      label: "Stationen im Katalog",
      value: formatNumber(bundle.station_count),
      detail: `${formatNumber(bundle.country_count)} Laender, EU plus CH/NO`,
    },
    {
      label: "Ladepunkte",
      value: formatNumber(bundle.charger_count),
      detail: "Statischer Open-Data-Katalog",
    },
    {
      label: "Schnelllader",
      value: formatNumber(records.fast_chargers_total || countrySum(openStatic, "fast_station_count")),
      detail: "Standardgrenze ab 50 kW",
    },
    {
      label: "Live-Archivtage",
      value: formatNumber(management.day_count),
      detail: management.latest_date ? `Letzter Tag ${formatDate(management.latest_date)}` : "kein Index geladen",
    },
    {
      label: "Stations-Historien",
      value: formatNumber(stationIds),
      detail: "lokale Occupancy-Dateien im Bundle",
    },
  ];

  const container = document.getElementById("data-status-kpis");
  container.innerHTML = cards
    .map(
      (card) => `
        <article class="management-kpi">
          <div class="management-kpi-label">${card.label}</div>
          <div class="management-kpi-value">${card.value}</div>
          <div class="management-kpi-detail">${card.detail}</div>
        </article>
      `,
    )
    .join("");
}

function renderFreshness(data) {
  const summary = data.buildSummary || {};
  const management = data.managementIndex || {};
  const openStatic = data.openStatic || {};
  const rows = [
    {
      label: "Web-Build beendet",
      value: formatTimestamp(summary?.run?.finished_at),
      meta: ageLabel(summary?.run?.finished_at),
    },
    {
      label: "Open-Static Summary",
      value: formatTimestamp(openStatic.generated_at),
      meta: ageLabel(openStatic.generated_at),
    },
    {
      label: "Management-Index",
      value: formatTimestamp(management.generated_at),
      meta: ageLabel(management.generated_at),
    },
    {
      label: "Letzter Archivtag",
      value: management.latest_date ? formatDate(management.latest_date) : "n/a",
      meta: management.latest_date ? `${formatNumber(management.day_count)} Tage im Archiv` : "kein Archiv",
    },
  ];

  document.getElementById("freshness-list").innerHTML = rows
    .map(
      (row) => `
        <div class="data-status-row">
          <dt>${row.label}</dt>
          <dd>
            <strong>${row.value}</strong>
            <span>${row.meta}</span>
          </dd>
        </div>
      `,
    )
    .join("");
}

function renderContractChecks(data, failures) {
  const checks = [
    ["openStatic", "Open-Static Katalog", DATA_PATHS.openStatic],
    ["buildSummary", "Build Summary", DATA_PATHS.buildSummary],
    ["managementIndex", "Tagesarchiv-Index", DATA_PATHS.managementIndex],
    ["occupancyIndex", "Stations-Historienindex", DATA_PATHS.occupancyIndex],
  ];
  const failedKeys = new Set(failures.map((failure) => failure.key));

  document.getElementById("contract-checks").innerHTML = checks
    .map(([key, label, path]) => {
      const ok = data[key] && !failedKeys.has(key);
      return `
        <div class="data-contract-row ${ok ? "is-ok" : "is-error"}">
          <span class="data-status-pill">${ok ? "OK" : "Fehlt"}</span>
          <div>
            <strong>${label}</strong>
            <span>${path}</span>
          </div>
        </div>
      `;
    })
    .join("");
}

function renderCountries(openStatic) {
  const countries = Array.isArray(openStatic?.countries) ? [...openStatic.countries] : [];
  countries.sort((left, right) => Number(right.fast_station_count || 0) - Number(left.fast_station_count || 0));
  const rows = countries.slice(0, 10).map(
    (country) => `
      <tr>
        <td>${country.name || country.code}</td>
        <td>${formatNumber(country.fast_station_count)}</td>
        <td>${formatNumber(country.station_count)}</td>
        <td>${formatNumber(country.charger_count)}</td>
      </tr>
    `,
  );
  document.getElementById("country-status-body").innerHTML =
    rows.join("") || '<tr><td colspan="4">Keine Laenderdaten geladen.</td></tr>';
}

function renderSources(openStatic) {
  const sources = Array.isArray(openStatic?.sources) ? openStatic.sources : [];
  const approved = sources.filter((source) => String(source.license || "").includes("approved")).length;
  const pending = sources.filter((source) => String(source.license || "").includes("pending")).length;
  const countries = new Set(sources.map((source) => source.country_code).filter(Boolean));
  const rows = [
    ["Quellen", formatNumber(sources.length)],
    ["Laender mit Quellen", formatNumber(countries.size)],
    ["Lizenz geprueft", formatNumber(approved)],
    ["Review offen", formatNumber(pending)],
  ];

  document.getElementById("source-status").innerHTML = `
    <div class="data-source-metrics">
      ${rows
        .map(
          ([label, value]) => `
            <div>
              <span>${label}</span>
              <strong>${value}</strong>
            </div>
          `,
        )
        .join("")}
    </div>
    <p>
      Die maschinenlesbare Quellenliste bleibt Teil von
      <code>data/open_static_summary.json</code>.
    </p>
  `;
}

function renderOperationalStatus(operational, liveEuHealth, germanyLive) {
  const componentsContainer = document.getElementById("operational-components");
  const summary = document.getElementById("operational-summary");
  if (!componentsContainer || !summary) {
    return;
  }
  if (!operational) {
    summary.textContent = "Der Live-Betriebsstatus ist nicht erreichbar.";
    componentsContainer.innerHTML = '<div class="data-contract-row unavailable"><span class="data-status-pill">Nicht erreichbar</span><div><strong>Live-eu Status-API</strong><span>https://live-eu.woladen.de/commercial/v1/status?view=operational</span></div></div>';
    return;
  }

  const overall = stateClass(operational.overall_state);
  summary.innerHTML = `<strong class="data-status-emphasis ${overall}">${escapeHtml(stateLabel(operational.overall_state))}</strong><span>Geprüft ${escapeHtml(formatTimestamp(operational.generated_at))}</span>`;
  const componentLabels = {
    commercial_ingestion: "Ingestion und Provider-Queues",
    live_sqlite: "Live-SQLite",
    afir_query_api: "AFIR-Abfrage-API",
    nightly_archival: "Nächtliche Archive",
    postgres_management: "Management-PostgreSQL",
    report_generation: "Berichtserzeugung",
    live_api: "Live-API",
    web_frontend: "woladen.de Frontend",
    usage_statistics: "Nutzungsstatistik",
  };
  componentsContainer.innerHTML = Object.entries(operational.components || {})
    .map(([key, state]) => `<div class="data-contract-row ${stateClass(state)}"><span class="data-status-pill">${escapeHtml(stateLabel(state))}</span><div><strong>${escapeHtml(componentLabels[key] || key)}</strong><span>${escapeHtml(state)}</span></div></div>`)
    .join("");

  const ingestion = operational.ingestion || {};
  const countries = Array.isArray(ingestion.countries) ? ingestion.countries : [];
  document.getElementById("operational-country-body").innerHTML = countries.length
    ? countries.map((row) => `<tr><td>${escapeHtml(row.country_code)}</td><td>${formatNumber(row.provider_count)}</td><td>${escapeHtml(formatTimestamp(row.last_received_at))}</td><td>${formatNumber(row.pending_count)}</td><td>${formatNumber(row.processing_count)}</td><td>${formatNumber(row.failed_count)}</td></tr>`).join("")
    : '<tr><td colspan="6">Keine Länder-Queues geladen.</td></tr>';

  const providers = Array.isArray(ingestion.providers) ? ingestion.providers : [];
  document.getElementById("operational-provider-body").innerHTML = providers.length
    ? providers.map((row) => `<tr><td>${escapeHtml(row.country_code)}</td><td><strong>${escapeHtml(row.display_name || row.source_uid)}</strong><span class="data-table-subline">${escapeHtml(row.source_uid)}</span></td><td>${escapeHtml(formatTimestamp(row.last_received_at))}</td><td>${formatNumber(row.pending_count)}</td><td>${formatNumber(row.processing_count)}</td><td>${formatNumber(row.failed_count)}</td></tr>`).join("")
    : '<tr><td colspan="6">Keine Provider-Queues geladen.</td></tr>';

  const archive = operational.archive || {};
  const archiveRows = Array.isArray(archive.countries) ? archive.countries : [];
  document.getElementById("operational-archive-body").innerHTML = archiveRows.length
    ? archiveRows.map((row) => `<tr><td>${escapeHtml(row.country_code)}</td><td>${formatNumber(row.archive_count)}</td><td>${formatBytes(row.size_bytes)}</td><td>${escapeHtml(row.latest_archive_date || "n/a")}</td><td>${escapeHtml(formatTimestamp(row.latest_archive_at))}</td></tr>`).join("")
    : '<tr><td colspan="5">Keine lokalen Archive gefunden.</td></tr>';

  const details = [
    ["Live-SQLite", operational.live_sqlite, `${formatNumber(operational.live_sqlite?.counts?.current_evse_count)} aktuelle Ladepunkte`],
    ["AFIR-Abfrage-API", operational.afir, `${formatNumber(operational.afir?.field_count)} Felder`],
    ["Management-PostgreSQL", operational.postgres, operational.postgres?.latest_archive_date ? `Letzter Tag ${operational.postgres.latest_archive_date}` : (operational.postgres?.reason || "")],
    ["Berichtserzeugung", operational.reports, `${formatNumber(operational.reports?.artifact_count)} Artefakte`],
    ["Live-eu API", liveEuHealth ? { state: liveEuHealth.ok ? "healthy" : "degraded" } : { state: "unavailable" }, "Health-Endpoint live-eu"],
    ["Deutschland live API", germanyLive ? { state: germanyLive.ok ? "healthy" : "degraded" } : { state: "unavailable" }, germanyLive ? `${formatNumber(germanyLive.queue_pending_count)} wartend` : "Health-Endpoint nicht erreichbar"],
    ["Nutzungsstatistik", operational.usage_statistics, "Web-App und Google Analytics"],
  ];
  document.getElementById("operational-detail-body").innerHTML = details
    .map(([label, item, detail]) => `<tr><td>${escapeHtml(label)}</td><td><span class="data-status-pill ${stateClass(item?.state)}">${escapeHtml(stateLabel(item?.state))}</span></td><td>${escapeHtml(detail || "")}</td></tr>`)
    .join("");
}

async function init() {
  try {
    const { data, failures } = await loadStatusData();
    renderKpis(data);
    renderFreshness(data);
    renderContractChecks(data, failures);
    renderCountries(data.openStatic);
    renderSources(data.openStatic);
    renderOperationalStatus(data.operational, data.liveEuHealth, data.germanyLive);

    if (failures.length) {
      const operationalFailure = failures.some((failure) => failure.key === "operational");
      setAlert(`${failures.length} Statusquelle(n) konnten nicht geladen werden${operationalFailure ? "; der Live-Betriebsstatus ist nicht erreichbar" : ""}.`, true);
    } else {
      setAlert("Öffentliche Daten und Live-Betriebsstatus wurden erfolgreich geladen.");
    }
  } catch (error) {
    setAlert(`Datenstatus konnte nicht aufgebaut werden: ${error.message}`, true);
  }
}

init();
