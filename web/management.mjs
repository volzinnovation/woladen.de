const MANAGEMENT_INDEX_PATH = "./data/management/index.json";
const TOP_STATIONS_LIMIT = 10;
const ANDROID_WEB_LINK = "https://play.google.com/store/apps/details?id=de.woladen.android";
const ANDROID_STORE_LINK = "market://details?id=de.woladen.android";

export const OVERVIEW_METRICS = {
  afir_stations_observed: {
    label: "Stationen im Tagesarchiv",
    description:
      "Stationen, für die im Tagesarchiv mindestens eine AFIR-Live-Beobachtung vorlag. Delta-Anbieter ohne Push können hier unterzählen.",
    kind: "count",
  },
  delta_delivery_without_push_provider_count: {
    label: "Delta-Anbieter ohne Push",
    description:
      "Delta-Lieferanten, bei denen im Tagesarchiv nur Polling-Delta-Beobachtungen ohne Push-Verkehr ankamen.",
    kind: "count",
  },
  stations_with_disruptions: {
    label: "Stationen mit Störungen",
    description: "Stationen mit mindestens einer gemeldeten Störung im Tagesverlauf.",
    kind: "count",
  },
  disruptions_at_end_of_day: {
    label: "Störungen am Tagesende",
    description: "Stationen, die am Ende des Tages noch mindestens eine Störung hatten.",
    kind: "count",
  },
  high_utilization_stations: {
    label: "Stationen mit hoher Auslastung",
    description: "Stationen mit Wechseln zwischen frei und belegt im Tagesverlauf.",
    kind: "count",
  },
  observations_total: {
    label: "AFIR Statusbeobachtungen",
    description: "Extrahierte Ladepunkt-Statusbeobachtungen im Tagesverlauf.",
    kind: "count",
  },
};

const DATE_LABEL_FORMAT = new Intl.DateTimeFormat("de-DE", {
  day: "2-digit",
  month: "2-digit",
  year: "numeric",
});
const WEEKDAY_DATE_LABEL_FORMAT = new Intl.DateTimeFormat("de-DE", {
  weekday: "long",
  day: "2-digit",
  month: "2-digit",
  year: "numeric",
});
const TIMESTAMP_LABEL_FORMAT = new Intl.DateTimeFormat("de-DE", {
  day: "2-digit",
  month: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
});
const TABLE_SORT_COLLATOR = new Intl.Collator("de-DE", {
  numeric: true,
  sensitivity: "base",
});

function numberFormat(value) {
  return new Intl.NumberFormat("de-DE").format(Number(value || 0));
}

function decimalFormat(value, digits = 1) {
  return new Intl.NumberFormat("de-DE", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(Number(value || 0));
}

function durationHoursFormat(seconds) {
  const hours = Number(seconds || 0) / 3600;
  return `${decimalFormat(hours, 1)} h`;
}

function megabytesFormat(bytes) {
  return `${decimalFormat(Number(bytes || 0) / 1_000_000, 1)} MB`;
}

function optionalNumber(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const text = String(value).trim();
  if (!text) {
    return null;
  }
  const number = Number(text);
  return Number.isFinite(number) ? number : null;
}

function firstNumber(...values) {
  for (const value of values) {
    const number = optionalNumber(value);
    if (number !== null) {
      return number;
    }
  }
  return null;
}

function optionalNumberFormat(value) {
  const number = optionalNumber(value);
  return number === null ? "" : numberFormat(number);
}

function optionalDecimalFormat(value, digits = 1) {
  const number = optionalNumber(value);
  return number === null ? "" : decimalFormat(number, digits);
}

function numericSortValue(value) {
  const number = optionalNumber(value);
  return number === null ? "" : String(number);
}

function timestampSortValue(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? text : String(parsed.getTime());
}

function timestampFormat(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return text;
  }
  return TIMESTAMP_LABEL_FORMAT.format(parsed);
}

export function buildProviderReportMetrics(row) {
  const receivedMessagesTotal = firstNumber(row?.received_messages_total, row?.messages_total) ?? 0;
  const observationsTotal = firstNumber(row?.observations_total) ?? 0;
  const uniqueChargersReferencedTotal = firstNumber(
    row?.daily_mapped_stations_observed,
    row?.unique_chargers_referenced_total,
    row?.mapped_stations_observed,
  );
  const uniqueBundleChargersReferencedTotal = firstNumber(
    row?.daily_mapped_stations_observed_in_bundle,
    row?.unique_bundle_chargers_referenced_total,
    row?.mapped_stations_observed_in_bundle,
  );
  const bundleMappedChargersTotal = firstNumber(
    row?.bundle_mapped_chargers_total,
    row?.static_matched_station_count_in_bundle,
  );
  const explicitMessagesPerCharger = firstNumber(row?.messages_per_charger);
  const explicitObservationsPerCharger = firstNumber(row?.observations_per_charger);
  const explicitBundleWithoutUpdates = firstNumber(row?.bundle_chargers_without_updates_total);
  const messagesPerCharger =
    explicitMessagesPerCharger ??
    (uniqueChargersReferencedTotal && uniqueChargersReferencedTotal > 0
      ? receivedMessagesTotal / uniqueChargersReferencedTotal
      : null);
  const observationsPerCharger =
    explicitObservationsPerCharger ??
    (uniqueChargersReferencedTotal && uniqueChargersReferencedTotal > 0
      ? observationsTotal / uniqueChargersReferencedTotal
      : null);
  const bundleChargersWithoutUpdatesTotal =
    explicitBundleWithoutUpdates ??
    (bundleMappedChargersTotal !== null && uniqueBundleChargersReferencedTotal !== null
      ? Math.max(0, bundleMappedChargersTotal - uniqueBundleChargersReferencedTotal)
      : null);

  return {
    receivedMessagesTotal,
    observationsTotal,
    uniqueChargersReferencedTotal,
    uniqueBundleChargersReferencedTotal,
    bundleMappedChargersTotal,
    bundleChargersWithoutUpdatesTotal,
    messagesPerCharger,
    observationsPerCharger,
  };
}

export function normalizeManagementDate(value) {
  const text = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return "";
  }
  const parsed = new Date(`${text}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  return text;
}

export function snapshotPathForDate(dateText) {
  const normalized = normalizeManagementDate(dateText);
  if (!normalized) {
    return "";
  }
  const [year, month, day] = normalized.split("-");
  return `./data/management/days/${year}/${month}/${day}/snapshot.json`;
}

export function buildManagementSubtitle(dateText) {
  const normalized = normalizeManagementDate(dateText);
  if (!normalized) {
    return "Störungen und Auslastung öffentlicher Ladestationen in angebundenen europäischen Ländern.";
  }
  const label = WEEKDAY_DATE_LABEL_FORMAT.format(new Date(`${normalized}T00:00:00Z`));
  const capitalizedLabel = label.charAt(0).toUpperCase() + label.slice(1);
  return `Störungen und Auslastung öffentlicher Ladestationen in angebundenen europäischen Ländern am ${capitalizedLabel}`;
}

export function buildOverviewSeries(trends, metricKey) {
  const metric = OVERVIEW_METRICS[metricKey] || OVERVIEW_METRICS.stations_with_disruptions;
  const rows = Array.isArray(trends?.summary_series) ? trends.summary_series : [];
  return {
    label: metric.label,
    description: metric.description || "",
    kind: metric.kind,
    labels: rows.map((row) => formatDateLabel(row.snapshot_date)),
    values: rows.map((row) => Number(row?.[metricKey] || 0)),
  };
}

export function buildSummaryCards(snapshot) {
  const summary = snapshot?.summary || {};
  const cards = [
    {
      label: "Stationen im Tagesarchiv",
      value: numberFormat(summary.daily_afir_stations_observed ?? summary.afir_stations_observed),
      detail:
        "Stationen, für die im Tagesarchiv mindestens eine AFIR-Live-Beobachtung vorlag. Delta-Anbieter ohne Push können hier unterzählen.",
    },
    {
      label: "Stationen mit Störungen",
      value: numberFormat(summary.stations_with_disruptions),
      detail: "Hier gab es im Tagesverlauf mindestens eine Störung.",
    },
    {
      label: "Störungen am Tagesende",
      value: numberFormat(summary.disruptions_at_end_of_day),
      detail: "Diese Stationen hatten am Ende des Tages noch mindestens eine Störung.",
    },
    {
      label: "Stationen mit hoher Auslastung",
      value: numberFormat(summary.high_utilization_stations),
      detail: "Hier war besonders viel los.",
    },
    {
      label: "AFIR Statusbeobachtungen",
      value: numberFormat(summary.observations_total),
      detail: "Extrahierte Ladepunkt-Statusmeldungen im Tagesverlauf.",
    },
  ];
  const deltaWarningCount = optionalNumber(summary.delta_delivery_without_push_provider_count) ?? 0;
  if (deltaWarningCount > 0) {
    cards.splice(1, 0, {
      label: "Delta-Anbieter ohne Push",
      value: numberFormat(deltaWarningCount),
      detail: "Bei diesen Anbietern zählt der Tageswert nur beobachtete Delta-Meldungen.",
    });
  }
  return cards;
}

export function buildStationRows(snapshot, key) {
  const rows = Array.isArray(snapshot?.[key]) ? [...snapshot[key]] : [];
  if (key === "broken_stations") {
    rows.sort((left, right) => {
      const outageDelta =
        Number(right?.out_of_order_duration_seconds_total || 0) -
        Number(left?.out_of_order_duration_seconds_total || 0);
      if (outageDelta !== 0) {
        return outageDelta;
      }
      const brokenDelta =
        Number(right?.current_broken_charger_count || 0) -
        Number(left?.current_broken_charger_count || 0);
      if (brokenDelta !== 0) {
        return brokenDelta;
      }
      const affectedDelta =
        Number(right?.affected_charger_count || 0) -
        Number(left?.affected_charger_count || 0);
      if (affectedDelta !== 0) {
        return affectedDelta;
      }
      return String(left?.station_id || "").localeCompare(String(right?.station_id || ""));
    });
    return rows.slice(0, TOP_STATIONS_LIMIT);
  }
  rows.sort((left, right) => {
    const busyDelta =
      Number(right?.busy_transition_count || 0) - Number(left?.busy_transition_count || 0);
    if (busyDelta !== 0) {
      return busyDelta;
    }
    return String(left?.station_id || "").localeCompare(String(right?.station_id || ""));
  });
  return rows.slice(0, TOP_STATIONS_LIMIT);
}

export function buildProviderRows(snapshot) {
  const rows = Array.isArray(snapshot?.provider_reports) ? [...snapshot.provider_reports] : [];
  rows.sort((left, right) => {
    const observationDelta = Number(right?.observations_total || 0) - Number(left?.observations_total || 0);
    if (observationDelta !== 0) {
      return observationDelta;
    }
    const messageDelta = Number(right?.messages_total || 0) - Number(left?.messages_total || 0);
    if (messageDelta !== 0) {
      return messageDelta;
    }
    return String(left?.display_name || left?.provider_uid || "").localeCompare(
      String(right?.display_name || right?.provider_uid || ""),
    );
  });
  return rows;
}

function formatDateLabel(value) {
  const normalized = normalizeManagementDate(value);
  if (!normalized) {
    return String(value || "");
  }
  return DATE_LABEL_FORMAT.format(new Date(`${normalized}T00:00:00Z`));
}

function stationTitle(row) {
  const address = String(row?.address || "").trim();
  const operator = String(row?.operator || "").trim();
  if (address) {
    return address;
  }
  if (operator) {
    return operator;
  }
  return String(row?.station_id || "").trim();
}

function stationMeta(row) {
  const parts = [];
  const operator = String(row?.operator || "").trim();
  const city = String(row?.city || "").trim();
  if (operator) {
    parts.push(operator);
  }
  if (city && !operator) {
    parts.push(city);
  }
  return parts.join(" · ");
}

function setSelectOptions(select, options, selectedValue) {
  select.innerHTML = "";
  for (const option of options) {
    const element = document.createElement("option");
    element.value = option.value;
    element.textContent = option.label;
    if (option.value === selectedValue) {
      element.selected = true;
    }
    select.appendChild(element);
  }
}

function isAndroid() {
  return /Android/i.test(navigator.userAgent || "");
}

function wireAppPromoLinks() {
  const googleHref = isAndroid() ? ANDROID_STORE_LINK : ANDROID_WEB_LINK;
  const googleLink = document.getElementById("management-app-link-google");
  const googleBadge = document.getElementById("management-app-badge-google");
  if (googleLink) {
    googleLink.href = googleHref;
  }
  if (googleBadge) {
    googleBadge.href = googleHref;
  }
}

function wireAppPromoDismiss() {
  const dismissButton = document.getElementById("management-app-dismiss");
  const promo = document.getElementById("management-app-promo");
  if (!dismissButton || !promo) {
    return;
  }
  dismissButton.addEventListener("click", () => {
    promo.remove();
  });
}

async function fetchJson(path) {
  const response = await fetch(path, { cache: "no-cache" });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${path}`);
  }
  return await response.json();
}

async function waitForChart() {
  if (typeof Chart !== "undefined") {
    return;
  }
  await new Promise((resolve, reject) => {
    const deadline = window.setTimeout(() => reject(new Error("Chart.js wurde nicht geladen.")), 5000);
    const poll = window.setInterval(() => {
      if (typeof Chart !== "undefined") {
        window.clearTimeout(deadline);
        window.clearInterval(poll);
        resolve();
      }
    }, 50);
  });
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function escapeAttribute(value) {
  return escapeHtml(value);
}

export function compareTableSortValues(leftValue, rightValue, type = "text") {
  if (type === "number") {
    return Number(leftValue) - Number(rightValue);
  }
  if (type === "date") {
    const leftDate = Number(leftValue);
    const rightDate = Number(rightValue);
    if (Number.isFinite(leftDate) && Number.isFinite(rightDate)) {
      return leftDate - rightDate;
    }
  }
  return TABLE_SORT_COLLATOR.compare(String(leftValue || ""), String(rightValue || ""));
}

function sortableHeaderRow(table) {
  const rows = Array.from(table.tHead?.rows || []);
  return (
    rows.find((row) => row.classList.contains("sorttop") || row.classList.contains("sortTop")) ||
    rows.at(-1) ||
    null
  );
}

function tableBodyRows(table) {
  return Array.from(table.tBodies?.[0]?.rows || []);
}

function markTableOriginalOrder(table) {
  tableBodyRows(table).forEach((row, index) => {
    row.dataset.sortOriginalIndex = String(index);
  });
}

function setSortableHeaderState(table, activeColumnIndex, direction) {
  const headerRow = sortableHeaderRow(table);
  if (!headerRow) {
    return;
  }
  for (const th of Array.from(headerRow.cells)) {
    const columnIndex = Number(th.dataset.sortColumn || th.cellIndex);
    const isActive = columnIndex === activeColumnIndex && direction !== "none";
    th.setAttribute("aria-sort", isActive ? direction : "none");
    const button = th.querySelector(".management-sort-button");
    if (button) {
      button.dataset.sortDirection = isActive ? direction : "none";
    }
  }
}

function sortTableRows(table, columnIndex, type, direction) {
  const tbody = table.tBodies?.[0];
  if (!tbody) {
    return;
  }
  const rows = tableBodyRows(table);
  const sign = direction === "descending" ? -1 : 1;
  rows.sort((leftRow, rightRow) => {
    const leftOriginal = Number(leftRow.dataset.sortOriginalIndex || 0);
    const rightOriginal = Number(rightRow.dataset.sortOriginalIndex || 0);
    if (direction === "none") {
      return leftOriginal - rightOriginal;
    }
    const leftValue = String(leftRow.cells[columnIndex]?.dataset.sortValue ?? leftRow.cells[columnIndex]?.textContent ?? "").trim();
    const rightValue = String(rightRow.cells[columnIndex]?.dataset.sortValue ?? rightRow.cells[columnIndex]?.textContent ?? "").trim();
    const leftBlank = leftValue === "";
    const rightBlank = rightValue === "";
    if (leftBlank || rightBlank) {
      if (leftBlank === rightBlank) {
        return leftOriginal - rightOriginal;
      }
      return leftBlank ? 1 : -1;
    }
    const valueDelta = compareTableSortValues(leftValue, rightValue, type);
    return valueDelta === 0 ? leftOriginal - rightOriginal : valueDelta * sign;
  });
  rows.forEach((row) => tbody.appendChild(row));
}

function toggleTableSort(table, columnIndex, type) {
  const currentColumn = Number(table.dataset.sortColumn || -1);
  const currentDirection = table.dataset.sortDirection || "none";
  let nextDirection = "ascending";
  if (currentColumn === columnIndex && currentDirection === "ascending") {
    nextDirection = "descending";
  } else if (currentColumn === columnIndex && currentDirection === "descending") {
    nextDirection = "none";
  }

  table.dataset.sortColumn = nextDirection === "none" ? "" : String(columnIndex);
  table.dataset.sortDirection = nextDirection;
  sortTableRows(table, columnIndex, type, nextDirection);
  setSortableHeaderState(table, columnIndex, nextDirection);
}

function wireSortableTable(table) {
  if (table.dataset.sortableInitialized === "true") {
    return;
  }
  const headerRow = sortableHeaderRow(table);
  if (!headerRow) {
    return;
  }
  for (const th of Array.from(headerRow.cells)) {
    if (th.classList.contains("unsortable") || th.dataset.sortable === "false") {
      continue;
    }
    const columnIndex = th.cellIndex;
    const sortType = th.dataset.sortType || "text";
    const label = th.textContent.trim();
    const button = document.createElement("button");
    const labelSpan = document.createElement("span");
    const indicator = document.createElement("span");
    button.type = "button";
    button.className = "management-sort-button";
    button.dataset.sortDirection = "none";
    button.setAttribute("aria-label", `${label} sortieren`);
    labelSpan.className = "management-sort-label";
    labelSpan.textContent = label;
    indicator.className = "management-sort-indicator";
    indicator.setAttribute("aria-hidden", "true");
    button.append(labelSpan, indicator);
    button.addEventListener("click", () => toggleTableSort(table, columnIndex, sortType));
    th.textContent = "";
    th.dataset.sortColumn = String(columnIndex);
    th.setAttribute("aria-sort", "none");
    th.appendChild(button);
  }
  table.dataset.sortableInitialized = "true";
}

function wireSortableTables(root = document) {
  root.querySelectorAll("table.management-sortable").forEach(wireSortableTable);
}

function resetSortableTable(table) {
  markTableOriginalOrder(table);
  table.dataset.sortColumn = "";
  table.dataset.sortDirection = "none";
  setSortableHeaderState(table, -1, "none");
}

function resetSortableTables(root = document) {
  root.querySelectorAll("table.management-sortable").forEach(resetSortableTable);
}

function chartThemeColor(index = 0) {
  const palette = ["#12664f", "#d1633c", "#27689c", "#b48832", "#8c5b6a"];
  return palette[index % palette.length];
}

function createLineChart(canvasId, series, { colorIndex = 0, color = null } = {}) {
  const canvas = document.getElementById(canvasId);
  const seriesColor = color ?? chartThemeColor(colorIndex);
  return new Chart(canvas.getContext("2d"), {
    type: "line",
    data: {
      labels: series.labels,
      datasets: [
        {
          label: series.label,
          data: series.values,
          borderColor: seriesColor,
          backgroundColor: seriesColor,
          pointBackgroundColor: seriesColor,
          pointBorderColor: seriesColor,
          tension: 0.25,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
      },
      scales: {
        y: { beginAtZero: true },
      },
    },
  });
}

function renderKpis(snapshot) {
  const cards = buildSummaryCards(snapshot);
  const host = document.getElementById("management-kpis");
  host.innerHTML = "";
  for (const cardInfo of cards) {
    const card = document.createElement("article");
    card.className = "management-kpi";
    card.innerHTML = `
      <div class="management-kpi-label">${escapeHtml(cardInfo.label)}</div>
      <div class="management-kpi-value">${escapeHtml(cardInfo.value)}</div>
      <div class="management-kpi-detail">${escapeHtml(cardInfo.detail)}</div>
    `;
    host.appendChild(card);
  }
}

function renderBrokenStations(snapshot) {
  const rows = buildStationRows(snapshot, "broken_stations");
  const tbody = document.getElementById("broken-stations-body");
  tbody.innerHTML = "";
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="6">Für diesen Tag wurden keine gestörten Stationen erkannt.</td></tr>';
    return;
  }
  for (const row of rows) {
    const stationCell = row.station_url
      ? `<a href="${escapeAttribute(row.station_url)}">${escapeHtml(stationTitle(row))}</a>`
      : `<span>${escapeHtml(stationTitle(row))}</span>`;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>
        ${stationCell}
        <div class="provider-sub">${escapeHtml(stationMeta(row))}</div>
      </td>
      <td data-sort-value="${escapeAttribute(row.city || "")}">${escapeHtml(row.city || "")}</td>
      <td data-sort-value="${numericSortValue(row.affected_charger_count)}">${numberFormat(row.affected_charger_count)}</td>
      <td data-sort-value="${numericSortValue(row.current_broken_charger_count)}">${numberFormat(row.current_broken_charger_count)}</td>
      <td data-sort-value="${numericSortValue(row.out_of_order_duration_seconds_total)}">${durationHoursFormat(row.out_of_order_duration_seconds_total)}</td>
      <td data-sort-value="${escapeAttribute(row.status_label || "")}">${escapeHtml(row.status_label || "")}</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderBusyStations(snapshot) {
  const rows = buildStationRows(snapshot, "busiest_stations");
  const tbody = document.getElementById("busy-stations-body");
  tbody.innerHTML = "";
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="5">Für diesen Tag wurden keine Stationen mit hoher Auslastung erkannt.</td></tr>';
    return;
  }
  for (const row of rows) {
    const stationCell = row.station_url
      ? `<a href="${escapeAttribute(row.station_url)}">${escapeHtml(stationTitle(row))}</a>`
      : `<span>${escapeHtml(stationTitle(row))}</span>`;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>
        ${stationCell}
        <div class="provider-sub">${escapeHtml(stationMeta(row))}</div>
      </td>
      <td data-sort-value="${escapeAttribute(row.city || "")}">${escapeHtml(row.city || "")}</td>
      <td data-sort-value="${numericSortValue(row.busy_transition_count)}">${numberFormat(row.busy_transition_count)}</td>
      <td data-sort-value="${numericSortValue(row.busy_evse_count)}">${numberFormat(row.busy_evse_count)}</td>
      <td data-sort-value="${numericSortValue(row.max_power_kw)}">${numberFormat(row.max_power_kw)} kW</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderProviderReports(snapshot) {
  const rows = buildProviderRows(snapshot);
  const tbody = document.getElementById("provider-reports-body");
  if (!tbody) {
    return;
  }
  tbody.innerHTML = "";
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="11">Für diesen Tag wurden keine Anbieterberichte veröffentlicht.</td></tr>';
    return;
  }
  for (const row of rows) {
    const tr = document.createElement("tr");
    const displayName = row.display_name || row.provider_uid || "";
    const publisher = row.publisher || row.provider_uid || "";
    const metrics = buildProviderReportMetrics(row);
    const coverageWarning =
      Number(row.delta_delivery_without_push || 0) > 0
        ? '<div class="provider-sub provider-sub-warning">Delta ohne Push, Tageswert unterzählt</div>'
        : "";
    tr.innerHTML = `
      <td data-sort-value="${escapeAttribute(displayName)}">
        <span>${escapeHtml(displayName)}</span>
        <div class="provider-sub">${escapeHtml(publisher)}</div>
        ${coverageWarning}
      </td>
      <td data-sort-value="${numericSortValue(metrics.observationsTotal)}">${numberFormat(metrics.observationsTotal)}</td>
      <td data-sort-value="${numericSortValue(metrics.observationsPerCharger)}">${optionalDecimalFormat(metrics.observationsPerCharger, 1)}</td>
      <td data-sort-value="${numericSortValue(metrics.receivedMessagesTotal)}">${numberFormat(metrics.receivedMessagesTotal)}</td>
      <td data-sort-value="${numericSortValue(metrics.uniqueChargersReferencedTotal)}">${optionalNumberFormat(metrics.uniqueChargersReferencedTotal)}</td>
      <td data-sort-value="${numericSortValue(metrics.bundleChargersWithoutUpdatesTotal)}">${optionalNumberFormat(metrics.bundleChargersWithoutUpdatesTotal)}</td>
      <td data-sort-value="${numericSortValue(row.push_messages_total)}">${numberFormat(row.push_messages_total)}</td>
      <td data-sort-value="${numericSortValue(row.http_response_messages_total)}">${numberFormat(row.http_response_messages_total)}</td>
      <td data-sort-value="${numericSortValue(Number(row.fetch_failure_messages_total || 0) + Number(row.http_error_messages_total || 0))}">${numberFormat(Number(row.fetch_failure_messages_total || 0) + Number(row.http_error_messages_total || 0))}</td>
      <td data-sort-value="${numericSortValue(row.payload_byte_length_total)}">${megabytesFormat(row.payload_byte_length_total)}</td>
      <td data-sort-value="${escapeAttribute(timestampSortValue(row.latest_message_timestamp))}">${escapeHtml(timestampFormat(row.latest_message_timestamp))}</td>
    `;
    tbody.appendChild(tr);
  }
}

async function initManagementPage() {
  const status = document.getElementById("management-status");
  wireAppPromoLinks();
  wireAppPromoDismiss();
  const indexPayload = await fetchJson(MANAGEMENT_INDEX_PATH);
  const trendsPayload = await fetchJson("./data/management/trends.json");
  await waitForChart();
  wireSortableTables();
  const availableDates = Array.isArray(indexPayload.available_dates) ? indexPayload.available_dates : [];
  if (!availableDates.length) {
    throw new Error("Keine Tagesauswertungen verfügbar.");
  }

  const url = new URL(window.location.href);
  let currentDate =
    normalizeManagementDate(url.searchParams.get("date")) ||
    indexPayload.latest_date ||
    availableDates.at(-1);
  if (!availableDates.includes(currentDate)) {
    currentDate = availableDates.at(-1);
  }

  let currentSnapshot = null;
  let overviewChart = null;

  const datePicker = document.getElementById("management-date");
  const prevDay = document.getElementById("management-prev-day");
  const nextDay = document.getElementById("management-next-day");
  const overviewMetricSelect = document.getElementById("management-overview-metric");
  const overviewControl = document.getElementById("management-overview-control");
  const controlsPanel = document.querySelector(".management-controls");

  const overviewMetricOptions = Object.entries(OVERVIEW_METRICS).map(([value, meta]) => ({
    value,
    label: meta.label,
  }));

  setSelectOptions(overviewMetricSelect, overviewMetricOptions, "stations_with_disruptions");
  if (overviewMetricOptions.length <= 1) {
    overviewControl?.setAttribute("hidden", "");
    controlsPanel?.classList.add("management-controls--single");
  } else {
    overviewControl?.removeAttribute("hidden");
    controlsPanel?.classList.remove("management-controls--single");
  }

  function syncUrl() {
    url.searchParams.set("date", currentDate);
    history.replaceState({}, "", url);
  }

  function updateDateControls() {
    datePicker.value = currentDate;
    const index = availableDates.indexOf(currentDate);
    prevDay.disabled = index <= 0;
    nextDay.disabled = index < 0 || index >= availableDates.length - 1;
  }

  function renderCharts() {
    if (overviewChart) overviewChart.destroy();
    const selectedMetric = OVERVIEW_METRICS[overviewMetricSelect.value] || OVERVIEW_METRICS.stations_with_disruptions;
    const title = document.getElementById("management-overview-title");
    const description = document.getElementById("management-overview-description");
    if (title) {
      title.textContent = selectedMetric.label;
    }
    if (description) {
      description.textContent = selectedMetric.description || "";
    }
    overviewChart = createLineChart(
      "management-overview-chart",
      buildOverviewSeries(trendsPayload, overviewMetricSelect.value),
      { color: "#000000" },
    );
  }

  async function loadSnapshot(targetDate) {
    currentSnapshot = await fetchJson(snapshotPathForDate(targetDate));
    currentDate = targetDate;
    syncUrl();
    updateDateControls();
    renderKpis(currentSnapshot);
    renderBrokenStations(currentSnapshot);
    renderBusyStations(currentSnapshot);
    renderProviderReports(currentSnapshot);
    resetSortableTables();
    renderCharts();

    const summary = currentSnapshot.summary || {};
    const subtitle = document.getElementById("management-subtitle");
    if (subtitle) {
      subtitle.textContent = buildManagementSubtitle(currentDate);
    }
    status.textContent = "";
    status.hidden = true;
    status.classList.remove("is-error");
  }

  datePicker.addEventListener("change", () => {
    const nextValue = normalizeManagementDate(datePicker.value);
    if (nextValue && availableDates.includes(nextValue)) {
      loadSnapshot(nextValue).catch(renderError);
    }
  });
  prevDay.addEventListener("click", () => {
    const index = availableDates.indexOf(currentDate);
    if (index > 0) {
      loadSnapshot(availableDates[index - 1]).catch(renderError);
    }
  });
  nextDay.addEventListener("click", () => {
    const index = availableDates.indexOf(currentDate);
    if (index >= 0 && index < availableDates.length - 1) {
      loadSnapshot(availableDates[index + 1]).catch(renderError);
    }
  });
  overviewMetricSelect.addEventListener("change", renderCharts);

  function renderError(error) {
    console.error(error);
    status.textContent = `Die Tagesauswertung konnte nicht geladen werden: ${error?.message || error}`;
    status.classList.add("is-error");
    status.hidden = false;
  }

  updateDateControls();
  await loadSnapshot(currentDate);
}

if (typeof window !== "undefined" && typeof document !== "undefined") {
  initManagementPage().catch((error) => {
    const status = document.getElementById("management-status");
    if (status) {
      status.textContent = `Die Tagesauswertung konnte nicht geladen werden: ${error?.message || error}`;
      status.classList.add("is-error");
      status.hidden = false;
    }
    console.error(error);
  });
}
