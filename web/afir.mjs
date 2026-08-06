import {
  afirChargingPointCount,
  afirCountryDisplayName,
  afirPointCoverage,
  afirAggregateFieldsUrl,
  afirStationDetailUrl,
  createAfirDataSource,
  nextAfirLevel,
  scopeFromAfirDimensions,
} from "./afir-api.mjs?v=20260806-station-query1";
import {
  createAfirLoadingIndicator,
} from "./afir-loading.mjs?v=20260730-afir-progress1";

const PAGE_SIZE = 100;
const LEVEL_LABELS = {
  country: "Länder",
  provider: "Anbieter",
  operator: "Betreiber",
  location: "Standorte",
  point: "Ladepunkte",
};
const COUNTRY_NAMES = new Intl.DisplayNames(["de"], { type: "region" });
const NUMBER = new Intl.NumberFormat("de-DE");
const TABLE_SORT_COLLATOR = new Intl.Collator("de", {
  numeric: true,
  sensitivity: "base",
});
const AFIR_SORT_COLUMNS = new Set([
  "identity",
  "static",
  "dynamic",
  "freshness",
  "station_count",
  "charging_point_count",
]);
const AFIR_SORT_DIRECTIONS = new Set(["asc", "desc"]);
const tableSortState = new Map();

function element(id) {
  return document.getElementById(id);
}

function configuredBaseUrl() {
  return (
    String(window.WOLADEN_AFIR_API_BASE_URL || "").trim() ||
    "https://live-eu.woladen.de/v1/afir-compliance"
  );
}

function percent(value) {
  if (value === null || value === undefined || value === "") {
    return "nicht bewertet";
  }
  const number = Number(value);
  return Number.isFinite(number)
    ? `${new Intl.NumberFormat("de-DE", {
        maximumFractionDigits: 1,
      }).format(number)} %`
    : "nicht bewertet";
}

function duration(seconds) {
  if (seconds === null || seconds === undefined || seconds === "") {
    return "nicht bewertet";
  }
  const value = Number(seconds);
  if (!Number.isFinite(value)) return "nicht bewertet";
  if (value < 60) return `${Math.round(value)} s`;
  if (value < 3600) return `${Math.round(value / 60)} min`;
  if (value < 86400) {
    return `${new Intl.NumberFormat("de-DE", {
      maximumFractionDigits: 1,
    }).format(value / 3600)} h`;
  }
  return `${new Intl.NumberFormat("de-DE", {
    maximumFractionDigits: 1,
  }).format(value / 86400)} d`;
}

function sourceTimestamp(value) {
  const text = String(value || "").trim();
  if (!text) return "nicht bewertet";
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return text;
  return `${new Intl.DateTimeFormat("de-DE", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "UTC",
  }).format(parsed)} UTC`;
}

function coverageState(summary) {
  return String(afirPointCoverage(summary)?.coverage_state || "unassessed");
}

function coveragePercent(summary) {
  return afirPointCoverage(summary)?.coverage_pct;
}

function pointCoverageRatio(coverage, phase = "current") {
  const phases = {
    current: {
      countKey: "present_current_count",
      percentKey: "coverage_pct",
      denominatorKey: "denominator",
    },
    previous: {
      countKey: "present_previous_count",
      denominatorKey: "denominator",
    },
    both: {
      countKey: "present_in_both_distinct_releases_count",
      denominatorKey: "denominator",
    },
  };
  const selectedPhase = phases[phase] || phases.current;
  const numerator = Number(coverage?.[selectedPhase.countKey] || 0);
  const denominator = Number(
    coverage?.[selectedPhase.denominatorKey] || 0,
  );
  const ratio = denominator > 0 ? (numerator / denominator) * 100 : null;
  return `${NUMBER.format(numerator)} / ${NUMBER.format(
    denominator,
  )} · ${percent(ratio)}`;
}

function identityForGroup(level, dimensions) {
  const countryCode = String(dimensions?.country_code || "");
  const values = {
    eu27: {
      primary: "Europäische Union",
      secondary: "EU-27 · 27 Mitgliedstaaten",
    },
    country: {
      primary:
        afirCountryDisplayName(countryCode, COUNTRY_NAMES) || "Ohne Land",
      secondary: countryCode,
    },
    provider: {
      primary: dimensions?.provider_uid || "Anbieter nicht angegeben",
      secondary: countryCode,
    },
    operator: {
      primary: dimensions?.operator_id || "Betreiber nicht angegeben",
      secondary: dimensions?.provider_uid || "",
    },
    location: {
      primary: dimensions?.location_id || "Standort nicht angegeben",
      secondary: dimensions?.operator_id || dimensions?.provider_uid || "",
    },
    point: {
      primary: dimensions?.point_id || "Ladepunkt nicht angegeben",
      secondary:
        dimensions?.detail_station_id ||
        dimensions?.station_id ||
        dimensions?.location_id ||
        dimensions?.durable_entity_key ||
        "",
    },
  };
  return values[level];
}

function sourceDisplayName(sourceUid) {
  const tokens = String(sourceUid || "")
    .trim()
    .toLowerCase()
    .split("_")
    .filter(Boolean);
  if (tokens.length > 1 && /^[a-z]{2}$/.test(tokens[0])) {
    tokens.shift();
  }
  while (
    tokens.length &&
    new Set([
      "afir",
      "baseline",
      "charge_points",
      "charging",
      "datex",
      "dynamic",
      "evse",
      "locations",
      "register",
      "status",
      "static",
      "tariffs",
    ]).has(tokens[tokens.length - 1])
  ) {
    tokens.pop();
  }
  return tokens.join("_");
}

function sourceDescription(group) {
  const sourceUids = Array.isArray(group?.source_uids)
    ? group.source_uids
    : [];
  const labels = [...new Set(sourceUids.map(sourceDisplayName).filter(Boolean))];
  return labels.length ? labels.map((label) => `via ${label}`).join(", ") : "";
}

function appendCell(row, content, className = "") {
  const cell = document.createElement("td");
  if (className) cell.className = className;
  if (content instanceof Node) cell.append(content);
  else cell.textContent = String(content ?? "");
  row.append(cell);
  return cell;
}

function setSortValue(cell, value) {
  if (value !== null && value !== undefined && value !== "") {
    cell.dataset.sortValue = String(value);
  } else {
    cell.dataset.sortValue = "";
  }
  return cell;
}

function sortableHeaders(table) {
  return Array.from(table.tHead?.rows || []).flatMap((row) =>
    Array.from(row.cells),
  );
}

function tableRows(table) {
  return Array.from(table.tBodies?.[0]?.rows || []);
}

function headerColumnIndex(header) {
  const explicit = String(header.dataset.sortColumn || "").trim();
  return explicit ? Number(explicit) : header.cellIndex;
}

function compareSortValues(left, right, type) {
  if (type === "number") {
    const leftNumber = Number(left);
    const rightNumber = Number(right);
    if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
      return leftNumber - rightNumber;
    }
    if (Number.isFinite(leftNumber)) return -1;
    if (Number.isFinite(rightNumber)) return 1;
  }
  return TABLE_SORT_COLLATOR.compare(String(left), String(right));
}

function updateSortHeaderState(table, activeColumn, direction) {
  for (const header of sortableHeaders(table)) {
    const active =
      headerColumnIndex(header) === activeColumn && direction !== "none";
    header.setAttribute("aria-sort", active ? direction : "none");
    const button = header.querySelector(".afir-sort-button");
    if (button) button.dataset.sortDirection = active ? direction : "none";
  }
}

function sortTable(table, column, type, direction) {
  const body = table.tBodies?.[0];
  if (!body) return;
  const rows = tableRows(table);
  rows.sort((leftRow, rightRow) => {
    if (direction === "none") {
      return Number(leftRow.dataset.sortOriginalIndex || 0) -
        Number(rightRow.dataset.sortOriginalIndex || 0);
    }
    const leftValue = String(
      leftRow.cells[column]?.dataset.sortValue ||
        leftRow.cells[column]?.textContent ||
        "",
    ).trim();
    const rightValue = String(
      rightRow.cells[column]?.dataset.sortValue ||
        rightRow.cells[column]?.textContent ||
        "",
    ).trim();
    if (!leftValue || !rightValue) {
      if (!leftValue && !rightValue) {
        return Number(leftRow.dataset.sortOriginalIndex || 0) -
          Number(rightRow.dataset.sortOriginalIndex || 0);
      }
      return leftValue ? -1 : 1;
    }
    const result = compareSortValues(leftValue, rightValue, type);
    if (result) return direction === "descending" ? -result : result;
    return Number(leftRow.dataset.sortOriginalIndex || 0) -
      Number(rightRow.dataset.sortOriginalIndex || 0);
  }).forEach((row) => body.append(row));
}

function refreshSortableTable(table) {
  if (!table) return;
  tableRows(table).forEach((row, index) => {
    if (!Object.hasOwn(row.dataset, "sortOriginalIndex")) {
      row.dataset.sortOriginalIndex = String(index);
    }
  });
  const tableKey = String(table.dataset.tableKey || "");
  const sort = tableSortState.get(tableKey) || {
    column: -1,
    direction: "none",
  };
  if (sort.column >= 0 && sort.direction !== "none") {
    const header = sortableHeaders(table).find(
      (candidate) => headerColumnIndex(candidate) === sort.column,
    );
    sortTable(table, sort.column, header?.dataset.sortType || "text", sort.direction);
  }
  updateSortHeaderState(table, sort.column, sort.direction);
}

function toggleTableSort(table, header) {
  if (table.dataset.serverSort === "true") {
    const column = String(header.dataset.sortKey || "identity");
    const direction =
      state.sortColumn === column && state.sortDirection === "asc"
        ? "desc"
        : "asc";
    state.sortColumn = column;
    state.sortDirection = direction;
    state.offset = 0;
    updateUrl();
    void loadGroups();
    return;
  }
  const tableKey = String(table.dataset.tableKey || "");
  const column = headerColumnIndex(header);
  const current = tableSortState.get(tableKey) || {
    column: -1,
    direction: "none",
  };
  let direction = "ascending";
  if (current.column === column) {
    direction = current.direction === "ascending" ? "descending" : "none";
  }
  tableSortState.set(tableKey, { column, direction });
  tableRows(table).forEach((row, index) => {
    if (!row.dataset.sortOriginalIndex) {
      row.dataset.sortOriginalIndex = String(index);
    }
  });
  sortTable(table, column, header.dataset.sortType || "text", direction);
  updateSortHeaderState(table, column, direction);
}

function wireSortableTables() {
  document.querySelectorAll("table.afir-sortable").forEach((table) => {
    const serverSort = table.dataset.serverSort === "true";
    for (const header of sortableHeaders(table)) {
      if (
        header.dataset.sortable === "false" ||
        header.classList.contains("unsortable") ||
        header.dataset.sortWired === "true"
      ) {
        continue;
      }
      const label = header.dataset.sortLabel || header.textContent.trim();
      const button = document.createElement("button");
      const labelSpan = document.createElement("span");
      const indicator = document.createElement("span");
      button.type = "button";
      button.className = "afir-sort-button";
      button.dataset.sortDirection = "none";
      button.setAttribute("aria-label", `${label} sortieren`);
      labelSpan.className = "afir-sort-label";
      labelSpan.textContent = header.textContent.trim();
      indicator.className = "afir-sort-indicator";
      indicator.setAttribute("aria-hidden", "true");
      button.append(labelSpan, indicator);
      header.replaceChildren(button);
      header.dataset.sortColumn = String(header.cellIndex);
      header.dataset.sortWired = "true";
      header.setAttribute("aria-sort", "none");
      button.addEventListener("click", () => toggleTableSort(table, header));
    }
    if (serverSort) {
      const activeHeader = sortableHeaders(table).find(
        (header) => header.dataset.sortKey === state.sortColumn,
      );
      if (activeHeader) {
        tableSortState.set(table.dataset.tableKey, {
          column: headerColumnIndex(activeHeader),
          direction:
            state.sortDirection === "desc" ? "descending" : "ascending",
        });
      }
    }
    refreshSortableTable(table);
  });
}

function metric(summary) {
  const span = document.createElement("span");
  span.className = "afir-metric";
  span.dataset.state = coverageState(summary);
  span.textContent = percent(coveragePercent(summary));
  return span;
}

function pointCoverageSortValue(coverage, phase = "current") {
  const countKeys = {
    current: "present_current_count",
    previous: "present_previous_count",
    both: "present_in_both_distinct_releases_count",
  };
  const value = coverage?.[countKeys[phase] || countKeys.current];
  return value === null || value === undefined ? "" : Number(value);
}

function groupSearchText(group) {
  return Object.values(group?.dimensions || {})
    .join(" ")
    .toLocaleLowerCase("de");
}

function renderSearchSummary() {
  const summary = element("afir-search-summary");
  const value = element("afir-search-summary-value");
  if (!summary || !value) return;
  const query = String(state.searchQuery || "").trim();
  summary.hidden = !query;
  value.textContent = query ? `„${query}“` : "";
}

function fieldContext(group) {
  return Object.values(group?.dimensions || {})
    .filter(Boolean)
    .join(" · ");
}

const state = {
  level: "country",
  scope: {},
  openFields: false,
  trail: [],
  offset: 0,
  payload: null,
  selectedGroup: null,
  loading: false,
  sortColumn: "identity",
  sortDirection: "asc",
  searchQuery: "",
};

const source = createAfirDataSource({ baseUrl: configuredBaseUrl() });
const loadingIndicator = createAfirLoadingIndicator({
  root: element("afir-loading"),
  progress: element("afir-progress"),
  label: element("afir-loading-label"),
  detail: element("afir-loading-detail"),
  busyRegion: element("afir-main"),
});

function updateUrl() {
  const query = new URLSearchParams();
  query.set("level", state.level);
  query.set("sort", state.sortColumn);
  query.set("direction", state.sortDirection);
  if (state.searchQuery) query.set("search", state.searchQuery);
  if (state.openFields && state.selectedGroup) {
    query.set("view", "fields");
  }
  for (const [key, value] of Object.entries(state.scope)) {
    if (value) query.set(key, value);
  }
  history.replaceState(null, "", `./afir.html?${query.toString()}`);
}

function restoreUrl() {
  const query = new URLSearchParams(location.search);
  const requestedLevel = query.get("level");
  if (Object.hasOwn(LEVEL_LABELS, requestedLevel)) {
    state.level = requestedLevel;
  }
  const requestedSort = String(query.get("sort") || "identity").trim();
  const requestedDirection = String(query.get("direction") || "asc").trim();
  state.sortColumn = AFIR_SORT_COLUMNS.has(requestedSort)
    ? requestedSort
    : "identity";
  state.sortDirection = AFIR_SORT_DIRECTIONS.has(requestedDirection)
    ? requestedDirection
    : "asc";
  const requestedSearch = String(query.get("search") || "").trim();
  state.searchQuery = requestedSearch.length >= 3 ? requestedSearch : "";
  state.openFields = query.get("view") === "fields";
  state.scope = Object.fromEntries(
    [
      "country_code",
      "provider_uid",
      "operator_id",
      "location_id",
      "station_id",
      "point_id",
      "durable_entity_key",
    ]
      .map((key) => [key, String(query.get(key) || "").trim()])
      .filter(([, value]) => value),
  );
  const hierarchy = [
    ["country", "country_code"],
    ["provider", "provider_uid"],
    ["operator", "operator_id"],
    ["location", "location_id"],
    ["point", "point_id"],
  ];
  const currentIndex = hierarchy.findIndex(([level]) => level === state.level);
  state.trail = [];
  for (let index = 0; index < currentIndex; index += 1) {
    const [level, key] = hierarchy[index];
    const value = state.scope[key];
    if (!value) continue;
    const scope = {};
    for (let ancestor = 0; ancestor <= index; ancestor += 1) {
      const ancestorKey = hierarchy[ancestor][1];
      if (state.scope[ancestorKey]) scope[ancestorKey] = state.scope[ancestorKey];
    }
    state.trail.push({
      label: value,
      level,
      scope,
    });
  }
}

function hasScopeMatch(dimensions, scope) {
  return Object.entries(scope).every(
    ([key, value]) => String(dimensions?.[key] || "").trim() === value,
  );
}

function resolveFieldGroupFromUrl(payload = null) {
  if (!payload) return null;
  if (state.level === "country" && Object.keys(state.scope).length === 0) {
    return payload.eu27_aggregate || null;
  }
  const matched = (payload.groups || []).find((group) =>
    hasScopeMatch(group?.dimensions || {}, state.scope),
  );
  if (matched) return matched;
  return payload.groups?.length === 1 ? payload.groups[0] : null;
}

function setStatus(message, tone = "") {
  const status = element("afir-status");
  status.textContent = message;
  status.dataset.tone = tone;
}

function renderBreadcrumb() {
  const root = element("afir-breadcrumb");
  root.replaceChildren();
  const entries = [
    { label: "Alle Länder", level: "country", scope: {}, trailLength: 0 },
    ...state.trail.map((entry, index) => ({
      ...entry,
      trailLength: index + 1,
    })),
  ];
  entries.forEach((entry, index) => {
    if (index) {
      const separator = document.createElement("span");
      separator.textContent = "›";
      root.append(separator);
    }
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = entry.label;
    button.addEventListener("click", () => {
      state.level = entry.level;
      state.scope = { ...entry.scope };
      state.trail = state.trail.slice(0, entry.trailLength);
      state.offset = 0;
      state.selectedGroup = null;
      updateUrl();
      void loadGroups();
    });
    root.append(button);
  });
  const current = document.createElement("span");
  current.textContent = LEVEL_LABELS[state.level];
  root.append(document.createTextNode("›"), current);
}

function breadcrumbLabel(level, dimensions) {
  const keys = {
    country: "country_code",
    provider: "provider_uid",
    operator: "operator_id",
    location: "location_id",
    point: "point_id",
  };
  return String(dimensions?.[keys[level]] || "").trim() || LEVEL_LABELS[level];
}

function renderFields(group) {
  const section = element("afir-field-section");
  const fields = Array.isArray(group?.fields) ? group.fields : [];
  const dynamicFields = fields.filter((field) => field.data_kind === "dynamic");
  const staticFields = fields.filter((field) => field.data_kind !== "dynamic");

  function renderFieldRows(body, fieldRows, dynamic) {
    body.replaceChildren();
    for (const field of fieldRows) {
      const row = document.createElement("tr");
      const code = document.createElement("span");
      code.className = "afir-field-code";
      code.textContent = field.field_id || "–";
      const identity = document.createElement("div");
      identity.append(code);
      setSortValue(appendCell(row, identity), field.field_id || field.technical_key);
      setSortValue(
        appendCell(row, field.label || field.technical_key || "–"),
        field.label || field.technical_key,
      );
      const pointCoverage = field.charging_point_coverage || {};
      setSortValue(
        appendCell(row, pointCoverageRatio(pointCoverage, "current")),
        pointCoverageSortValue(pointCoverage, "current"),
      );
      if (dynamic) {
        setSortValue(
          appendCell(row, pointCoverageRatio(pointCoverage, "previous")),
          pointCoverageSortValue(pointCoverage, "previous"),
        );
        setSortValue(
          appendCell(row, pointCoverageRatio(pointCoverage, "both")),
          pointCoverageSortValue(pointCoverage, "both"),
        );
        const age = field.freshness?.source_observed_age_seconds?.p50;
        setSortValue(appendCell(row, duration(age)), age);
      }
      body.append(row);
    }
  }

  renderFieldRows(element("afir-dynamic-fields"), dynamicFields, true);
  renderFieldRows(element("afir-static-fields"), staticFields, false);
  element("afir-dynamic-field-count").textContent =
    `${NUMBER.format(dynamicFields.length)} Felder`;
  element("afir-static-field-count").textContent =
    `${NUMBER.format(staticFields.length)} Felder`;
  for (const table of [
    element("afir-dynamic-fields-table"),
    element("afir-static-fields-table"),
  ]) {
    refreshSortableTable(table);
  }
  element("afir-field-title").textContent = `${fields.length} AFIR-Felder`;
  element("afir-field-context").textContent = fieldContext(group);
  const detailLink = element("afir-detail-link");
  const detailUrl = afirStationDetailUrl(group?.dimensions);
  detailLink.hidden = !detailUrl;
  if (detailUrl) detailLink.href = detailUrl;
  section.hidden = false;
}

function drillInto(group) {
  const nextLevel = nextAfirLevel(state.level);
  if (!nextLevel) {
    state.selectedGroup = group;
    state.openFields = true;
    updateUrl();
    renderGroups();
    renderFields(group);
    element("afir-field-section").scrollIntoView({
      behavior: "smooth",
      block: "start",
    });
    return;
  }
  const breadcrumbScope = { ...state.scope };
  const scopeKeyByLevel = {
    country: "country_code",
    provider: "provider_uid",
    operator: "operator_id",
    location: "location_id",
    point: "point_id",
  };
  const scopeKey = scopeKeyByLevel[state.level];
  if (scopeKey && group.dimensions?.[scopeKey]) {
    breadcrumbScope[scopeKey] = group.dimensions[scopeKey];
  }
  state.trail.push({
    label: breadcrumbLabel(state.level, group.dimensions),
    level: state.level,
    scope: breadcrumbScope,
  });
  state.level = nextLevel;
  state.scope = scopeFromAfirDimensions(group.dimensions);
  state.offset = 0;
  state.selectedGroup = null;
  state.openFields = false;
  element("afir-search").value = "";
  updateUrl();
  void loadGroups();
}

function appendGroupCells(row, group, identity, actionLabel = "→") {
  const identityBlock = document.createElement("div");
  const primary = document.createElement("strong");
  primary.textContent = identity.primary;
  const secondary = document.createElement("small");
  const sourceText =
    state.level === "country" ? "" : sourceDescription(group);
  secondary.textContent = sourceText || identity.secondary;
  identityBlock.append(primary, secondary);
  if (state.level === "point") {
    const updated = document.createElement("small");
    updated.className = "afir-group-updated";
    updated.textContent = `Stand: ${sourceTimestamp(group.static_last_updated)}`;
    identityBlock.append(updated);
  }
  setSortValue(appendCell(row, identityBlock, "afir-identity"), identity.primary);
  setSortValue(
    appendCell(row, metric(group.static_compliance)),
    coveragePercent(group.static_compliance),
  );
  setSortValue(
    appendCell(row, metric(group.dynamic_compliance)),
    coveragePercent(group.dynamic_compliance),
  );
  const freshness =
    group.dynamic_compliance?.freshness_state === "assessed"
      ? group.source_release_cadence?.source_release_gap_seconds?.p50
      : "";
  setSortValue(
    appendCell(
    row,
      freshness === "" ? "nicht bewertet" : duration(freshness),
    ),
    freshness,
  );
  if (state.level !== "point") {
    const stationCount = Number(group.entity_counts?.station_count || 0);
    const chargingPointCount = afirChargingPointCount(group);
    setSortValue(
      appendCell(row, NUMBER.format(stationCount), "afir-count-stations"),
      stationCount,
    );
    setSortValue(
      appendCell(row, NUMBER.format(chargingPointCount), "afir-count-points"),
      chargingPointCount,
    );
  }
  appendCell(row, actionLabel, "afir-open");
}

function updateAggregateCountColumns() {
  const table = element("afir-groups-table");
  if (!table) return;
  const hidden = state.level === "point";
  table.querySelectorAll(
    '[data-sort-key="station_count"], [data-sort-key="charging_point_count"], .afir-count-stations, .afir-count-points',
  ).forEach((cell) => {
    cell.hidden = hidden;
  });
}

function selectAggregateFields(group) {
  state.selectedGroup = group;
  state.openFields = true;
  updateUrl();
  renderEu27Aggregate();
  renderGroups();
  renderFields(group);
  element("afir-field-section").scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
}

function renderEu27Aggregate() {
  const section = element("afir-eu27-section");
  const body = element("afir-eu27-group");
  body.replaceChildren();
  const aggregate =
    state.level === "country" && Object.keys(state.scope).length === 0
      ? state.payload?.eu27_aggregate
      : null;
  section.hidden = !aggregate;
  if (!aggregate) return;

  const row = document.createElement("tr");
  row.tabIndex = 0;
  row.dataset.actionable = "true";
  row.dataset.aggregate = "eu27";
  row.dataset.selected = String(aggregate === state.selectedGroup);
  appendGroupCells(
    row,
    aggregate,
    identityForGroup("eu27", aggregate.dimensions),
    "",
  );
  const fieldsLink = document.createElement("a");
  fieldsLink.className = "afir-detail-link";
  fieldsLink.href = afirAggregateFieldsUrl("country", aggregate.dimensions || {});
  fieldsLink.textContent = "Felder →";
  fieldsLink.setAttribute("aria-label", "AFIR-Felder für EU-27 anzeigen");
  fieldsLink.addEventListener("click", (event) => {
    event.stopPropagation();
  });
  row.lastElementChild.replaceChildren(fieldsLink);
  row.lastElementChild.className = "";
  row.addEventListener("click", () => selectAggregateFields(aggregate));
  row.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      selectAggregateFields(aggregate);
    }
  });
  body.append(row);
  refreshSortableTable(element("afir-eu27-table"));
}

function renderGroups() {
  const body = element("afir-groups");
  body.replaceChildren();
  updateAggregateCountColumns();
  const groups = state.payload?.groups || [];
  for (const group of groups) {
    const row = document.createElement("tr");
    row.tabIndex = 0;
    row.dataset.actionable = "true";
    row.dataset.selected = String(group === state.selectedGroup);
    const identity = identityForGroup(state.level, group.dimensions);
    appendGroupCells(row, group, identity, "→");
    if (state.level === "point") {
      const detailUrl = afirStationDetailUrl(group.dimensions);
      if (detailUrl) {
        const detailLink = document.createElement("a");
        detailLink.className = "afir-detail-link";
        detailLink.href = detailUrl;
        detailLink.textContent = "Detailansicht ↗";
        detailLink.setAttribute(
          "aria-label",
          `Aktuelle Detailansicht für ${identity.primary} öffnen`,
        );
        detailLink.addEventListener("click", (event) => {
          event.stopPropagation();
        });
        row.lastElementChild.replaceChildren(detailLink);
        row.lastElementChild.className = "";
      }
    } else {
      const fieldsLink = document.createElement("a");
      fieldsLink.className = "afir-detail-link";
      fieldsLink.href = afirAggregateFieldsUrl(state.level, group.dimensions);
      fieldsLink.textContent = "Felder →";
      fieldsLink.setAttribute(
        "aria-label",
        `AFIR-Felder für ${identity.primary} anzeigen`,
      );
      fieldsLink.addEventListener("click", (event) => {
        event.stopPropagation();
      });
      row.lastElementChild.replaceChildren(fieldsLink);
      row.lastElementChild.className = "";
    }
    row.addEventListener("click", () => drillInto(group));
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        drillInto(group);
      }
    });
    body.append(row);
  }
  if (!groups.length && !state.loading) {
    const row = document.createElement("tr");
    const cell = appendCell(
      row,
      state.searchQuery
        ? "Auf dieser Seite wurde kein passender Eintrag gefunden."
        : "Für diese Ebene liegen noch keine bewertbaren Daten vor.",
    );
    cell.colSpan = state.level === "point" ? 5 : 7;
    body.append(row);
  }
  refreshSortableTable(element("afir-groups-table"));
}

function renderPagination() {
  const pagination = state.payload?.pagination || {};
  const total = Number(pagination.total_group_count || 0);
  const start = total ? state.offset + 1 : 0;
  const end = Math.min(state.offset + PAGE_SIZE, total);
  element("afir-page-status").textContent =
    `${NUMBER.format(start)}–${NUMBER.format(end)} von ${NUMBER.format(total)}`;
  element("afir-previous-page").disabled = state.offset <= 0 || state.loading;
  element("afir-next-page").disabled =
    state.offset + PAGE_SIZE >= total || state.loading;
}

async function loadGroups() {
  const levelLabel = LEVEL_LABELS[state.level];
  const progressToken = loadingIndicator.start({
    labelText: `${levelLabel} werden geladen …`,
    detailText: "Live-Daten werden angefragt.",
  });
  let loadSucceeded = false;
  let loadedCount = 0;
  state.loading = true;
  state.payload = null;
  element("afir-field-section").hidden = true;
  element("afir-level-title").textContent = levelLabel;
  renderSearchSummary();
  renderBreadcrumb();
  renderEu27Aggregate();
  renderGroups();
  renderPagination();
  setStatus(`${levelLabel} werden geladen …`);
  try {
    const payload = await source.loadGroups({
      level: state.level,
      scope: state.scope,
      limit: PAGE_SIZE,
      offset: state.offset,
      sort: state.sortColumn,
      direction: state.sortDirection,
      search: state.searchQuery,
    });
    loadingIndicator.received(progressToken, {
      labelText: `${levelLabel} sind eingetroffen.`,
      detailText: "Tabelle und Feldsummen werden aufgebaut.",
    });
    state.payload = payload;
    const count = Number(payload.pagination?.total_group_count || 0);
    loadedCount = count;
    loadSucceeded = true;
    if (state.openFields) {
      state.selectedGroup = resolveFieldGroupFromUrl(payload);
      updateUrl();
    }
    setStatus(
      count
        ? `${NUMBER.format(count)} Einheiten auf dieser Ebene.`
        : "Noch keine bewertbaren Einheiten auf dieser Ebene.",
    );
  } catch (error) {
    setStatus(
      `Der Live-AFIR-Status ist derzeit nicht verfügbar: ${error.message}`,
      "error",
    );
    loadingIndicator.fail(progressToken, {
      labelText: "Live-AFIR-Status konnte nicht geladen werden.",
      detailText: "Die Fehlermeldung bleibt direkt unterhalb sichtbar.",
    });
  } finally {
    state.loading = false;
    renderEu27Aggregate();
    renderGroups();
    renderPagination();
    if (loadSucceeded) {
      loadingIndicator.succeed(progressToken, {
        labelText: `${levelLabel} sind bereit.`,
        detailText: loadedCount
          ? `${NUMBER.format(loadedCount)} Einheiten werden angezeigt.`
          : "Für diese Ebene liegen noch keine bewertbaren Einheiten vor.",
      });
    }
    if (state.openFields && state.selectedGroup) {
      renderFields(state.selectedGroup);
      element("afir-field-section").scrollIntoView({
        behavior: "smooth",
        block: "start",
      });
    }
  }
}

async function loadMeta() {
  try {
    const meta = await source.loadMeta();
    const count = Number(meta.entity_release_pair_count || 0);
    element("afir-meta-status").textContent = meta.available
      ? count
        ? "Live"
        : "Bereit, noch ohne Projektion"
      : "Nicht verfügbar";
    element("afir-meta-entities").textContent = NUMBER.format(count);
    element("afir-meta-fields").textContent =
      `${NUMBER.format(meta.field_count || 37)} AFIR-Felder`;
  } catch {
    element("afir-meta-status").textContent = "Nicht verfügbar";
  }
}

let searchTimer = null;
element("afir-search").addEventListener("input", (event) => {
  const value = String(event.target.value || "").trim();
  if (searchTimer !== null) clearTimeout(searchTimer);
  if (value && value.length < 3) {
    state.searchQuery = "";
    updateUrl();
    renderSearchSummary();
    setStatus("Bitte mindestens 3 Zeichen für die Suche eingeben.");
    renderGroups();
    return;
  }
  state.searchQuery = value;
  renderSearchSummary();
  searchTimer = setTimeout(() => {
    state.offset = 0;
    updateUrl();
    void loadGroups();
  }, 250);
});
element("afir-search-clear").addEventListener("click", () => {
  if (searchTimer !== null) clearTimeout(searchTimer);
  state.searchQuery = "";
  element("afir-search").value = "";
  state.offset = 0;
  updateUrl();
  renderSearchSummary();
  void loadGroups();
});
element("afir-previous-page").addEventListener("click", () => {
  state.offset = Math.max(0, state.offset - PAGE_SIZE);
  void loadGroups();
});
element("afir-next-page").addEventListener("click", () => {
  state.offset += PAGE_SIZE;
  void loadGroups();
});

restoreUrl();
element("afir-search").value = state.searchQuery;
renderSearchSummary();
wireSortableTables();
void Promise.all([loadMeta(), loadGroups()]);
