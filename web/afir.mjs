import {
  afirChargingPointCount,
  afirCountryDisplayName,
  afirPointCoverage,
  afirStationDetailUrl,
  createAfirDataSource,
  nextAfirLevel,
  scopeFromAfirDimensions,
} from "./afir-api.mjs?v=20260730-afir-eu27-fix1";

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

function coverageState(summary) {
  return String(afirPointCoverage(summary)?.coverage_state || "unassessed");
}

function coveragePercent(summary) {
  return afirPointCoverage(summary)?.coverage_pct;
}

function pointCoverageRatio(coverage, phase = "current") {
  const denominator = Number(coverage?.denominator || 0);
  const phases = {
    current: [
      "present_current_count",
      "coverage_pct",
    ],
    previous: [
      "present_previous_count",
      "previous_coverage_pct",
    ],
    both: [
      "present_in_both_distinct_releases_count",
      "two_release_coverage_pct",
    ],
  };
  const [countKey, percentKey] = phases[phase] || phases.current;
  const numerator = Number(coverage?.[countKey] || 0);
  return `${NUMBER.format(numerator)} / ${NUMBER.format(
    denominator,
  )} · ${percent(coverage?.[percentKey])}`;
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

function appendCell(row, content, className = "") {
  const cell = document.createElement("td");
  if (className) cell.className = className;
  if (content instanceof Node) cell.append(content);
  else cell.textContent = String(content ?? "");
  row.append(cell);
  return cell;
}

function metric(summary) {
  const span = document.createElement("span");
  span.className = "afir-metric";
  span.dataset.state = coverageState(summary);
  span.textContent = percent(coveragePercent(summary));
  return span;
}

function groupSearchText(group) {
  return Object.values(group?.dimensions || {})
    .join(" ")
    .toLocaleLowerCase("de");
}

function fieldContext(group) {
  return Object.values(group?.dimensions || {})
    .filter(Boolean)
    .join(" · ");
}

const state = {
  level: "country",
  scope: {},
  trail: [],
  offset: 0,
  payload: null,
  selectedGroup: null,
  loading: false,
};

const source = createAfirDataSource({ baseUrl: configuredBaseUrl() });

function updateUrl() {
  const query = new URLSearchParams();
  query.set("level", state.level);
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

function renderFields(group) {
  const section = element("afir-field-section");
  const body = element("afir-fields");
  body.replaceChildren();
  const fields = Array.isArray(group?.fields) ? group.fields : [];
  for (const field of fields) {
    const row = document.createElement("tr");
    const code = document.createElement("span");
    code.className = "afir-field-code";
    code.textContent = field.field_id || "–";
    const key = document.createElement("small");
    key.className = "afir-field-key";
    key.textContent = field.technical_key || "";
    const identity = document.createElement("div");
    identity.append(code, key);
    appendCell(row, identity);
    appendCell(row, field.label || field.technical_key || "–");
    appendCell(row, field.data_kind === "dynamic" ? "dynamisch" : "statisch");
    const pointCoverage = field.charging_point_coverage || {};
    appendCell(row, pointCoverageRatio(pointCoverage, "current"));
    appendCell(row, pointCoverageRatio(pointCoverage, "previous"));
    appendCell(row, pointCoverageRatio(pointCoverage, "both"));
    appendCell(
      row,
      field.data_kind === "dynamic"
        ? duration(field.freshness?.source_observed_age_seconds?.p50)
        : "–",
    );
    body.append(row);
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
    renderGroups();
    renderFields(group);
    element("afir-field-section").scrollIntoView({
      behavior: "smooth",
      block: "start",
    });
    return;
  }
  const identity = identityForGroup(state.level, group.dimensions);
  state.trail.push({
    label: identity.primary,
    level: state.level,
    scope: { ...state.scope },
  });
  state.level = nextLevel;
  state.scope = scopeFromAfirDimensions(group.dimensions);
  state.offset = 0;
  state.selectedGroup = null;
  element("afir-search").value = "";
  updateUrl();
  void loadGroups();
}

function appendGroupCells(row, group, identity, actionLabel = "→") {
  const identityBlock = document.createElement("div");
  const primary = document.createElement("strong");
  primary.textContent = identity.primary;
  const secondary = document.createElement("small");
  secondary.textContent = identity.secondary;
  identityBlock.append(primary, secondary);
  appendCell(row, identityBlock, "afir-identity");
  appendCell(row, metric(group.static_compliance));
  appendCell(row, metric(group.dynamic_compliance));
  appendCell(
    row,
    group.dynamic_compliance?.freshness_state === "assessed"
      ? duration(
          group.source_release_cadence?.source_release_gap_seconds?.p50,
        )
      : "nicht bewertet",
  );
  appendCell(row, NUMBER.format(group.entity_counts?.station_count || 0));
  appendCell(row, NUMBER.format(afirChargingPointCount(group)));
  appendCell(row, actionLabel, "afir-open");
}

function selectAggregateFields(group) {
  state.selectedGroup = group;
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
    "Felder →",
  );
  row.addEventListener("click", () => selectAggregateFields(aggregate));
  row.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      selectAggregateFields(aggregate);
    }
  });
  body.append(row);
}

function renderGroups() {
  const body = element("afir-groups");
  body.replaceChildren();
  const query = element("afir-search").value.trim().toLocaleLowerCase("de");
  const groups = (state.payload?.groups || []).filter(
    (group) => !query || groupSearchText(group).includes(query),
  );
  for (const group of groups) {
    const row = document.createElement("tr");
    row.tabIndex = 0;
    row.dataset.actionable = "true";
    row.dataset.selected = String(group === state.selectedGroup);
    const identity = identityForGroup(state.level, group.dimensions);
    const detailUrl =
      state.level === "point"
        ? afirStationDetailUrl(group.dimensions)
        : "";
    appendGroupCells(row, group, identity, detailUrl ? "" : "→");
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
      query
        ? "Auf dieser Seite wurde kein passender Eintrag gefunden."
        : "Für diese Ebene liegen noch keine bewertbaren Daten vor.",
    );
    cell.colSpan = 7;
    body.append(row);
  }
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
  state.loading = true;
  state.payload = null;
  element("afir-field-section").hidden = true;
  element("afir-level-title").textContent = LEVEL_LABELS[state.level];
  renderBreadcrumb();
  renderEu27Aggregate();
  renderGroups();
  renderPagination();
  setStatus(`${LEVEL_LABELS[state.level]} werden geladen …`);
  try {
    const payload = await source.loadGroups({
      level: state.level,
      scope: state.scope,
      limit: PAGE_SIZE,
      offset: state.offset,
    });
    state.payload = payload;
    const count = Number(payload.pagination?.total_group_count || 0);
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
  } finally {
    state.loading = false;
    renderEu27Aggregate();
    renderGroups();
    renderPagination();
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

element("afir-search").addEventListener("input", renderGroups);
element("afir-previous-page").addEventListener("click", () => {
  state.offset = Math.max(0, state.offset - PAGE_SIZE);
  void loadGroups();
});
element("afir-next-page").addEventListener("click", () => {
  state.offset += PAGE_SIZE;
  void loadGroups();
});

restoreUrl();
void Promise.all([loadMeta(), loadGroups()]);
