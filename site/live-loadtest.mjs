import {
  queryLiveApiBaseUrl,
  resolveLiveApiBaseUrl,
} from "./live-api.mjs";

const DEFAULT_BASE_URL = "https://live-eu.woladen.de";
const DEFAULT_LOOKUP_STATION_IDS = [
  "de_bw_000001",
  "de_by_000001",
  "de_be_000001",
  "de_hh_000001",
  "de_nw_000001",
];
const METRIC_CONFIG = [
  { key: "totalMs", label: "Gesamt" },
  { key: "requestEncodeMs", label: "Request JSON" },
  { key: "headersMs", label: "Bis Header" },
  { key: "bodyReadMs", label: "Body lesen" },
  { key: "responseParseMs", label: "JSON parse" },
  { key: "serverAppMs", label: "Server gesamt" },
  { key: "serverDbQueryMs", label: "SQLite Query" },
  { key: "serverDbDecodeMs", label: "SQLite Decode" },
  { key: "serverPayloadMs", label: "Payload bauen" },
  { key: "serverJsonEncodeMs", label: "JSON encode" },
  { key: "networkGapMs", label: "TTFB minus Server" },
  { key: "resourceTtfbMs", label: "Resource TTFB" },
  { key: "resourceDownloadMs", label: "Resource Download" },
];
const SCENARIO_LABELS = {
  lookup: "Stations-Lookup",
  detail: "Stations-Detail",
  list: "Stations-Liste",
  custom: "Eigener Request",
};

function mean(values) {
  if (!values.length) {
    return null;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

export function percentile(values, percentileRank) {
  if (!values.length) {
    return null;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const rank = Math.max(0, Math.min(1, percentileRank));
  const index = Math.ceil(sorted.length * rank) - 1;
  return sorted[Math.max(0, index)];
}

export function parseServerTiming(headerValue) {
  const metrics = {};
  const header = String(headerValue || "").trim();
  if (!header) {
    return metrics;
  }

  header.split(",").forEach((part) => {
    const tokens = part.trim().split(";").map((token) => token.trim()).filter(Boolean);
    if (!tokens.length) {
      return;
    }
    const [name, ...attributes] = tokens;
    const metric = { dur: null, desc: "" };
    attributes.forEach((attribute) => {
      const [rawKey, rawValue = ""] = attribute.split("=", 2);
      const key = String(rawKey || "").trim().toLowerCase();
      const value = String(rawValue || "").trim();
      if (key === "dur") {
        const numeric = Number(value);
        if (Number.isFinite(numeric)) {
          metric.dur = numeric;
        }
        return;
      }
      if (key === "desc") {
        metric.desc = value.replace(/^"|"$/g, "");
      }
    });
    metrics[name] = metric;
  });

  return metrics;
}

export function summarizeBenchmarkRuns(runs) {
  const safeRuns = Array.isArray(runs) ? runs : [];
  const successfulRuns = safeRuns.filter((run) => run && run.ok);
  const metrics = {};

  METRIC_CONFIG.forEach(({ key }) => {
    const values = successfulRuns
      .map((run) => Number(run[key]))
      .filter((value) => Number.isFinite(value));
    metrics[key] = {
      count: values.length,
      mean: mean(values),
      p50: percentile(values, 0.5),
      p95: percentile(values, 0.95),
    };
  });

  return {
    totalCount: safeRuns.length,
    okCount: successfulRuns.length,
    errorCount: safeRuns.length - successfulRuns.length,
    metrics,
  };
}

function formatMilliseconds(value) {
  if (!Number.isFinite(value)) {
    return "—";
  }
  return `${value.toFixed(value >= 100 ? 0 : 1)} ms`;
}

function formatNumber(value) {
  if (!Number.isFinite(value)) {
    return "—";
  }
  return new Intl.NumberFormat("de-DE").format(value);
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function parseStationIdsInput(value) {
  return String(value || "")
    .split(/[\n,;\s]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeBaseUrl(value) {
  const configured = queryLiveApiBaseUrl(window.location.href)
    || resolveLiveApiBaseUrl({
      configuredValue: "",
      locationHref: window.location.href,
      locationHostname: window.location.hostname,
    });
  const candidate = String(value || "").trim() || configured || DEFAULT_BASE_URL;
  return candidate.replace(/\/+$/, "");
}

function scenarioDefaults(baseUrl) {
  return {
    scenario: "lookup",
    baseUrl,
    method: "GET",
    path: "/v1/stations",
    stationId: "",
    stationIdsText: DEFAULT_LOOKUP_STATION_IDS.join("\n"),
    limit: 50,
    iterations: 24,
    concurrency: 4,
    timeoutMs: 12000,
    enableProfile: true,
    customBody: "{\n  \"station_ids\": [\"station-1\"]\n}",
  };
}

function collectControls(form) {
  return {
    form,
    scenario: form.elements.namedItem("scenario"),
    baseUrl: form.elements.namedItem("baseUrl"),
    method: form.elements.namedItem("method"),
    path: form.elements.namedItem("path"),
    stationId: form.elements.namedItem("stationId"),
    stationIds: form.elements.namedItem("stationIds"),
    limit: form.elements.namedItem("limit"),
    iterations: form.elements.namedItem("iterations"),
    concurrency: form.elements.namedItem("concurrency"),
    timeoutMs: form.elements.namedItem("timeoutMs"),
    enableProfile: form.elements.namedItem("enableProfile"),
    customBody: form.elements.namedItem("customBody"),
    lookupFields: document.getElementById("loadtest-lookup-fields"),
    detailFields: document.getElementById("loadtest-detail-fields"),
    listFields: document.getElementById("loadtest-list-fields"),
    methodField: document.getElementById("loadtest-method-field"),
    pathField: document.getElementById("loadtest-path-field"),
    bodyField: document.getElementById("loadtest-body-field"),
  };
}

function readFormState(controls) {
  return {
    scenario: controls.scenario.value,
    baseUrl: normalizeBaseUrl(controls.baseUrl.value),
    method: String(controls.method.value || "GET").toUpperCase(),
    path: String(controls.path.value || "").trim(),
    stationId: String(controls.stationId.value || "").trim(),
    stationIdsText: String(controls.stationIds.value || "").trim(),
    limit: Math.max(Number.parseInt(controls.limit.value, 10) || 1, 1),
    iterations: Math.max(Number.parseInt(controls.iterations.value, 10) || 1, 1),
    concurrency: Math.max(Number.parseInt(controls.concurrency.value, 10) || 1, 1),
    timeoutMs: Math.max(Number.parseInt(controls.timeoutMs.value, 10) || 1000, 250),
    enableProfile: Boolean(controls.enableProfile.checked),
    customBody: String(controls.customBody.value || "").trim(),
  };
}

function buildRequestDefinition(formState) {
  const scenario = formState.scenario;
  if (scenario === "lookup") {
    const stationIds = parseStationIdsInput(formState.stationIdsText).slice(0, 200);
    if (!stationIds.length) {
      throw new Error("Mindestens eine Station-ID für den Lookup angeben.");
    }
    return {
      scenario,
      label: SCENARIO_LABELS[scenario],
      method: "POST",
      path: "/v1/stations/lookup",
      body: { station_ids: stationIds },
    };
  }

  if (scenario === "detail") {
    if (!formState.stationId) {
      throw new Error("Für den Detail-Request wird eine Station-ID benötigt.");
    }
    return {
      scenario,
      label: SCENARIO_LABELS[scenario],
      method: "GET",
      path: `/v1/stations/${encodeURIComponent(formState.stationId)}`,
      body: null,
    };
  }

  if (scenario === "list") {
    return {
      scenario,
      label: SCENARIO_LABELS[scenario],
      method: "GET",
      path: `/v1/stations?limit=${encodeURIComponent(String(Math.min(formState.limit, 100)))}`,
      body: null,
    };
  }

  let customBody = null;
  if (formState.method !== "GET" && formState.method !== "HEAD" && formState.customBody) {
    try {
      customBody = JSON.parse(formState.customBody);
    } catch (error) {
      throw new Error("Custom JSON Body ist kein gültiges JSON.");
    }
  }
  if (!formState.path.startsWith("/")) {
    throw new Error("Der API-Pfad muss mit / beginnen.");
  }
  return {
    scenario,
    label: SCENARIO_LABELS[scenario],
    method: formState.method,
    path: formState.path,
    body: customBody,
  };
}

function setScenarioVisibility(controls, scenario) {
  const lookupVisible = scenario === "lookup";
  const detailVisible = scenario === "detail";
  const listVisible = scenario === "list";
  const customVisible = scenario === "custom";

  controls.lookupFields.hidden = !lookupVisible;
  controls.detailFields.hidden = !detailVisible;
  controls.listFields.hidden = !listVisible;
  controls.methodField.hidden = !customVisible;
  controls.pathField.hidden = !customVisible;
  controls.bodyField.hidden = !customVisible;
}

function syncScenarioFields(controls) {
  const scenario = controls.scenario.value;
  if (scenario === "lookup") {
    controls.method.value = "POST";
    controls.path.value = "/v1/stations/lookup";
  } else if (scenario === "detail") {
    controls.method.value = "GET";
    controls.path.value = `/v1/stations/${encodeURIComponent(String(controls.stationId.value || "").trim())}`;
  } else if (scenario === "list") {
    controls.method.value = "GET";
    controls.path.value = `/v1/stations?limit=${encodeURIComponent(String(Math.min(Number.parseInt(controls.limit.value, 10) || 1, 100)))}`;
  }
  setScenarioVisibility(controls, scenario);
}

function renderPreview(container, requestDefinition) {
  const bodyHtml = requestDefinition.body === null
    ? "<em>Kein Request-Body</em>"
    : `<pre>${escapeHtml(JSON.stringify(requestDefinition.body, null, 2))}</pre>`;
  container.innerHTML = `
    <div class="loadtest-preview-head">
      <strong>${escapeHtml(requestDefinition.label)}</strong>
      <span>${escapeHtml(requestDefinition.method)} ${escapeHtml(requestDefinition.path)}</span>
    </div>
    ${bodyHtml}
  `;
}

function readResourceTiming(url) {
  const entries = performance
    .getEntriesByName(url)
    .filter((entry) => entry.entryType === "resource");
  return entries.at(-1) || null;
}

function createAbortSignal(timeoutMs) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(new Error("timeout")), timeoutMs);
  return {
    signal: controller.signal,
    cancel: () => window.clearTimeout(timer),
  };
}

async function runSingleBenchmark(requestDefinition, formState, runIndex) {
  const requestUrl = new URL(requestDefinition.path, `${formState.baseUrl}/`);
  requestUrl.searchParams.set("__bench", `${Date.now()}-${runIndex}-${Math.random().toString(16).slice(2)}`);
  if (formState.enableProfile) {
    requestUrl.searchParams.set("profile", "1");
  }

  const result = {
    runIndex: runIndex + 1,
    ok: false,
    method: requestDefinition.method,
    path: `${requestUrl.pathname}${requestUrl.search}`,
    status: 0,
    requestEncodeMs: null,
    headersMs: null,
    bodyReadMs: null,
    responseParseMs: null,
    totalMs: null,
    serverAppMs: null,
    serverDbQueryMs: null,
    serverDbDecodeMs: null,
    serverPayloadMs: null,
    serverJsonEncodeMs: null,
    networkGapMs: null,
    resourceDnsMs: null,
    resourceConnectMs: null,
    resourceTlsMs: null,
    resourceTtfbMs: null,
    resourceDownloadMs: null,
    requestBytes: 0,
    responseBytes: 0,
    error: "",
  };

  let requestBodyText = "";
  if (requestDefinition.body !== null) {
    const encodeStartedAt = performance.now();
    requestBodyText = JSON.stringify(requestDefinition.body);
    result.requestEncodeMs = performance.now() - encodeStartedAt;
    result.requestBytes = new TextEncoder().encode(requestBodyText).length;
  } else {
    result.requestEncodeMs = 0;
  }

  const { signal, cancel } = createAbortSignal(formState.timeoutMs);
  const startedAt = performance.now();

  try {
    const response = await fetch(requestUrl.toString(), {
      method: requestDefinition.method,
      headers: requestBodyText
        ? {
          "Content-Type": "application/json",
          "x-woladen-profile": formState.enableProfile ? "1" : "0",
        }
        : {
          "x-woladen-profile": formState.enableProfile ? "1" : "0",
        },
      body: requestBodyText || undefined,
      cache: "no-store",
      signal,
    });
    result.headersMs = performance.now() - startedAt;
    result.status = response.status;

    const serverTiming = parseServerTiming(response.headers.get("server-timing") || "");
    result.serverAppMs = serverTiming.app?.dur ?? null;
    result.serverDbQueryMs = serverTiming["db-query"]?.dur ?? null;
    result.serverDbDecodeMs = serverTiming["db-decode"]?.dur ?? null;
    result.serverPayloadMs = serverTiming.payload?.dur ?? null;
    result.serverJsonEncodeMs = serverTiming["json-encode"]?.dur ?? null;

    const bodyReadStartedAt = performance.now();
    const responseText = await response.text();
    result.bodyReadMs = performance.now() - bodyReadStartedAt;
    result.responseBytes = new TextEncoder().encode(responseText).length;

    if (responseText) {
      const parseStartedAt = performance.now();
      JSON.parse(responseText);
      result.responseParseMs = performance.now() - parseStartedAt;
    } else {
      result.responseParseMs = 0;
    }

    result.totalMs = performance.now() - startedAt;
    if (Number.isFinite(result.headersMs) && Number.isFinite(result.serverAppMs)) {
      result.networkGapMs = Math.max(result.headersMs - result.serverAppMs, 0);
    }

    await new Promise((resolve) => window.setTimeout(resolve, 0));
    const resourceTiming = readResourceTiming(requestUrl.toString());
    if (resourceTiming) {
      result.resourceDnsMs = resourceTiming.domainLookupEnd - resourceTiming.domainLookupStart;
      result.resourceConnectMs = resourceTiming.connectEnd - resourceTiming.connectStart;
      result.resourceTlsMs = resourceTiming.secureConnectionStart > 0
        ? resourceTiming.connectEnd - resourceTiming.secureConnectionStart
        : 0;
      result.resourceTtfbMs = resourceTiming.responseStart - resourceTiming.requestStart;
      result.resourceDownloadMs = resourceTiming.responseEnd - resourceTiming.responseStart;
    }

    result.ok = response.ok;
    if (!response.ok) {
      result.error = `HTTP ${response.status}`;
    }
    return result;
  } catch (error) {
    result.totalMs = performance.now() - startedAt;
    result.error = error?.name === "AbortError"
      ? `Timeout nach ${formState.timeoutMs} ms`
      : String(error?.message || error || "Unbekannter Fehler");
    return result;
  } finally {
    cancel();
  }
}

async function runPool(taskFactories, concurrency, onProgress) {
  const results = new Array(taskFactories.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < taskFactories.length) {
      const taskIndex = nextIndex;
      nextIndex += 1;
      results[taskIndex] = await taskFactories[taskIndex]();
      onProgress(taskIndex + 1, taskFactories.length);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, taskFactories.length) }, () => worker()),
  );
  return results;
}

function renderSummary(container, summary) {
  const kpis = [
    {
      label: "Requests",
      value: formatNumber(summary.totalCount),
      detail: `${formatNumber(summary.okCount)} erfolgreich, ${formatNumber(summary.errorCount)} fehlerhaft`,
    },
    {
      label: "Mittel gesamt",
      value: formatMilliseconds(summary.metrics.totalMs.mean),
      detail: `p50 ${formatMilliseconds(summary.metrics.totalMs.p50)} · p95 ${formatMilliseconds(summary.metrics.totalMs.p95)}`,
    },
    {
      label: "Server gesamt",
      value: formatMilliseconds(summary.metrics.serverAppMs.mean),
      detail: `SQLite ${formatMilliseconds(summary.metrics.serverDbQueryMs.mean)} · Encode ${formatMilliseconds(summary.metrics.serverJsonEncodeMs.mean)}`,
    },
    {
      label: "Client Decode",
      value: formatMilliseconds(summary.metrics.responseParseMs.mean),
      detail: `Body lesen ${formatMilliseconds(summary.metrics.bodyReadMs.mean)}`,
    },
    {
      label: "Netz / TTFB",
      value: formatMilliseconds(summary.metrics.networkGapMs.mean),
      detail: `Resource TTFB ${formatMilliseconds(summary.metrics.resourceTtfbMs.mean)}`,
    },
  ];

  container.innerHTML = kpis.map((item) => `
    <article class="management-kpi">
      <div class="management-kpi-label">${escapeHtml(item.label)}</div>
      <div class="management-kpi-value">${escapeHtml(item.value)}</div>
      <div class="management-kpi-detail">${escapeHtml(item.detail)}</div>
    </article>
  `).join("");
}

function renderBreakdownTable(container, summary) {
  const rows = METRIC_CONFIG.map(({ key, label }) => {
    const metric = summary.metrics[key];
    return `
      <tr>
        <th scope="row">${escapeHtml(label)}</th>
        <td>${escapeHtml(formatMilliseconds(metric.mean))}</td>
        <td>${escapeHtml(formatMilliseconds(metric.p50))}</td>
        <td>${escapeHtml(formatMilliseconds(metric.p95))}</td>
      </tr>
    `;
  }).join("");

  container.innerHTML = `
    <table class="management-table loadtest-table">
      <thead>
        <tr>
          <th>Messwert</th>
          <th>Mittel</th>
          <th>p50</th>
          <th>p95</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderRunTable(container, runs) {
  const rows = runs.map((run) => `
    <tr>
      <td>${escapeHtml(String(run.runIndex))}</td>
      <td>${escapeHtml(run.ok ? "ok" : "error")}</td>
      <td>${escapeHtml(String(run.status || "—"))}</td>
      <td>${escapeHtml(formatMilliseconds(run.totalMs))}</td>
      <td>${escapeHtml(formatMilliseconds(run.serverDbQueryMs))}</td>
      <td>${escapeHtml(formatMilliseconds(run.serverJsonEncodeMs))}</td>
      <td>${escapeHtml(formatMilliseconds(run.responseParseMs))}</td>
      <td><code>${escapeHtml(run.path)}</code></td>
      <td>${escapeHtml(run.error || "—")}</td>
    </tr>
  `).join("");

  container.innerHTML = `
    <table class="management-table loadtest-table">
      <thead>
        <tr>
          <th>#</th>
          <th>Resultat</th>
          <th>HTTP</th>
          <th>Gesamt</th>
          <th>SQLite</th>
          <th>JSON encode</th>
          <th>JSON parse</th>
          <th>Pfad</th>
          <th>Fehler</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function setStatus(statusEl, message, tone = "") {
  statusEl.hidden = false;
  statusEl.className = `management-status loadtest-status${tone ? ` is-${tone}` : ""}`;
  statusEl.textContent = message;
}

function init() {
  const form = document.getElementById("loadtest-form");
  if (!(form instanceof HTMLFormElement)) {
    return;
  }

  const statusEl = document.getElementById("loadtest-status");
  const previewEl = document.getElementById("loadtest-preview");
  const kpisEl = document.getElementById("loadtest-kpis");
  const summaryTableEl = document.getElementById("loadtest-summary-table");
  const runTableEl = document.getElementById("loadtest-run-table");
  const controls = collectControls(form);

  const defaults = scenarioDefaults(normalizeBaseUrl(""));
  controls.baseUrl.value = defaults.baseUrl;
  controls.stationIds.value = defaults.stationIdsText;
  controls.limit.value = String(defaults.limit);
  controls.iterations.value = String(defaults.iterations);
  controls.concurrency.value = String(defaults.concurrency);
  controls.timeoutMs.value = String(defaults.timeoutMs);
  controls.enableProfile.checked = defaults.enableProfile;
  controls.method.value = defaults.method;
  controls.path.value = defaults.path;
  controls.customBody.value = defaults.customBody;

  controls.scenario.addEventListener("change", () => {
    syncScenarioFields(controls);
  });
  controls.stationId.addEventListener("input", () => syncScenarioFields(controls));
  controls.limit.addEventListener("input", () => syncScenarioFields(controls));
  syncScenarioFields(controls);

  form.addEventListener("submit", async (event) => {
    event.preventDefault();

    let formState;
    let requestDefinition;
    try {
      formState = readFormState(controls);
      requestDefinition = buildRequestDefinition(formState);
    } catch (error) {
      setStatus(statusEl, String(error?.message || error || "Ungültige Eingabe"), "error");
      return;
    }

    renderPreview(previewEl, requestDefinition);
    setStatus(statusEl, `Benchmark läuft: 0 / ${formState.iterations} abgeschlossen`, "running");
    kpisEl.innerHTML = "";
    summaryTableEl.innerHTML = "";
    runTableEl.innerHTML = "";

    const taskFactories = Array.from({ length: formState.iterations }, (_, runIndex) => {
      return () => runSingleBenchmark(requestDefinition, formState, runIndex);
    });

    const runs = await runPool(taskFactories, formState.concurrency, (completed, total) => {
      setStatus(statusEl, `Benchmark läuft: ${completed} / ${total} abgeschlossen`, "running");
    });

    const summary = summarizeBenchmarkRuns(runs);
    const okRatio = summary.totalCount ? Math.round((summary.okCount / summary.totalCount) * 100) : 0;
    setStatus(
      statusEl,
      `Benchmark fertig: ${summary.okCount}/${summary.totalCount} erfolgreich (${okRatio} %).`,
      summary.errorCount ? "error" : "ok",
    );
    renderSummary(kpisEl, summary);
    renderBreakdownTable(summaryTableEl, summary);
    renderRunTable(runTableEl, runs);
  });
}

if (typeof window !== "undefined") {
  window.addEventListener("DOMContentLoaded", init);
}
