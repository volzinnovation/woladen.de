import { createManagementDataSource } from "./management-api.mjs";

const TOP_STATIONS_LIMIT = 10;
const DISPLAYED_FULL_UTILIZATION_THRESHOLD = 0.9995;
const ANDROID_WEB_LINK = "https://play.google.com/store/apps/details?id=de.woladen.android";
const ANDROID_STORE_LINK = "market://details?id=de.woladen.android";
export const DEFAULT_WINDOW_DAYS = 1;
export const SUPPORTED_WINDOW_DAYS = [1, 7, 14, 28];
const COUNTRY_NAMES = new Intl.DisplayNames(["de"], { type: "region" });
const UNSPECIFIED_PROVIDER_LABEL = "Unbekannt";
const UNSPECIFIED_SOURCE_LABEL = "Datenquelle nicht angegeben";

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
  archive_messages_total: {
    label: "Empfangene AFIR-Meldungen",
    description:
      "Archivierte Push- und Abrufmeldungen des Tages. Eine Meldung kann eine einzelne Änderung oder viele Statusbeobachtungen enthalten.",
    kind: "count",
  },
  occupancy_share: {
    label: "Auslastung",
    description: "Anteil der belegten Zeit an der gemessenen Zeit.",
    kind: "percent",
  },
  out_of_order_share: {
    label: "Störungsanteil",
    description: "Anteil der als außer Betrieb gemeldeten Zeit an der gemessenen Zeit.",
    kind: "percent",
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

function percentFormat(value, digits = 1) {
  const number = optionalNumber(value);
  if (number === null) {
    return "–";
  }
  return new Intl.NumberFormat("de-DE", {
    style: "percent",
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(number);
}

export function windowLabel(windowDays) {
  const days = Number(windowDays);
  if (days === 1) {
    return "1 Tag";
  }
  const weeks = days / 7;
  if (!Number.isInteger(weeks) || weeks < 1) {
    return "";
  }
  return `${numberFormat(weeks)} ${weeks === 1 ? "Woche" : "Wochen"}`;
}

export function shouldShowOverviewChart(windowDays) {
  const days = Number(windowDays);
  return SUPPORTED_WINDOW_DAYS.includes(days) && days > 1;
}

export function rankedTableTitle(prefix, columnLabel, fallbackLabel = "Auslastung") {
  const normalizedPrefix = String(prefix || "").trim();
  const normalizedLabel = String(columnLabel || fallbackLabel).trim() || fallbackLabel;
  return `${normalizedPrefix} ${normalizedLabel}`.trim();
}

function secondsDurationFormat(seconds) {
  const value = Number(seconds || 0);
  if (value >= 86400) {
    return `${decimalFormat(value / 86400, 1)} Tage`;
  }
  return durationHoursFormat(value);
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
  return number === null ? "–" : numberFormat(number);
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
    return "–";
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return text;
  }
  return TIMESTAMP_LABEL_FORMAT.format(parsed);
}

function confidenceLabel(value) {
  return (
    {
      high: "Hoch",
      medium: "Mittel",
      low: "Niedrig",
    }[String(value || "").toLowerCase()] || "Nicht bewertet"
  );
}

const STATUS_METRIC_FIELDS = [
  "occupancy_share",
  "day_occupancy_share",
  "night_occupancy_share",
  "out_of_order_share",
  "day_out_of_order_share",
  "night_out_of_order_share",
];

export function statusMetricsAreUsable(summary) {
  if (summary?.status_metrics_available === false) {
    return false;
  }
  if (summary?.status_metrics_available === true) {
    return true;
  }
  const stationCount = optionalNumber(summary?.station_count) ?? 0;
  const observations =
    firstNumber(summary?.observations_total, summary?.status_change_count) ?? 0;
  const measuredSeconds = optionalNumber(summary?.measured_seconds) ?? 0;
  const statusDurations = [
    optionalNumber(summary?.occupied_seconds),
    optionalNumber(summary?.out_of_order_seconds),
    optionalNumber(summary?.unavailable_seconds),
  ];
  const nationallySuspiciousZeroMix =
    stationCount >= 1000 &&
    observations >= 10000 &&
    measuredSeconds > 0 &&
    statusDurations.every((value) => value === 0);
  return !nationallySuspiciousZeroMix;
}

function withoutStatusMetrics(row) {
  const sanitized = {
    ...row,
    status_metrics_available: false,
    static_stations_without_disruptions: null,
    static_stations_without_disruptions_share: null,
    static_disruptions_at_end_of_day: null,
    static_fully_out_of_service_stations: null,
  };
  for (const field of STATUS_METRIC_FIELDS) {
    sanitized[field] = null;
  }
  return sanitized;
}

function aggregateStatusMetricEvidence(rows) {
  return rows.reduce(
    (summary, row) => ({
      station_count: summary.station_count + Number(row?.station_count || 0),
      observations_total:
        summary.observations_total +
        Number(row?.observations_total ?? row?.status_change_count ?? 0),
      measured_seconds:
        summary.measured_seconds + Number(row?.measured_seconds || 0),
      occupied_seconds:
        summary.occupied_seconds + Number(row?.occupied_seconds || 0),
      out_of_order_seconds:
        summary.out_of_order_seconds + Number(row?.out_of_order_seconds || 0),
      unavailable_seconds:
        summary.unavailable_seconds + Number(row?.unavailable_seconds || 0),
    }),
    {
      station_count: 0,
      observations_total: 0,
      measured_seconds: 0,
      occupied_seconds: 0,
      out_of_order_seconds: 0,
      unavailable_seconds: 0,
    },
  );
}

export function buildProviderReportMetrics(row) {
  const receivedMessagesTotal = firstNumber(row?.received_messages_total, row?.messages_total) ?? 0;
  const observationsTotal = firstNumber(row?.observations_total) ?? 0;
  const uniqueChargersReferencedTotal = firstNumber(
    row?.static_station_count,
    row?.station_count,
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

export function normalizeCountryCode(value) {
  const code = String(value || "").trim().toUpperCase();
  return /^[A-Z]{2}$/.test(code) ? code : "";
}

export function normalizeProviderUid(value) {
  const providerUid = String(value || "").trim();
  return /^[A-Za-z0-9._:-]{1,160}$/.test(providerUid) ? providerUid : "";
}

function isUnspecifiedProviderName(value) {
  const normalized = String(value || "").trim().toLocaleLowerCase("de-DE");
  return (
    !normalized ||
    normalized === "unknown operator" ||
    normalized === "unknown provider" ||
    normalized === "unbekannt" ||
    normalized === "n/a"
  );
}

function humanizeProviderIdentifier(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  return text
    .split(/[_:.-]+/)
    .filter(Boolean)
    .map((part) => {
      const lower = part.toLocaleLowerCase("de-DE");
      if (lower === "realtime") {
        return "Echtzeit";
      }
      if (
        part.length <= 4 ||
        ["afir", "api", "datex", "eipa", "nap", "nobil", "ocpi"].includes(lower)
      ) {
        return part.toLocaleUpperCase("de-DE");
      }
      return `${part.charAt(0).toLocaleUpperCase("de-DE")}${part.slice(1)}`;
    })
    .join(" ");
}

export function providerDisplayName(row, { operatorMode = false } = {}) {
  const operatorName = String(row?.operator_brand || row?.operator_name || "").trim();
  if (operatorMode) {
    return isUnspecifiedProviderName(operatorName)
      ? UNSPECIFIED_PROVIDER_LABEL
      : operatorName;
  }

  const providerUid = String(row?.provider_uid || "").trim();
  const displayName = String(row?.display_name || "").trim();
  if (
    !isUnspecifiedProviderName(displayName) &&
    displayName !== providerUid &&
    !/^[a-z0-9]+(?:[_:-][a-z0-9]+)+$/.test(displayName)
  ) {
    return displayName;
  }
  const publisher = String(row?.publisher || "").trim();
  return (
    humanizeProviderIdentifier(publisher) ||
    humanizeProviderIdentifier(displayName) ||
    humanizeProviderIdentifier(providerUid) ||
    UNSPECIFIED_SOURCE_LABEL
  );
}

function providerSourceLabel(row) {
  const sourceProviderUids = Array.isArray(row?.source_provider_uids)
    ? row.source_provider_uids.map(humanizeProviderIdentifier).filter(Boolean)
    : [];
  if (sourceProviderUids.length) {
    return sourceProviderUids.join(", ");
  }
  const publisher = String(row?.publisher || "").trim();
  return humanizeProviderIdentifier(publisher) || "–";
}

export function countryName(countryCode) {
  const code = normalizeCountryCode(countryCode);
  if (!code) {
    return "";
  }
  return COUNTRY_NAMES.of(code) || code;
}

function countryFlag(countryCode) {
  return normalizeCountryCode(countryCode)
    .split("")
    .map((letter) => String.fromCodePoint(127397 + letter.charCodeAt(0)))
    .join("");
}

export function dateRangeForWindow(endDate, windowDays) {
  const normalized = normalizeManagementDate(endDate);
  const days = Number(windowDays);
  if (!normalized || !SUPPORTED_WINDOW_DAYS.includes(days)) {
    return { startDate: "", endDate: "" };
  }
  const start = new Date(`${normalized}T00:00:00Z`);
  start.setUTCDate(start.getUTCDate() - days + 1);
  return { startDate: start.toISOString().slice(0, 10), endDate: normalized };
}

export function snapshotPathForDate(dateText) {
  const normalized = normalizeManagementDate(dateText);
  if (!normalized) {
    return "";
  }
  const [year, month, day] = normalized.split("-");
  return `./data/management/days/${year}/${month}/${day}/snapshot.json`;
}

export function buildManagementSubtitle(dateText, countryCode = "") {
  const normalizedCountry = normalizeCountryCode(countryCode);
  if (normalizedCountry) {
    return `Störungen und Auslastung öffentlicher Ladestationen in ${countryName(normalizedCountry)}`;
  }
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
    values: rows.map((row) => {
      const value = Number(row?.[metricKey] || 0);
      return metric.kind === "percent" ? value * 100 : value;
    }),
  };
}

export function staticCatalogCountsForCountry(openStaticSummary, countryCode) {
  const normalizedCountry = normalizeCountryCode(countryCode);
  if (!normalizedCountry) {
    return { publicStationCount: null, publicChargerCount: null };
  }
  const countries = Array.isArray(openStaticSummary?.countries) ? openStaticSummary.countries : [];
  const country = countries.find(
    (row) => normalizeCountryCode(row?.code ?? row?.country_code) === normalizedCountry,
  );
  return {
    publicStationCount: optionalNumber(country?.station_count),
    publicChargerCount: optionalNumber(country?.charger_count),
  };
}

export function liveStationCoverage(summary, catalogStationCount = null) {
  const linkedStationCount = optionalNumber(summary?.measured_static_station_count);
  const observedLinkedStationCount = optionalNumber(
    summary?.observed_static_station_count,
  );
  const sourceStationCount = firstNumber(
    summary?.measured_dynamic_station_count,
    summary?.measured_station_count,
    summary?.observed_dynamic_station_count,
    summary?.station_count,
  );
  const reportedCoverage = optionalNumber(summary?.static_station_coverage);
  const linkageStatus = String(summary?.static_identifier_linkage_status || "");
  const hasVerifiedLinks = linkageStatus
    ? linkageStatus === "verified_matches"
    : (observedLinkedStationCount !== null && observedLinkedStationCount > 0) ||
      (linkedStationCount !== null && linkedStationCount > 0) ||
      (reportedCoverage !== null && reportedCoverage > 0);
  const liveStationCount = hasVerifiedLinks
    ? linkedStationCount
    : sourceStationCount;
  const liveStationShare =
    hasVerifiedLinks && Number(catalogStationCount) > 0
      ? reportedCoverage ??
        (linkedStationCount !== null
          ? linkedStationCount / Number(catalogStationCount)
          : null)
      : null;
  return {
    liveStationCount,
    liveStationShare,
    linkedStationCount,
    hasVerifiedLinks,
    scope: hasVerifiedLinks ? "verified_static_links" : "source_observations",
  };
}

export function buildSummaryCards(
  snapshot,
  { publicStationCount = null, publicChargerCount = null } = {},
) {
  const summary = snapshot?.summary || {};
  const statusMetricsAvailable = statusMetricsAreUsable(summary);
  const unavailableStatusDetail =
    "Die Quelle liefert derzeit keine auswertbaren Belegt- oder Störungszustände.";
  if (optionalNumber(summary.measured_station_coverage) !== null) {
    const catalogStationCount = firstNumber(
      summary.static_station_count,
      publicStationCount,
    );
    const catalogChargerCount = firstNumber(
      summary.static_charger_count,
      publicChargerCount,
    );
    const liveCoverageMetrics = liveStationCoverage(
      summary,
      catalogStationCount,
    );
    const liveStationCount = liveCoverageMetrics.liveStationCount;
    const liveCoverage = liveCoverageMetrics.liveStationShare;
    const stationsWithoutDisruptions = liveCoverageMetrics.hasVerifiedLinks
      ? optionalNumber(summary.static_stations_without_disruptions)
      : null;
    const disruptionFreeShare =
      liveCoverageMetrics.linkedStationCount > 0 &&
      stationsWithoutDisruptions !== null
        ? stationsWithoutDisruptions / liveCoverageMetrics.linkedStationCount
        : null;
    return [
      {
        key: "public-infrastructure",
        label: "Öffentliche Infrastruktur",
        metrics: [
          {
            key: "public-charging-points",
            label: "Ladepunkte",
            value: optionalNumberFormat(catalogChargerCount),
            reference: 1,
          },
          {
            key: "public-stations",
            label: "Ladestationen",
            value: optionalNumberFormat(catalogStationCount),
            reference: 2,
          },
        ],
      },
      {
        key: "live-data",
        label: "Stationen mit Live-Daten",
        reference: 3,
        detail: liveCoverageMetrics.hasVerifiedLinks
          ? ""
          : "Live-Status ist vorhanden; die Zuordnung zum öffentlichen Verzeichnis ist noch nicht verifiziert.",
        metrics: [
          {
            key: "live-stations",
            label: "Anzahl",
            value: optionalNumberFormat(liveStationCount),
          },
          {
            key: "live-station-share",
            label: "Anteil",
            value: percentFormat(liveCoverage),
          },
        ],
      },
      {
        key: "disruption-free",
        label: "Stationen ohne Störung",
        reference: 4,
        metrics: [
          {
            key: "disruption-free-stations",
            label: "Anzahl",
            value: statusMetricsAvailable
              ? optionalNumberFormat(stationsWithoutDisruptions)
              : "–",
          },
          {
            key: "disruption-free-share",
            label: "Anteil",
            value: statusMetricsAvailable ? percentFormat(disruptionFreeShare) : "–",
          },
        ],
        detail: statusMetricsAvailable ? "" : unavailableStatusDetail,
      },
      {
        key: "end-of-day",
        label: "Am Tagesende",
        metrics: [
          {
            key: "disruptions-at-end-of-day",
            label: "Gestört",
            value: statusMetricsAvailable
              ? optionalNumberFormat(summary.static_disruptions_at_end_of_day)
              : "–",
            reference: 7,
          },
          {
            key: "fully-out-of-service",
            label: "Außer Betrieb",
            value: statusMetricsAvailable
              ? optionalNumberFormat(summary.static_fully_out_of_service_stations)
              : "–",
            reference: 8,
          },
        ],
        detail: statusMetricsAvailable ? "" : unavailableStatusDetail,
      },
      {
        key: "occupancy",
        label: "Auslastung",
        reference: 5,
        metrics: [
          {
            key: "occupancy-total",
            label: "Gesamt",
            value: statusMetricsAvailable ? percentFormat(summary.occupancy_share) : "–",
          },
          {
            key: "occupancy-day",
            label: "Tag",
            value: statusMetricsAvailable ? percentFormat(summary.day_occupancy_share) : "–",
          },
          {
            key: "occupancy-night",
            label: "Nacht",
            value: statusMetricsAvailable ? percentFormat(summary.night_occupancy_share) : "–",
          },
        ],
        detail: statusMetricsAvailable ? "" : unavailableStatusDetail,
      },
      {
        key: "fault-share",
        label: "Störungsanteil",
        reference: 6,
        metrics: [
          {
            key: "fault-share-total",
            label: "Gesamt",
            value: statusMetricsAvailable ? percentFormat(summary.out_of_order_share) : "–",
          },
          {
            key: "fault-share-day",
            label: "Tag",
            value: statusMetricsAvailable ? percentFormat(summary.day_out_of_order_share) : "–",
          },
          {
            key: "fault-share-night",
            label: "Nacht",
            value: statusMetricsAvailable ? percentFormat(summary.night_out_of_order_share) : "–",
          },
        ],
        detail: statusMetricsAvailable ? "" : unavailableStatusDetail,
      },
    ];
  }
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
      label: "Empfangene AFIR-Meldungen",
      value: numberFormat(summary.archive_messages_total ?? summary.observations_total),
      detail: "Archivierte Push- und Abrufmeldungen des Tages.",
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
  return cards.map((card, index) => ({
    key: `legacy-${index + 1}`,
    label: card.label,
    detail: card.detail,
    metrics: [
      {
        key: "value",
        label: "Anzahl",
        value: card.value,
      },
    ],
  }));
}

export function buildCountryOverviewRows(
  countriesPayload,
  reportPayload,
  openStaticSummary = {},
) {
  const countries = Array.isArray(countriesPayload?.countries) ? countriesPayload.countries : [];
  const reportRows = Array.isArray(reportPayload?.rows) ? reportPayload.rows : [];
  const reportByCountry = new Map(
    reportRows.map((row) => [normalizeCountryCode(row?.country_code), row]),
  );
  return countries
    .map((country) => {
      const countryCode = normalizeCountryCode(country?.country_code);
      const report = reportByCountry.get(countryCode) || {};
      const catalogCounts = staticCatalogCountsForCountry(openStaticSummary, countryCode);
      const reportedPublicStationCount = optionalNumber(report.static_station_count);
      const reportedPublicChargerCount = optionalNumber(report.static_charger_count);
      const publicStationCount =
        reportedPublicStationCount !== null && reportedPublicStationCount > 0
          ? reportedPublicStationCount
          : catalogCounts.publicStationCount;
      const publicChargerCount =
        reportedPublicChargerCount !== null && reportedPublicChargerCount > 0
          ? reportedPublicChargerCount
          : catalogCounts.publicChargerCount;
      const liveCoverageMetrics = liveStationCoverage(
        report,
        publicStationCount,
      );
      const liveStationCount = liveCoverageMetrics.liveStationCount;
      const liveStationShare = liveCoverageMetrics.liveStationShare;
      const stationsWithoutDisruptions = liveCoverageMetrics.hasVerifiedLinks
        ? optionalNumber(report.static_stations_without_disruptions)
        : null;
      const stationsWithoutDisruptionsShare = liveCoverageMetrics.hasVerifiedLinks
        ? optionalNumber(report.static_stations_without_disruptions_share) ??
          (liveCoverageMetrics.linkedStationCount > 0 &&
          stationsWithoutDisruptions !== null
            ? stationsWithoutDisruptions / liveCoverageMetrics.linkedStationCount
            : null)
        : null;
      const row = {
        ...country,
        ...report,
        country_code: countryCode,
        country_name: countryName(countryCode),
        first_date: country.first_date,
        last_date: country.last_date,
        observed_days: country.observed_days,
        public_station_count: publicStationCount,
        public_charger_count: publicChargerCount,
        live_station_count: liveStationCount,
        live_station_share: liveStationShare,
        live_station_count_scope: liveCoverageMetrics.scope,
        stations_without_disruptions: stationsWithoutDisruptions,
        stations_without_disruptions_share: stationsWithoutDisruptionsShare,
      };
      return statusMetricsAreUsable(report)
        ? row
        : {
            ...withoutStatusMetrics(row),
            stations_without_disruptions: null,
            stations_without_disruptions_share: null,
          };
    })
    .filter((row) => row.country_code && optionalNumber(row.station_count) !== null)
    .sort((left, right) => TABLE_SORT_COLLATOR.compare(left.country_name, right.country_name));
}

const ROLLING_COUNTRY_SUMMARY_FIELDS = [
  "static_charger_count",
  "static_station_count",
  "observed_dynamic_station_count",
  "measured_dynamic_station_count",
  "measured_station_count",
  "observed_static_station_count",
  "measured_static_station_count",
  "static_identifier_linkage_status",
  "static_stations_without_disruptions",
  "static_station_coverage",
  "static_stations_without_disruptions_share",
  "occupancy_share",
  "day_occupancy_share",
  "night_occupancy_share",
  "out_of_order_share",
  "day_out_of_order_share",
  "night_out_of_order_share",
];

export function mergeRollingCountrySummary(snapshot, reportPayload, countryCode) {
  const normalizedCountry = normalizeCountryCode(countryCode);
  const reportRow = (Array.isArray(reportPayload?.rows) ? reportPayload.rows : []).find(
    (row) => normalizeCountryCode(row?.country_code) === normalizedCountry,
  );
  if (!reportRow) {
    return snapshot;
  }
  const rollingSummary = {};
  for (const field of ROLLING_COUNTRY_SUMMARY_FIELDS) {
    if (reportRow[field] !== null && reportRow[field] !== undefined) {
      rollingSummary[field] = reportRow[field];
    }
  }
  const combinedSummary = {
    ...(snapshot?.summary || {}),
    ...reportRow,
  };
  const statusMetricsAvailable = statusMetricsAreUsable(combinedSummary);
  const mergedSummary = {
    ...(snapshot?.summary || {}),
    ...rollingSummary,
    reporting_period_start_date: reportPayload?.start_date || "",
    reporting_period_end_date: reportPayload?.end_date || "",
  };
  return {
    ...snapshot,
    summary: statusMetricsAvailable
      ? { ...mergedSummary, status_metrics_available: true }
      : withoutStatusMetrics(mergedSummary),
  };
}

export function buildRollingProviderRows(reportPayload, healthPayload) {
  const reportRows = Array.isArray(reportPayload?.rows) ? reportPayload.rows : [];
  const healthRows = Array.isArray(healthPayload?.rows) ? healthPayload.rows : [];
  const operatorReport = reportPayload?.group_by === "operator";
  const statusMetricsAvailable = statusMetricsAreUsable(
    aggregateStatusMetricEvidence(reportRows),
  );
  const healthByProvider = new Map(
    healthRows.map((row) => [
      `${normalizeCountryCode(row?.country_code)}\u0000${String(row?.provider_uid || "")}`,
      row,
    ]),
  );
  return reportRows
    .filter((row) => !operatorReport || Number(row?.station_count || 0) >= 50)
    .map((row) => {
      const operatorMode =
        operatorReport ||
        Boolean(String(row?.operator_brand || row?.operator_name || "").trim());
      const sourceProviderUids = Array.isArray(row?.source_provider_uids)
        ? row.source_provider_uids.map((value) => String(value || "").trim()).filter(Boolean)
        : [];
      const health =
        healthByProvider.get(
          `${normalizeCountryCode(row?.country_code)}\u0000${String(row?.provider_uid || "")}`,
        ) || {};
      const publisher =
        health.publisher ||
        String(row?.publisher || "").trim() ||
        (sourceProviderUids.length ? sourceProviderUids.join(", ") : "");
      const operatorBrand = operatorMode
        ? providerDisplayName(row, { operatorMode: true })
        : "";
      const displayName = providerDisplayName(
        {
          ...row,
          display_name: health.display_name || row.display_name,
          publisher,
        },
        { operatorMode },
      );
      const mergedRow = {
        ...row,
        operator_brand: operatorBrand,
        display_name: displayName,
        publisher,
        source_provider_uids: sourceProviderUids,
        transport_failure_count:
          Number(health.fetch_failure_messages_total || 0) +
          Number(health.http_error_messages_total || 0),
        provider_message_days: Number(health.provider_message_days || 0),
      };
      return statusMetricsAvailable ? mergedRow : withoutStatusMetrics(mergedRow);
    })
    .sort((left, right) => {
      const stationDelta = Number(right?.station_count || 0) - Number(left?.station_count || 0);
      return stationDelta || TABLE_SORT_COLLATOR.compare(left.display_name, right.display_name);
    });
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
  const eligibleRows = rows.filter((row) => {
    const utilization = optionalNumber(row?.day_occupancy_share);
    return utilization === null || utilization < DISPLAYED_FULL_UTILIZATION_THRESHOLD;
  });
  eligibleRows.sort((left, right) => {
    const busyDelta =
      Number(right?.busy_transition_count || 0) - Number(left?.busy_transition_count || 0);
    if (busyDelta !== 0) {
      return busyDelta;
    }
    return String(left?.station_id || "").localeCompare(String(right?.station_id || ""));
  });
  return eligibleRows.slice(0, TOP_STATIONS_LIMIT);
}

export function buildProviderRows(snapshot) {
  const rows = Array.isArray(snapshot?.provider_reports) ? [...snapshot.provider_reports] : [];
  const operatorMode = rows.some((row) => String(row?.operator_brand || "").trim());
  rows.sort((left, right) => {
    if (operatorMode) {
      const leftStations = Number(left?.static_station_count || left?.station_count || left?.mapped_stations_observed || 0);
      const rightStations = Number(right?.static_station_count || right?.station_count || right?.mapped_stations_observed || 0);
      const stationDelta = rightStations - leftStations;
      if (stationDelta !== 0) {
        return stationDelta;
      }
    }
    const observationDelta = Number(right?.observations_total || 0) - Number(left?.observations_total || 0);
    if (observationDelta !== 0) {
      return observationDelta;
    }
    const messageDelta = Number(right?.messages_total || 0) - Number(left?.messages_total || 0);
    if (messageDelta !== 0) {
      return messageDelta;
    }
    return String(left?.operator_brand || left?.display_name || left?.provider_uid || "").localeCompare(
      String(right?.operator_brand || right?.display_name || right?.provider_uid || ""),
    );
  });
  return rows;
}

export function buildDailySourceRows(snapshot) {
  return buildProviderRows(snapshot).filter((row) =>
    Boolean(String(row?.provider_uid || "").trim()),
  );
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

function stationUrl(row) {
  if (row?.station_url) {
    return String(row.station_url);
  }
  const stationId = String(row?.station_id || "").trim();
  return stationId ? `./?station=${encodeURIComponent(stationId)}` : "";
}

function managementCountryUrl(countryCode, dateText = "", windowDays = "") {
  const query = new URLSearchParams({ country: normalizeCountryCode(countryCode) });
  if (normalizeManagementDate(dateText)) {
    query.set("date", dateText);
  }
  if (SUPPORTED_WINDOW_DAYS.includes(Number(windowDays))) {
    query.set("window", String(windowDays));
  }
  return `./management.html?${query.toString()}`;
}

function managementOverviewUrl(dateText = "", windowDays = "") {
  const query = new URLSearchParams();
  if (normalizeManagementDate(dateText)) {
    query.set("date", dateText);
  }
  if (SUPPORTED_WINDOW_DAYS.includes(Number(windowDays))) {
    query.set("window", String(windowDays));
  }
  const queryText = query.toString();
  return `./management.html${queryText ? `?${queryText}` : ""}`;
}

function managementProviderUrl(providerUid, countryCode = "", dateText = "") {
  const query = new URLSearchParams({ provider: normalizeProviderUid(providerUid) });
  if (normalizeCountryCode(countryCode)) {
    query.set("country", normalizeCountryCode(countryCode));
  }
  if (normalizeManagementDate(dateText)) {
    query.set("date", dateText);
  }
  return `./management.html?${query.toString()}`;
}

export function buildProviderProfileSeries(profilePayload) {
  const rows = Array.isArray(profilePayload?.rows) ? profilePayload.rows : [];
  const byHour = new Map(rows.map((row) => [Number(row?.local_hour), row]));
  const labels = Array.from({ length: 24 }, (_, hour) => `${String(hour).padStart(2, "0")}:00`);
  return {
    labels,
    occupancy: labels.map((_, hour) => Number(byHour.get(hour)?.occupancy_share || 0) * 100),
    outages: labels.map((_, hour) => Number(byHour.get(hour)?.out_of_order_share || 0) * 100),
  };
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

async function loadOpenStaticSummary() {
  try {
    const response = await fetch("./data/open_static_summary.json", {
      cache: "no-store",
      headers: { Accept: "application/json" },
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.warn("Der statische Katalogvergleich ist nicht verfügbar.", error);
    return { countries: [] };
  }
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

function sortableHeaderCells(table) {
  const rows = Array.from(table.tHead?.rows || []);
  const preferredRow = rows.find(
    (row) => row.classList.contains("sorttop") || row.classList.contains("sortTop"),
  );
  const cells = preferredRow
    ? Array.from(preferredRow.cells)
    : rows.flatMap((row) => Array.from(row.cells));
  return cells.filter((th) => th.colSpan === 1);
}

function sortableColumnIndex(th) {
  const explicitColumn = String(th?.dataset?.sortColumn ?? "").trim();
  return explicitColumn ? Number(explicitColumn) : th?.cellIndex ?? -1;
}

function sortableHeaderForColumn(table, columnIndex) {
  return sortableHeaderCells(table).find(
    (th) => sortableColumnIndex(th) === columnIndex,
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

function updateTableRowNumbers(table) {
  tableBodyRows(table).forEach((row, index) => {
    const rank = String(index + 1);
    row.querySelectorAll("[data-row-number]").forEach((cell) => {
      cell.textContent = rank;
    });
  });
}

function updateSortableTableTitle(table) {
  const titleId = String(table.dataset.sortTitleId || "").trim();
  const title = titleId ? document.getElementById(titleId) : null;
  if (!title) {
    return;
  }
  const columnIndex = Number(table.dataset.sortColumn);
  const columnHeader = Number.isInteger(columnIndex)
    ? sortableHeaderForColumn(table, columnIndex)
    : null;
  const columnLabel =
    columnHeader?.dataset.sortLabel ||
    columnHeader?.querySelector(".management-sort-label")?.textContent ||
    "";
  title.textContent = rankedTableTitle(
    table.dataset.sortTitlePrefix,
    columnLabel,
    "Auslastung",
  );
}

function setSortableHeaderState(table, activeColumnIndex, direction) {
  const headers = sortableHeaderCells(table);
  if (!headers.length) {
    return;
  }
  for (const th of headers) {
    const columnIndex = sortableColumnIndex(th);
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
  updateTableRowNumbers(table);
}

function toggleTableSort(table, columnIndex, type) {
  const currentColumn = Number(table.dataset.sortColumn || -1);
  const currentDirection = table.dataset.sortDirection || "none";
  let nextDirection;
  if (table.dataset.rankRows === "true") {
    if (currentColumn === columnIndex) {
      nextDirection = currentDirection === "descending" ? "ascending" : "descending";
    } else {
      nextDirection = type === "text" ? "ascending" : "descending";
    }
  } else {
    nextDirection = "ascending";
    if (currentColumn === columnIndex && currentDirection === "ascending") {
      nextDirection = "descending";
    } else if (currentColumn === columnIndex && currentDirection === "descending") {
      nextDirection = "none";
    }
  }

  table.dataset.sortColumn = nextDirection === "none" ? "" : String(columnIndex);
  table.dataset.sortDirection = nextDirection;
  sortTableRows(table, columnIndex, type, nextDirection);
  setSortableHeaderState(table, columnIndex, nextDirection);
  updateSortableTableTitle(table);
}

function alignNumericTableColumns(table) {
  const headers = sortableHeaderCells(table);
  if (!headers.length) {
    return;
  }
  for (const th of headers) {
    if (!["number", "date"].includes(th.dataset.sortType)) {
      continue;
    }
    const columnIndex = sortableColumnIndex(th);
    th.classList.add("management-table-numeric");
    for (const row of tableBodyRows(table)) {
      row.cells[columnIndex]?.classList.add("management-table-numeric");
    }
  }
}

function wireSortableTable(table) {
  alignNumericTableColumns(table);
  if (table.dataset.sortableInitialized === "true") {
    return;
  }
  const headers = sortableHeaderCells(table);
  if (!headers.length) {
    return;
  }
  for (const th of headers) {
    if (th.classList.contains("unsortable") || th.dataset.sortable === "false") {
      continue;
    }
    const columnIndex = sortableColumnIndex(th);
    const sortType = th.dataset.sortType || "text";
    const label = th.dataset.sortLabel || th.textContent.trim();
    const visibleLabel = th.textContent.trim();
    const detail = String(th.dataset.sortDetail || "").trim();
    const button = document.createElement("button");
    const labelSpan = document.createElement("span");
    const indicator = document.createElement("span");
    button.type = "button";
    button.className = "management-sort-button";
    button.dataset.sortDirection = "none";
    button.setAttribute("aria-label", `${label} sortieren`);
    labelSpan.className = "management-sort-label";
    labelSpan.textContent = visibleLabel;
    if (detail) {
      const detailSpan = document.createElement("span");
      detailSpan.className = "management-sort-detail";
      detailSpan.textContent = detail;
      labelSpan.appendChild(detailSpan);
    }
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
  alignNumericTableColumns(table);
  markTableOriginalOrder(table);
  const defaultColumnText = String(table.dataset.defaultSortColumn || "").trim();
  const defaultColumn = defaultColumnText ? Number(defaultColumnText) : -1;
  const defaultDirection = table.dataset.defaultSortDirection || "none";
  if (Number.isInteger(defaultColumn) && defaultColumn >= 0 && defaultDirection !== "none") {
    const sortType =
      sortableHeaderForColumn(table, defaultColumn)?.dataset.sortType || "text";
    table.dataset.sortColumn = String(defaultColumn);
    table.dataset.sortDirection = defaultDirection;
    sortTableRows(table, defaultColumn, sortType, defaultDirection);
    setSortableHeaderState(table, defaultColumn, defaultDirection);
  } else {
    table.dataset.sortColumn = "";
    table.dataset.sortDirection = "none";
    setSortableHeaderState(table, -1, "none");
    updateTableRowNumbers(table);
  }
  updateSortableTableTitle(table);
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
        y: {
          beginAtZero: true,
          ticks:
            series.kind === "percent"
              ? { callback: (value) => `${new Intl.NumberFormat("de-DE").format(value)} %` }
              : {},
        },
      },
    },
  });
}

function createProviderProfileChart(profilePayload) {
  const canvas = document.getElementById("management-provider-profile-chart");
  const series = buildProviderProfileSeries(profilePayload);
  return new Chart(canvas.getContext("2d"), {
    type: "line",
    data: {
      labels: series.labels,
      datasets: [
        {
          label: "Auslastung",
          data: series.occupancy,
          borderColor: "#12664f",
          backgroundColor: "#12664f",
          tension: 0.25,
        },
        {
          label: "Störungsanteil",
          data: series.outages,
          borderColor: "#d1633c",
          backgroundColor: "#d1633c",
          tension: 0.25,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          beginAtZero: true,
          ticks: { callback: (value) => `${new Intl.NumberFormat("de-DE").format(value)} %` },
        },
      },
    },
  });
}

function renderSummaryStrip(host, items) {
  host.innerHTML = "";
  for (const item of items) {
    const element = document.createElement("div");
    element.className = "management-summary-item";
    element.innerHTML = `
      <span>${escapeHtml(item.label)}</span>
      <strong>${escapeHtml(item.value)}</strong>
      ${item.detail ? `<small>${escapeHtml(item.detail)}</small>` : ""}
    `;
    host.appendChild(element);
  }
}

function renderCountryOverview(
  countriesPayload,
  reportPayload,
  openStaticSummary,
  dateText,
  windowDays,
) {
  const rows = buildCountryOverviewRows(
    countriesPayload,
    reportPayload,
    openStaticSummary,
  );
  const tbody = document.getElementById("management-country-body");
  const coverageBody = document.getElementById("management-country-coverage-body");
  const countryCount = document.getElementById("management-country-count");
  tbody.innerHTML = "";
  coverageBody.innerHTML = "";
  countryCount.textContent = `${numberFormat(rows.length)} Länder`;
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="13">Für diesen Tag liegen keine Länderberichte vor.</td></tr>';
    coverageBody.innerHTML =
      '<tr><td colspan="9">Für diesen Berichtszeitraum liegen keine Länderkennzahlen vor.</td></tr>';
  }
  for (const row of rows) {
    const detailUrl = managementCountryUrl(row.country_code, dateText, windowDays);
    const tr = document.createElement("tr");
    tr.className = "management-country-row";
    tr.innerHTML = `
      <td class="management-rank-column" data-row-number></td>
      <td data-sort-value="${escapeAttribute(row.country_name)}">
        <a class="management-country-link" href="${escapeAttribute(detailUrl)}">
          <span class="management-country-flag" aria-hidden="true">${countryFlag(row.country_code)}</span>
          <strong>${escapeHtml(row.country_name)}</strong>
        </a>
      </td>
      <td data-sort-value="${escapeAttribute(row.country_code)}">${escapeHtml(row.country_code)}</td>
      <td data-sort-value="${escapeAttribute(row.first_date || "")}">${escapeHtml(formatDateLabel(row.first_date) || "–")}</td>
      <td data-sort-value="${numericSortValue(row.station_count)}">${numberFormat(row.station_count)}</td>
      <td data-sort-value="${numericSortValue(row.observed_evse_days ?? row.observed_evses)}">${numberFormat(row.observed_evse_days ?? row.observed_evses)}</td>
      <td data-sort-value="${numericSortValue(row.occupancy_share)}">${percentFormat(row.occupancy_share)}</td>
      <td data-sort-value="${numericSortValue(row.day_occupancy_share)}">${percentFormat(row.day_occupancy_share)}</td>
      <td data-sort-value="${numericSortValue(row.night_occupancy_share)}">${percentFormat(row.night_occupancy_share)}</td>
      <td data-sort-value="${numericSortValue(row.out_of_order_share)}">${percentFormat(row.out_of_order_share)}</td>
      <td data-sort-value="${numericSortValue(row.day_out_of_order_share)}">${percentFormat(row.day_out_of_order_share)}</td>
      <td data-sort-value="${numericSortValue(row.night_out_of_order_share)}">${percentFormat(row.night_out_of_order_share)}</td>
      <td><a class="management-row-action" href="${escapeAttribute(detailUrl)}" aria-label="${escapeAttribute(row.country_name)} öffnen">→</a></td>
    `;
    tbody.appendChild(tr);

    const coverageRow = document.createElement("tr");
    coverageRow.className = "management-country-row";
    coverageRow.innerHTML = `
      <td data-sort-value="${escapeAttribute(row.country_name)}">
        <a class="management-country-link" href="${escapeAttribute(detailUrl)}">
          <span class="management-country-flag" aria-hidden="true">${countryFlag(row.country_code)}</span>
          <strong>${escapeHtml(row.country_name)}</strong>
        </a>
      </td>
      <td data-sort-value="${escapeAttribute(row.country_code)}">${escapeHtml(row.country_code)}</td>
      <td data-sort-value="${numericSortValue(row.public_charger_count)}">
        ${optionalNumberFormat(row.public_charger_count)}
      </td>
      <td data-sort-value="${numericSortValue(row.public_station_count)}">
        ${optionalNumberFormat(row.public_station_count)}
      </td>
      <td data-sort-value="${numericSortValue(row.live_station_count)}">${optionalNumberFormat(row.live_station_count)}</td>
      <td data-sort-value="${numericSortValue(row.live_station_share)}">${percentFormat(row.live_station_share)}</td>
      <td data-sort-value="${numericSortValue(row.stations_without_disruptions)}">${optionalNumberFormat(row.stations_without_disruptions)}</td>
      <td data-sort-value="${numericSortValue(row.stations_without_disruptions_share)}">${percentFormat(row.stations_without_disruptions_share)}</td>
      <td><a class="management-row-action" href="${escapeAttribute(detailUrl)}" aria-label="${escapeAttribute(row.country_name)} öffnen">→</a></td>
    `;
    coverageBody.appendChild(coverageRow);
  }
}

function renderDataQuality(snapshot, windowDays) {
  const summary = snapshot?.summary || {};
  const host = document.getElementById("management-data-quality");
  const statusMetricsAvailable = statusMetricsAreUsable(summary);
  const providerErrors =
    Number(summary.fetch_failure_messages_total || 0) +
    Number(summary.http_error_messages_total || 0);
  renderSummaryStrip(host, [
    {
      label: "Konfidenz",
      value: statusMetricsAvailable
        ? confidenceLabel(summary.confidence_label)
        : "Nicht auswertbar",
      detail: statusMetricsAvailable
        ? "Bewertung der Datenabdeckung"
        : "Belegung und Störungen fehlen",
    },
    {
      label: "Koordinaten",
      value: percentFormat(summary.coordinate_coverage),
      detail: "für geografische Auswertungen",
    },
    {
      label: "Anbieterfehler",
      value: numberFormat(providerErrors),
      detail: "Abruf- und HTTP-Fehler am Tag",
    },
    {
      label: "Historie",
      value: windowLabel(windowDays),
      detail: "gewählter Berichtszeitraum",
    },
  ]);
}

function managementReferenceMarkup(reference) {
  const referenceNumber = Number(reference);
  if (!Number.isInteger(referenceNumber)) {
    return "";
  }
  const footnoteId = `management-footnote-${referenceNumber}`;
  const footnoteText = String(document.getElementById(footnoteId)?.textContent || "")
    .replace(/\s+/g, " ")
    .trim();
  return `<sup class="management-reference"><a href="#${footnoteId}" aria-label="Fußnote ${referenceNumber}" aria-describedby="${footnoteId}"${footnoteText ? ` data-footnote="${escapeAttribute(footnoteText)}"` : ""}>[${referenceNumber}]</a></sup>`;
}

function renderKpis(snapshot, options = {}) {
  const groups = buildSummaryCards(snapshot, options);
  const host = document.getElementById("management-kpis");
  host.innerHTML = "";
  for (const groupInfo of groups) {
    const metrics = Array.isArray(groupInfo.metrics) ? groupInfo.metrics : [];
    const group = document.createElement("section");
    group.className = `management-kpi-group management-kpi-group--${Math.min(metrics.length, 3)}`;
    group.dataset.metricGroup = String(groupInfo.key || "");
    const metricsMarkup = metrics
      .map(
        (metric) => `
          <article class="management-kpi" data-metric="${escapeAttribute(metric.key || "")}">
            <div class="management-kpi-label">
              ${escapeHtml(metric.label)}${managementReferenceMarkup(metric.reference)}
            </div>
            <div class="management-kpi-value">${escapeHtml(metric.value)}</div>
          </article>
        `,
      )
      .join("");
    group.innerHTML = `
      <header class="management-kpi-group-head">
        <h2>${escapeHtml(groupInfo.label)}${managementReferenceMarkup(groupInfo.reference)}</h2>
        ${groupInfo.detail ? `<p>${escapeHtml(groupInfo.detail)}</p>` : ""}
      </header>
      <div class="management-kpi-group-grid" style="--management-kpi-columns: ${Math.max(1, metrics.length)}">
        ${metricsMarkup}
      </div>
    `;
    host.appendChild(group);
  }
}

function renderBrokenStations(snapshot, windowDays) {
  const rows = buildStationRows(snapshot, "broken_stations");
  const statusMetricsAvailable = statusMetricsAreUsable(snapshot?.summary || {});
  const tbody = document.getElementById("broken-stations-body");
  document.getElementById("management-broken-stations-description").textContent =
    `Top 10 der Ladestationen mit den meisten Störungen über ${windowLabel(windowDays)}.`;
  tbody.innerHTML = "";
  if (!statusMetricsAvailable) {
    tbody.innerHTML = '<tr><td colspan="5">Belegung und Störungen sind für diese Quelle derzeit nicht auswertbar.</td></tr>';
    return;
  }
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="5">Für diesen Berichtszeitraum wurden keine gestörten Stationen erkannt.</td></tr>';
    return;
  }
  for (const row of rows) {
    const detailUrl = stationUrl(row);
    const stationCell = detailUrl
      ? `<a href="${escapeAttribute(detailUrl)}">${escapeHtml(stationTitle(row))}</a>`
      : `<span>${escapeHtml(stationTitle(row))}</span>`;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${stationCell}</td>
      <td data-sort-value="${numericSortValue(row.affected_charger_count)}">${numberFormat(row.affected_charger_count)}</td>
      <td data-sort-value="${numericSortValue(row.current_broken_charger_count)}">${numberFormat(row.current_broken_charger_count)}</td>
      <td data-sort-value="${numericSortValue(row.out_of_order_share)}">${percentFormat(row.out_of_order_share)}</td>
      <td data-sort-value="${numericSortValue(row.out_of_order_duration_seconds_total)}">${durationHoursFormat(row.out_of_order_duration_seconds_total)}</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderBusyStations(snapshot, windowDays) {
  const rows = buildStationRows(snapshot, "busiest_stations");
  const statusMetricsAvailable = statusMetricsAreUsable(snapshot?.summary || {});
  const tbody = document.getElementById("busy-stations-body");
  document.getElementById("management-busy-stations-description").textContent =
    `Top 10 nach Auslastung am Tag und Statuswechseln über ${windowLabel(windowDays)}.`;
  tbody.innerHTML = "";
  if (!statusMetricsAvailable) {
    tbody.innerHTML = '<tr><td colspan="5">Belegung und Störungen sind für diese Quelle derzeit nicht auswertbar.</td></tr>';
    return;
  }
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="5">Für diesen Berichtszeitraum wurden keine Stationen mit hoher Auslastung erkannt.</td></tr>';
    return;
  }
  for (const row of rows) {
    const detailUrl = stationUrl(row);
    const stationCell = detailUrl
      ? `<a href="${escapeAttribute(detailUrl)}">${escapeHtml(stationTitle(row))}</a>`
      : `<span>${escapeHtml(stationTitle(row))}</span>`;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${stationCell}</td>
      <td data-sort-value="${numericSortValue(row.charging_points_count)}">${numberFormat(row.charging_points_count)}</td>
      <td data-sort-value="${numericSortValue(row.day_occupancy_share)}">${percentFormat(row.day_occupancy_share)}</td>
      <td data-sort-value="${numericSortValue(row.occupied_seconds)}">${secondsDurationFormat(row.occupied_seconds)}</td>
      <td data-sort-value="${numericSortValue(row.busy_transition_count)}">${numberFormat(row.busy_transition_count)}</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderRollingProviderReports(
  reportPayload,
  healthPayload,
  { countryCode = "", dateText = "", linkProviders = true } = {},
) {
  const rows = buildRollingProviderRows(reportPayload, healthPayload);
  const tbody = document.getElementById("provider-window-body");
  const operatorMode =
    reportPayload?.group_by === "operator" ||
    rows.some((row) => String(row?.operator_brand || "").trim());
  tbody.innerHTML = "";
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="10">Für diesen Zeitraum liegen keine Anbieterdaten vor.</td></tr>';
    return;
  }
  for (const row of rows) {
    const tr = document.createElement("tr");
    const displayName = providerDisplayName(row, { operatorMode });
    const canLinkProvider = linkProviders && row.provider_uid && !row.operator_brand;
    const providerUrl = canLinkProvider
      ? managementProviderUrl(row.provider_uid, row.country_code || countryCode, dateText)
      : "";
    const providerName = canLinkProvider
      ? `<a href="${escapeAttribute(providerUrl)}">${escapeHtml(displayName)}</a>`
      : `<span>${escapeHtml(displayName)}</span>`;
    tr.innerHTML = `
      <td data-sort-value="${escapeAttribute(displayName)}">
        ${providerName}
      </td>
      <td data-sort-value="${numericSortValue(row.station_count)}">${numberFormat(row.station_count)}</td>
      <td data-sort-value="${numericSortValue(row.measured_days)}">${numberFormat(row.measured_days)}</td>
      <td data-sort-value="${numericSortValue(row.occupancy_share)}">${percentFormat(row.occupancy_share)}</td>
      <td data-sort-value="${numericSortValue(row.day_occupancy_share)}">${percentFormat(row.day_occupancy_share)}</td>
      <td data-sort-value="${numericSortValue(row.night_occupancy_share)}">${percentFormat(row.night_occupancy_share)}</td>
      <td data-sort-value="${numericSortValue(row.out_of_order_share)}">${percentFormat(row.out_of_order_share)}</td>
      <td data-sort-value="${numericSortValue(row.day_out_of_order_share)}">${percentFormat(row.day_out_of_order_share)}</td>
      <td data-sort-value="${numericSortValue(row.night_out_of_order_share)}">${percentFormat(row.night_out_of_order_share)}</td>
      <td data-sort-value="${numericSortValue(row.transport_failure_count)}">${numberFormat(row.transport_failure_count)}</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderProviderReports(
  snapshot,
  { countryCode = "", dateText = "", linkProviders = true } = {},
) {
  const rows = buildDailySourceRows(snapshot);
  const tbody = document.getElementById("provider-reports-body");
  if (!tbody) {
    return;
  }
  tbody.innerHTML = "";
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="10">Für diesen Tag liegen keine Quelldaten vor.</td></tr>';
    return;
  }
  for (const row of rows) {
    const tr = document.createElement("tr");
    const displayName = providerDisplayName(row);
    const metrics = buildProviderReportMetrics(row);
    const coverageWarning =
      Number(row.delta_delivery_without_push || 0) > 0
        ? "Delta ohne Push; Tageswert kann zu niedrig sein"
        : "–";
    const transportFields = [
      row.push_messages_total,
      row.http_response_messages_total,
      row.fetch_failure_messages_total,
      row.http_error_messages_total,
      row.payload_byte_length_total,
    ];
    const transportDataAvailable =
      row.provider_health_available !== false &&
      transportFields.some((value) => value !== null && value !== undefined);
    const transportErrors =
      Number(row.fetch_failure_messages_total || 0) +
      Number(row.http_error_messages_total || 0);
    const canLinkProvider = linkProviders && row.provider_uid && !row.operator_brand;
    const providerUrl = canLinkProvider
      ? managementProviderUrl(row.provider_uid, row.country_code || countryCode, dateText)
      : "";
    const providerName = canLinkProvider
      ? `<a href="${escapeAttribute(providerUrl)}">${escapeHtml(displayName)}</a>`
      : `<span>${escapeHtml(displayName)}</span>`;
    tr.innerHTML = `
      <td data-sort-value="${escapeAttribute(displayName)}">
        ${providerName}
      </td>
      <td data-sort-value="${numericSortValue(metrics.observationsTotal)}">${numberFormat(metrics.observationsTotal)}</td>
      <td data-sort-value="${numericSortValue(metrics.receivedMessagesTotal)}">${numberFormat(metrics.receivedMessagesTotal)}</td>
      <td data-sort-value="${numericSortValue(metrics.uniqueChargersReferencedTotal)}">${optionalNumberFormat(metrics.uniqueChargersReferencedTotal)}</td>
      <td data-sort-value="${numericSortValue(row.push_messages_total)}">${transportDataAvailable ? numberFormat(row.push_messages_total) : "–"}</td>
      <td data-sort-value="${numericSortValue(row.http_response_messages_total)}">${transportDataAvailable ? numberFormat(row.http_response_messages_total) : "–"}</td>
      <td data-sort-value="${numericSortValue(transportErrors)}">${transportDataAvailable ? numberFormat(transportErrors) : "–"}</td>
      <td data-sort-value="${numericSortValue(row.payload_byte_length_total)}">${transportDataAvailable ? megabytesFormat(row.payload_byte_length_total) : "–"}</td>
      <td data-sort-value="${escapeAttribute(timestampSortValue(row.latest_message_timestamp))}">${escapeHtml(timestampFormat(row.latest_message_timestamp))}</td>
      <td data-sort-value="${escapeAttribute(coverageWarning === "–" ? "" : coverageWarning)}">${escapeHtml(coverageWarning)}</td>
    `;
    tbody.appendChild(tr);
  }
}

async function initManagementPage() {
  const status = document.getElementById("management-status");
  wireAppPromoLinks();
  wireAppPromoDismiss();
  const dataSource = createManagementDataSource({
    apiBaseUrl: window.WOLADEN_MANAGEMENT_API_BASE_URL || "",
    staticFallbackEnabled: window.WOLADEN_MANAGEMENT_STATIC_FALLBACK_ENABLED !== false,
  });
  const indexPayload = await dataSource.loadIndex();
  const countriesPayload = await dataSource.loadCountries();
  const openStaticSummary = await loadOpenStaticSummary();
  const availableDates = Array.isArray(indexPayload.available_dates) ? indexPayload.available_dates : [];
  if (!availableDates.length) {
    throw new Error("Keine Tagesauswertungen verfügbar.");
  }

  const url = new URL(window.location.href);
  const requestedCountry = normalizeCountryCode(url.searchParams.get("country"));
  const requestedProvider = normalizeProviderUid(url.searchParams.get("provider"));
  const countryEntry = (countriesPayload.countries || []).find(
    (row) => normalizeCountryCode(row?.country_code) === requestedCountry,
  );
  const overviewHost = document.getElementById("management-country-overview");
  const detailHost = document.getElementById("management-detail");
  let currentDate =
    normalizeManagementDate(url.searchParams.get("date")) ||
    indexPayload.latest_date ||
    availableDates.at(-1);
  if (!availableDates.includes(currentDate)) {
    currentDate = availableDates.at(-1);
  }

  function syncUrl({ countryCode = "", providerUid = "", windowDays = "" } = {}) {
    url.searchParams.set("date", currentDate);
    if (countryCode) {
      url.searchParams.set("country", countryCode);
    } else {
      url.searchParams.delete("country");
    }
    if (providerUid) {
      url.searchParams.set("provider", providerUid);
    } else {
      url.searchParams.delete("provider");
    }
    if (windowDays) {
      url.searchParams.set("window", String(windowDays));
    } else {
      url.searchParams.delete("window");
    }
    history.replaceState({}, "", url);
  }

  function updateDateControls(datePicker, prevDay, nextDay, dates) {
    datePicker.value = currentDate;
    datePicker.min = dates[0] || "";
    datePicker.max = dates.at(-1) || "";
    const index = dates.indexOf(currentDate);
    prevDay.disabled = index <= 0;
    nextDay.disabled = index < 0 || index >= dates.length - 1;
  }

  function renderError(error, prefix = "Die Managementauswertung konnte nicht geladen werden") {
    console.error(error);
    status.textContent = `${prefix}: ${error?.message || error}`;
    status.classList.add("is-error");
    status.hidden = false;
  }

  function showLoading(message) {
    status.textContent = message;
    status.classList.remove("is-error");
    status.hidden = false;
  }

  function hideStatus() {
    status.textContent = "";
    status.classList.remove("is-error");
    status.hidden = true;
  }

  if (!requestedProvider && (!requestedCountry || !countryEntry)) {
    overviewHost.hidden = false;
    detailHost.hidden = true;
    document.getElementById("management-title").textContent =
      "Öffentliche Ladeinfrastruktur in Europa";
    document.getElementById("management-subtitle").textContent =
      "Länder vergleichen und Details öffnen.";
    document.title = "woladen.de | Öffentliche Ladeinfrastruktur in Europa";
    wireSortableTables(overviewHost);

    const datePicker = document.getElementById("management-overview-date");
    const prevDay = document.getElementById("management-overview-prev-day");
    const nextDay = document.getElementById("management-overview-next-day");
    const windowSelect = document.getElementById("management-overview-window-days");
    let overviewWindowDays = SUPPORTED_WINDOW_DAYS.includes(Number(url.searchParams.get("window")))
      ? Number(url.searchParams.get("window"))
      : DEFAULT_WINDOW_DAYS;
    windowSelect.value = String(overviewWindowDays);

    async function loadOverview(targetDate, windowDays = overviewWindowDays) {
      showLoading("Länderdaten werden geladen …");
      const range = dateRangeForWindow(targetDate, windowDays);
      const reportPayload = await dataSource.loadCountryOverview(targetDate, {
        startDate: range.startDate,
        endDate: range.endDate,
      });
      currentDate = targetDate;
      overviewWindowDays = windowDays;
      syncUrl({ windowDays: overviewWindowDays });
      updateDateControls(datePicker, prevDay, nextDay, availableDates);
      windowSelect.value = String(overviewWindowDays);
      renderCountryOverview(
        countriesPayload,
        reportPayload,
        openStaticSummary,
        currentDate,
        overviewWindowDays,
      );
      resetSortableTables(overviewHost);
      document.documentElement.dataset.managementDataSource = dataSource.currentSource();
      document.getElementById("management-subtitle").textContent =
        overviewWindowDays === 1
          ? `Länder am ${formatDateLabel(currentDate)}.`
          : `${windowLabel(overviewWindowDays)} bis ${formatDateLabel(currentDate)}.`;
      hideStatus();
    }

    datePicker.addEventListener("change", () => {
      const nextValue = normalizeManagementDate(datePicker.value);
      if (nextValue && availableDates.includes(nextValue)) {
        loadOverview(nextValue).catch(renderError);
      }
    });
    prevDay.addEventListener("click", () => {
      const index = availableDates.indexOf(currentDate);
      if (index > 0) {
        loadOverview(availableDates[index - 1]).catch(renderError);
      }
    });
    nextDay.addEventListener("click", () => {
      const index = availableDates.indexOf(currentDate);
      if (index >= 0 && index < availableDates.length - 1) {
        loadOverview(availableDates[index + 1]).catch(renderError);
      }
    });
    windowSelect.addEventListener("change", () => {
      const nextWindow = Number(windowSelect.value);
      if (SUPPORTED_WINDOW_DAYS.includes(nextWindow)) {
        loadOverview(currentDate, nextWindow).catch(renderError);
      }
    });
    updateDateControls(datePicker, prevDay, nextDay, availableDates);
    await loadOverview(currentDate);
    return;
  }

  overviewHost.hidden = true;
  detailHost.hidden = false;
  const countryCode = countryEntry ? requestedCountry : "";
  const providerUid = requestedProvider;
  const countryDates = countryEntry
    ? availableDates.filter(
        (dateText) =>
          (!countryEntry.first_date || dateText >= countryEntry.first_date) &&
          (!countryEntry.last_date || dateText <= countryEntry.last_date),
      )
    : availableDates;
  if (!countryDates.includes(currentDate)) {
    currentDate = countryDates.at(-1) || availableDates.at(-1);
  }
  let currentWindowDays = SUPPORTED_WINDOW_DAYS.includes(Number(url.searchParams.get("window")))
    ? Number(url.searchParams.get("window"))
    : DEFAULT_WINDOW_DAYS;
  let currentSnapshot = null;
  let trendsPayload = { summary_series: [] };
  let overviewChart = null;
  let providerProfileChart = null;

  const title = document.getElementById("management-title");
  const subtitle = document.getElementById("management-subtitle");
  const kicker = document.getElementById("management-kicker");
  if (providerUid) {
    title.textContent = `Ladenetz ${providerUid}`;
    kicker.innerHTML = countryCode
      ? `<a href="./management.html">Managementübersicht</a> · <a href="${escapeAttribute(managementCountryUrl(countryCode, currentDate))}">${countryFlag(countryCode)} ${escapeHtml(countryName(countryCode))}</a> · <a href="./status.html">Datenstatus</a>`
      : '<a href="./management.html">Managementübersicht</a> · <a href="./status.html">Datenstatus</a>';
    document.title = `woladen.de | Anbieteranalyse ${providerUid}`;
  } else {
    title.textContent = `${countryFlag(countryCode)} Ladeinfrastruktur in ${countryName(countryCode)}`;
    kicker.innerHTML = '<a href="./management.html">Managementübersicht</a> · <a href="./status.html">Datenstatus</a>';
    document.title = `woladen.de | Managementanalyse ${countryName(countryCode)}`;
  }

  const datePicker = document.getElementById("management-date");
  const prevDay = document.getElementById("management-prev-day");
  const nextDay = document.getElementById("management-next-day");
  const overviewMetricSelect = document.getElementById("management-overview-metric");
  const overviewChartSection = document.getElementById("management-overview-chart-section");
  const windowSelect = document.getElementById("management-window-days");
  const providerProfilePanel = document.getElementById("management-provider-profile-panel");
  const detailBackLink = document.getElementById("management-detail-back");
  providerProfilePanel.hidden = !providerUid;
  const overviewMetricOptions = Object.entries(OVERVIEW_METRICS).map(([value, meta]) => ({
    value,
    label: meta.label,
  }));

  setSelectOptions(overviewMetricSelect, overviewMetricOptions, "stations_with_disruptions");
  windowSelect.value = String(currentWindowDays);
  await waitForChart();
  wireSortableTables(detailHost);

  function renderCharts() {
    if (overviewChart) {
      overviewChart.destroy();
      overviewChart = null;
    }
    const showOverviewChart =
      shouldShowOverviewChart(currentWindowDays) &&
      statusMetricsAreUsable(currentSnapshot?.summary || {});
    overviewChartSection.hidden = !showOverviewChart;
    if (!showOverviewChart) {
      return;
    }
    const selectedMetric =
      OVERVIEW_METRICS[overviewMetricSelect.value] || OVERVIEW_METRICS.stations_with_disruptions;
    document.getElementById("management-overview-title").textContent = selectedMetric.label;
    document.getElementById("management-overview-description").textContent =
      `${selectedMetric.description || ""} Zeitraum: ${windowLabel(currentWindowDays)}.`;
    overviewChart = createLineChart(
      "management-overview-chart",
      buildOverviewSeries(trendsPayload, overviewMetricSelect.value),
      { color: "#000000" },
    );
  }

  async function loadSnapshot(targetDate, windowDays = currentWindowDays) {
    showLoading(`${providerUid || countryName(countryCode)} wird geladen …`);
    const loaded = await dataSource.loadSnapshot(targetDate, {
      countryCode,
      providerUid,
      trendDays: windowDays,
    });
    const range = dateRangeForWindow(targetDate, windowDays);
    let countryReport = { rows: [] };
    let providerReport = { rows: [] };
    let providerHealth = { rows: [] };
    if (loaded.source === "postgresql") {
      const detailReportGroupBy = providerUid ? "provider" : "operator";
      const [countryResult, providerResult, healthResult] = await Promise.allSettled([
        !providerUid && countryCode
          ? dataSource.loadReport({
              startDate: range.startDate,
              endDate: range.endDate,
              countryCode,
              groupBy: "country",
            })
          : Promise.resolve({ rows: [] }),
        dataSource.loadReport({
          startDate: range.startDate,
          endDate: range.endDate,
          countryCode,
          providerUid,
          groupBy: detailReportGroupBy,
        }),
        providerUid
          ? dataSource.loadProviderHealth({
              startDate: range.startDate,
              endDate: range.endDate,
              countryCode,
              providerUid,
            })
          : Promise.resolve({ rows: [] }),
      ]);
      if (countryResult.status === "fulfilled") {
        countryReport = countryResult.value;
      } else {
        console.warn("Die Länderkennzahlen für den Berichtszeitraum sind nicht verfügbar.", countryResult.reason);
      }
      if (providerResult.status === "fulfilled") {
        providerReport = providerResult.value;
      } else {
        console.warn("Historische Anbieteraggregation ist nicht verfügbar.", providerResult.reason);
      }
      if (healthResult.status === "fulfilled") {
        providerHealth = healthResult.value;
      } else {
        console.warn("Historische Anbieterqualität ist nicht verfügbar.", healthResult.reason);
      }
    }
    currentSnapshot =
      loaded.source === "postgresql" && !providerUid
        ? mergeRollingCountrySummary(loaded.snapshot, countryReport, countryCode)
        : loaded.snapshot;
    trendsPayload = loaded.trends;
    currentWindowDays = windowDays;
    document.documentElement.dataset.managementDataSource = loaded.source;
    currentDate = targetDate;
    syncUrl({ countryCode, providerUid, windowDays: currentWindowDays });
    if (providerUid && countryCode) {
      detailBackLink.href = managementCountryUrl(countryCode, currentDate, currentWindowDays);
      detailBackLink.textContent = `← Zu ${countryName(countryCode)}`;
    } else {
      detailBackLink.href = managementOverviewUrl(currentDate, currentWindowDays);
      detailBackLink.textContent = "← Zur Länderübersicht";
    }
    updateDateControls(datePicker, prevDay, nextDay, countryDates);
    windowSelect.value = String(currentWindowDays);
    renderKpis(currentSnapshot, {
      ...(providerUid
        ? { publicStationCount: null, publicChargerCount: null }
        : staticCatalogCountsForCountry(openStaticSummary, countryCode)),
    });
    renderDataQuality(currentSnapshot, currentWindowDays);
    renderBrokenStations(currentSnapshot, currentWindowDays);
    renderBusyStations(currentSnapshot, currentWindowDays);
    renderProviderReports(currentSnapshot, {
      countryCode,
      dateText: currentDate,
      linkProviders: !providerUid && loaded.source === "postgresql",
    });
    // The raw interval profile is intentionally bounded. Rolling report and
    // reliability tables use the selected day/week window, while the
    // expensive hourly chart stays responsive on the live database path.
    const profileWindowDays = Math.min(currentWindowDays, 7);
    const profileRange = dateRangeForWindow(currentDate, profileWindowDays);
    if (providerUid && providerProfileChart) {
      providerProfileChart.destroy();
      providerProfileChart = null;
    }
    const providerProfilePromise =
      loaded.source === "postgresql" && providerUid
        ? dataSource.loadProfile({
            startDate: profileRange.startDate,
            endDate: profileRange.endDate,
            countryCode,
            providerUid,
            groupBy: "hour",
          }).then(
            (payload) => ({ payload, error: null }),
            (error) => ({ payload: { rows: [] }, error }),
          )
        : null;
    renderRollingProviderReports(providerReport, providerHealth, {
      countryCode,
      dateText: currentDate,
      linkProviders: !providerUid && providerReport?.group_by === "provider",
    });
    resetSortableTables(detailHost);
    renderCharts();
    if (providerUid) {
      const providerRow = currentSnapshot.provider_reports?.[0] || providerReport.rows?.[0] || {};
      const providerName = providerRow.display_name || providerRow.provider_uid || providerUid;
      const scope = countryCode ? ` in ${countryName(countryCode)}` : "";
      title.textContent = `Ladenetz ${providerName}${scope}`;
      document.title = `woladen.de | Anbieteranalyse ${providerName}`;
    }

    subtitle.textContent = providerUid
      ? `Auslastung und Störungen am ${formatDateLabel(currentDate)}.`
      : `${buildManagementSubtitle(currentDate, countryCode)}.`;
    hideStatus();
    if (providerProfilePromise) {
      const profileDescription = document.getElementById("management-provider-profile-description");
      profileDescription.textContent = "Das stündliche Anbieterprofil wird geladen …";
      const profileResult = await providerProfilePromise;
      if (!profileResult.error) {
        if (providerProfileChart) {
          providerProfileChart.destroy();
        }
        providerProfileChart = createProviderProfileChart(profileResult.payload);
        profileDescription.textContent =
          `Auslastung und Störungsanteil je Ortszeitstunde über ${windowLabel(profileWindowDays)}.`;
      } else {
        console.warn("Das stündliche Anbieterprofil ist nicht verfügbar.", profileResult.error);
        profileDescription.textContent = "Für diesen Zeitraum ist kein stündliches Anbieterprofil verfügbar.";
      }
    }
  }

  datePicker.addEventListener("change", () => {
    const nextValue = normalizeManagementDate(datePicker.value);
    if (nextValue && countryDates.includes(nextValue)) {
      loadSnapshot(nextValue).catch(renderError);
    }
  });
  prevDay.addEventListener("click", () => {
    const index = countryDates.indexOf(currentDate);
    if (index > 0) {
      loadSnapshot(countryDates[index - 1]).catch(renderError);
    }
  });
  nextDay.addEventListener("click", () => {
    const index = countryDates.indexOf(currentDate);
    if (index >= 0 && index < countryDates.length - 1) {
      loadSnapshot(countryDates[index + 1]).catch(renderError);
    }
  });
  overviewMetricSelect.addEventListener("change", renderCharts);
  windowSelect.addEventListener("change", () => {
    const nextWindow = Number(windowSelect.value);
    if (SUPPORTED_WINDOW_DAYS.includes(nextWindow)) {
      loadSnapshot(currentDate, nextWindow).catch(renderError);
    }
  });

  updateDateControls(datePicker, prevDay, nextDay, countryDates);
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
