const LICENSE_DATE_RE = /_(\d{4}-\d{2}-\d{2})$/;

function normalizeCode(value) {
  return String(value || "").trim().toUpperCase();
}

function normalizeSourceUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function sortMappedCountries(left, right) {
  const nameCompare = (left.name || left.code).localeCompare(
    right.name || right.code,
    "de",
  );
  return nameCompare || left.code.localeCompare(right.code, "de");
}

function sortBundleSources(left, right) {
  const leftKey = `${left.countryCode || "ZZ"}:${left.displayName || left.sourceUid}`;
  const rightKey = `${right.countryCode || "ZZ"}:${right.displayName || right.sourceUid}`;
  return leftKey.localeCompare(rightKey, "de");
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function stripLeadingCountryCode(label, countryCode) {
  if (!label || !countryCode) {
    return label;
  }
  const duplicatePrefix = new RegExp(
    `^${escapeRegExp(countryCode)}(?:\\s*:\\s*|\\s+)`,
    "i",
  );
  return label.replace(duplicatePrefix, "").trim();
}

export function normalizeMappedCountries(summaryData) {
  const countries = Array.isArray(summaryData)
    ? summaryData
    : Array.isArray(summaryData?.countries)
      ? summaryData.countries
      : [];
  return countries
    .map((country) => ({
      code: normalizeCode(country?.code ?? country?.country_code),
      name: String(country?.name || country?.country_name || "").trim(),
      stationCount: Number(
        country?.station_count ?? country?.stationCount ?? country?.stations ?? 0,
      ) || 0,
      chargerCount: Number(
        country?.charger_count ?? country?.chargerCount ?? country?.chargers ?? 0,
      ) || 0,
      fastStationCount: Number(
        country?.fast_station_count ??
          country?.fastStationCount ??
          country?.fast_stations ??
          0,
      ) || 0,
    }))
    .filter((country) => country.code)
    .sort(sortMappedCountries);
}

export function normalizeBundleSources(summaryData) {
  const sources = Array.isArray(summaryData)
    ? summaryData
    : Array.isArray(summaryData?.sources)
      ? summaryData.sources
      : [];
  const seen = new Set();
  return sources
    .map((source) => ({
      countryCode: normalizeCode(source?.country_code ?? source?.countryCode),
      sourceUid: String(source?.source_uid ?? source?.sourceUid ?? "").trim(),
      displayName: String(
        source?.display_name ??
          source?.displayName ??
          source?.source_name ??
          source?.sourceName ??
          source?.source_uid ??
          "",
      ).trim(),
      sourceUrl: normalizeSourceUrl(
        source?.source_url ?? source?.sourceUrl ?? source?.url,
      ),
      license: String(source?.license ?? "").trim(),
      licenseUrl: String(source?.license_url ?? source?.licenseUrl ?? "").trim(),
    }))
    .filter((source) => source.countryCode || source.displayName || source.sourceUrl)
    .filter((source) => {
      const key = [
        source.countryCode,
        source.sourceUid,
        source.sourceUrl,
        source.displayName,
      ].join("\u0001");
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    })
    .sort(sortBundleSources);
}

export function formatBundleSourceTitle(source) {
  const countryCode = normalizeCode(source?.countryCode ?? source?.country_code);
  const rawLabel = String(
    source?.displayName ??
      source?.display_name ??
      source?.sourceUid ??
      source?.source_uid ??
      source?.sourceUrl ??
      source?.source_url ??
      source?.url ??
      "Datenquelle",
  ).trim() || "Datenquelle";
  const label = stripLeadingCountryCode(rawLabel, countryCode) || rawLabel;
  return countryCode ? `${countryCode}: ${label}` : label;
}

export function formatLicenseStatus(value) {
  const license = String(value || "").trim();
  if (!license) {
    return "";
  }
  if (license === "ODbL-1.0") {
    return "ODbL 1.0";
  }
  const dateMatch = license.match(LICENSE_DATE_RE);
  const suffix = dateMatch ? ` (${dateMatch[1]})` : "";
  if (license.startsWith("human_terms_review_approved_for_open_static_bundle")) {
    return `Human Review für Open-Static-Bundle freigegeben${suffix}`;
  }
  if (license.startsWith("source_terms_pending_human_publication_review")) {
    return `Quellbedingungen vor Veröffentlichung im Human Review${suffix}`;
  }
  return license.replaceAll("_", " ");
}
