const DETAIL_TIMESTAMP_FORMATTER = new Intl.DateTimeFormat("de-DE", {
  day: "2-digit",
  month: "2-digit",
  year: "numeric",
  hour: "2-digit",
  minute: "2-digit",
});

function dateFromEpochNumber(value) {
  if (!Number.isFinite(value)) {
    return null;
  }
  const absolute = Math.abs(value);
  if (absolute >= 1_000_000_000_000) {
    return new Date(value);
  }
  if (absolute >= 1_000_000_000) {
    return new Date(value * 1000);
  }
  return null;
}

function dateFromTimestampValue(value) {
  if (typeof value === "number") {
    return dateFromEpochNumber(value);
  }

  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }
  if (/^-?\d+(?:\.\d+)?$/.test(raw)) {
    return dateFromEpochNumber(Number(raw));
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

export function formatDetailTimestamp(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }

  const date = dateFromTimestampValue(value);
  if (!date || Number.isNaN(date.getTime())) {
    return raw;
  }
  return DETAIL_TIMESTAMP_FORMATTER.format(date);
}
