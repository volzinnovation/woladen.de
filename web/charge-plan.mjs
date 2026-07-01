export const CHARGE_PLAN_DEFAULTS = Object.freeze({
  minutes: 30,
  targetKwh: 35,
  efficiency: 0.72,
});

const MINUTES_MIN = 5;
const MINUTES_MAX = 90;
const TARGET_KWH_MIN = 5;
const TARGET_KWH_MAX = 120;
const EFFICIENCY_MIN = 0.1;
const EFFICIENCY_MAX = 1;

function finiteNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function normalizePositiveInteger(value, fallback, min, max) {
  return Math.round(clamp(finiteNumber(value, fallback), min, max));
}

export function normalizeChargePlanSettings(source = {}) {
  return {
    minutes: normalizePositiveInteger(
      source.minutes,
      CHARGE_PLAN_DEFAULTS.minutes,
      MINUTES_MIN,
      MINUTES_MAX,
    ),
    targetKwh: normalizePositiveInteger(
      source.targetKwh,
      CHARGE_PLAN_DEFAULTS.targetKwh,
      TARGET_KWH_MIN,
      TARGET_KWH_MAX,
    ),
    efficiency: clamp(
      finiteNumber(source.efficiency, CHARGE_PLAN_DEFAULTS.efficiency),
      EFFICIENCY_MIN,
      EFFICIENCY_MAX,
    ),
  };
}

export function estimateSessionKwh(maxPowerKw, source = {}) {
  const settings = normalizeChargePlanSettings(source);
  const powerKw = Math.max(0, finiteNumber(maxPowerKw, 0));
  if (powerKw <= 0) {
    return 0;
  }
  return Math.round(powerKw * (settings.minutes / 60) * settings.efficiency * 10) / 10;
}

export function classifyChargePlanFit(source = {}) {
  const settings = normalizeChargePlanSettings(source);
  const estimatedKwh = estimateSessionKwh(source.maxPowerKw, settings);
  const targetKwh = settings.targetKwh;
  const ratio = targetKwh > 0 ? estimatedKwh / targetKwh : 0;
  const availableEvses = Math.max(0, finiteNumber(source.availableEvses, 0));
  const totalEvses = Math.max(0, finiteNumber(source.totalEvses, 0));
  const availabilityStatus = String(source.availabilityStatus || "").trim();

  if (estimatedKwh <= 0) {
    return { tier: "unknown", estimatedKwh: 0, ratio: 0 };
  }
  if (availabilityStatus === "out_of_order") {
    return { tier: "unavailable", estimatedKwh, ratio };
  }
  if (totalEvses > 0 && availableEvses <= 0 && availabilityStatus === "occupied") {
    return { tier: "busy", estimatedKwh, ratio };
  }
  if (ratio >= 1.15) {
    return { tier: "great", estimatedKwh, ratio };
  }
  if (ratio >= 0.95) {
    return { tier: "good", estimatedKwh, ratio };
  }
  if (ratio >= 0.65) {
    return { tier: "partial", estimatedKwh, ratio };
  }
  return { tier: "slow", estimatedKwh, ratio };
}
