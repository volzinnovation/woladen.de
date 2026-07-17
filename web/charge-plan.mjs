export const CHARGE_PLAN_DEFAULTS = Object.freeze({
  batteryKwh: 75,
  consumptionKwhPer100Km: 18,
  chargeFromPercent: 10,
  chargeToPercent: 80,
  averageChargingKw: 120,
  efficiency: 0.9,
});

function finiteNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function rounded(value, digits = 0) {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

export function normalizeChargePlanSettings(source = {}) {
  const chargeFromPercent = Math.round(clamp(finiteNumber(source.chargeFromPercent, 10), 10, 90));
  const chargeToPercent = Math.round(clamp(
    finiteNumber(source.chargeToPercent, CHARGE_PLAN_DEFAULTS.chargeToPercent),
    chargeFromPercent,
    90,
  ));
  return {
    batteryKwh: rounded(clamp(finiteNumber(source.batteryKwh, 75), 10, 200), 1),
    consumptionKwhPer100Km: rounded(clamp(finiteNumber(source.consumptionKwhPer100Km, 18), 5, 50), 1),
    chargeFromPercent,
    chargeToPercent,
    averageChargingKw: Math.round(clamp(finiteNumber(source.averageChargingKw, 120), 3, 400)),
    efficiency: clamp(finiteNumber(source.efficiency, 0.9), 0.1, 1),
  };
}

export function calculateChargeNeed(source = {}) {
  const settings = normalizeChargePlanSettings(source);
  const targetKwh = rounded(
    settings.batteryKwh * (settings.chargeToPercent - settings.chargeFromPercent) / 100,
    1,
  );
  const addedRangeKm = Math.round(targetKwh / settings.consumptionKwhPer100Km * 100);
  return { targetKwh, addedRangeKm };
}

export function estimateChargeMinutes(maxPowerKw, source = {}) {
  const settings = normalizeChargePlanSettings(source);
  const { targetKwh } = calculateChargeNeed(settings);
  const effectivePowerKw = Math.min(
    Math.max(0, finiteNumber(maxPowerKw, 0)),
    settings.averageChargingKw,
  );
  if (effectivePowerKw <= 0 || targetKwh <= 0) return 0;
  return Math.ceil(targetKwh / (effectivePowerKw * settings.efficiency) * 60);
}

export function classifyChargePlanFit(source = {}) {
  const settings = normalizeChargePlanSettings(source);
  const { targetKwh, addedRangeKm } = calculateChargeNeed(settings);
  const maxPowerKw = Math.max(0, finiteNumber(source.maxPowerKw, 0));
  const estimatedMinutes = estimateChargeMinutes(maxPowerKw, settings);
  const ratio = settings.averageChargingKw > 0
    ? Math.min(maxPowerKw, settings.averageChargingKw) / settings.averageChargingKw
    : 0;
  const availableEvses = Math.max(0, finiteNumber(source.availableEvses, 0));
  const totalEvses = Math.max(0, finiteNumber(source.totalEvses, 0));
  const availabilityStatus = String(source.availabilityStatus || "").trim();
  const result = { estimatedKwh: targetKwh, estimatedMinutes, addedRangeKm, ratio };

  if (estimatedMinutes <= 0) return { ...result, tier: "unknown" };
  if (availabilityStatus === "out_of_order") return { ...result, tier: "unavailable" };
  if (totalEvses > 0 && availableEvses <= 0 && availabilityStatus === "occupied") {
    return { ...result, tier: "busy" };
  }
  if (ratio >= 0.9) return { ...result, tier: "great" };
  if (ratio >= 0.65) return { ...result, tier: "good" };
  if (ratio >= 0.35) return { ...result, tier: "partial" };
  return { ...result, tier: "slow" };
}

export function parseStoredChargePlan(raw) {
  try {
    return normalizeChargePlanSettings(JSON.parse(String(raw || "{}")));
  } catch {
    return normalizeChargePlanSettings();
  }
}

export function serializeStoredChargePlan(settings) {
  return JSON.stringify(normalizeChargePlanSettings(settings));
}
