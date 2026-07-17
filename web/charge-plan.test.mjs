import test from "node:test";
import assert from "node:assert/strict";

import {
  calculateChargeNeed,
  classifyChargePlanFit,
  estimateChargeMinutes,
  normalizeChargePlanSettings,
  parseStoredChargePlan,
  serializeStoredChargePlan,
} from "./charge-plan.mjs";

test("charging need is derived from battery state and consumption", () => {
  assert.deepEqual(calculateChargeNeed({ batteryKwh: 80, chargeFromPercent: 10, chargeToPercent: 90, consumptionKwhPer100Km: 20 }), { targetKwh: 64, addedRangeKm: 320 });
  assert.equal(estimateChargeMinutes(150, { batteryKwh: 80, chargeFromPercent: 10, chargeToPercent: 90, averageChargingKw: 100, efficiency: 0.8 }), 48);
});

test("charge plan settings are clamped to useful mobile controls", () => {
  const settings = normalizeChargePlanSettings({ batteryKwh: 500, consumptionKwhPer100Km: 2, chargeFromPercent: 95, chargeToPercent: 5, averageChargingKw: 999 });
  assert.equal(settings.batteryKwh, 200);
  assert.equal(settings.consumptionKwhPer100Km, 5);
  assert.equal(settings.chargeFromPercent, 90);
  assert.equal(settings.chargeToPercent, 90);
  assert.equal(settings.averageChargingKw, 400);
});

test("fast chargers are classified as strong fits for the target break", () => {
  assert.equal(
    classifyChargePlanFit({ maxPowerKw: 150, averageChargingKw: 150 }).tier,
    "great",
  );
  assert.equal(
    classifyChargePlanFit({ maxPowerKw: 100, averageChargingKw: 150 }).tier,
    "good",
  );
});

test("lower-power stops are classified as partial or slow", () => {
  assert.equal(
    classifyChargePlanFit({ maxPowerKw: 80, averageChargingKw: 150 }).tier,
    "partial",
  );
  assert.equal(
    classifyChargePlanFit({ maxPowerKw: 40, averageChargingKw: 150 }).tier,
    "slow",
  );
});

test("settings round-trip through local storage payload", () => {
  const stored = serializeStoredChargePlan({ batteryKwh: 82, chargeFromPercent: 20, chargeToPercent: 90 });
  assert.equal(parseStoredChargePlan(stored).batteryKwh, 82);
  assert.equal(parseStoredChargePlan(stored).chargeToPercent, 90);
});

test("live occupied and unavailable states override energy fit labels", () => {
  assert.equal(
    classifyChargePlanFit({
      maxPowerKw: 300,
      minutes: 30,
      targetKwh: 35,
      availabilityStatus: "occupied",
      totalEvses: 4,
      availableEvses: 0,
    }).tier,
    "busy",
  );
  assert.equal(
    classifyChargePlanFit({
      maxPowerKw: 300,
      minutes: 30,
      targetKwh: 35,
      availabilityStatus: "out_of_order",
      totalEvses: 4,
      availableEvses: 0,
    }).tier,
    "unavailable",
  );
});
