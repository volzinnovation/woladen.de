import test from "node:test";
import assert from "node:assert/strict";

import {
  classifyChargePlanFit,
  estimateSessionKwh,
  normalizeChargePlanSettings,
} from "./charge-plan.mjs";

test("session estimate applies realistic stop duration and taper efficiency", () => {
  assert.equal(estimateSessionKwh(150, { minutes: 30, efficiency: 0.72 }), 54);
  assert.equal(estimateSessionKwh(0, { minutes: 30 }), 0);
});

test("charge plan settings are clamped to useful mobile controls", () => {
  assert.deepEqual(
    normalizeChargePlanSettings({ minutes: 2, targetKwh: 500, efficiency: 1.8 }),
    { minutes: 5, targetKwh: 120, efficiency: 1 },
  );
  assert.deepEqual(
    normalizeChargePlanSettings({ minutes: 95, targetKwh: 2, efficiency: -1 }),
    { minutes: 90, targetKwh: 5, efficiency: 0.1 },
  );
});

test("fast chargers are classified as strong fits for the target break", () => {
  assert.equal(
    classifyChargePlanFit({ maxPowerKw: 150, minutes: 30, targetKwh: 35 }).tier,
    "great",
  );
  assert.equal(
    classifyChargePlanFit({ maxPowerKw: 100, minutes: 30, targetKwh: 35 }).tier,
    "good",
  );
});

test("lower-power stops are classified as partial or slow", () => {
  assert.equal(
    classifyChargePlanFit({ maxPowerKw: 80, minutes: 25, targetKwh: 35 }).tier,
    "partial",
  );
  assert.equal(
    classifyChargePlanFit({ maxPowerKw: 50, minutes: 20, targetKwh: 45 }).tier,
    "slow",
  );
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
