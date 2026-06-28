import assert from "node:assert/strict";
import test from "node:test";

import { formatDetailTimestamp } from "./time-format.mjs";

const FORMATTED_DETAIL_TIMESTAMP_RE = /^\d{2}\.\d{2}\.\d{4},? \d{2}:\d{2}$/;

test("formatDetailTimestamp formats Unix millisecond timestamps", () => {
  const formatted = formatDetailTimestamp(1777543086728);

  assert.match(formatted, FORMATTED_DETAIL_TIMESTAMP_RE);
  assert.notEqual(formatted, "1777543086728");
});

test("formatDetailTimestamp formats numeric timestamp strings", () => {
  const formattedFromString = formatDetailTimestamp("1777543086728");
  const formattedFromNumber = formatDetailTimestamp(1777543086728);

  assert.equal(formattedFromString, formattedFromNumber);
});

test("formatDetailTimestamp treats Unix second timestamps as seconds", () => {
  assert.equal(formatDetailTimestamp(1777543086), formatDetailTimestamp(1777543086000));
});

test("formatDetailTimestamp preserves non-timestamp scalar text", () => {
  assert.equal(formatDetailTimestamp("4"), "4");
  assert.equal(formatDetailTimestamp("unknown"), "unknown");
});
