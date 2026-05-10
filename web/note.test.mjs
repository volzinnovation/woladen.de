import test from "node:test";
import assert from "node:assert/strict";

import {
  getUserNote,
  normalizeNote,
  parseStoredNotes,
  serializeStoredNotes,
} from "./note.mjs";

test("normalizes personal station notes", () => {
  assert.equal(normalizeNote("  gut erreichbar\r\nnachts hell  "), "gut erreichbar\nnachts hell");
  assert.equal(normalizeNote(null), "");
  assert.equal(normalizeNote("x".repeat(700)).length, 600);
});

test("parses and serializes stored station notes", () => {
  const notes = parseStoredNotes(JSON.stringify({
    "station-b": " zweite Notiz ",
    "station-a": "erste Notiz",
    "station-empty": "   ",
  }));

  assert.equal(getUserNote(notes, "station-a"), "erste Notiz");
  assert.equal(getUserNote(notes, "station-b"), "zweite Notiz");
  assert.equal(getUserNote(notes, "station-empty"), "");
  assert.equal(
    serializeStoredNotes(notes),
    JSON.stringify({ "station-a": "erste Notiz", "station-b": "zweite Notiz" }),
  );
});
