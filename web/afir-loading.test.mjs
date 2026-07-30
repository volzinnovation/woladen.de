import assert from "node:assert/strict";
import test from "node:test";

import { createAfirLoadingIndicator } from "./afir-loading.mjs";

function fakeElement({ hidden = false } = {}) {
  const attributes = new Map();
  const properties = new Map();
  return {
    hidden,
    dataset: {},
    textContent: "",
    style: {
      setProperty(name, value) {
        properties.set(name, value);
      },
      getPropertyValue(name) {
        return properties.get(name) || "";
      },
    },
    setAttribute(name, value) {
      attributes.set(name, String(value));
    },
    removeAttribute(name) {
      attributes.delete(name);
    },
    getAttribute(name) {
      return attributes.get(name) ?? null;
    },
    hasAttribute(name) {
      return attributes.has(name);
    },
  };
}

function fakeClock() {
  let nextId = 1;
  const tasks = new Map();
  return {
    schedule(callback, delayMs) {
      const id = nextId;
      nextId += 1;
      tasks.set(id, { callback, delayMs });
      return id;
    },
    cancel(id) {
      tasks.delete(id);
    },
    runNext() {
      const next = [...tasks.entries()].sort(
        ([leftId, left], [rightId, right]) =>
          left.delayMs - right.delayMs || leftId - rightId,
      )[0];
      if (!next) return false;
      tasks.delete(next[0]);
      next[1].callback();
      return true;
    },
    get size() {
      return tasks.size;
    },
  };
}

function fixture() {
  const clock = fakeClock();
  const elements = {
    root: fakeElement({ hidden: true }),
    progress: fakeElement(),
    label: fakeElement(),
    detail: fakeElement(),
    busyRegion: fakeElement(),
  };
  return {
    clock,
    elements,
    indicator: createAfirLoadingIndicator({
      ...elements,
      schedule: clock.schedule,
      cancelSchedule: clock.cancel,
      successVisibleMs: 1,
    }),
  };
}

test("AFIR loading indicator announces indeterminate, received, and ready states", () => {
  const { clock, elements, indicator } = fixture();
  const token = indicator.start({
    labelText: "Länder werden geladen …",
    detailText: "Verbindung wird hergestellt.",
  });

  assert.equal(elements.root.hidden, false);
  assert.equal(elements.root.dataset.state, "loading");
  assert.equal(elements.root.dataset.mode, "indeterminate");
  assert.equal(elements.progress.hasAttribute("aria-valuenow"), false);
  assert.match(
    elements.progress.getAttribute("aria-valuetext"),
    /Länder werden geladen/,
  );
  assert.equal(elements.busyRegion.getAttribute("aria-busy"), "true");

  clock.runNext();
  assert.match(elements.detail.textContent, /37 AFIR-Felder/);

  assert.equal(indicator.received(token), true);
  assert.equal(elements.root.dataset.mode, "determinate");
  assert.equal(elements.progress.getAttribute("aria-valuenow"), "85");

  assert.equal(indicator.succeed(token), true);
  assert.equal(elements.progress.getAttribute("aria-valuenow"), "100");
  assert.equal(elements.busyRegion.getAttribute("aria-busy"), "false");
  assert.equal(elements.root.hidden, false);
  assert.equal(clock.size, 1);

  clock.runNext();
  assert.equal(elements.root.hidden, true);
});

test("AFIR loading indicator keeps its error state visible", () => {
  const { clock, elements, indicator } = fixture();
  const token = indicator.start();

  assert.equal(
    indicator.fail(token, {
      detailText: "Die eigentliche Fehlermeldung bleibt sichtbar.",
    }),
    true,
  );
  assert.equal(elements.root.hidden, false);
  assert.equal(elements.root.dataset.state, "error");
  assert.equal(elements.root.dataset.mode, "error");
  assert.equal(elements.progress.hasAttribute("aria-valuenow"), false);
  assert.equal(elements.progress.getAttribute("aria-invalid"), "true");
  assert.equal(elements.busyRegion.getAttribute("aria-busy"), "false");
  assert.equal(clock.size, 0);
});

test("stale AFIR loads cannot hide or overwrite a newer request", () => {
  const { elements, indicator } = fixture();
  const staleToken = indicator.start({ labelText: "Länder" });
  const activeToken = indicator.start({ labelText: "Anbieter" });

  assert.equal(indicator.succeed(staleToken), false);
  assert.equal(indicator.fail(staleToken), false);
  assert.equal(elements.label.textContent, "Anbieter");
  assert.equal(elements.root.hidden, false);
  assert.equal(indicator.received(activeToken), true);
});
