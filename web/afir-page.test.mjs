import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const html = await readFile(
  new URL("./afir.html", import.meta.url),
  "utf8",
);

test("AFIR page states the complete approved F3 rule", () => {
  assert.match(
    html,
    /vollständigen, eindeutigen Basistarif/,
  );
  assert.match(
    html,
    /Eine Preisänderung ist nicht erforderlich/,
  );
  assert.match(
    html,
    /mehrere konkurrierende Basistarife/,
  );
  assert.match(
    html,
    /eine sachfremde Preisspanne erfüllen F3 nicht/,
  );
});
