const LONG_RUNNING_MESSAGES = Object.freeze([
  {
    delayMs: 8_000,
    text: "Die 37 AFIR-Felder werden für diese Ebene zusammengefasst.",
  },
  {
    delayMs: 30_000,
    text: "Die Berechnung läuft weiter. Ein kalter Start kann bis zu einer Minute benötigen.",
  },
]);

function requiredElement(value, name) {
  if (!value) {
    throw new Error(`AFIR loading indicator is missing ${name}.`);
  }
  return value;
}

export function createAfirLoadingIndicator({
  root,
  progress,
  label,
  detail,
  busyRegion,
  schedule = globalThis.setTimeout,
  cancelSchedule = globalThis.clearTimeout,
  successVisibleMs = 650,
} = {}) {
  const rootElement = requiredElement(root, "root");
  const progressElement = requiredElement(progress, "progress");
  const labelElement = requiredElement(label, "label");
  const detailElement = requiredElement(detail, "detail");
  const busyElement = requiredElement(busyRegion, "busy region");
  let activeToken = 0;
  let timers = [];

  function clearTimers() {
    timers.forEach((timer) => cancelSchedule(timer));
    timers = [];
  }

  function setBusy(value) {
    busyElement.setAttribute("aria-busy", value ? "true" : "false");
  }

  function setAnnouncement(labelText, detailText) {
    labelElement.textContent = labelText;
    detailElement.textContent = detailText;
    progressElement.setAttribute(
      "aria-valuetext",
      `${labelText} ${detailText}`.trim(),
    );
  }

  function setDeterminateProgress(value) {
    const normalized = Math.max(0, Math.min(100, Number(value) || 0));
    rootElement.style.setProperty("--afir-progress", `${normalized}%`);
    progressElement.setAttribute("aria-valuenow", String(normalized));
  }

  function isActive(token) {
    return token === activeToken;
  }

  function start({
    labelText = "Live-Daten werden geladen …",
    detailText = "Verbindung zum Live-Dienst wird hergestellt.",
  } = {}) {
    activeToken += 1;
    const token = activeToken;
    clearTimers();
    rootElement.hidden = false;
    rootElement.dataset.state = "loading";
    rootElement.dataset.mode = "indeterminate";
    rootElement.style.setProperty("--afir-progress", "0%");
    progressElement.removeAttribute("aria-invalid");
    progressElement.removeAttribute("aria-valuenow");
    setAnnouncement(labelText, detailText);
    setBusy(true);

    for (const message of LONG_RUNNING_MESSAGES) {
      timers.push(
        schedule(() => {
          if (!isActive(token) || rootElement.dataset.state !== "loading") {
            return;
          }
          setAnnouncement(labelElement.textContent, message.text);
        }, message.delayMs),
      );
    }
    return token;
  }

  function received(
    token,
    {
      labelText = "Live-Daten sind eingetroffen.",
      detailText = "Die Ansicht wird aufgebaut.",
    } = {},
  ) {
    if (!isActive(token)) return false;
    clearTimers();
    rootElement.dataset.state = "loading";
    rootElement.dataset.mode = "determinate";
    setDeterminateProgress(85);
    setAnnouncement(labelText, detailText);
    return true;
  }

  function succeed(
    token,
    {
      labelText = "Ansicht ist bereit.",
      detailText = "Die aktuellen AFIR-Werte werden angezeigt.",
    } = {},
  ) {
    if (!isActive(token)) return false;
    clearTimers();
    rootElement.dataset.state = "success";
    rootElement.dataset.mode = "determinate";
    setDeterminateProgress(100);
    setAnnouncement(labelText, detailText);
    setBusy(false);
    timers.push(
      schedule(() => {
        if (isActive(token) && rootElement.dataset.state === "success") {
          rootElement.hidden = true;
        }
      }, successVisibleMs),
    );
    return true;
  }

  function fail(
    token,
    {
      labelText = "Live-Daten konnten nicht geladen werden.",
      detailText = "Die Fehlermeldung bleibt unterhalb sichtbar.",
    } = {},
  ) {
    if (!isActive(token)) return false;
    clearTimers();
    rootElement.hidden = false;
    rootElement.dataset.state = "error";
    rootElement.dataset.mode = "error";
    rootElement.style.setProperty("--afir-progress", "100%");
    progressElement.removeAttribute("aria-valuenow");
    progressElement.setAttribute("aria-invalid", "true");
    setAnnouncement(labelText, detailText);
    setBusy(false);
    return true;
  }

  return Object.freeze({
    start,
    received,
    succeed,
    fail,
  });
}
