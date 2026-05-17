const MAX_NOTE_LENGTH = 600;

export function normalizeNote(value) {
  const normalized = String(value || "")
    .replace(/\r\n?/g, "\n")
    .trim();
  return normalized.slice(0, MAX_NOTE_LENGTH);
}

export function parseStoredNotes(raw) {
  const notes = new Map();
  if (!raw) {
    return notes;
  }

  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    return notes;
  }

  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return notes;
  }

  Object.entries(payload).forEach(([stationId, value]) => {
    const id = String(stationId || "").trim();
    const note = normalizeNote(value);
    if (id && note) {
      notes.set(id, note);
    }
  });
  return notes;
}

export function serializeStoredNotes(notes) {
  const entries = Array.from(notes instanceof Map ? notes.entries() : [])
    .map(([stationId, value]) => [String(stationId || "").trim(), normalizeNote(value)])
    .filter(([stationId, note]) => stationId && note)
    .sort(([left], [right]) => left.localeCompare(right));

  return JSON.stringify(Object.fromEntries(entries));
}

export function getUserNote(notes, stationId) {
  const id = String(stationId || "").trim();
  if (!id || !(notes instanceof Map)) {
    return "";
  }
  return normalizeNote(notes.get(id));
}
