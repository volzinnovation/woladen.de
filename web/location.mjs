export const LOCATION_PERMISSION_UNKNOWN = "unknown";
export const LOCATION_PERMISSION_GRANTED = "granted";
export const LOCATION_PERMISSION_PROMPT = "prompt";
export const LOCATION_PERMISSION_DENIED = "denied";
export const LOCATION_PERMISSION_UNSUPPORTED = "unsupported";

export const LOCATION_REQUEST_IDLE = "idle";
export const LOCATION_REQUEST_PENDING = "pending";
export const LOCATION_REQUEST_ERROR = "error";
export const LOCATION_REQUEST_READY = "ready";

export const LOCATION_ERROR_PERMISSION_DENIED = "permission_denied";
export const LOCATION_ERROR_POSITION_UNAVAILABLE = "position_unavailable";
export const LOCATION_ERROR_TIMEOUT = "timeout";
export const LOCATION_ERROR_UNSUPPORTED = "unsupported";
export const LOCATION_ERROR_UNKNOWN = "unknown";

export function normalizeLocationPermissionState(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === LOCATION_PERMISSION_GRANTED) {
    return LOCATION_PERMISSION_GRANTED;
  }
  if (raw === LOCATION_PERMISSION_PROMPT) {
    return LOCATION_PERMISSION_PROMPT;
  }
  if (raw === LOCATION_PERMISSION_DENIED) {
    return LOCATION_PERMISSION_DENIED;
  }
  if (raw === LOCATION_PERMISSION_UNSUPPORTED) {
    return LOCATION_PERMISSION_UNSUPPORTED;
  }
  return LOCATION_PERMISSION_UNKNOWN;
}

export function mapGeolocationError(error) {
  const code = Number(error?.code);
  if (code === 1) {
    return {
      code: LOCATION_ERROR_PERMISSION_DENIED,
      title: "Standortfreigabe benötigt",
      message: "Aktiviere den Standortzugriff, damit die Liste nahe Ladepunkte laden kann.",
    };
  }
  if (code === 2) {
    return {
      code: LOCATION_ERROR_POSITION_UNAVAILABLE,
      title: "Standort konnte nicht ermittelt werden",
      message: "Bitte prüfe, ob dein Gerät gerade einen Standort liefern kann, und versuche es erneut.",
    };
  }
  if (code === 3) {
    return {
      code: LOCATION_ERROR_TIMEOUT,
      title: "Standortsuche dauert zu lange",
      message: "Bitte versuche es erneut oder prüfe, ob der Standortzugriff aktiviert ist.",
    };
  }
  if (String(error?.code || "").trim() === LOCATION_ERROR_UNSUPPORTED) {
    return {
      code: LOCATION_ERROR_UNSUPPORTED,
      title: "Standort nicht verfügbar",
      message: "Dieser Browser unterstützt keine Standortabfrage.",
    };
  }
  return {
    code: LOCATION_ERROR_UNKNOWN,
    title: "Standort konnte nicht ermittelt werden",
    message: "Bitte versuche es erneut.",
  };
}

export function requestBrowserLocation(geolocation, options = {}) {
  if (!geolocation || typeof geolocation.getCurrentPosition !== "function") {
    return Promise.reject(mapGeolocationError({ code: LOCATION_ERROR_UNSUPPORTED }));
  }

  return new Promise((resolve, reject) => {
    geolocation.getCurrentPosition(
      (position) => {
        resolve({
          lat: Number(position?.coords?.latitude),
          lon: Number(position?.coords?.longitude),
          raw: position,
        });
      },
      (error) => {
        reject(mapGeolocationError(error));
      },
      options,
    );
  });
}

export function shouldAttemptStartupLocation({
  alreadyRequested = false,
  hasLocation = false,
  permissionState = LOCATION_PERMISSION_UNKNOWN,
  geolocationSupported = true,
} = {}) {
  if (alreadyRequested || hasLocation || !geolocationSupported) {
    return false;
  }
  const normalizedPermission = normalizeLocationPermissionState(permissionState);
  return normalizedPermission === LOCATION_PERMISSION_GRANTED;
}

export function getLocationLookupViewModel({
  hasLocation = false,
  isRequesting = false,
  permissionState = LOCATION_PERMISSION_UNKNOWN,
  errorCode = "",
  geolocationSupported = true,
} = {}) {
  if (hasLocation) {
    return {
      kind: LOCATION_REQUEST_READY,
      title: "",
      message: "",
      actionLabel: "",
      blocksStationList: false,
    };
  }

  if (!geolocationSupported || permissionState === LOCATION_PERMISSION_UNSUPPORTED || errorCode === LOCATION_ERROR_UNSUPPORTED) {
    return {
      kind: LOCATION_REQUEST_ERROR,
      title: "Standort nicht verfügbar",
      message: "Dieser Browser unterstützt keine Standortabfrage.",
      actionLabel: "",
      blocksStationList: true,
    };
  }

  if (isRequesting) {
    return {
      kind: LOCATION_REQUEST_PENDING,
      title: "Standort wird ermittelt",
      message: "Die Liste lädt Ladepunkte, sobald der erste Standort bestimmt wurde.",
      actionLabel: "",
      blocksStationList: true,
    };
  }

  if (permissionState === LOCATION_PERMISSION_DENIED || errorCode === LOCATION_ERROR_PERMISSION_DENIED) {
    return {
      kind: LOCATION_REQUEST_ERROR,
      title: "Standortfreigabe benötigt",
      message: "Aktiviere den Standortzugriff, damit die Liste nahe Ladepunkte laden kann.",
      actionLabel: "Erneut versuchen",
      blocksStationList: true,
    };
  }

  if (errorCode === LOCATION_ERROR_TIMEOUT) {
    return {
      kind: LOCATION_REQUEST_ERROR,
      title: "Standortsuche dauert zu lange",
      message: "Bitte versuche es erneut oder prüfe, ob der Standortzugriff aktiviert ist.",
      actionLabel: "Erneut versuchen",
      blocksStationList: true,
    };
  }

  if (errorCode === LOCATION_ERROR_POSITION_UNAVAILABLE) {
    return {
      kind: LOCATION_REQUEST_ERROR,
      title: "Standort konnte nicht ermittelt werden",
      message: "Bitte prüfe, ob dein Gerät gerade einen Standort liefern kann, und versuche es erneut.",
      actionLabel: "Erneut versuchen",
      blocksStationList: true,
    };
  }

  if (errorCode === LOCATION_ERROR_UNKNOWN) {
    return {
      kind: LOCATION_REQUEST_ERROR,
      title: "Standort konnte nicht ermittelt werden",
      message: "Bitte versuche es erneut.",
      actionLabel: "Erneut versuchen",
      blocksStationList: true,
    };
  }

  return {
    kind: LOCATION_REQUEST_IDLE,
    title: "Standort freigeben",
    message: "Nahe Ladepunkte werden geladen, sobald dein Standort freigegeben ist.",
    actionLabel: "Standort freigeben",
    blocksStationList: true,
  };
}
