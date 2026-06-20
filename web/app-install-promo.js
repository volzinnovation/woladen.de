(function () {
  const PROMO_SELECTOR = "[data-app-install-promo]";
  const STORAGE_KEY = "woladen_language_v1";
  const ANDROID_WEB_LINK =
    "https://play.google.com/store/apps/details?id=de.woladen.android";
  const ANDROID_STORE_LINK = "market://details?id=de.woladen.android";
  const IOS_LINK = "https://apps.apple.com/de/app/wo-laden/id6759499459";
  let dismissed = false;

  const COPY = {
    en: {
      aria: "woladen app",
      kicker: "Also as an app",
      title: "woladen, always with you",
      beforeIos: "Free for",
      iosLink: "iPhone on the App Store",
      betweenLinks: "and",
      androidLink: "Android on Google Play",
      afterAndroid: ".",
      linksLabel: "Store links",
      iosAria: "Open woladen in the App Store",
      iosTitle: "Open in the App Store",
      iosAlt: "Download on the App Store",
      androidAria: "Open woladen on Google Play",
      androidTitle: "Open on Google Play",
      androidAlt: "Get it on Google Play",
      dismiss: "Hide app note",
    },
    de: {
      aria: "woladen App",
      kicker: "Auch als App",
      title: "woladen, immer dabei",
      beforeIos: "Kostenlos für",
      iosLink: "iPhone im App Store",
      betweenLinks: "und",
      androidLink: "Android bei Google Play",
      afterAndroid: ".",
      linksLabel: "Store-Links",
      iosAria: "woladen im App Store öffnen",
      iosTitle: "Im App Store öffnen",
      iosAlt: "Im App Store laden",
      androidAria: "woladen bei Google Play öffnen",
      androidTitle: "Bei Google Play öffnen",
      androidAlt: "Jetzt bei Google Play",
      dismiss: "App-Hinweis ausblenden",
    },
    nl: {
      aria: "woladen app",
      kicker: "Ook als app",
      title: "woladen, altijd bij je",
      beforeIos: "Gratis voor",
      iosLink: "iPhone in de App Store",
      betweenLinks: "en",
      androidLink: "Android op Google Play",
      afterAndroid: ".",
      linksLabel: "Store-links",
      iosAria: "woladen in de App Store openen",
      iosTitle: "Openen in de App Store",
      iosAlt: "Download in de App Store",
      androidAria: "woladen op Google Play openen",
      androidTitle: "Openen op Google Play",
      androidAlt: "Ontdek het op Google Play",
      dismiss: "App-melding verbergen",
    },
  };

  function isAndroid() {
    return /Android/i.test(navigator.userAgent || "");
  }

  function normalizeLanguage(value) {
    const raw = String(value || "").trim().toLowerCase().replace("_", "-");
    const base = raw.split("-")[0];
    return COPY[base] ? base : "en";
  }

  function activeLanguage() {
    const params = new URLSearchParams(window.location.search);
    const requested = normalizeLanguage(params.get("lang") || params.get("language") || "");
    if (requested !== "en") {
      return requested;
    }
    try {
      const stored = normalizeLanguage(window.localStorage?.getItem(STORAGE_KEY) || "");
      if (stored !== "en") {
        return stored;
      }
    } catch {
      // Local storage may be disabled.
    }
    return normalizeLanguage(document.documentElement.lang || navigator.language || "en");
  }

  function copy() {
    return COPY[activeLanguage()] || COPY.en;
  }

  function dismissPromo() {
    dismissed = true;
    document.querySelectorAll(PROMO_SELECTOR).forEach((promo) => promo.remove());
  }

  function rootContainers() {
    return Array.from(
      document.querySelectorAll("#view-list, #view-map .map-controls-overlay, #view-info, .legal-shell"),
    );
  }

  function promoMarkup() {
    const text = copy();
    const androidLink = isAndroid() ? ANDROID_STORE_LINK : ANDROID_WEB_LINK;
    return `
      <div class="app-install-head">
        <div class="app-install-copy">
          <p class="app-install-kicker">${text.kicker}</p>
          <h2>${text.title}</h2>
          <p>
            ${text.beforeIos}
            <a class="app-install-copy-link" href="${IOS_LINK}">${text.iosLink}</a>
            ${text.betweenLinks}
            <a
              class="app-install-copy-link"
              href="${androidLink}"
              >${text.androidLink}</a
            >${text.afterAndroid}
          </p>
        </div>
        <div class="app-install-links" aria-label="${text.linksLabel}">
          <a
            class="app-install-link"
            href="${IOS_LINK}"
            aria-label="${text.iosAria}"
            title="${text.iosTitle}"
          >
            <img
              class="app-install-store-badge app-install-store-badge--apple"
              src="img/app-store-badge.svg"
              alt="${text.iosAlt}"
              width="250"
              height="83"
              decoding="async"
            />
          </a>
          <a
            class="app-install-link"
            href="${androidLink}"
            aria-label="${text.androidAria}"
            title="${text.androidTitle}"
          >
            <img
              class="app-install-store-badge app-install-store-badge--google"
              src="img/google-play-badge.png"
              alt="${text.androidAlt}"
              width="646"
              height="250"
              decoding="async"
            />
          </a>
        </div>
        <button
          class="app-install-dismiss"
          type="button"
          aria-label="${text.dismiss}"
        >
          ×
        </button>
      </div>
    `;
  }

  function buildPromos() {
    if (dismissed) return;
    const text = copy();
    rootContainers().forEach((container) => {
      const existing = Array.from(container.children).find((child) =>
        child.matches?.(PROMO_SELECTOR),
      );
      const promo = existing || document.createElement("section");
      promo.className = "app-install-promo";
      promo.dataset.appInstallPromo = "";
      promo.setAttribute("aria-label", text.aria);
      promo.innerHTML = promoMarkup();
      if (!existing) {
        container.prepend(promo);
      }
      promo
        .querySelector(".app-install-dismiss")
        ?.addEventListener("click", dismissPromo);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", buildPromos, { once: true });
  }
  buildPromos();
  new MutationObserver(buildPromos).observe(document.documentElement, {
    attributes: true,
    attributeFilter: ["lang"],
  });
})();
