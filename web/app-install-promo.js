(function () {
  const PROMO_ID = "app-install-promo";
  const ANDROID_WEB_LINK =
    "https://play.google.com/store/apps/details?id=de.woladen.android";
  const ANDROID_STORE_LINK = "market://details?id=de.woladen.android";
  const IOS_LINK = "https://apps.apple.com/de/app/wo-laden/id6759499459";

  function isAndroid() {
    return /Android/i.test(navigator.userAgent || "");
  }

  function dismissPromo() {
    document.getElementById(PROMO_ID)?.remove();
  }

  function rootContainer() {
    return document.querySelector("#view-list, .legal-shell");
  }

  function promoMarkup() {
    return `
      <div class="app-install-head">
        <div class="app-install-copy">
          <p class="app-install-kicker">Also as an app</p>
          <h2>Where to charge? Always with you</h2>
          <p>
            Free for
            <a class="app-install-copy-link" href="${IOS_LINK}">iPhone on the App Store</a>
            and
            <a
              class="app-install-copy-link"
              href="${isAndroid() ? ANDROID_STORE_LINK : ANDROID_WEB_LINK}"
              >Android on Google Play</a
            >.
          </p>
        </div>
        <div class="app-install-links" aria-label="Store-Links">
          <a
            class="app-install-link"
            href="${IOS_LINK}"
            aria-label="Open wo-laden in the App Store"
            title="Open in the App Store"
          >
            <img
              class="app-install-store-badge app-install-store-badge--apple"
              src="img/app-store-badge.svg"
              alt="Download on the App Store"
              width="250"
              height="83"
              decoding="async"
            />
          </a>
          <a
            class="app-install-link"
            href="${isAndroid() ? ANDROID_STORE_LINK : ANDROID_WEB_LINK}"
            aria-label="Open wo-laden on Google Play"
            title="Open on Google Play"
          >
            <img
              class="app-install-store-badge app-install-store-badge--google"
              src="img/google-play-badge.png"
              alt="Get it on Google Play"
              width="646"
              height="250"
              decoding="async"
            />
          </a>
        </div>
        <button
          class="app-install-dismiss"
          type="button"
          aria-label="Hide app note"
        >
          ×
        </button>
      </div>
    `;
  }

  function buildPromo() {
    if (document.getElementById(PROMO_ID)) return;

    const container = rootContainer();
    if (!container) return;

    const promo = document.createElement("section");
    promo.id = PROMO_ID;
    promo.className = "app-install-promo";
    promo.setAttribute("aria-label", "wo-laden app");
    promo.innerHTML = promoMarkup();
    container.prepend(promo);

    promo
      .querySelector(".app-install-dismiss")
      ?.addEventListener("click", dismissPromo);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", buildPromo, { once: true });
  }
  buildPromo();
})();
