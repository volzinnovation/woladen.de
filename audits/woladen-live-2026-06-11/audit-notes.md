# woladen.de Live Design, UX, and Accessibility Audit

Date: 2026-06-11  
Surface: https://woladen.de/  
Viewport: Mobile, 390 x 844  
Audit mode: Combined UX and accessibility audit  
User goal: Find a useful fast-charging station in Germany, understand nearby amenities and live charging context, and decide whether to save or navigate to the station.  
Accessibility target: Identify visible risks related to perceivable content, semantic clarity, keyboard/focus behavior, touch targets, state communication, and responsive resilience. This is screenshot- and DOM-observation-based, not a full WCAG conformance test.

## Step 1: Start list

Screenshot: `screenshots/01-start-list.png`

What the user sees:
- A prominent app-install promo appears above the main content.
- The primary view is `Ladepunkte in der Nähe` with a compact `Filter` button.
- A location-permission card says `Standortfreigabe benötigt`, followed by charger cards sorted by apparent amenity density rather than distance.
- Bottom navigation exposes `Liste`, `Karte`, `Favoriten`, and `Info`.

Screen behavior:
- The list renders without a blocking loader.
- Because location is unavailable, the app falls back to a useful nationwide list and offers `Erneut versuchen`.
- The bottom navigation remains fixed and visible.

Strengths:
- The fallback state is productive: users can still browse stations instead of hitting a dead end.
- Charger cards communicate operator, city, power, connector count, amenity count, and amenity chips in a scan-friendly format.
- The bottom navigation is predictable and reachable on mobile.

UX issues:
- The app-install promo takes a large share of the first viewport and pushes the actual charger discovery task down.
- The list title says `in der Nähe` while the permission state means results are not actually nearby. That mismatch can reduce trust.
- The first actionable next step competes between app-install links, dismiss, filter, retry location, and the first result.

Accessibility risks:
- The dismiss button is visually just `×`; it has an accessible name in the DOM, but the visible control may be hard to understand for some users.
- The permission card uses a large primary button, but the state change after retry cannot be verified from the screenshot alone.
- Amenity chips are visually small; touch behavior and screen-reader verbosity need interaction testing.

Evidence limits:
- I did not grant location permission, so distance sorting and permission prompt handling are not validated in this step.
- Contrast and focus indicators are observed visually but not measured with an automated contrast checker.

Recommendations:
- Change the list title or add a short state label when location is unavailable, for example `Empfehlungen in Deutschland` plus a `Nach Entfernung sortieren` action.
- Consider collapsing or reducing the app-install promo after first exposure so the charger-finding task owns more of the first viewport.
- Keep the fallback list; it is a strong resilience pattern.

## Step 2: Filter sheet

Screenshot: `screenshots/02-filter-sheet.png`

What the user sees:
- A bottom sheet titled `Filter` over a dimmed app background.
- Controls for operator, amenity-name search, `Jetzt geöffnet`, minimum charging power, and amenity category icons.
- A large sticky `Anwenden` button at the bottom of the viewport.

Screen behavior:
- The sheet opens in place without navigating away from the list.
- The underlying page remains visible but visually de-emphasized.
- The apply button is available without needing to scroll.

Strengths:
- The filter categories map well to the product promise: operator, power, open amenities, and amenity types are all decision-relevant.
- The bottom sheet pattern feels appropriate for mobile and preserves context.
- The `Anwenden` button has strong visual weight and a good touch target.

UX issues:
- The sheet has a lot of controls in one view; users may not immediately know whether amenity icons are toggle filters, informational shortcuts, or categories.
- `Jetzt geöffnet` depends on opening-hours data quality, but the UI does not state that this may be incomplete.
- The operator dropdown likely contains many long names; the collapsed state is clean, but selecting from it may be cumbersome on mobile.

Accessibility risks:
- The close control is a small `×`; it likely has an accessible label in markup, but visible affordance is weak.
- Amenity icons rely on pictograms plus small labels; icon contrast and recognizability need closer checking, especially at mobile size.
- It is not visible from the screenshot whether focus is moved into the modal or trapped inside it for keyboard and screen-reader users.

Evidence limits:
- I have not yet keyboard-tested focus order, Escape handling, or whether background content is hidden from assistive technology while the modal is open.
- I have not opened the operator native select, so long-option usability is not assessed here.

Recommendations:
- Add a short state label for icon filters, for example selected chips or a count near `Angebote vor Ort`, once users tap categories.
- Ensure the modal has a dialog role, labelled title, focus management, Escape close, and background inertness.
- Consider explaining `Jetzt geöffnet` with a concise note such as `sofern Öffnungszeiten bekannt sind`.

## Step 3: Amenity filter selected

Screenshot: `screenshots/03-bakery-filter-selected.png`

What the user sees:
- The `Bäckerei` amenity icon is selected in the grid.
- The selected item changes from a muted grey icon tile to a saturated blue tile.
- The `Anwenden` button remains fixed at the bottom.

Screen behavior:
- Tapping the amenity toggles a selected visual state without leaving the sheet.
- The current minimum power remains at the fast-charger default of 50 kW.

Strengths:
- The selected state is visually distinct for sighted users.
- The sticky apply action keeps the next step obvious after selection.
- The grid supports a quick, domain-specific task: finding a place to spend charging time.

UX issues:
- The selected filter is only shown on the icon itself; there is no textual summary such as `1 Angebot ausgewählt`.
- The blue selected tile uses a hue that is visually disconnected from the app’s teal primary action color, which may make the state feel like an icon artifact rather than selection.
- The user cannot preview result count before applying.

Accessibility risks:
- DOM inspection shows the amenity option is a `div` with class `amenity-toggle active`, not a native button or checkbox.
- The selected state is not exposed as `aria-pressed`, `aria-selected`, or `checked`.
- The control has no `tabindex`, so keyboard users may not be able to reach or toggle it.
- Icon-only color change may be insufficient for users with color-vision differences unless the tile state also has a non-color cue.

Evidence limits:
- I verified DOM attributes for this step, but not the full keyboard path through the modal.
- I did not run a screen reader, so spoken output is inferred from semantics, not directly heard.

Recommendations:
- Convert amenity toggles to real `button type="button"` elements with `aria-pressed`, or checkboxes with visible labels.
- Add a non-color selected cue, such as a check mark or selected border, and a summary like `Bäckerei ausgewählt`.
- Consider showing an estimated result count or immediate list count update before users apply.

## Step 4: Filtered list results

Screenshot: `screenshots/04-filtered-list-results.png`

What the user sees:
- The app returns to the list.
- The visible list header still says `Ladepunkte in der Nähe`.
- The list filter button still says `Filter`.
- The first visible cards look nearly identical to the unfiltered state, and none of the first two visible cards show `Bäckerei` in their visible amenity chips.

Screen behavior:
- The filter sheet closes successfully.
- DOM observation shows the hidden map filter label changed to `Filter (1)`, but the visible list filter button did not show that count.
- The filtered criteria are not summarized in the list view.

Strengths:
- Applying a filter does not disrupt the user’s place in the core list workflow.
- The list remains populated, so the interaction does not create an empty or confusing blank state.

UX issues:
- The visible list view gives weak confirmation that any filter is active.
- The selected amenity is not obvious in the first returned cards; users may wonder whether the filter worked.
- The `in der Nähe` copy remains misleading while location permission is unavailable.

Accessibility risks:
- State change communication relies on visual content update only; there is no visible live-region style feedback such as `Filter angewendet`.
- If screen-reader users cannot reach the original amenity toggle, they may also have no reliable way to confirm the active filter from the list.
- The result count is not announced or shown.

Evidence limits:
- I did not inspect the full result set; some returned cards may include bakeries further down or in hidden amenity details.
- I did not test whether a screen reader announces the modal close and list update.

Recommendations:
- Update the list filter button to `Filter (1)` when filters are active, matching the map label.
- Add a compact active-filter row under the header, for example `Bäckerei · ab 50 kW`, with a clear remove action.
- Show a result count or brief confirmation after applying filters.

## Step 5: Station detail

Screenshot: `screenshots/05-station-detail.png`

What the user sees:
- A full-screen detail sheet for `Hamburger Energiewerke GmbH`.
- A mini-map, station title, favorite star, power/connector chip, full address, rating block, personal note field, and Google/Apple navigation actions.
- The close button floats at the top right.

Screen behavior:
- Tapping the first list card opens the detail sheet.
- The mini-map renders with nearby context and a station marker.
- The detail content begins with task-relevant facts, then rating and note controls.

Strengths:
- The detail view gives users the main decision data quickly: location, charging capacity, connector count, and route actions.
- Google and Apple route buttons are large, clear, and placed near the address.
- The mini-map provides useful spatial context without requiring a full map switch.

UX issues:
- The top strip behind the close button shows blurred underlying page content before the mini-map starts, which makes the sheet boundary feel visually ambiguous.
- The address is long and duplicated enough to become hard to scan.
- The selected `Bäckerei` filter is not reflected in the visible first detail viewport, even though the note placeholder mentions `Bäckerei um die Ecke`.
- Rating and personal note are prominent before the amenity list; for a first-time discovery task, nearby offers may deserve earlier visibility.

Accessibility risks:
- DOM inspection shows the close button has no accessible name or visible text in this detail modal.
- List cards that opened this detail are non-semantic clickable `div`s, so keyboard and assistive-technology access is likely weak.
- The favorite star has an accessible label of `Favorit`, but the current saved/unsaved state may not be exposed clearly as a pressed state.
- The mini-map is visual context; alternative text or a textual distance/context summary is needed for non-visual users.

Evidence limits:
- I did not test external navigation handoff to Google or Apple.
- I did not test whether focus moves into the detail sheet or returns to the triggering card on close.
- I did not measure contrast for map markers or chips.

Recommendations:
- Give the close button an accessible name such as `Details schließen`.
- Make station cards native buttons or links, or add button role, keyboard handlers, focus styles, and clear labels.
- Put a compact amenity preview or matched-filter summary above rating/note when the user arrived from an amenity filter.
- Tighten the hero layout so the top of the detail sheet reads as one coherent surface.

## Step 6: Favorite selected in detail

Screenshot: `screenshots/06-favorite-selected.png`

What the user sees:
- The star control changes from an outline to a filled gold star with a visible border.
- The rest of the detail sheet remains in place.

Screen behavior:
- Tapping the star toggles the favorite state without leaving the detail view.
- No toast or text confirmation appears in the visible viewport.

Strengths:
- The visual state change is immediate and easy to notice.
- The control is placed near the station title, which matches the mental model of saving this station.
- The star target appears large enough for touch use.

UX issues:
- There is no textual confirmation such as `Als Favorit gespeichert`.
- The star's square active border may read like focus rather than saved state.
- Users may not know whether the favorite was saved only locally or synced; the app later implies local behavior, but not at the moment of save.

Accessibility risks:
- The button remains labelled `Favorit` with no `aria-pressed` or changed label like `Favorit entfernen`.
- Screen-reader users may not receive confirmation that the state changed.
- The filled-star state relies mainly on color and icon fill.

Evidence limits:
- I did not test persistence after reload.
- I did not test whether the state change is announced by assistive technology.

Recommendations:
- Add `aria-pressed="true/false"` and update the accessible name to clarify action and state.
- Show a lightweight confirmation message or inline text after saving.
- If favorites are local-only, state that near the save action or in Favorites.

## Step 7: Favorites view

Screenshot: `screenshots/07-favorites-view.png`

What the user sees:
- The `Favoriten` tab is active.
- The saved `Hamburger Energiewerke GmbH` station appears as a card.
- A sort dropdown offers `Entfernung` and `Sterne`.

Screen behavior:
- Closing detail and switching tabs preserves the saved station.
- The bottom nav clearly marks `Favoriten` as active.
- The list has substantial empty space after the single saved card.

Strengths:
- The favorite is easy to find after saving.
- The sort control is compact and relevant for larger favorite lists.
- Active tab styling is clear.

UX issues:
- There is no explanatory local-only persistence note in the Favorites view.
- With one favorite, the sort control has low immediate value and competes with the title.
- The favorite card does not expose when or why it was saved, nor any quick remove affordance.

Accessibility risks:
- Favorite cards use the same non-semantic clickable `div` pattern as the main list.
- The active navigation item is visually clear, but the DOM state for selected tab should be verified with `aria-current` or equivalent.
- The sort control label is visually minimal; screen-reader labeling should be checked.

Evidence limits:
- I did not test re-opening the favorite card from this view.
- I did not test sorting with multiple favorites.

Recommendations:
- Add a short empty/sparse-state hint when only one favorite exists, for example `Favoriten werden auf diesem Gerät gespeichert`.
- Make station cards keyboard-accessible and semantically interactive across both list and favorites.
- Consider a quick remove or overflow action for saved stations.

## Step 8: Map view with active filter

Screenshot: `screenshots/08-map-view-filtered.png`

What the user sees:
- A full-screen Germany map covered by dense station markers.
- Floating controls at the top: locate button and `Filter (1)`.
- Bottom navigation remains visible with `Karte` active.

Screen behavior:
- The map loads tiles and markers.
- The active filter count is clearer here than on the list view.
- The map defaults to a nationwide view because precise location is unavailable.

Strengths:
- The active filter state is visible in the map control.
- The locate button has a clear target and an accessible label of `Mein Standort`.
- The map keeps navigation and filtering available without leaving the visual context.

UX issues:
- Marker density is overwhelming at the default Germany zoom; many markers overlap and compete visually.
- It is hard to predict which marker a tap will select.
- The meaning of orange, grey, gold, and red markers depends on the Info legend, which is not present in context.
- The active filter count does not say which filter is active.

Accessibility risks:
- Dense map markers are difficult for low-vision, motor-impaired, and keyboard users.
- The map is mostly visual; non-visual users need an equivalent list, but switching between list and map can lose spatial context.
- Marker color meaning may not be sufficiently distinguishable without shape, label, or legend support.

Evidence limits:
- I did not accept or deny a browser location permission prompt.
- I did not test keyboard panning, marker focus, or screen-reader access to marker content.

Recommendations:
- Use clustering or a lower-density marker strategy at national zoom.
- Show active filter chips in the map control, not just a count.
- Add a small legend or marker-status key near the map, especially when red status markers are present.
- Provide a robust list equivalent for map results and keep list/map filters visibly in sync.

## Step 9: Map marker opens station detail

Screenshot: `screenshots/09-map-marker-detail.png`

What the user sees:
- A station detail sheet for `Shell Deutschland GmbH` after tapping a map marker.
- The first viewport shows max power, connector count, live availability, price, 24/7 status, address, and live connector rows.
- The map remains implied in the blurred background and mini-map hero.

Screen behavior:
- A marker tap opens detail directly, without a popup preview.
- The detail view uses the same structure as list-opened details.
- Live status is more prominent here than in the previous detail because this station has occupancy and price data.

Strengths:
- Direct marker-to-detail is efficient for touch users once they successfully hit a marker.
- Live availability, price, and 24/7 chips are highly decision-relevant and easy to scan.
- The detail layout handles richer live data without feeling broken.

UX issues:
- Without an intermediate popup, an accidental marker tap fully changes context into a detail sheet.
- The marker tap target is difficult to choose accurately at the dense nationwide zoom.
- The detail still does not say which map filter is active or why this station matched.

Accessibility risks:
- DOM inspection shows sampled map markers are SVG paths with no role, accessible name, title, or tabindex.
- Keyboard and screen-reader users likely cannot discover or activate individual map markers.
- Live data timestamps are visible in rows, but the freshness and meaning of source coverage should be reviewed for assistive clarity.

Evidence limits:
- I tested one marker tap only, not all marker types or red status markers.
- I did not test zoom gestures, clustering behavior, or keyboard alternatives for markers.

Recommendations:
- Add accessible marker equivalents, such as a synchronized visible list of map results or focusable marker controls with meaningful names.
- Consider a lightweight marker preview before opening full detail, especially at dense zoom.
- Preserve or repeat active-filter context in map-opened detail views.

## Cross-Step Summary

Strengths:
- The baseline no-location state remains usable.
- The product-specific data model is strong: power, connector count, amenities, live occupancy, price, and hours all support the charging-wait decision.
- Detail pages are information-rich and route actions are clear.
- Favorites work across detail and Favorites view in the tested session.

Primary UX risks:
- List and map do not communicate active filters consistently.
- Nationwide map marker density overwhelms the first map view.
- Location-unavailable copy conflicts with `in der Nähe`.
- Important amenity-match context is not carried into filtered results or detail.

Primary accessibility risks:
- Clickable station cards and amenity filters are non-semantic `div` controls.
- Map markers are unlabeled SVG paths without keyboard access.
- Modal focus behavior, focus return, and screen-reader background inertness need verification.
- Several icon-only state changes need stronger accessible names and pressed/selected state.

Verification gaps:
- No location permission flow was accepted or denied.
- No full keyboard-only pass or screen-reader pass was performed.
- No automated contrast measurement was run.
- External Google/Apple route handoff was not tested.
