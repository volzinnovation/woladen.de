# Android Play Console Submission Checklist

Last reviewed: 2026-03-28

This memo is specific to the current Android app in this repository and the current Google Play guidance as of the date above.

## Current app behavior

Observed in the current codebase:

- Package name: `de.woladen.android`
- Sensitive permissions requested:
  - `ACCESS_FINE_LOCATION`
  - `ACCESS_COARSE_LOCATION`
- Location is optional for app use and is only requested after an explicit user action. When used, it is used to:
  - center the map
  - sort/filter nearby chargers
- A privacy-policy page exists at `https://woladen.de/privacy.html`.
- The app links to the privacy policy from the Info tab.
- The app stores local favorites in `SharedPreferences`.
- The app lets the user import a local data bundle via Android's Storage Access Framework.
- The app loads map tiles from `https://tile.openstreetmap.org/...`.
- The app opens external links for:
  - Google Maps navigation
  - `geo:` intents
  - website/email/license links
- The app does not contain evidence of:
  - ads SDKs
  - analytics SDKs
  - crash-reporting SDKs
  - user accounts or login
  - in-app purchases
  - background location
  - broad file access permissions

Code references:

- `android/app/src/main/AndroidManifest.xml`
- `android/app/src/main/java/de/woladen/android/MainActivity.kt`
- `android/app/src/main/java/de/woladen/android/service/LocationService.kt`
- `android/app/src/main/java/de/woladen/android/store/FavoritesStore.kt`
- `android/app/src/main/java/de/woladen/android/ui/MapTabView.kt`
- `android/app/src/main/java/de/woladen/android/ui/components/OsmdroidMapViews.kt`
- `android/app/src/main/java/de/woladen/android/ui/InfoTabView.kt`
- `android/app/src/main/java/de/woladen/android/ui/StationDetailSheet.kt`

## Remaining pre-submit decisions

1. Publish the privacy policy URL in Play Console.

- Use `https://woladen.de/privacy.html`.
- Use Moonshots Studios GmbH as the legal contact information consistently with the studios site.

2. Decide whether to keep `ACCESS_FINE_LOCATION`.

- If you only need "near me" sorting and map centering, `ACCESS_COARSE_LOCATION` may be enough.
- Keeping `ACCESS_FINE_LOCATION` is possible, but it increases review scrutiny and makes the privacy story harder.

## Recommended Data safety answers

Important: the safest answer set for the current build is the conservative one below.

Why conservative:

- The app requests precise location.
- The map can center on the user's location.
- The map loads third-party tiles from OpenStreetMap over the network.
- Inference: once the map is centered near the user, those tile requests can reveal the user's location area to a third party.

### Data collection and security

Recommended answers for the current build:

- Does your app collect or share any of the required user data types? `Yes`
- Is all user data collected by your app encrypted in transit? `Yes`
- Do you provide a way for users to request that their data is deleted? `No`

Rationale:

- Tile requests use HTTPS.
- Cleartext is disabled in `network_security_config.xml`.
- There is no account-based deletion workflow or server-side deletion request flow in the app.

### Data types

Recommended declarations for the current build:

- `Precise location`: `Collected`, `Shared`
- `Approximate location`: `Do not declare separately if Play only needs the highest-sensitivity location type you use; if the form requires both, mark Approximate location consistently with your final policy interpretation`
- `Personal info`: `No`
- `Financial info`: `No`
- `Health and fitness`: `No`
- `Messages`: `No`
- `Photos and videos`: `No`
- `Audio files`: `No`
- `Files and docs`: `No`
- `Calendar`: `No`
- `Contacts`: `No`
- `App activity`: `No`
- `Web browsing`: `No`
- `Crash logs`: `No`
- `Diagnostics`: `No`
- `Other app performance data`: `No`
- `Device or other IDs`: `No`

Notes:

- Favorites are stored only on-device, so they are not "collected" for Data safety purposes.
- User-selected data-bundle imports are processed on-device only and do not appear to be transmitted off-device.
- Opening Google Maps or `geo:` links is user-initiated. That is not the main reason for the conservative location answer above.

### Usage and handling for location

If you declare location for the current build, use:

- Collected: `Yes`
- Shared: `Yes`
- Is this data processed ephemerally? `No`
- Is collection required or optional? `Optional`
- Purpose: `App functionality`

Do not mark:

- `Analytics`
- `Advertising or marketing`
- `Developer communications`
- `Account management`
- `Fraud prevention, security, and compliance`

### Cleaner alternative if you change the app first

If you make all of the changes below before release, you may be able to use a much simpler Data safety declaration:

- remove automatic location prompt on first launch
- avoid automatic map centering from permission-granted location
- consider dropping `ACCESS_FINE_LOCATION`
- ensure location is only used locally unless the user explicitly initiates an external navigation action

Even then, because the map uses a third-party tile server, you should re-evaluate whether any location-derived network traffic remains.

## Recommended App content answers

These are the recommended Play Console answers for this app unless the product scope changes.

### Privacy policy

- `Required`
- Add the public URL to the store listing.
- Add the same privacy policy link inside the app.

### Ads

- `No`

Rationale:

- No ad SDKs or in-app ad surfaces were found in the Android app code.

### App access

- `No, all functionality is available without special access`

Rationale:

- No login, membership, OTP, or reviewer-only access flow exists.

### Target audience and content

Recommended answer:

- `18 and over`

Reasoning:

- This is a utility for EV drivers.
- Choosing only adult audiences avoids unnecessary Families-policy ambiguity.

If you intentionally want teenagers included, review the Families implications carefully before selecting younger age bands.

### Content ratings

Expected questionnaire answers:

- Violence: `No`
- Fear/horror: `No`
- Sexual content: `No`
- Gambling: `No`
- Drugs/alcohol/tobacco encouragement: `No`
- Profanity/crude humor: `No`
- User-generated content: `No`
- User-to-user sharing/public posting: `No`
- Purchases/real-money transactions inside app: `No`
- Location sharing between users: `No`

Inference:

- The resulting rating is likely very low (for example, Everyone / PEGI 3 / USK 0), but the final rating is assigned by the IARC questionnaire.

### News and Magazine apps

- `No`

Rationale:

- The app is a charger/amenity finder, not a news product.

### COVID-19 contact tracing / status

- `No`

### Sensitive permissions declarations

Expected result:

- No special permissions declaration should be needed for the current manifest, because the app does not request:
  - SMS
  - Call Log
  - background location
  - `MANAGE_EXTERNAL_STORAGE`
  - broad photo/video permissions

Location still remains subject to the general sensitive-permissions policy and review.

### Health apps / Financial features / other dynamic declarations

If these forms appear in Play Console for this app, the current code suggests:

- Health apps: `No`
- Financial features: `No`
- Government / official-affiliation forms: `No`

## Store listing checklist

Before sending the app for review:

- Upload the signed `.aab`
- Add app title
- Add short description
- Add full description
- Add support email
- Add website URL if available
- Add privacy policy URL
- Add phone number if you want public support contact
- Add app icon
- Add feature graphic
- Add phone screenshots
- Choose category
- Choose tags
- Complete Data safety
- Complete App content declarations
- Complete content rating questionnaire
- Confirm no ads label
- Confirm no app access instructions are needed
- Check release notes

## Recommended pre-submit changes

These are the two highest-value changes before publishing:

1. Change location permission flow to user-initiated only.

- This is the biggest review-risk reduction.

2. Add a privacy policy page and link it in-app.

- This is required for a clean Play submission with location permissions.

Optional third change:

3. Reduce location scope from fine to coarse if product requirements allow it.

## Official sources used

- Android developer verification overview:
  - https://developer.android.com/developer-verification
- Register on Google Play Console:
  - https://developer.android.com/developer-verification/guides/google-play-console
- Android developer verification FAQ:
  - https://developer.android.com/developer-verification/guides/faq
- Use Play App Signing:
  - https://support.google.com/googleplay/android-developer/answer/9842756
- Prepare your app for review:
  - https://support.google.com/googleplay/android-developer/answer/9859455
- Data safety form:
  - https://support.google.com/googleplay/android-developer/answer/10787469
- Permissions and APIs that access sensitive information:
  - https://support.google.com/googleplay/android-developer/answer/16558241
