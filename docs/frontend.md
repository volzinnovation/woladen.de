# Frontend

Frontend is a pure HTML, CSS and JavaScript application deployed via Github Pages with source in the web folder and deployment action copies to site.

Rethink the current frontend code and create a new, modern, responsive and user-friendly frontend for the application. The frontend should be easy to navigate and use, and it should be easy to find the information that the user is looking for. The frontend should also be easy to maintain and update.

The primary purpose of the application is to find chargers with amenities nearby based on the dataset chargers_fast.geojson.

The app should display a list of chargers nearby to the user GPS location, sorted by distance.

Users should be able to filter that list based on the following criteria: Operator (dropdown populated by operators.json), Power (slider), and amenities. Instead of the checkboxes used in the current approach use icons fomr the img folder, icons are availble in two resolutions (similar to iphone formating x2 for double resolution).

When the user clicks on a charger in the list, a modal should open with the details of the charger. The modal should display the following information:

- Operator
- Power
- Amenities nearby with icons name and opening times
- a small mini map showing the charger and its surroundings (250m radius)
- Button with link to Google Maps/Apple Maps for directions
- Back button to return to the list

Next to the list we should have a map view (based on OpenStreetMap) showing all chargers nearby to the user GPS location, sorted by distance. The map should display the same information as the list, but in a visual format. The map should also display the user's current location and the direction they are facing.

Use different icons for the chargers depending on the total amount of amenities nearby (gold => more than 10, silver => more than 5, bronze => more than 1, grey => none). Location should be marked with the typical icon used in more apps.

Also include a info tab in the navigation bar at the bottom (where users can choose from list, map, favorites, info). The info tab should display information about the app, the data sources, source code on github, contact info to the author and how to use the app.

A last feature is the ability of users to mark chargers as favorites. This should be done by clicking on a star icon in the charger details modal. The favorites should be displayed in the favorites tab in the navigation bar at the bottom. The favorites should be sorted by distance to the user's current location. And stored in LocalStorage of the browser.

Consider other features that you might find useful. Think about the user experience and what would make the app more useful and user-friendly.
