package de.woladen.android.ui.components

import android.content.Context
import android.graphics.Bitmap
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.drawable.BitmapDrawable
import android.graphics.drawable.Drawable
import android.location.Location
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalLifecycleOwner
import androidx.compose.ui.viewinterop.AndroidView
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import de.woladen.android.model.GeoJsonFeature
import org.maplibre.android.annotations.Icon
import org.maplibre.android.annotations.IconFactory
import org.maplibre.android.annotations.MarkerOptions
import org.maplibre.android.camera.CameraUpdateFactory
import org.maplibre.android.geometry.LatLng
import org.maplibre.android.geometry.LatLngBounds
import org.maplibre.android.maps.MapLibreMap
import org.maplibre.android.maps.MapView
import org.maplibre.android.maps.Style
import kotlin.math.atan2
import kotlin.math.cos
import kotlin.math.sin
import kotlin.math.sqrt

data class DetailMapPoint(
    val id: String,
    val latitude: Double,
    val longitude: Double,
    val title: String,
    val isStation: Boolean,
    val isFavorite: Boolean = false,
    val amenityKey: String? = null
)

@Composable
fun MainMapView(
    features: List<GeoJsonFeature>,
    userLocation: Location?,
    favoriteStationIds: Set<String>,
    markerTint: (GeoJsonFeature) -> String,
    onFeatureTap: (GeoJsonFeature) -> Unit,
    onMapIdle: (Double, Double) -> Unit,
    onMapReady: (MapLibreMap) -> Unit,
    modifier: Modifier = Modifier
) {
    val context = LocalContext.current
    val mapView = rememberMapLibreMapViewWithLifecycle()
    var map by remember { mutableStateOf<MapLibreMap?>(null) }
    var styleReady by remember { mutableStateOf(false) }
    var lastFeatureSignature by remember { mutableStateOf<List<String>>(emptyList()) }
    var lastUserLocation by remember { mutableStateOf<Pair<Double, Double>?>(null) }
    var featureById by remember { mutableStateOf<Map<String, GeoJsonFeature>>(emptyMap()) }

    val iconFactory = remember(context) { IconFactory.getInstance(context) }
    val stationIcons = remember(context, iconFactory) {
        val keys = listOf("gold", "silver", "bronze", "gray")
        buildMap {
            for (key in keys) {
                put(key, createMapLibreCircleIcon(iconFactory, context, markerColorForKey(key), diameterDp = 16f))
            }
            put("favorite", iconFactory.fromBitmap(drawableToBitmap(createFavoriteMarkerDrawable(context))))
        }
    }
    val userIcon = remember(context, iconFactory) {
        createMapLibreCircleIcon(iconFactory, context, Color.BLUE, diameterDp = 10f, strokeDp = 1f)
    }

    AndroidView(
        modifier = modifier,
        factory = {
            mapView.apply {
                getMapAsync { mapLibreMap ->
                    map = mapLibreMap
                    mapLibreMap.uiSettings.setCompassEnabled(false)
                    mapLibreMap.uiSettings.setLogoEnabled(true)
                    mapLibreMap.uiSettings.setAttributionEnabled(true)
                    mapLibreMap.uiSettings.setDoubleTapGesturesEnabled(true)
                    mapLibreMap.uiSettings.setRotateGesturesEnabled(false)

                    mapLibreMap.setStyle(Style.Builder().fromJson(STANDARD_OSM_STYLE_JSON)) {
                        styleReady = true
                        mapLibreMap.moveCamera(
                            CameraUpdateFactory.newLatLngZoom(LatLng(51.1657, 10.4515), 6.0)
                        )
                        onMapReady(mapLibreMap)
                    }

                    mapLibreMap.setOnMarkerClickListener { marker ->
                        val snippet = marker.snippet?.toString()
                            ?: return@setOnMarkerClickListener false
                        if (!snippet.startsWith(STATION_SNIPPET_PREFIX)) return@setOnMarkerClickListener false
                        val featureId = snippet.removePrefix(STATION_SNIPPET_PREFIX)
                        val feature = featureById[featureId] ?: return@setOnMarkerClickListener false
                        onFeatureTap(feature)
                        true
                    }

                    mapLibreMap.addOnMapClickListener { latLng ->
                        val feature = nearestFeatureForTap(
                            features = featureById.values,
                            tapLat = latLng.latitude,
                            tapLon = latLng.longitude,
                            zoom = mapLibreMap.cameraPosition.zoom
                        )
                        if (feature != null) {
                            onFeatureTap(feature)
                            true
                        } else {
                            false
                        }
                    }

                    mapLibreMap.addOnCameraIdleListener {
                        val center = mapLibreMap.cameraPosition.target ?: return@addOnCameraIdleListener
                        onMapIdle(center.latitude, center.longitude)
                    }
                }
            }
        },
        update = { _ ->
            val mapLibreMap = map ?: return@AndroidView
            if (!styleReady) return@AndroidView

            val featureSignature = features.map { feature ->
                val isFavorite = favoriteStationIds.contains(feature.properties.stationId)
                "${feature.id}:${feature.latitude}:${feature.longitude}:${markerTint(feature)}:$isFavorite"
            }
            val currentUserLocation = userLocation?.let { it.latitude to it.longitude }

            if (featureSignature == lastFeatureSignature && currentUserLocation == lastUserLocation) {
                return@AndroidView
            }

            featureById = features.associateBy { it.id }
            mapLibreMap.clear()

            for (feature in features) {
                val baseIconKey = markerTint(feature)
                val iconKey = if (favoriteStationIds.contains(feature.properties.stationId)) {
                    "favorite"
                } else {
                    baseIconKey
                }
                mapLibreMap.addMarker(
                    MarkerOptions()
                        .position(LatLng(feature.latitude, feature.longitude))
                        .title(feature.properties.operatorName)
                        .snippet("$STATION_SNIPPET_PREFIX${feature.id}")
                        .icon(stationIcons[iconKey] ?: stationIcons.getValue("gray"))
                )
            }

            if (userLocation != null) {
                mapLibreMap.addMarker(
                    MarkerOptions()
                        .position(LatLng(userLocation.latitude, userLocation.longitude))
                        .title("Mein Standort")
                        .snippet(USER_SNIPPET)
                        .icon(userIcon)
                )
            }

            lastFeatureSignature = featureSignature
            lastUserLocation = currentUserLocation
        }
    )
}

@Composable
fun DetailMiniMapView(
    points: List<DetailMapPoint>,
    modifier: Modifier = Modifier
) {
    val context = LocalContext.current
    val mapView = rememberMapLibreMapViewWithLifecycle()
    var map by remember { mutableStateOf<MapLibreMap?>(null) }
    var styleReady by remember { mutableStateOf(false) }
    var lastPointSignature by remember { mutableStateOf<List<String>>(emptyList()) }

    val iconFactory = remember(context) { IconFactory.getInstance(context) }
    val stationIcon = remember(context, iconFactory) {
        createMapLibreCircleIcon(iconFactory, context, Color.rgb(0, 150, 136), diameterDp = 16f)
    }
    val favoriteStationIcon = remember(context, iconFactory) {
        iconFactory.fromBitmap(drawableToBitmap(createFavoriteMarkerDrawable(context)))
    }
    val defaultAmenityIcon = remember(context, iconFactory) {
        createMapLibreCircleIcon(iconFactory, context, Color.rgb(250, 173, 20), diameterDp = 8f)
    }
    val amenityIconsByKey = remember(context, iconFactory) {
        mutableMapOf<String, Icon>()
    }

    AndroidView(
        modifier = modifier,
        factory = {
            mapView.apply {
                getMapAsync { mapLibreMap ->
                    map = mapLibreMap
                    mapLibreMap.uiSettings.setAllGesturesEnabled(false)
                    mapLibreMap.uiSettings.setCompassEnabled(false)
                    mapLibreMap.uiSettings.setLogoEnabled(false)

                    mapLibreMap.setStyle(Style.Builder().fromJson(STANDARD_OSM_STYLE_JSON)) {
                        styleReady = true
                    }
                }
            }
        },
        update = { view ->
            val mapLibreMap = map ?: return@AndroidView
            if (!styleReady) return@AndroidView

            val signature = points.map { point ->
                "${point.id}:${point.latitude}:${point.longitude}:${point.isStation}:${point.isFavorite}:${point.amenityKey.orEmpty()}"
            }
            if (signature == lastPointSignature) {
                return@AndroidView
            }

            mapLibreMap.clear()
            for (point in points) {
                mapLibreMap.addMarker(
                    MarkerOptions()
                        .position(LatLng(point.latitude, point.longitude))
                        .title(point.title)
                        .snippet(if (point.isStation) STATION_SNIPPET else AMENITY_SNIPPET)
                        .icon(
                            if (point.isStation) {
                                if (point.isFavorite) favoriteStationIcon else stationIcon
                            } else {
                                point.amenityKey?.let { key ->
                                    amenityIconsByKey.getOrPut(key) {
                                        val resId = amenityIconResIdOrNull(key)
                                        if (resId != null) {
                                            createMapLibreResourceIcon(
                                                iconFactory = iconFactory,
                                                context = context,
                                                resId = resId,
                                                scaleFactor = 0.66f
                                            )
                                        } else {
                                            defaultAmenityIcon
                                        }
                                    }
                                } ?: defaultAmenityIcon
                            }
                        )
                )
            }

            view.post {
                if (points.isEmpty()) {
                    return@post
                }
                if (points.size == 1) {
                    val point = points.first()
                    mapLibreMap.moveCamera(
                        CameraUpdateFactory.newLatLngZoom(LatLng(point.latitude, point.longitude), 16.0)
                    )
                    return@post
                }

                val boundsBuilder = LatLngBounds.Builder()
                for (point in points) {
                    boundsBuilder.include(LatLng(point.latitude, point.longitude))
                }
                runCatching {
                    mapLibreMap.easeCamera(
                        CameraUpdateFactory.newLatLngBounds(boundsBuilder.build(), 48),
                        300
                    )
                }
            }

            lastPointSignature = signature
        }
    )
}

@Composable
private fun rememberMapLibreMapViewWithLifecycle(): MapView {
    val context = LocalContext.current
    val lifecycle = LocalLifecycleOwner.current.lifecycle
    val mapView = remember {
        MapView(context).apply {
            onCreate(null)
        }
    }

    DisposableEffect(lifecycle, mapView) {
        val observer = LifecycleEventObserver { _, event ->
            when (event) {
                Lifecycle.Event.ON_START -> mapView.onStart()
                Lifecycle.Event.ON_RESUME -> mapView.onResume()
                Lifecycle.Event.ON_PAUSE -> mapView.onPause()
                Lifecycle.Event.ON_STOP -> mapView.onStop()
                Lifecycle.Event.ON_DESTROY -> mapView.onDestroy()
                else -> Unit
            }
        }
        lifecycle.addObserver(observer)
        onDispose {
            lifecycle.removeObserver(observer)
        }
    }

    return mapView
}

private fun createMapLibreCircleIcon(
    iconFactory: IconFactory,
    context: Context,
    fillColor: Int,
    diameterDp: Float,
    strokeDp: Float = 1.5f,
    strokeColor: Int = Color.WHITE
): Icon {
    val drawable = createCircleMarkerDrawable(
        context = context,
        fillColor = fillColor,
        diameterDp = diameterDp,
        strokeDp = strokeDp,
        strokeColor = strokeColor
    )
    return iconFactory.fromBitmap(drawableToBitmap(drawable))
}

private fun createMapLibreResourceIcon(
    iconFactory: IconFactory,
    context: Context,
    resId: Int,
    scaleFactor: Float = 1f
): Icon {
    val sourceBitmap = drawableToBitmap(
        requireNotNull(context.getDrawable(resId)) {
            "Drawable not found for resId=$resId"
        }
    )
    val targetWidth = (sourceBitmap.width * scaleFactor).toInt().coerceAtLeast(1)
    val targetHeight = (sourceBitmap.height * scaleFactor).toInt().coerceAtLeast(1)
    val scaledBitmap = if (targetWidth == sourceBitmap.width && targetHeight == sourceBitmap.height) {
        sourceBitmap
    } else {
        Bitmap.createScaledBitmap(sourceBitmap, targetWidth, targetHeight, true)
    }
    return iconFactory.fromBitmap(
        scaledBitmap
    )
}

private fun drawableToBitmap(drawable: Drawable): Bitmap {
    if (drawable is BitmapDrawable && drawable.bitmap != null) {
        return drawable.bitmap
    }
    val width = drawable.intrinsicWidth.coerceAtLeast(1)
    val height = drawable.intrinsicHeight.coerceAtLeast(1)
    val bitmap = Bitmap.createBitmap(width, height, Bitmap.Config.ARGB_8888)
    val canvas = Canvas(bitmap)
    drawable.setBounds(0, 0, canvas.width, canvas.height)
    drawable.draw(canvas)
    return bitmap
}

private fun nearestFeatureForTap(
    features: Collection<GeoJsonFeature>,
    tapLat: Double,
    tapLon: Double,
    zoom: Double
): GeoJsonFeature? {
    if (features.isEmpty()) return null

    val maxDistanceMeters = when {
        zoom >= 15.0 -> 80.0
        zoom >= 13.0 -> 140.0
        zoom >= 11.0 -> 260.0
        zoom >= 9.0 -> 480.0
        else -> 720.0
    }

    return features
        .asSequence()
        .map { feature ->
            feature to distanceMeters(
                latitudeA = tapLat,
                longitudeA = tapLon,
                latitudeB = feature.latitude,
                longitudeB = feature.longitude
            )
        }
        .filter { it.second <= maxDistanceMeters }
        .minByOrNull { it.second }
        ?.first
}

private fun distanceMeters(
    latitudeA: Double,
    longitudeA: Double,
    latitudeB: Double,
    longitudeB: Double
): Double {
    val latRadA = Math.toRadians(latitudeA)
    val latRadB = Math.toRadians(latitudeB)
    val dLat = latRadB - latRadA
    val dLon = Math.toRadians(longitudeB - longitudeA)

    val a = sin(dLat / 2) * sin(dLat / 2) +
        cos(latRadA) * cos(latRadB) * sin(dLon / 2) * sin(dLon / 2)
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return EARTH_RADIUS_METERS * c
}

private const val STATION_SNIPPET_PREFIX = "station:"
private const val STATION_SNIPPET = "station"
private const val USER_SNIPPET = "user"
private const val AMENITY_SNIPPET = "amenity"
private const val EARTH_RADIUS_METERS = 6_371_000.0

private const val STANDARD_OSM_STYLE_JSON = """
{
  "version": 8,
  "name": "OpenStreetMap Standard",
  "sources": {
    "osm-standard": {
      "type": "raster",
      "tiles": ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      "tileSize": 256,
      "attribution": "© OpenStreetMap contributors"
    }
  },
  "layers": [
    {
      "id": "osm-standard",
      "type": "raster",
      "source": "osm-standard"
    }
  ]
}
"""
