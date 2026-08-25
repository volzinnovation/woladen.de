package de.woladen.android.model

data class CatalogSearchResponse(
    val stations: List<CatalogStation>,
    val source: String,
    val returnedCount: Int
)

data class CatalogStationDetail(
    val station: CatalogStation,
    val chargers: List<CatalogCharger>,
    val amenitiesTotal: Int?,
    val amenityCounts: Map<String, Int>,
    val amenityExamples: List<AmenityExample>
)

data class CatalogStation(
    val stationId: String,
    val countryCode: String,
    val sourceUid: String,
    val sourceStationId: String,
    val license: String,
    val providerUid: String,
    val operatorName: String,
    val stationName: String,
    val address: String,
    val postalCode: String,
    val city: String,
    val latitude: Double,
    val longitude: Double,
    val chargerCount: Int,
    val maxPowerKw: Double?,
    val connectorTypes: String,
    val sourceUrl: String,
    val publicBundleStatus: String,
    val openingHours: String,
    val paymentMethods: String,
    val authMethods: String,
    val greenEnergy: Boolean?,
    val greenEnergyDisplay: Boolean?,
    val helpdeskPhone: String,
    val priceDisplay: String,
    val priceEnergyEurKwhMin: Double?,
    val priceEnergyEurKwhMax: Double?,
    val priceCurrency: String,
    val priceQuality: String,
    val serviceSupport: String,
    val supportsBankCard: Boolean?,
    val supportsContactlessCard: Boolean?,
    val supportsAdhocPayment: String,
    val paymentProviders: String,
    val supportsContractPayment: Boolean?,
    val detailLastUpdated: String,
    val amenitiesTotal: Int,
    val amenityCounts: Map<String, Int>,
    val nearestAmenityKind: String,
    val nearestAmenityName: String,
    val nearestAmenityDistanceM: Double?,
    val liveSummary: LiveStationSummary?
)

data class CatalogCharger(
    val chargerId: String,
    val sourceUid: String,
    val providerUid: String,
    val sourceStationId: String,
    val sourceEvseId: String,
    val connectorId: String,
    val connectorType: String,
    val currentType: String,
    val maxPowerKw: Double?,
    val operatorName: String,
    val license: String,
    val sourceUrl: String,
    val publicBundleStatus: String
)
