import Foundation
import CoreLocation

private let catalogDetailSourceName = "woladen live-eu catalog"

struct CatalogSearchResponse: Decodable {
    let stations: [CatalogStation]
}

struct CatalogStationDetailResponse: Decodable {
    let station: CatalogStation
    let chargers: [CatalogCharger]
    let amenities: CatalogAmenities?

    func feature(preserving existing: GeoJSONFeature?) -> GeoJSONFeature {
        station.feature(chargers: chargers, amenities: amenities, preserving: existing)
    }
}

struct CatalogStation: Decodable {
    let stationID: String
    let countryCode: String
    let sourceUID: String
    let sourceStationID: String
    let license: String
    let providerUID: String
    let operatorName: String
    let stationName: String
    let address: String
    let postalCode: String
    let city: String
    let latitude: Double
    let longitude: Double
    let chargerCount: Int
    let maxPowerKW: Double
    let connectorTypes: String
    let sourceURL: String
    let publicBundleStatus: String
    let openingHours: String
    let paymentMethods: String
    let authMethods: String
    let greenEnergy: Bool?
    let greenEnergyDisplay: Bool?
    let helpdeskPhone: String
    let priceDisplay: String
    let priceCurrency: String
    let priceEnergyEURKwhMin: String
    let priceEnergyEURKwhMax: String
    let priceQuality: String
    let serviceSupport: String
    let supportsBankCard: Bool?
    let supportsContactlessCard: Bool?
    let supportsAdhocPayment: String
    let paymentProviders: String
    let supportsContractPayment: Bool?
    let detailLastUpdated: String
    let amenitiesTotal: Int
    let nearestAmenityKind: String
    let nearestAmenityName: String
    let nearestAmenityDistanceM: Double?
    let amenityCategoryCounts: [String: Int]
    let distanceM: Double?

    let availabilityStatus: AvailabilityStatus?
    let availableEVSEs: Int
    let occupiedEVSEs: Int
    let outOfOrderEVSEs: Int
    let unknownEVSEs: Int
    let totalEVSEs: Int
    let sourceObservedAt: String
    let fetchedAt: String
    let ingestedAt: String
    let dailyAnalysisDataAvailable: Bool
    let frequentlyOutOfOrderDailyAnalysis: Bool
    let frequentlyOccupiedDailyAnalysis: Bool
    let dailyAnalysisOutOfOrderColor: String
    let dailyAnalysisOccupiedColor: String

    enum CodingKeys: String, CodingKey {
        case stationID = "station_id"
        case countryCode = "country_code"
        case sourceUID = "source_uid"
        case sourceStationID = "source_station_id"
        case license
        case providerUID = "provider_uid"
        case operatorName = "operator_name"
        case stationName = "station_name"
        case address
        case postalCode = "postal_code"
        case city
        case latitude
        case longitude
        case chargerCount = "charger_count"
        case maxPowerKW = "max_power_kw"
        case connectorTypes = "connector_types"
        case sourceURL = "source_url"
        case publicBundleStatus = "public_bundle_status"
        case openingHours = "opening_hours"
        case paymentMethods = "payment_methods"
        case authMethods = "auth_methods"
        case greenEnergy = "green_energy"
        case greenEnergyDisplay = "green_energy_display"
        case helpdeskPhone = "helpdesk_phone"
        case priceDisplay = "price_display"
        case priceCurrency = "price_currency"
        case priceEnergyEURKwhMin = "price_energy_eur_kwh_min"
        case priceEnergyEURKwhMax = "price_energy_eur_kwh_max"
        case priceQuality = "price_quality"
        case serviceSupport = "service_support"
        case supportsBankCard = "supports_bank_card"
        case supportsContactlessCard = "supports_contactless_card"
        case supportsAdhocPayment = "supports_adhoc_payment"
        case paymentProviders = "payment_providers"
        case supportsContractPayment = "supports_contract_payment"
        case detailLastUpdated = "detail_last_updated"
        case amenitiesTotal = "amenities_total"
        case nearestAmenityKind = "nearest_amenity_kind"
        case nearestAmenityName = "nearest_amenity_name"
        case nearestAmenityDistanceM = "nearest_amenity_distance_m"
        case amenityCategoryCounts = "amenity_category_counts"
        case distanceM = "distance_m"
        case availabilityStatus = "availability_status"
        case availableEVSEs = "available_evses"
        case occupiedEVSEs = "occupied_evses"
        case outOfOrderEVSEs = "out_of_order_evses"
        case unknownEVSEs = "unknown_evses"
        case totalEVSEs = "total_evses"
        case sourceObservedAt = "source_observed_at"
        case fetchedAt = "fetched_at"
        case ingestedAt = "ingested_at"
        case dailyAnalysisDataAvailable = "daily_analysis_data_available"
        case frequentlyOutOfOrderDailyAnalysis = "frequently_out_of_order_daily_analysis"
        case frequentlyOccupiedDailyAnalysis = "frequently_occupied_daily_analysis"
        case dailyAnalysisOutOfOrderColor = "daily_analysis_out_of_order_color"
        case dailyAnalysisOccupiedColor = "daily_analysis_occupied_color"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        stationID = container.decodeLossyString(forKey: .stationID)
        countryCode = container.decodeLossyString(forKey: .countryCode)
        sourceUID = container.decodeLossyString(forKey: .sourceUID)
        sourceStationID = container.decodeLossyString(forKey: .sourceStationID)
        license = container.decodeLossyString(forKey: .license)
        providerUID = container.decodeLossyString(forKey: .providerUID)
        operatorName = container.decodeLossyString(forKey: .operatorName)
        stationName = container.decodeLossyString(forKey: .stationName)
        address = container.decodeLossyString(forKey: .address)
        postalCode = container.decodeLossyString(forKey: .postalCode)
        city = container.decodeLossyString(forKey: .city)
        latitude = container.decodeLossyDouble(forKey: .latitude) ?? 0
        longitude = container.decodeLossyDouble(forKey: .longitude) ?? 0
        chargerCount = container.decodeLossyInt(forKey: .chargerCount) ?? 0
        maxPowerKW = container.decodeLossyDouble(forKey: .maxPowerKW) ?? 0
        connectorTypes = container.decodeLossyString(forKey: .connectorTypes)
        sourceURL = container.decodeLossyString(forKey: .sourceURL)
        publicBundleStatus = container.decodeLossyString(forKey: .publicBundleStatus)
        openingHours = container.decodeLossyString(forKey: .openingHours)
        paymentMethods = container.decodeLossyString(forKey: .paymentMethods)
        authMethods = container.decodeLossyString(forKey: .authMethods)
        greenEnergy = container.decodeLossyBool(forKey: .greenEnergy)
        greenEnergyDisplay = container.decodeLossyBool(forKey: .greenEnergyDisplay)
        helpdeskPhone = container.decodeLossyString(forKey: .helpdeskPhone)
        priceDisplay = container.decodeLossyString(forKey: .priceDisplay)
        priceCurrency = container.decodeLossyString(forKey: .priceCurrency)
        priceEnergyEURKwhMin = container.decodeLossyString(forKey: .priceEnergyEURKwhMin)
        priceEnergyEURKwhMax = container.decodeLossyString(forKey: .priceEnergyEURKwhMax)
        priceQuality = container.decodeLossyString(forKey: .priceQuality)
        serviceSupport = container.decodeLossyString(forKey: .serviceSupport)
        supportsBankCard = container.decodeLossyBool(forKey: .supportsBankCard)
        supportsContactlessCard = container.decodeLossyBool(forKey: .supportsContactlessCard)
        supportsAdhocPayment = container.decodeLossyString(forKey: .supportsAdhocPayment)
        paymentProviders = container.decodeLossyString(forKey: .paymentProviders)
        supportsContractPayment = container.decodeLossyBool(forKey: .supportsContractPayment)
        detailLastUpdated = container.decodeLossyString(forKey: .detailLastUpdated)
        amenitiesTotal = container.decodeLossyInt(forKey: .amenitiesTotal) ?? 0
        nearestAmenityKind = container.decodeLossyString(forKey: .nearestAmenityKind)
        nearestAmenityName = container.decodeLossyString(forKey: .nearestAmenityName)
        nearestAmenityDistanceM = container.decodeLossyDouble(forKey: .nearestAmenityDistanceM)
        amenityCategoryCounts = container.decodeLossyIntDictionary(forKey: .amenityCategoryCounts)
        distanceM = container.decodeLossyDouble(forKey: .distanceM)

        let rawStatus = container.decodeLossyString(forKey: .availabilityStatus)
            .trimmingCharacters(in: .whitespacesAndNewlines)
        availabilityStatus = rawStatus.isEmpty ? nil : AvailabilityStatus(rawValue: rawStatus)
        availableEVSEs = container.decodeLossyInt(forKey: .availableEVSEs) ?? 0
        occupiedEVSEs = container.decodeLossyInt(forKey: .occupiedEVSEs) ?? 0
        outOfOrderEVSEs = container.decodeLossyInt(forKey: .outOfOrderEVSEs) ?? 0
        unknownEVSEs = container.decodeLossyInt(forKey: .unknownEVSEs) ?? 0
        totalEVSEs = container.decodeLossyInt(forKey: .totalEVSEs) ?? 0
        sourceObservedAt = container.decodeLossyString(forKey: .sourceObservedAt)
        fetchedAt = container.decodeLossyString(forKey: .fetchedAt)
        ingestedAt = container.decodeLossyString(forKey: .ingestedAt)
        dailyAnalysisDataAvailable = container.decodeLossyBool(forKey: .dailyAnalysisDataAvailable) ?? false
        frequentlyOutOfOrderDailyAnalysis = container.decodeLossyBool(forKey: .frequentlyOutOfOrderDailyAnalysis) ?? false
        frequentlyOccupiedDailyAnalysis = container.decodeLossyBool(forKey: .frequentlyOccupiedDailyAnalysis) ?? false
        dailyAnalysisOutOfOrderColor = container.decodeLossyString(forKey: .dailyAnalysisOutOfOrderColor)
        dailyAnalysisOccupiedColor = container.decodeLossyString(forKey: .dailyAnalysisOccupiedColor)
    }

    func feature(
        chargers: [CatalogCharger] = [],
        amenities: CatalogAmenities? = nil,
        preserving existing: GeoJSONFeature? = nil,
        routeMetadata: RouteStationMetadata? = nil
    ) -> GeoJSONFeature {
        let properties = chargerProperties(chargers: chargers, amenities: amenities)
        return GeoJSONFeature(
            id: properties.stationID,
            geometry: GeoJSONPointGeometry(type: "Point", coordinates: [longitude, latitude]),
            properties: properties,
            liveSummary: liveSummary ?? existing?.liveSummary,
            liveDetail: existing?.liveDetail,
            routeMetadata: routeMetadata ?? existing?.routeMetadata
        )
    }

    private var liveSummary: LiveStationSummary? {
        guard hasLiveSummarySignal else { return nil }
        return LiveStationSummary(
            stationID: stationID,
            availabilityStatus: availabilityStatus ?? .unknown,
            availableEVSEs: availableEVSEs,
            occupiedEVSEs: occupiedEVSEs,
            outOfOrderEVSEs: outOfOrderEVSEs,
            unknownEVSEs: unknownEVSEs,
            totalEVSEs: totalEVSEs,
            priceDisplay: priceDisplay,
            priceCurrency: priceCurrency,
            priceEnergyEURKwhMin: priceEnergyEURKwhMin,
            priceEnergyEURKwhMax: priceEnergyEURKwhMax,
            sourceObservedAt: sourceObservedAt,
            fetchedAt: fetchedAt,
            ingestedAt: ingestedAt,
            dailyAnalysisDataAvailable: dailyAnalysisDataAvailable,
            frequentlyOutOfOrderDailyAnalysis: frequentlyOutOfOrderDailyAnalysis,
            frequentlyOccupiedDailyAnalysis: frequentlyOccupiedDailyAnalysis,
            dailyAnalysisOutOfOrderColor: dailyAnalysisOutOfOrderColor,
            dailyAnalysisOccupiedColor: dailyAnalysisOccupiedColor
        )
    }

    private var hasLiveSummarySignal: Bool {
        availabilityStatus != nil ||
        availableEVSEs > 0 ||
        occupiedEVSEs > 0 ||
        outOfOrderEVSEs > 0 ||
        unknownEVSEs > 0 ||
        totalEVSEs > 0 ||
        !sourceObservedAt.isEmpty ||
        !fetchedAt.isEmpty ||
        !ingestedAt.isEmpty ||
        dailyAnalysisDataAvailable ||
        frequentlyOutOfOrderDailyAnalysis ||
        frequentlyOccupiedDailyAnalysis ||
        !dailyAnalysisOutOfOrderColor.isEmpty ||
        !dailyAnalysisOccupiedColor.isEmpty
    }

    private func chargerProperties(chargers: [CatalogCharger], amenities: CatalogAmenities?) -> ChargerProperties {
        let normalizedCounts = normalizedAmenityCounts(amenities?.amenityCategoryCounts ?? amenityCategoryCounts)
        let detailExamples = amenities?.amenityExamples ?? []
        let examples = detailExamples.isEmpty ? nearestAmenityExamples() : detailExamples
        let chargerTotal = chargers.isEmpty ? chargerCount : chargers.count
        let maxChargerPower = chargers.map(\.maxPowerKW).filter { $0 > 0 }.max() ?? 0
        let effectiveMaxPower = max(maxPowerKW, maxChargerPower)
        let effectiveTotalEVSEs = totalEVSEs > 0 ? totalEVSEs : chargerTotal

        return ChargerProperties(
            stationID: stationID,
            operatorName: firstNonEmpty(operatorName, stationName, "Unbekannter Betreiber"),
            status: publicBundleStatus,
            maxPowerKW: effectiveMaxPower,
            chargingPointsCount: max(chargerTotal, 1),
            maxIndividualPowerKW: effectiveMaxPower,
            postcode: postalCode,
            city: city,
            address: address,
            occupancySourceUID: providerUID,
            occupancySourceName: firstNonEmpty(providerUID, sourceUID),
            occupancyStatus: availabilityStatus?.rawValue ?? "",
            occupancyLastUpdated: firstNonEmpty(sourceObservedAt, fetchedAt, ingestedAt),
            occupancyTotalEVSEs: effectiveTotalEVSEs,
            occupancyAvailableEVSEs: availableEVSEs,
            occupancyOccupiedEVSEs: occupiedEVSEs,
            occupancyChargingEVSEs: 0,
            occupancyOutOfOrderEVSEs: outOfOrderEVSEs,
            occupancyUnknownEVSEs: unknownEVSEs,
            detailSourceUID: sourceUID,
            detailSourceName: catalogDetailSourceName,
            detailLastUpdated: detailLastUpdated,
            datexSiteID: sourceStationID,
            datexStationIDs: sourceStationID,
            datexChargePointIDs: chargers.map(\.sourceEVSEID).filter { !$0.isEmpty }.joined(separator: ", "),
            priceDisplay: priceDisplay,
            priceEnergyEURKwhMin: priceEnergyEURKwhMin,
            priceEnergyEURKwhMax: priceEnergyEURKwhMax,
            priceCurrency: priceCurrency,
            priceQuality: priceQuality,
            openingHoursDisplay: openingHours,
            openingHoursIs24_7: isAlwaysOpen(openingHours),
            helpdeskPhone: helpdeskPhone,
            paymentMethodsDisplay: uniqueDisplayValues([
                formattedCSV(paymentMethods),
                formattedCSV(paymentProviders),
                formattedCSV(supportsAdhocPayment)
            ]).joined(separator: ", "),
            authMethodsDisplay: formattedCSV(authMethods),
            connectorTypesDisplay: connectorDisplay(chargers: chargers),
            currentTypesDisplay: currentTypeDisplay(chargers: chargers),
            connectorCount: chargerTotal,
            greenEnergy: greenEnergy ?? greenEnergyDisplay,
            serviceTypesDisplay: serviceSupport,
            detailsJSON: "",
            amenitiesTotal: max(amenities?.amenitiesTotal ?? amenitiesTotal, normalizedCounts.values.reduce(0, +)),
            amenitiesSource: catalogDetailSourceName,
            amenityExamples: examples,
            amenityCounts: normalizedCounts
        )
    }

    private func nearestAmenityExamples() -> [AmenityExample] {
        let category = normalizedAmenityCategory(nearestAmenityKind)
        guard category != "other" || !nearestAmenityName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return []
        }
        return [
            AmenityExample(
                category: category,
                name: nearestAmenityName.isEmpty ? nil : nearestAmenityName,
                openingHours: nil,
                distanceM: nearestAmenityDistanceM,
                lat: nil,
                lon: nil
            )
        ]
    }

    private func connectorDisplay(chargers: [CatalogCharger]) -> String {
        if !connectorTypes.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return formattedCSV(connectorTypes)
        }
        return uniqueDisplayValues(chargers.map(\.connectorType)).joined(separator: ", ")
    }

    private func currentTypeDisplay(chargers: [CatalogCharger]) -> String {
        uniqueDisplayValues(chargers.map(\.currentType)).joined(separator: ", ")
    }
}

struct CatalogCharger: Decodable {
    let chargerID: String
    let sourceUID: String
    let providerUID: String
    let sourceStationID: String
    let sourceEVSEID: String
    let connectorID: String
    let connectorType: String
    let currentType: String
    let maxPowerKW: Double
    let operatorName: String

    enum CodingKeys: String, CodingKey {
        case chargerID = "charger_id"
        case sourceUID = "source_uid"
        case providerUID = "provider_uid"
        case sourceStationID = "source_station_id"
        case sourceEVSEID = "source_evse_id"
        case connectorID = "connector_id"
        case connectorType = "connector_type"
        case currentType = "current_type"
        case maxPowerKW = "max_power_kw"
        case operatorName = "operator_name"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        chargerID = container.decodeLossyString(forKey: .chargerID)
        sourceUID = container.decodeLossyString(forKey: .sourceUID)
        providerUID = container.decodeLossyString(forKey: .providerUID)
        sourceStationID = container.decodeLossyString(forKey: .sourceStationID)
        sourceEVSEID = container.decodeLossyString(forKey: .sourceEVSEID)
        connectorID = container.decodeLossyString(forKey: .connectorID)
        connectorType = formattedCode(container.decodeLossyString(forKey: .connectorType))
        currentType = formattedCode(container.decodeLossyString(forKey: .currentType))
        maxPowerKW = container.decodeLossyDouble(forKey: .maxPowerKW) ?? 0
        operatorName = container.decodeLossyString(forKey: .operatorName)
    }
}

struct CatalogAmenities: Decodable {
    let amenitiesTotal: Int
    let amenityCategoryCounts: [String: Int]
    let amenityExamples: [AmenityExample]

    enum CodingKeys: String, CodingKey {
        case amenitiesTotal = "amenities_total"
        case amenityCategoryCounts = "amenity_category_counts"
        case amenityExamples = "amenity_examples"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        amenitiesTotal = container.decodeLossyInt(forKey: .amenitiesTotal) ?? 0
        amenityCategoryCounts = container.decodeLossyIntDictionary(forKey: .amenityCategoryCounts)
        amenityExamples = (try? container.decode([AmenityExample].self, forKey: .amenityExamples)) ?? []
    }
}

struct CatalogInfoSummary: Decodable {
    let openStaticSummary: OpenStaticSummary?
    let buildSummary: BundleBuildSummary?

    enum CodingKeys: String, CodingKey {
        case openStaticSummary = "open_static_summary"
        case openStaticSummaryCamel = "openStaticSummary"
        case summary
        case buildSummary = "build_summary"
        case bundle
        case countries
        case sources
    }

    init(openStaticSummary: OpenStaticSummary?, buildSummary: BundleBuildSummary?) {
        self.openStaticSummary = openStaticSummary
        self.buildSummary = buildSummary
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        if container.contains(.bundle) || container.contains(.countries) || container.contains(.sources) {
            openStaticSummary = try OpenStaticSummary(from: decoder)
            buildSummary = nil
        } else {
            openStaticSummary =
                (try? container.decode(OpenStaticSummary.self, forKey: .openStaticSummary)) ??
                (try? container.decode(OpenStaticSummary.self, forKey: .openStaticSummaryCamel))
            buildSummary =
                (try? container.decode(BundleBuildSummary.self, forKey: .summary)) ??
                (try? container.decode(BundleBuildSummary.self, forKey: .buildSummary))
        }
    }

    var generatedAt: String {
        firstNonEmpty(openStaticSummary?.generatedAt ?? "", buildSummary?.run?.finishedAt ?? "")
    }

    var stationCount: Int {
        if let count = openStaticSummary?.bundle?.stationCount, count > 0 {
            return count
        }
        let countryTotal = countries.reduce(0) { $0 + $1.stationCount }
        if countryTotal > 0 {
            return countryTotal
        }
        return buildSummary?.records?.fullRegistryActiveStationsTotal ?? 0
    }

    var chargerCount: Int {
        if let count = openStaticSummary?.bundle?.chargerCount, count > 0 {
            return count
        }
        let countryTotal = countries.reduce(0) { $0 + $1.chargerCount }
        if countryTotal > 0 {
            return countryTotal
        }
        return buildSummary?.records?.rawRows ?? 0
    }

    var countries: [OpenStaticCountry] {
        openStaticSummary?.countries ?? []
    }

    var sources: [OpenStaticSource] {
        openStaticSummary?.normalizedSources ?? []
    }

    func sortedCountries(locale: Locale = .current) -> [OpenStaticCountry] {
        countries.sorted { lhs, rhs in
            let leftName = lhs.localizedName(locale: locale)
            let rightName = rhs.localizedName(locale: locale)
            if leftName == rightName {
                return lhs.code < rhs.code
            }
            return leftName.localizedCompare(rightName) == .orderedAscending
        }
    }

    func countrySourceLinks(for countryCode: String) -> [InfoSourceLink] {
        let code = countryCode.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        if code == "DE" {
            return [
                InfoSourceLink(
                    label: "Mobilithek",
                    urlString: "https://mobilithek.info/offers/842113170303512576"
                )
            ]
        }

        var seen: Set<String> = []
        return sources
            .filter { $0.countryCode == code }
            .compactMap { source in
                let label = source.compactCountrySourceLabel
                guard !label.isEmpty else { return nil }
                let key = "\(label)\u{1}\(source.sourceURL)"
                guard seen.insert(key).inserted else { return nil }
                return InfoSourceLink(label: label, urlString: source.sourceURL)
            }
    }

    func dataSourceLinks(locale: Locale = .current) -> [InfoSourceLink] {
        var seen: Set<String> = []
        return sources
            .sorted { lhs, rhs in
                let left = "\(lhs.countryCode):\(lhs.displayName)"
                let right = "\(rhs.countryCode):\(rhs.displayName)"
                return left.localizedCompare(right) == .orderedAscending
            }
            .compactMap { source in
                let label = source.bundleSourceTitle
                guard !label.isEmpty else { return nil }
                let key = "\(label)\u{1}\(source.sourceURL)"
                guard seen.insert(key).inserted else { return nil }
                return InfoSourceLink(label: label, urlString: source.sourceURL)
            }
    }
}

struct OpenStaticSummary: Decodable {
    let bundle: OpenStaticBundle?
    let countries: [OpenStaticCountry]
    let generatedAt: String
    let schemaVersion: Int
    private let rawSources: [OpenStaticSource]

    enum CodingKeys: String, CodingKey {
        case bundle
        case countries
        case generatedAt = "generated_at"
        case schemaVersion = "schema_version"
        case sources
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        bundle = try? container.decode(OpenStaticBundle.self, forKey: .bundle)
        countries = (try? container.decode([OpenStaticCountry].self, forKey: .countries)) ?? []
        generatedAt = container.decodeLossyString(forKey: .generatedAt)
        schemaVersion = container.decodeLossyInt(forKey: .schemaVersion) ?? 0
        rawSources = (try? container.decode([OpenStaticSource].self, forKey: .sources)) ?? []
    }

    var normalizedSources: [OpenStaticSource] {
        var seen: Set<String> = []
        return rawSources.filter { source in
            let key = [
                source.countryCode,
                source.sourceUID,
                source.sourceURL,
                source.displayName
            ].joined(separator: "\u{1}")
            guard !seen.contains(key) else { return false }
            seen.insert(key)
            return !source.countryCode.isEmpty || !source.displayName.isEmpty || !source.sourceURL.isEmpty
        }
    }
}

struct OpenStaticBundle: Decodable {
    let stationCount: Int
    let chargerCount: Int
    let countryCount: Int
    let schemaVersion: Int

    enum CodingKeys: String, CodingKey {
        case stationCount = "station_count"
        case chargerCount = "charger_count"
        case countryCount = "country_count"
        case schemaVersion = "schema_version"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        stationCount = container.decodeLossyInt(forKey: .stationCount) ?? 0
        chargerCount = container.decodeLossyInt(forKey: .chargerCount) ?? 0
        countryCount = container.decodeLossyInt(forKey: .countryCount) ?? 0
        schemaVersion = container.decodeLossyInt(forKey: .schemaVersion) ?? 0
    }
}

struct OpenStaticCountry: Decodable, Identifiable {
    let code: String
    let name: String
    let stationCount: Int
    let chargerCount: Int
    let fastStationCount: Int

    var id: String { code }

    enum CodingKeys: String, CodingKey {
        case code
        case countryCode = "country_code"
        case name
        case countryName = "country_name"
        case stationCount = "station_count"
        case stationCountCamel = "stationCount"
        case stations
        case chargerCount = "charger_count"
        case chargerCountCamel = "chargerCount"
        case chargers
        case fastStationCount = "fast_station_count"
        case fastStationCountCamel = "fastStationCount"
        case fastStations = "fast_stations"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        code = firstNonEmpty(
            container.decodeLossyString(forKey: .code),
            container.decodeLossyString(forKey: .countryCode)
        ).uppercased()
        name = firstNonEmpty(
            container.decodeLossyString(forKey: .name),
            container.decodeLossyString(forKey: .countryName)
        )
        stationCount =
            container.decodeLossyInt(forKey: .stationCount) ??
            container.decodeLossyInt(forKey: .stationCountCamel) ??
            container.decodeLossyInt(forKey: .stations) ??
            0
        chargerCount =
            container.decodeLossyInt(forKey: .chargerCount) ??
            container.decodeLossyInt(forKey: .chargerCountCamel) ??
            container.decodeLossyInt(forKey: .chargers) ??
            0
        fastStationCount =
            container.decodeLossyInt(forKey: .fastStationCount) ??
            container.decodeLossyInt(forKey: .fastStationCountCamel) ??
            container.decodeLossyInt(forKey: .fastStations) ??
            0
    }

    func localizedName(locale: Locale = .current) -> String {
        locale.localizedString(forRegionCode: code) ?? firstNonEmpty(name, code)
    }
}

struct OpenStaticSource: Decodable {
    let countryCode: String
    let sourceUID: String
    let displayName: String
    let sourceURL: String
    let license: String
    let licenseURL: String

    enum CodingKeys: String, CodingKey {
        case countryCode = "country_code"
        case countryCodeCamel = "countryCode"
        case sourceUID = "source_uid"
        case sourceUIDCamel = "sourceUid"
        case displayName = "display_name"
        case displayNameCamel = "displayName"
        case sourceName = "source_name"
        case sourceNameCamel = "sourceName"
        case sourceURL = "source_url"
        case sourceURLCamel = "sourceUrl"
        case url
        case license
        case licenseURL = "license_url"
        case licenseURLCamel = "licenseUrl"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        countryCode = firstNonEmpty(
            container.decodeLossyString(forKey: .countryCode),
            container.decodeLossyString(forKey: .countryCodeCamel)
        ).uppercased()
        sourceUID = firstNonEmpty(
            container.decodeLossyString(forKey: .sourceUID),
            container.decodeLossyString(forKey: .sourceUIDCamel)
        )
        displayName = firstNonEmpty(
            container.decodeLossyString(forKey: .displayName),
            container.decodeLossyString(forKey: .displayNameCamel),
            container.decodeLossyString(forKey: .sourceName),
            container.decodeLossyString(forKey: .sourceNameCamel),
            sourceUID
        )
        sourceURL = normalizedSourceURL(
            firstNonEmpty(
                container.decodeLossyString(forKey: .sourceURL),
                container.decodeLossyString(forKey: .sourceURLCamel),
                container.decodeLossyString(forKey: .url)
            )
        )
        license = container.decodeLossyString(forKey: .license)
        licenseURL = firstNonEmpty(
            container.decodeLossyString(forKey: .licenseURL),
            container.decodeLossyString(forKey: .licenseURLCamel)
        )
    }

    var bundleSourceTitle: String {
        let rawLabel = firstNonEmpty(displayName, sourceUID, sourceURL, "Datenquelle")
        let label = stripLeadingCountryCode(rawLabel, countryCode)
        return countryCode.isEmpty ? label : "\(countryCode): \(label)"
    }

    var compactCountrySourceLabel: String {
        stripLeadingCountryCode(firstNonEmpty(displayName, bundleSourceTitle), countryCode)
    }
}

struct InfoSourceLink: Identifiable, Hashable {
    let label: String
    let urlString: String

    var id: String { "\(label)\u{1}\(urlString)" }

    var url: URL? {
        let trimmed = urlString.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        return URL(string: trimmed)
    }
}

struct BundleBuildSummary: Decodable {
    let run: BundleBuildRun?
    let records: BundleBuildRecords?
}

struct BundleBuildRun: Decodable {
    let startedAt: String
    let finishedAt: String

    enum CodingKeys: String, CodingKey {
        case startedAt = "started_at"
        case finishedAt = "finished_at"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        startedAt = container.decodeLossyString(forKey: .startedAt)
        finishedAt = container.decodeLossyString(forKey: .finishedAt)
    }
}

struct BundleBuildRecords: Decodable {
    let rawRows: Int
    let fullRegistryActiveStationsTotal: Int

    enum CodingKeys: String, CodingKey {
        case rawRows = "raw_rows"
        case fullRegistryActiveStationsTotal = "full_registry_active_stations_total"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        rawRows = container.decodeLossyInt(forKey: .rawRows) ?? 0
        fullRegistryActiveStationsTotal = container.decodeLossyInt(forKey: .fullRegistryActiveStationsTotal) ?? 0
    }
}

private func normalizedAmenityCounts(_ raw: [String: Int]) -> [String: Int] {
    var counts: [String: Int] = [:]
    for (key, value) in raw where value > 0 {
        counts[normalizedAmenityKey(key), default: 0] += value
    }
    return counts
}

private func formattedCSV(_ value: String) -> String {
    uniqueDisplayValues(
        value
            .split(separator: ",")
            .map(String.init)
    ).joined(separator: ", ")
}

private func uniqueDisplayValues(_ values: [String]) -> [String] {
    var seen: Set<String> = []
    var result: [String] = []
    for value in values {
        let formatted = formattedCode(value)
        guard !formatted.isEmpty else { continue }
        let key = formatted.lowercased()
        guard seen.insert(key).inserted else { continue }
        result.append(formatted)
    }
    return result
}

private func formattedCode(_ value: String) -> String {
    value
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .replacingOccurrences(of: "_", with: " ")
        .replacingOccurrences(of: "-", with: " ")
}

private func isAlwaysOpen(_ value: String) -> Bool {
    let normalized = value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return ["24/7", "24x7", "always open"].contains(normalized)
}

private func firstNonEmpty(_ values: String...) -> String {
    for value in values {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmed.isEmpty {
            return trimmed
        }
    }
    return ""
}

private func normalizedSourceURL(_ value: String) -> String {
    value
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .replacingOccurrences(of: "/+$", with: "", options: .regularExpression)
}

private func stripLeadingCountryCode(_ label: String, _ countryCode: String) -> String {
    let trimmed = label.trimmingCharacters(in: .whitespacesAndNewlines)
    let code = countryCode.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty, !code.isEmpty else { return trimmed }
    let pattern = "^\(NSRegularExpression.escapedPattern(for: code))(?:\\s*:\\s*|\\s+)"
    return trimmed
        .replacingOccurrences(of: pattern, with: "", options: [.regularExpression, .caseInsensitive])
        .trimmingCharacters(in: .whitespacesAndNewlines)
}

private extension KeyedDecodingContainer {
    func decodeLossyString(forKey key: Key) -> String {
        if let value = try? decode(String.self, forKey: key) {
            return value
        }
        if let value = try? decode(Double.self, forKey: key) {
            return String(value)
        }
        if let value = try? decode(Int.self, forKey: key) {
            return String(value)
        }
        if let value = try? decode(Bool.self, forKey: key) {
            return value ? "true" : "false"
        }
        return ""
    }

    func decodeLossyInt(forKey key: Key) -> Int? {
        if let value = try? decode(Int.self, forKey: key) {
            return value
        }
        if let value = try? decode(Double.self, forKey: key) {
            return Int(value)
        }
        if let value = try? decode(String.self, forKey: key) {
            let normalized = value.replacingOccurrences(of: ",", with: ".")
            if let integer = Int(normalized) {
                return integer
            }
            if let double = Double(normalized) {
                return Int(double)
            }
        }
        return nil
    }

    func decodeLossyDouble(forKey key: Key) -> Double? {
        if let value = try? decode(Double.self, forKey: key) {
            return value
        }
        if let value = try? decode(Int.self, forKey: key) {
            return Double(value)
        }
        if let value = try? decode(String.self, forKey: key) {
            return Double(value.replacingOccurrences(of: ",", with: "."))
        }
        return nil
    }

    func decodeLossyBool(forKey key: Key) -> Bool? {
        if let value = try? decode(Bool.self, forKey: key) {
            return value
        }
        if let value = try? decode(Int.self, forKey: key) {
            return value != 0
        }
        if let value = try? decode(String.self, forKey: key) {
            switch value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
            case "1", "true", "yes", "y", "ja":
                return true
            case "0", "false", "no", "n", "nein":
                return false
            default:
                return nil
            }
        }
        return nil
    }

    func decodeLossyIntDictionary(forKey key: Key) -> [String: Int] {
        if let value = try? decode([String: Int].self, forKey: key) {
            return value
        }
        if let value = try? decode([String: Double].self, forKey: key) {
            return value.mapValues(Int.init)
        }
        if let value = try? decode([String: String].self, forKey: key) {
            return value.reduce(into: [:]) { result, item in
                let normalized = item.value.replacingOccurrences(of: ",", with: ".")
                if let integer = Int(normalized) {
                    result[item.key] = integer
                } else if let double = Double(normalized) {
                    result[item.key] = Int(double)
                } else {
                    result[item.key] = 0
                }
            }
        }
        return [:]
    }
}
