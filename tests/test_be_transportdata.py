from __future__ import annotations

import gzip
import io
import json

from commercial_backend.be_transportdata import (
    ECO_MOVEMENT_STATIC_SOURCE_UID,
    ECO_MOVEMENT_STATIC_URL,
    ENERGYVISION_LOCATIONS_SOURCE_UID,
    ENERGYVISION_STAGING_BASE_URL,
    ENERGYVISION_STAGING_OCPI_VERSION,
    ENERGYVISION_TARIFFS_SOURCE_UID,
    GROUP_INDIGO_STATIC_SOURCE_UID,
    GROUP_INDIGO_STATIC_URL,
    MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
    MONTA_AFIR_CHARGE_POINTS_URL,
    ROAD_OCPI_LOCATIONS_SOURCE_UID,
    ROAD_OCPI_LOCATIONS_URL,
    SOURCE_REGISTRY_BY_UID,
    attach_energyvision_tariff_prices,
    energyvision_module_url,
    energyvision_tariff_price_lookup,
    iter_eco_movement_static_rows_from_binary_stream,
    iter_energyvision_location_rows_from_binary_stream,
    iter_group_indigo_static_rows_from_binary_stream,
    iter_monta_charge_point_rows_from_binary_stream,
    parse_eco_movement_static_payload,
    parse_energyvision_locations_payload,
    parse_energyvision_tariffs_payload,
    parse_monta_charge_points_payload,
    parse_road_locations_payload,
)


def _binary_stream(payload: object, *, gzip_payload: bool = False) -> io.BufferedReader:
    raw = json.dumps(payload).encode("utf-8")
    if gzip_payload:
        raw = gzip.compress(raw)
    return io.BufferedReader(io.BytesIO(raw))


def test_be_registry_keeps_provider_urls_credential_safe():
    assert ENERGYVISION_LOCATIONS_SOURCE_UID in SOURCE_REGISTRY_BY_UID
    assert ENERGYVISION_TARIFFS_SOURCE_UID in SOURCE_REGISTRY_BY_UID
    assert ECO_MOVEMENT_STATIC_SOURCE_UID in SOURCE_REGISTRY_BY_UID
    assert MONTA_AFIR_CHARGE_POINTS_SOURCE_UID in SOURCE_REGISTRY_BY_UID
    assert ROAD_OCPI_LOCATIONS_SOURCE_UID in SOURCE_REGISTRY_BY_UID
    assert GROUP_INDIGO_STATIC_SOURCE_UID in SOURCE_REGISTRY_BY_UID

    eco_source = SOURCE_REGISTRY_BY_UID[ECO_MOVEMENT_STATIC_SOURCE_UID]
    assert eco_source.endpoint_url == ECO_MOVEMENT_STATIC_URL
    assert "token=" not in eco_source.endpoint_url.lower()
    assert "TRANSPORTDATA_BE_ECO_MOVEMENT_TOKEN" in eco_source.credential_env
    assert eco_source.access_status == "token_not_available_public_token_not_tracked"

    monta_source = SOURCE_REGISTRY_BY_UID[MONTA_AFIR_CHARGE_POINTS_SOURCE_UID]
    assert monta_source.endpoint_url == MONTA_AFIR_CHARGE_POINTS_URL
    assert "TRANSPORTDATA_BE_MONTA_CLIENT_ID" in monta_source.credential_env
    assert "TRANSPORTDATA_BE_MONTA_CLIENT_SECRET" in monta_source.credential_env

    road_source = SOURCE_REGISTRY_BY_UID[ROAD_OCPI_LOCATIONS_SOURCE_UID]
    assert road_source.endpoint_url == ROAD_OCPI_LOCATIONS_URL
    assert road_source.credential_env == ()
    assert road_source.access_status == "no_auth_json_verified_2026_06_07"

    indigo_source = SOURCE_REGISTRY_BY_UID[GROUP_INDIGO_STATIC_SOURCE_UID]
    assert indigo_source.endpoint_url == GROUP_INDIGO_STATIC_URL
    assert indigo_source.credential_env == ()
    assert indigo_source.private_dynamic_bundle_status == "not_applicable_static_only"


def test_energyvision_staging_module_urls_are_explicitly_versioned():
    assert energyvision_module_url(
        base_url=ENERGYVISION_STAGING_BASE_URL,
        ocpi_version=ENERGYVISION_STAGING_OCPI_VERSION,
        module="locations",
    ) == "https://ocpi.myev-dev.be/cpo/2.2.1/locations/"


def test_energyvision_locations_parser_preserves_static_and_dynamic_fields():
    payload = {
        "data": [
            {
                "id": "LOC-1",
                "party_id": "EVB",
                "name": "EnergyVision Test Hub",
                "operator": {"name": "EnergyVision"},
                "address": "Stationstraat 1",
                "city": "Brussels",
                "postal_code": "1000",
                "coordinates": {"latitude": "50.8503", "longitude": "4.3517"},
                "opening_times": {"twentyfourseven": True},
                "last_updated": "2026-04-30T10:00:00Z",
                "evses": [
                    {
                        "uid": "1",
                        "evse_id": "BE*EVB*E1",
                        "status": "AVAILABLE",
                        "last_updated": "2026-04-30T10:01:00Z",
                        "connectors": [
                            {
                                "id": "1",
                                "standard": "IEC_62196_T2_COMBO",
                                "power_type": "DC",
                                "voltage": 500,
                                "amperage": 300,
                                "tariff_ids": ["tariff-a"],
                            }
                        ],
                    }
                ],
            }
        ]
    }

    rows = parse_energyvision_locations_payload(payload)

    assert len(rows) == 1
    row = rows[0]
    assert row["source_uid"] == ENERGYVISION_LOCATIONS_SOURCE_UID
    assert row["station_id"] == "be:be_energyvision_ocpi_locations:evb-loc-1"
    assert row["charger_id"] == "be:be_energyvision_ocpi_locations:evse:be-evb-e1"
    assert row["availability_status"] == "free"
    assert row["station_name"] == "EnergyVision Test Hub"
    assert row["address"] == "Stationstraat 1"
    assert row["connector_id"] == "1"
    assert row["current_type"] == "DC"
    assert row["opening_hours"] == "24/7"
    assert row["max_power_kw"] == 150.0
    assert row["connector_types"] == "IEC_62196_T2_COMBO"
    assert row["tariff_ids"] == "tariff-a"
    assert row["raw_static"]


def test_energyvision_locations_streaming_parser_handles_gzip():
    payload = {
        "data": [
            {
                "id": "LOC-2",
                "party_id": "EVB",
                "evses": [{"evse_id": "BE*EVB*E2", "status": "OUTOFORDER"}],
            }
        ]
    }

    rows = list(iter_energyvision_location_rows_from_binary_stream(_binary_stream(payload, gzip_payload=True)))

    assert len(rows) == 1
    assert rows[0]["availability_status"] == "out_of_order"


def test_energyvision_locations_parser_accepts_ocpi_2_2_1_shape():
    payload = {
        "data": [
            {
                "country_code": "BE",
                "party_id": "EVB",
                "id": "LOC-221",
                "publish": True,
                "operator": {"name": "EnergyVision"},
                "address": "OCPI staging 1",
                "city": "Ghent",
                "postal_code": "9000",
                "coordinates": {"latitude": "51.0543", "longitude": "3.7174"},
                "last_updated": "2026-05-06T10:00:00Z",
                "evses": [
                    {
                        "uid": "EVSE-221",
                        "evse_id": "BE*EVB*E221",
                        "status": "CHARGING",
                        "connectors": [
                            {
                                "id": "1",
                                "standard": "IEC_62196_T2_COMBO",
                                "format": "CABLE",
                                "power_type": "DC",
                                "max_electric_power": 300000,
                                "tariff_ids": ["BE-EVB-T-221"],
                                "last_updated": "2026-05-06T10:00:00Z",
                            }
                        ],
                        "last_updated": "2026-05-06T10:00:00Z",
                    }
                ],
            }
        ],
        "status_code": 1000,
        "timestamp": "2026-05-06T10:00:00Z",
    }

    rows = parse_energyvision_locations_payload(payload)

    assert len(rows) == 1
    assert rows[0]["station_id"] == "be:be_energyvision_ocpi_locations:evb-loc-221"
    assert rows[0]["charger_id"] == "be:be_energyvision_ocpi_locations:evse:be-evb-e221"
    assert rows[0]["availability_status"] == "occupied"
    assert rows[0]["max_power_kw"] == 300.0
    assert rows[0]["tariff_ids"] == "BE-EVB-T-221"


def test_road_locations_parser_preserves_current_status_rows():
    payload = {
        "data": [
            {
                "country_code": "BE",
                "party_id": "EFL",
                "id": "LOC-1",
                "operator": {"name": "Road"},
                "address": "Roadstraat 1",
                "city": "Brussels",
                "postal_code": "1000",
                "coordinates": {"latitude": "50.8503", "longitude": "4.3517"},
                "last_updated": "2026-06-07T12:13:46.164Z",
                "evses": [
                    {
                        "uid": "EVSE-1",
                        "evse_id": "BE*EFL*E1",
                        "status": "CHARGING",
                        "last_updated": "2026-06-07T12:13:46.164Z",
                        "connectors": [
                            {
                                "id": "1",
                                "standard": "IEC_62196_T2_COMBO",
                                "power_type": "DC",
                                "max_electric_power": 150000,
                            }
                        ],
                    }
                ],
            }
        ]
    }

    rows = parse_road_locations_payload(payload)

    assert len(rows) == 1
    assert rows[0]["source_uid"] == ROAD_OCPI_LOCATIONS_SOURCE_UID
    assert rows[0]["station_id"] == "be:be_road_ocpi_locations:efl-loc-1"
    assert rows[0]["charger_id"] == "be:be_road_ocpi_locations:evse:be-efl-e1"
    assert rows[0]["availability_status"] == "occupied"
    assert rows[0]["source_observed_at"] == "2026-06-07T12:13:46.164Z"
    assert rows[0]["max_power_kw"] == 150.0


def test_energyvision_ac_three_phase_power_uses_phase_voltage():
    payload = {
        "data": [
            {
                "id": "LOC-3P",
                "party_id": "EVB",
                "evses": [
                    {
                        "evse_id": "BE*EVB*E3P",
                        "connectors": [
                            {
                                "standard": "IEC_62196_T2",
                                "power_type": "AC_3_PHASE",
                                "voltage": 230,
                                "amperage": 32,
                            }
                        ],
                    }
                ],
            }
        ]
    }

    rows = parse_energyvision_locations_payload(payload)

    assert rows[0]["max_power_kw"] == 22.08


def test_energyvision_tariffs_parser_keeps_raw_tariff_details():
    payload = {
        "data": [
            {
                "id": "tariff-a",
                "currency": "EUR",
                "type": "REGULAR",
                "last_updated": "2026-04-30T10:00:00Z",
                "elements": [{"price_components": [{"type": "ENERGY", "price": 0.55}]}],
            }
        ]
    }

    rows = parse_energyvision_tariffs_payload(payload)

    assert rows == [
        {
            "country_code": "BE",
            "source_uid": ENERGYVISION_TARIFFS_SOURCE_UID,
            "tariff_id": "tariff-a",
            "currency": "EUR",
            "type": "REGULAR",
            "last_updated": "2026-04-30T10:00:00Z",
            "elements": '[{"price_components":[{"price":0.55,"type":"ENERGY"}]}]',
            "raw_static": (
                '{"currency":"EUR","elements":[{"price_components":[{"price":0.55,"type":"ENERGY"}]}],'
                '"id":"tariff-a","last_updated":"2026-04-30T10:00:00Z","type":"REGULAR"}'
            ),
            "price_display": "0,55 €/kWh",
            "price_currency": "EUR",
            "price_energy_eur_kwh_min": "0.55",
            "price_energy_eur_kwh_max": "0.55",
            "price_time_eur_min_min": None,
            "price_time_eur_min_max": None,
            "price_quality": "source_ocpi_tariff",
            "price_complex": False,
            "price_source_text": "tariff-a",
        }
    ]


def test_energyvision_locations_can_join_tariff_prices():
    tariffs = parse_energyvision_tariffs_payload(
        {
            "data": [
                {
                    "id": "tariff-a",
                    "currency": "EUR",
                    "elements": [{"price_components": [{"type": "ENERGY", "price": 0.55}]}],
                }
            ]
        }
    )
    locations = {
        "data": [
            {
                "id": "LOC-1",
                "party_id": "EVB",
                "evses": [
                    {
                        "evse_id": "BE*EVB*E1",
                        "status": "AVAILABLE",
                        "connectors": [{"tariff_ids": ["tariff-a"]}],
                    }
                ],
            }
        ]
    }

    rows = parse_energyvision_locations_payload(
        locations,
        tariff_lookup=energyvision_tariff_price_lookup(tariffs),
    )
    attached_rows = list(attach_energyvision_tariff_prices(parse_energyvision_locations_payload(locations), tariffs))

    assert rows[0]["price_display"] == "0,55 €/kWh"
    assert rows[0]["price_energy_eur_kwh_min"] == "0.55"
    assert attached_rows[0]["price_display"] == "0,55 €/kWh"


def test_eco_movement_datex_static_parser_creates_join_keys():
    payload = {
        "publication": {
            "energyInfrastructureSite": [
                {
                    "reference": {"id": "site-1"},
                    "operatorName": "Eco Operator",
                    "energyInfrastructureStation": [
                        {
                            "reference": {"id": "station-1"},
                            "refillPoint": [
                                {
                                    "aegiElectricChargingPoint": {
                                        "reference": {"id": "BE*ECO*E1"},
                                        "connector": "ccs",
                                    }
                                }
                            ],
                        }
                    ],
                }
            ]
        }
    }

    rows = parse_eco_movement_static_payload(payload)

    assert len(rows) == 1
    assert rows[0]["source_uid"] == ECO_MOVEMENT_STATIC_SOURCE_UID
    assert rows[0]["station_id"] == "be:be_eco_movement_static_datex:station-1"
    assert rows[0]["charger_id"] == "be:be_eco_movement_static_datex:evse:be-eco-e1"
    assert rows[0]["operator_name"] == "Eco Operator"
    assert rows[0]["raw_static"]


def test_eco_movement_datex_xml_static_parser_creates_join_keys():
    payload = b"""<?xml version="1.0" encoding="UTF-8"?>
<d2:payload xmlns:d2="http://datex2.eu/schema/3/d2Payload"
  xmlns:com="http://datex2.eu/schema/3/common"
  xmlns:egi="http://datex2.eu/schema/3/energyInfrastructure"
  xmlns:fac="http://datex2.eu/schema/3/facilities"
  xmlns:loc="http://datex2.eu/schema/3/locationReferencing"
  xmlns:locx="http://datex2.eu/schema/3/locationExtension">
  <egi:energyInfrastructureTable id="1">
    <egi:energyInfrastructureSite id="site-xml" version="1">
      <fac:name><com:values><com:value lang="en">Site name</com:value></com:values></fac:name>
      <fac:locationReference>
        <loc:pointByCoordinates>
          <loc:pointCoordinates>
            <loc:latitude>51.2096</loc:latitude>
            <loc:longitude>3.4440</loc:longitude>
          </loc:pointCoordinates>
        </loc:pointByCoordinates>
        <loc:_pointLocationExtension>
          <locx:facilityLocation>
            <locx:address>
              <locx:postcode>9990</locx:postcode>
              <locx:city>Maldegem</locx:city>
              <locx:addressLine order="0">
                <locx:type>street</locx:type>
                <locx:text>Westeindestraat 7</locx:text>
              </locx:addressLine>
            </locx:address>
          </locx:facilityLocation>
        </loc:_pointLocationExtension>
      </fac:locationReference>
      <fac:operator>
        <fac:name><com:values><com:value lang="en">Eco XML Operator</com:value></com:values></fac:name>
      </fac:operator>
      <egi:energyInfrastructureStation id="1" version="1">
        <egi:authenticationAndIdentificationMethods>rfid</egi:authenticationAndIdentificationMethods>
        <egi:refillPoint id="BE-ECO-E1" version="1">
          <fac:externalIdentifier>BE*ECO*E1</fac:externalIdentifier>
          <egi:connector>
            <egi:connectorType>iec62196T2</egi:connectorType>
            <egi:chargingMode>mode3AC3p</egi:chargingMode>
            <egi:maxPowerAtSocket>22000</egi:maxPowerAtSocket>
          </egi:connector>
        </egi:refillPoint>
      </egi:energyInfrastructureStation>
    </egi:energyInfrastructureSite>
  </egi:energyInfrastructureTable>
</d2:payload>"""

    rows = list(iter_eco_movement_static_rows_from_binary_stream(io.BufferedReader(io.BytesIO(payload))))

    assert len(rows) == 1
    assert rows[0]["source_uid"] == ECO_MOVEMENT_STATIC_SOURCE_UID
    assert rows[0]["station_id"] == "be:be_eco_movement_static_datex:site-xml"
    assert rows[0]["charger_id"] == "be:be_eco_movement_static_datex:evse:be-eco-e1"
    assert rows[0]["operator_name"] == "Eco XML Operator"
    assert rows[0]["source_site_id"] == "site-xml"
    assert rows[0]["source_station_id"] == "site-xml"
    assert rows[0]["source_evse_id"] == "BE*ECO*E1"
    assert rows[0]["connector_count"] == 1
    assert rows[0]["max_power_kw"] == 22.0
    assert rows[0]["connector_types"] == "iec62196T2"
    assert rows[0]["raw_static"]


def test_group_indigo_datex_xml_static_parser_creates_join_keys():
    payload = b"""<?xml version="1.0" encoding="UTF-8"?>
<d2:payload xmlns:d2="http://datex2.eu/schema/3/d2Payload"
  xmlns:com="http://datex2.eu/schema/3/common"
  xmlns:egi="http://datex2.eu/schema/3/energyInfrastructure"
  xmlns:fac="http://datex2.eu/schema/3/facilities"
  xmlns:loc="http://datex2.eu/schema/3/locationReferencing">
  <egi:energyInfrastructureTable id="1">
    <egi:energyInfrastructureSite id="indigo-site" version="1">
      <fac:name><com:values><com:value lang="en">Indigo site</com:value></com:values></fac:name>
      <fac:operator>
        <fac:name><com:values><com:value lang="en">Group INDIGO</com:value></com:values></fac:name>
      </fac:operator>
      <fac:locationReference>
        <loc:pointByCoordinates>
          <loc:pointCoordinates>
            <loc:latitude>50.8503</loc:latitude>
            <loc:longitude>4.3517</loc:longitude>
          </loc:pointCoordinates>
        </loc:pointByCoordinates>
      </fac:locationReference>
      <egi:energyInfrastructureStation id="1" version="1">
        <egi:refillPoint id="BE-IND-E1" version="1">
          <fac:externalIdentifier>BE*IND*E1</fac:externalIdentifier>
          <egi:connector>
            <egi:connectorType>iec62196T2COMBO</egi:connectorType>
            <egi:chargingMode>mode4DC</egi:chargingMode>
            <egi:maxPowerAtSocket>50000</egi:maxPowerAtSocket>
          </egi:connector>
        </egi:refillPoint>
      </egi:energyInfrastructureStation>
    </egi:energyInfrastructureSite>
  </egi:energyInfrastructureTable>
</d2:payload>"""

    rows = list(iter_group_indigo_static_rows_from_binary_stream(io.BufferedReader(io.BytesIO(payload))))

    assert len(rows) == 1
    assert rows[0]["source_uid"] == GROUP_INDIGO_STATIC_SOURCE_UID
    assert rows[0]["station_id"] == "be:be_group_indigo_datex_static:indigo-site"
    assert rows[0]["charger_id"] == "be:be_group_indigo_datex_static:evse:be-ind-e1"
    assert rows[0]["operator_name"] == "Group INDIGO"
    assert rows[0]["max_power_kw"] == 50.0
    assert rows[0]["connector_types"] == "iec62196T2COMBO"


def test_monta_parser_handles_charge_points_and_connector_status():
    payload = {
        "data": [
            {
                "id": "cp-1",
                "siteId": "site-1",
                "operator": {"name": "Monta Operator"},
                "location": {
                    "address": "Rue Test 2",
                    "city": "Antwerp",
                    "postalCode": "2000",
                    "latitude": "51.2194",
                    "longitude": "4.4025",
                },
                "connectors": [
                    {
                        "id": "conn-1",
                        "evseId": "BE*MON*A1",
                        "status": "AVAILABLE",
                        "standard": "IEC_62196_T2_COMBO",
                        "maxElectricPower": 175000,
                    },
                    {"id": "conn-2", "evseId": "BE*MON*A2", "status": "FAULTED"},
                ],
                "updatedAt": "2026-04-30T11:00:00Z",
            }
        ]
    }

    rows = parse_monta_charge_points_payload(payload)

    assert len(rows) == 2
    assert rows[0]["source_uid"] == MONTA_AFIR_CHARGE_POINTS_SOURCE_UID
    assert rows[0]["station_id"] == "be:be_monta_afir_charge_points:site-1"
    assert rows[0]["availability_status"] == "free"
    assert rows[0]["max_power_kw"] == 175.0
    assert rows[1]["availability_status"] == "out_of_order"


def test_monta_streaming_parser_reads_data_array():
    payload = {"data": [{"id": "cp-2", "evseId": "BE*MON*B1", "status": "CHARGING"}]}

    rows = list(iter_monta_charge_point_rows_from_binary_stream(_binary_stream(payload)))

    assert len(rows) == 1
    assert rows[0]["availability_status"] == "occupied"
