from __future__ import annotations

import io

from commercial_backend.hu_nap import (
    HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID,
    HU_NAP_MOBILITI_STATIC_SOURCE_UID,
    compact_count_summary,
    count_xml_records_from_binary_stream,
    iter_eco_movement_static_rows_from_binary_stream,
    iter_mobiliti_static_rows_from_binary_stream,
)


def test_eco_movement_static_parser_preserves_public_evse_id_and_counts_fast_site():
    payload = b"""
    <EnergyInfrastructureTablePublication xmlns:com="http://datex2.eu/schema/3/common"
      xmlns:fac="http://datex2.eu/schema/3/facilities"
      xmlns:lr="http://datex2.eu/schema/3/locationReferencing"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns="http://datex2.eu/schema/3/energyInfrastructure">
      <com:publicationTime>2026-05-13T10:01:54Z</com:publicationTime>
      <energyInfrastructureTable>
        <energyInfrastructureSite id="eco-site-1">
          <fac:name><com:values><com:value>Shell HU Fast</com:value></com:values></fac:name>
          <fac:locationReference>
            <lr:pointCoordinates><lr:latitude>47.4979</lr:latitude><lr:longitude>19.0402</lr:longitude></lr:pointCoordinates>
          </fac:locationReference>
          <fac:operator id="SHELL"><fac:name><com:values><com:value>Shell Recharge</com:value></com:values></fac:name><fac:telephoneNumber>+361234567</fac:telephoneNumber></fac:operator>
          <energyInfrastructureStation id="station-1">
            <authenticationAndIdentificationMethods>app</authenticationAndIdentificationMethods>
            <refillPoint xsi:type="ElectricChargingPoint" id="internal-1">
              <externalIdentifier>HU*SHE*E*0001</externalIdentifier>
              <connector id="1">
                <connectorType>iec62196T2COMBO</connectorType>
                <chargingMode>mode4DC</chargingMode>
                <connectorFormat>cableMode3</connectorFormat>
                <maxPowerAtSocket>350000</maxPowerAtSocket>
              </connector>
            </refillPoint>
          </energyInfrastructureStation>
        </energyInfrastructureSite>
      </energyInfrastructureTable>
    </EnergyInfrastructureTablePublication>
    """

    rows = list(iter_eco_movement_static_rows_from_binary_stream(io.BytesIO(payload)))

    assert len(rows) == 1
    row = rows[0]
    assert row["country_code"] == "HU"
    assert row["source_uid"] == HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID
    assert row["station_id"] == "hu:nap:eco-movement:eco-site-1"
    assert row["charger_id"] == "hu:nap:eco-movement:evse:hu*she*e*0001"
    assert row["source_evse_id"] == "HU*SHE*E*0001"
    assert row["source_evse_alias_ids"] == ["internal-1"]
    assert row["public_evse_id"] == "HU*SHE*E*0001"
    assert row["operator_name"] == "Shell Recharge"
    assert row["station_name"] == "Shell HU Fast"
    assert row["max_power_kw"] == 350
    assert row["latitude"] == 47.4979
    assert row["longitude"] == 19.0402
    assert row["auth_methods"] == "app"
    assert row["helpdesk_phone"] == "+361234567"
    assert compact_count_summary(rows) == {"row_count": 1, "station_count": 1, "fast_station_count": 1}


def test_mobiliti_static_parser_uses_refill_id_and_site_huf_pricing():
    payload = b"""
    <EnergyInfrastructureTablePublication xmlns:com="http://datex2.eu/schema/3/common"
      xmlns:fac="http://datex2.eu/schema/3/facilities"
      xmlns:lr="http://datex2.eu/schema/3/locationReferencing"
      xmlns:lex="http://datex2.eu/schema/3/locationExtension"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns="http://datex2.eu/schema/3/energyInfrastructure">
      <com:publicationTime>2026-05-11T00:00:11.828Z</com:publicationTime>
      <energyInfrastructureTable>
        <energyInfrastructureSite id="91257407">
          <fac:name><com:values><com:value>Mobiliti Test</com:value></com:values></fac:name>
          <fac:locationReference>
            <lr:_locationReferenceExtension>
              <lr:facilityLocation>
                <lex:address>
                  <lex:postcode>1111</lex:postcode>
                  <lex:city><com:values><com:value>Budapest</com:value></com:values></lex:city>
                  <lex:addressLine order="1"><lex:text><com:values><com:value>Test utca 1</com:value></com:values></lex:text></lex:addressLine>
                </lex:address>
              </lr:facilityLocation>
            </lr:_locationReferenceExtension>
            <lr:coordinatesForDisplay><lr:latitude>47.5</lr:latitude><lr:longitude>19.05</lr:longitude></lr:coordinatesForDisplay>
          </fac:locationReference>
          <rates>
            <applicableCurrency>HUF</applicableCurrency>
            <dcChargerPrice>400.0</dcChargerPrice>
            <dcChargerBaseOfCalculation>KWH</dcChargerBaseOfCalculation>
            <acChargerPrice>400.0</acChargerPrice>
            <acChargerBaseOfCalculation>KWH</acChargerBaseOfCalculation>
            <rapidChargerPrice>400.0</rapidChargerPrice>
            <rapidChargerBaseOfCalculation>KWH</rapidChargerBaseOfCalculation>
          </rates>
          <energyInfrastructureStation id="station-1">
            <fac:operator id="MVM"><fac:name><com:values><com:value>MVM Mobiliti</com:value></com:values></fac:name><fac:telephoneNumber>+361111111</fac:telephoneNumber></fac:operator>
            <refillPoint xsi:type="ElectricChargingPoint" id="MOBI-1">
              <isGreenEnergy>false</isGreenEnergy>
              <connector id="1">
                <connectorType>iec62196T2</connectorType>
                <chargingMode>mode3AC3p</chargingMode>
                <maxPowerAtSocket>22000</maxPowerAtSocket>
              </connector>
            </refillPoint>
          </energyInfrastructureStation>
        </energyInfrastructureSite>
      </energyInfrastructureTable>
    </EnergyInfrastructureTablePublication>
    """

    rows = list(iter_mobiliti_static_rows_from_binary_stream(io.BytesIO(payload)))

    assert len(rows) == 1
    row = rows[0]
    assert row["source_uid"] == HU_NAP_MOBILITI_STATIC_SOURCE_UID
    assert row["station_id"] == "hu:nap:mobiliti:91257407"
    assert row["charger_id"] == "hu:nap:mobiliti:evse:mobi-1"
    assert row["source_evse_id"] == "MOBI-1"
    assert row["source_evse_alias_ids"] == []
    assert row["public_evse_id"] == ""
    assert row["operator_name"] == "MVM Mobiliti"
    assert row["address"] == "Test utca 1"
    assert row["city"] == "Budapest"
    assert row["postal_code"] == "1111"
    assert row["green_energy"] == "false"
    assert row["helpdesk_phone"] == "+361111111"
    assert row["price_display"] == "400 HUF/kWh"
    assert row["price_currency"] == "HUF"
    assert row["price_energy_eur_kwh_min"] == ""
    assert row["price_quality"] == "source_hu_nap_datex_pricing_details_huf"


def test_hu_xml_counter_handles_ack_only_subscription_payload():
    payload = b"""
    <d2LogicalModel xmlns:com="http://datex2.eu/schema/3/common"
      xmlns="http://datex2.eu/schema/3/messageContainer">
      <exchangeInformation>
        <com:exchangeContext><com:codedExchangeProtocol>snapshotPull</com:codedExchangeProtocol></com:exchangeContext>
      </exchangeInformation>
    </d2LogicalModel>
    """

    summary = count_xml_records_from_binary_stream(io.BytesIO(payload))

    assert summary["root"] == "d2LogicalModel"
    assert summary["site_count"] == 0
    assert summary["station_count"] == 0
    assert summary["refill_point_count"] == 0
    assert summary["connector_count"] == 0
