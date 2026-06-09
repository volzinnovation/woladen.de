from __future__ import annotations

import io

from commercial_backend.si_nap import count_xml_records_from_binary_stream
from commercial_backend.si_nap import iter_status_rows_from_binary_stream
from commercial_backend.si_nap import iter_table_rows_from_binary_stream


def _stream(payload: bytes) -> io.BufferedReader:
    return io.BufferedReader(io.BytesIO(payload))


def test_si_table_parser_extracts_static_rows_prices_and_suppresses_duplicate_evse():
    payload = b"""
    <EnergyInfrastructureTablePublication xmlns:com="http://datex2.eu/schema/3/common"
      xmlns:fac="http://datex2.eu/schema/3/facilities"
      xmlns:lr="http://datex2.eu/schema/3/locationReferencing"
      xmlns:lex="http://datex2.eu/schema/3/locationExtension"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns="http://datex2.eu/schema/3/energyInfrastructure">
      <com:publicationTime>2026-05-13T07:25:02Z</com:publicationTime>
      <energyInfrastructureTable id="eit1">
        <energyInfrastructureSite id="site-1">
          <fac:name><com:values><com:value lang="sl">Testna polnilnica</com:value></com:values></fac:name>
          <fac:operatingHours>
            <fac:overallPeriod>
              <com:validPeriod>
                <com:recurringTimePeriodOfDay>
                  <com:startTimeOfPeriod>00:00:00+01:00</com:startTimeOfPeriod>
                  <com:endTimeOfPeriod>23:59:59+01:00</com:endTimeOfPeriod>
                </com:recurringTimePeriodOfDay>
                <com:recurringDayWeekMonthPeriod><com:applicableDay>monday</com:applicableDay></com:recurringDayWeekMonthPeriod>
              </com:validPeriod>
              <com:validPeriod>
                <com:recurringTimePeriodOfDay>
                  <com:startTimeOfPeriod>00:00:00+01:00</com:startTimeOfPeriod>
                  <com:endTimeOfPeriod>23:59:59+01:00</com:endTimeOfPeriod>
                </com:recurringTimePeriodOfDay>
                <com:recurringDayWeekMonthPeriod><com:applicableDay>tuesday</com:applicableDay></com:recurringDayWeekMonthPeriod>
              </com:validPeriod>
              <com:validPeriod><com:recurringTimePeriodOfDay><com:startTimeOfPeriod>00:00:00+01:00</com:startTimeOfPeriod><com:endTimeOfPeriod>23:59:59+01:00</com:endTimeOfPeriod></com:recurringTimePeriodOfDay><com:recurringDayWeekMonthPeriod><com:applicableDay>wednesday</com:applicableDay></com:recurringDayWeekMonthPeriod></com:validPeriod>
              <com:validPeriod><com:recurringTimePeriodOfDay><com:startTimeOfPeriod>00:00:00+01:00</com:startTimeOfPeriod><com:endTimeOfPeriod>23:59:59+01:00</com:endTimeOfPeriod></com:recurringTimePeriodOfDay><com:recurringDayWeekMonthPeriod><com:applicableDay>thursday</com:applicableDay></com:recurringDayWeekMonthPeriod></com:validPeriod>
              <com:validPeriod><com:recurringTimePeriodOfDay><com:startTimeOfPeriod>00:00:00+01:00</com:startTimeOfPeriod><com:endTimeOfPeriod>23:59:59+01:00</com:endTimeOfPeriod></com:recurringTimePeriodOfDay><com:recurringDayWeekMonthPeriod><com:applicableDay>friday</com:applicableDay></com:recurringDayWeekMonthPeriod></com:validPeriod>
              <com:validPeriod><com:recurringTimePeriodOfDay><com:startTimeOfPeriod>00:00:00+01:00</com:startTimeOfPeriod><com:endTimeOfPeriod>23:59:59+01:00</com:endTimeOfPeriod></com:recurringTimePeriodOfDay><com:recurringDayWeekMonthPeriod><com:applicableDay>saturday</com:applicableDay></com:recurringDayWeekMonthPeriod></com:validPeriod>
              <com:validPeriod><com:recurringTimePeriodOfDay><com:startTimeOfPeriod>00:00:00+01:00</com:startTimeOfPeriod><com:endTimeOfPeriod>23:59:59+01:00</com:endTimeOfPeriod></com:recurringTimePeriodOfDay><com:recurringDayWeekMonthPeriod><com:applicableDay>sunday</com:applicableDay></com:recurringDayWeekMonthPeriod></com:validPeriod>
            </fac:overallPeriod>
          </fac:operatingHours>
          <fac:locationReference>
            <lr:_locationReferenceExtension>
              <lr:facilityLocation>
                <lex:address>
                  <lex:postcode>1000</lex:postcode>
                  <lex:city><com:values><com:value lang="sl">Ljubljana</com:value></com:values></lex:city>
                  <lex:addressLine order="1"><lex:text><com:values><com:value lang="sl">Testna cesta 1</com:value></com:values></lex:text></lex:addressLine>
                </lex:address>
              </lr:facilityLocation>
            </lr:_locationReferenceExtension>
            <lr:coordinatesForDisplay><lr:latitude>46.05</lr:latitude><lr:longitude>14.50</lr:longitude></lr:coordinatesForDisplay>
          </fac:locationReference>
          <fac:operator id="SI*TST"><fac:name><com:values><com:value>Test Operator</com:value></com:values></fac:name></fac:operator>
          <fac:applicableForVehicles><com:vehicleType>car</com:vehicleType></fac:applicableForVehicles>
          <energyInfrastructureStation id="station-1">
            <authenticationAndIdentificationMethods>rfid</authenticationAndIdentificationMethods>
            <refillPoint xsi:type="ElectricChargingPoint" id="SI*TST*E1">
              <fac:rates>
                <fac:applicableCurrency>EUR</fac:applicableCurrency>
                <fac:energyPricingPolicy><pricingPolicy>pricePerDeliveryUnit</pricingPolicy></fac:energyPricingPolicy>
                <fac:rateLineCollection>
                  <fac:rateLine><fac:rateLineType>perUnit</fac:rateLineType><fac:value>0.25</fac:value></fac:rateLine>
                </fac:rateLineCollection>
              </fac:rates>
              <connector><connectorType>iec62196T2COMBO</connectorType><maxPowerAtSocket>150000</maxPowerAtSocket></connector>
            </refillPoint>
            <refillPoint xsi:type="ElectricChargingPoint" id="SI*TST*E1">
              <connector><connectorType>iec62196T2COMBO</connectorType><maxPowerAtSocket>150000</maxPowerAtSocket></connector>
            </refillPoint>
          </energyInfrastructureStation>
        </energyInfrastructureSite>
      </energyInfrastructureTable>
    </EnergyInfrastructureTablePublication>
    """

    rows = list(iter_table_rows_from_binary_stream(_stream(payload)))

    assert len(rows) == 1
    assert rows[0]["country_code"] == "SI"
    assert rows[0]["source_uid"] == "si_nap_prometej_energy_infrastructure_table"
    assert rows[0]["station_id"] == "si:nap:site-1"
    assert rows[0]["charger_id"] == "si:nap:evse:si*tst*e1"
    assert rows[0]["operator_name"] == "Test Operator"
    assert rows[0]["station_name"] == "Testna polnilnica"
    assert rows[0]["address"] == "Testna cesta 1"
    assert rows[0]["city"] == "Ljubljana"
    assert rows[0]["opening_hours"] == "24/7"
    assert rows[0]["connector_types"] == "iec62196T2COMBO"
    assert rows[0]["current_type"] == "DC"
    assert rows[0]["max_power_kw"] == 150
    assert rows[0]["price_display"] == "0,25 EUR/kWh"
    assert rows[0]["price_energy_eur_kwh_min"] == "0.25"


def test_si_status_parser_maps_dynamic_status_rows():
    payload = b"""
    <EnergyInfrastructureStatusPublication xmlns:com="http://datex2.eu/schema/3/common"
      xmlns:fac="http://datex2.eu/schema/3/facilities"
      xmlns="http://datex2.eu/schema/3/energyInfrastructure">
      <com:publicationTime>2026-05-13T07:26:46Z</com:publicationTime>
      <energyInfrastructureSiteStatus>
        <fac:reference id="site-1" targetClass="EnergyInfrastructureSite" />
        <energyInfrastructureStationStatus>
          <fac:reference id="station-1" targetClass="EnergyInfrastructureStation" />
          <isAvailable>true</isAvailable>
          <refillPointStatus>
            <fac:reference id="SI*TST*E1" targetClass="ElectricChargingPoint" />
            <fac:lastUpdated>2026-05-13T07:26:00Z</fac:lastUpdated>
            <status>charging</status>
          </refillPointStatus>
        </energyInfrastructureStationStatus>
        <energyInfrastructureStationStatus>
          <fac:reference id="station-2" targetClass="EnergyInfrastructureStation" />
          <isAvailable>false</isAvailable>
          <refillPointStatus>
            <fac:reference id="SI*TST*E2" targetClass="ElectricChargingPoint" />
            <status>unknown</status>
          </refillPointStatus>
        </energyInfrastructureStationStatus>
      </energyInfrastructureSiteStatus>
    </EnergyInfrastructureStatusPublication>
    """

    rows = list(iter_status_rows_from_binary_stream(_stream(payload)))

    assert [row["charger_id"] for row in rows] == [
        "si:nap:evse:si*tst*e1",
        "si:nap:evse:si*tst*e2",
    ]
    assert rows[0]["availability_status"] == "occupied"
    assert rows[0]["source_observed_at"] == "2026-05-13T07:26:00Z"
    assert rows[1]["availability_status"] == "out_of_order"
    assert rows[1]["source_observed_at"] == "2026-05-13T07:26:46Z"


def test_si_xml_record_counter_counts_static_and_status_publications():
    payload = b"""
    <EnergyInfrastructureStatusPublication xmlns:com="http://datex2.eu/schema/3/common"
      xmlns:fac="http://datex2.eu/schema/3/facilities"
      xmlns="http://datex2.eu/schema/3/energyInfrastructure">
      <com:publicationTime>2026-05-13T07:26:46Z</com:publicationTime>
      <energyInfrastructureSiteStatus>
        <fac:reference id="site-1" />
        <energyInfrastructureStationStatus>
          <fac:reference id="station-1" />
          <refillPointStatus><fac:reference id="SI*TST*E1" /><status>available</status></refillPointStatus>
        </energyInfrastructureStationStatus>
      </energyInfrastructureSiteStatus>
    </EnergyInfrastructureStatusPublication>
    """

    summary = count_xml_records_from_binary_stream(_stream(payload))

    assert summary["root"] == "EnergyInfrastructureStatusPublication"
    assert summary["site_count"] == 1
    assert summary["station_count"] == 1
    assert summary["refill_point_count"] == 1
    assert summary["status_values"] == {"available": 1}
