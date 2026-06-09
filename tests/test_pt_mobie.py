from __future__ import annotations

import io
from dataclasses import replace

from commercial_backend.pt_mobie import (
    PT_MOBIE_STATIC_SOURCE_UID,
    PT_MOBIE_STATUS_SOURCE_UID,
    count_xml_records_from_binary_stream,
    iter_static_rows_from_binary_stream,
    iter_status_rows_from_binary_stream,
)
from commercial_backend.config import AppConfig
from scripts import commercial_fetch_pt_mobie as fetch_pt


def test_iter_static_rows_preserves_public_evse_id_and_internal_status_alias():
    payload = b"""
    <EnergyInfrastructureTablePublication xmlns:com="http://datex2.eu/schema/3/common"
      xmlns:fac="http://datex2.eu/schema/3/facilities"
      xmlns:lr="http://datex2.eu/schema/3/locationReferencing"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns="http://datex2.eu/schema/3/energyInfrastructure">
      <com:publicationTime>2026-05-13T03:00:04.200Z</com:publicationTime>
      <energyInfrastructureTable>
        <energyInfrastructureSite id="AMD-00051">
          <fac:name><com:values><com:value>Amadora Test</com:value></com:values></fac:name>
          <fac:externalIdentifier>AMD-00051</fac:externalIdentifier>
          <fac:locationReference>
            <lr:pointCoordinates><lr:latitude>38.78791</lr:latitude><lr:longitude>-9.226767</lr:longitude></lr:pointCoordinates>
          </fac:locationReference>
          <fac:operator id="EZC3"><fac:name><com:values><com:value>MOBI.E Operator</com:value></com:values></fac:name></fac:operator>
          <energyInfrastructureStation id="AMD-00051">
            <authenticationAndIdentificationMethods>app</authenticationAndIdentificationMethods>
            <refillPoint xsi:type="ElectricChargingPoint" id="AMD-00051-1">
              <externalIdentifier>PT*EZC*E*AMD*00051*1</externalIdentifier>
              <rates>
                <applicableCurrency>EUR</applicableCurrency>
                <rateLine><rateLineType>perUnit</rateLineType><value>0.31</value></rateLine>
              </rates>
              <connector>
                <connectorType>iec62196T2</connectorType>
                <chargingMode>mode3AC3p</chargingMode>
                <connectorFormat>socket</connectorFormat>
                <maxPowerAtSocket>11000.0</maxPowerAtSocket>
              </connector>
            </refillPoint>
          </energyInfrastructureStation>
        </energyInfrastructureSite>
      </energyInfrastructureTable>
    </EnergyInfrastructureTablePublication>
    """

    rows = list(iter_static_rows_from_binary_stream(io.BytesIO(payload)))

    assert len(rows) == 1
    row = rows[0]
    assert row["country_code"] == "PT"
    assert row["source_uid"] == PT_MOBIE_STATIC_SOURCE_UID
    assert row["station_id"] == "pt:mobie:amd-00051"
    assert row["charger_id"] == "pt:mobie:evse:pt*ezc*e*amd*00051*1"
    assert row["source_evse_id"] == "PT*EZC*E*AMD*00051*1"
    assert row["source_evse_alias_ids"] == ["AMD-00051-1"]
    assert row["public_evse_id"] == "PT*EZC*E*AMD*00051*1"
    assert row["max_power_kw"] == 11.0
    assert row["latitude"] == 38.78791
    assert row["longitude"] == -9.226767
    assert row["auth_methods"] == "app"
    assert row["price_energy_eur_kwh_min"] == "0.31"
    assert row["price_quality"] == "source_pt_mobie_datex_rates_exact"


def test_iter_status_rows_uses_internal_refill_point_reference_for_live_join():
    payload = b"""
    <EnergyInfrastructureStatusPublication xmlns:com="http://datex2.eu/schema/3/common"
      xmlns:fac="http://datex2.eu/schema/3/facilities"
      xmlns="http://datex2.eu/schema/3/energyInfrastructure">
      <com:publicationTime>2026-05-13T16:05:03.501Z</com:publicationTime>
      <energyInfrastructureSiteStatus>
        <fac:reference id="AMD-00051" targetClass="EnergyInfrastructureSite" />
        <energyInfrastructureStationStatus>
          <fac:reference id="AMD-00051" targetClass="EnergyInfrastructureStation" />
          <isAvailable>true</isAvailable>
          <refillPointStatus>
            <fac:reference id="AMD-00051-1" targetClass="FacilityObject" />
            <status>available</status>
          </refillPointStatus>
          <refillPointStatus>
            <fac:reference id="AMD-00051-2" targetClass="FacilityObject" />
            <status>outOfOrder</status>
          </refillPointStatus>
          <refillPointStatus>
            <fac:reference id="AMD-00051-3" targetClass="FacilityObject" />
            <status>removed</status>
          </refillPointStatus>
        </energyInfrastructureStationStatus>
      </energyInfrastructureSiteStatus>
    </EnergyInfrastructureStatusPublication>
    """

    rows = list(iter_status_rows_from_binary_stream(io.BytesIO(payload)))

    assert [row["source_uid"] for row in rows] == [PT_MOBIE_STATUS_SOURCE_UID] * 3
    assert [row["source_evse_id"] for row in rows] == ["AMD-00051-1", "AMD-00051-2", "AMD-00051-3"]
    assert [row["availability_status"] for row in rows] == ["free", "out_of_order", "unknown"]
    assert rows[0]["station_id"] == "pt:mobie:amd-00051"
    assert rows[0]["source_observed_at"] == "2026-05-13T16:05:03.501Z"


def test_count_xml_records_summarizes_pt_mobie_table_and_status_payloads():
    payload = b"""
    <EnergyInfrastructureStatusPublication xmlns:com="http://datex2.eu/schema/3/common"
      xmlns="http://datex2.eu/schema/3/energyInfrastructure">
      <com:publicationTime>2026-05-13T16:05:03.501Z</com:publicationTime>
      <energyInfrastructureSiteStatus>
        <energyInfrastructureStationStatus>
          <refillPointStatus><status>available</status></refillPointStatus>
          <refillPointStatus><status>charging</status></refillPointStatus>
        </energyInfrastructureStationStatus>
      </energyInfrastructureSiteStatus>
    </EnergyInfrastructureStatusPublication>
    """

    summary = count_xml_records_from_binary_stream(io.BytesIO(payload))

    assert summary["root"] == "EnergyInfrastructureStatusPublication"
    assert summary["publication_time"] == "2026-05-13T16:05:03.501Z"
    assert summary["site_count"] == 1
    assert summary["station_count"] == 1
    assert summary["refill_point_count"] == 2
    assert summary["status_values"] == {"available": 1, "charging": 1}


def test_invalid_pt_status_payload_is_quarantined_before_queueing(tmp_path):
    config = replace(AppConfig(), raw_payload_dir=tmp_path / "raw")
    temp_path = config.raw_payload_dir / "_incoming" / "broken.xml"
    temp_path.parent.mkdir(parents=True)
    payload = b"<EnergyInfrastructureStatusPublication><broken>"
    temp_path.write_bytes(payload)

    try:
        fetch_pt._validate_datex_payload(
            config=config,
            spec=fetch_pt.SOURCES["status-datex"],
            temp_path=temp_path,
            payload_sha256="a" * 64,
            byte_length=len(payload),
            content_encoding="",
        )
    except RuntimeError as exc:
        assert "invalid_pt_mobie_payload:status-datex" in str(exc)
    else:
        raise AssertionError("invalid PT XML should fail validation")

    assert not temp_path.exists()
    invalid_files = list((config.raw_payload_dir / "_invalid").rglob("*.xml"))
    assert len(invalid_files) == 1
    manifests = list((config.raw_payload_dir / "_invalid").rglob("invalid_payloads.ndjson"))
    assert len(manifests) == 1
    assert "pt_mobie_datex_status" in manifests[0].read_text(encoding="utf-8")
