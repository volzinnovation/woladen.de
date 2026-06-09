from __future__ import annotations

from commercial_backend.fi_digitraffic import count_datex_records
from scripts import commercial_fetch_fi_digitraffic as fetch_fi


def test_fi_fetch_defaults_to_status_datex_snapshot():
    specs = fetch_fi._selected_sources("statuses-datex")

    assert [spec.key for spec in specs] == ["statuses-datex"]
    assert specs[0].source_uid == "fi_digitraffic_afir_datex_statuses"
    assert specs[0].task_kind == "parse_dynamic_payload"


def test_fi_all_datex_queues_locations_and_statuses_for_parsing():
    specs = fetch_fi._selected_sources("all-datex")

    assert [spec.key for spec in specs] == ["statuses-datex", "locations-datex"]
    assert [spec.task_kind for spec in specs] == ["parse_dynamic_payload", "parse_static_payload"]


def test_fi_datex_record_counter_counts_publication_records():
    count = count_datex_records(
        {
            "payload": [
                {
                    "egiEnergyInfrastructureStatusPublication": {
                        "energyInfrastructureSiteStatus": [{"id": "a"}, {"id": "b"}]
                    }
                },
                {
                    "egiEnergyInfrastructureStatusPublication": {
                        "energyInfrastructureSiteStatus": [{"id": "c"}]
                    }
                },
            ]
        },
        publication_key="egiEnergyInfrastructureStatusPublication",
        record_key="energyInfrastructureSiteStatus",
    )

    assert count == 3
