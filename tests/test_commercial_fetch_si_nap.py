from __future__ import annotations

from scripts import commercial_fetch_si_nap as fetch_si


def test_si_fetch_defaults_to_status_snapshot():
    specs = fetch_si._selected_sources("status")

    assert [spec.key for spec in specs] == ["status"]
    assert specs[0].source_uid == "si_nap_prometej_energy_infrastructure_status"
    assert specs[0].task_kind == "parse_dynamic_payload"


def test_si_fetch_all_queues_table_then_status():
    specs = fetch_si._selected_sources("all")

    assert [spec.key for spec in specs] == ["table", "status"]
    assert [spec.task_kind for spec in specs] == ["parse_static_payload", "parse_dynamic_payload"]


def test_si_credentials_use_standard_email_and_env_password():
    email, password = fetch_si._credentials_from_env({"SI_NAP_PASSWORD": "secret"})

    assert email == "raphael.volz@hs-pforzheim.de"
    assert password == "secret"
