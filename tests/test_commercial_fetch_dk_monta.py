from __future__ import annotations

from scripts import commercial_fetch_dk_monta as fetch_dk


def test_afir_charge_points_url_uses_documented_country_and_one_based_page():
    url, query = fetch_dk.afir_charge_points_url(country="dk", page=0, per_page=5000)

    assert url == "https://public-api.monta.com/api/v1/afir/charge-points?country=DK&page=1&perPage=1000"
    assert query == "country=DK&page=1&perPage=1000"


def test_afir_charge_points_url_supports_be_monta_public_api_country():
    url, query = fetch_dk.afir_charge_points_url(country="be", page=2, per_page=1000)

    assert url == "https://public-api.monta.com/api/v1/afir/charge-points?country=BE&page=2&perPage=1000"
    assert query == "country=BE&page=2&perPage=1000"


def test_source_specs_select_be_source_uid():
    source = fetch_dk.table_source_for_country("BE")

    assert source.country_code == "BE"
    assert source.source_uid == "be_monta_afir_charge_points"
    assert source.provider_uid == "be_monta"


def test_status_source_specs_select_be_dynamic_source_uid():
    source = fetch_dk.status_source_for_country("BE")

    assert source.country_code == "BE"
    assert source.source_uid == "be_monta_afir_evse_status"
    assert source.provider_uid == "be_monta"
    assert source.task_kind == "parse_dynamic_payload"


def test_next_page_from_table_payload_uses_monta_meta_total():
    payload = {"meta": {"page": 1, "perPage": 1000, "total": 3393}}

    assert fetch_dk.next_page_from_table_payload(payload, 1) == 2
    assert fetch_dk.next_page_from_table_payload({"meta": {"page": 4, "perPage": 1000, "total": 3393}}, 4) is None


def test_afir_status_url_escapes_ocpi_evse_id():
    assert fetch_dk.afir_status_url("DK*MON*E100001").endswith(
        "/api/v1/afir/charge-points/DK%2AMON%2AE100001/status"
    )


def test_secret_hint_names_local_dk_monta_files():
    hint = fetch_dk.secret_hint()

    assert "secret/dk_monta_client_ID.txt" in hint
    assert "secret/dk_monta_pwd.txt" in hint
    assert "MONTA_PUBLIC_CLIENT_ID" in hint
    assert "DK_MONTA_CLIENT_ID" in hint
