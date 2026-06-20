from __future__ import annotations

from pathlib import Path


def test_open_static_workflow_default_includes_belgium_energyvision():
    workflow = Path(".github/workflows/build-open-static-sqlite-bundle.yml").read_text(encoding="utf-8")

    assert 'default: "AT,BE,CH,CY,CZ,DE,DK,ES,FI,FR,GR,HU,LT,LU,LV,MT,NL,NO,PL,PT,SE,SI"' in workflow
    assert "Checkout woladen.de data repo" not in workflow
    assert 'requirements.txt -r requirements-open-static.txt' in workflow
    assert '--woladen-de-data-dir "$GITHUB_WORKSPACE/data"' in workflow
    assert "TRANSPORTDATA_BE_ENERGYVISION_PROD_TOKEN: ${{ secrets.TRANSPORTDATA_BE_ENERGYVISION_PROD_TOKEN }}" in workflow
    assert "Missing TRANSPORTDATA_BE_ENERGYVISION_PROD_TOKEN GitHub Actions secret" in workflow
    assert "scripts/commercial_fetch_be_transportdata.py --source energyvision-tariffs" in workflow
    assert "scripts/commercial_fetch_be_transportdata.py --source energyvision-locations" in workflow
    assert "scripts/commercial_fetch_be_transportdata.py --source road-locations" in workflow
    assert "scripts/commercial_fetch_be_transportdata.py --source indigo-static" in workflow
    assert "MONTA_PUBLIC_CLIENT_ID: ${{ secrets.MONTA_PUBLIC_CLIENT_ID || secrets.DK_MONTA_CLIENT_ID }}" in workflow
    assert "MONTA_PUBLIC_CLIENT_SECRET: ${{ secrets.MONTA_PUBLIC_CLIENT_SECRET || secrets.DK_MONTA_CLIENT_SECRET }}" in workflow
    assert "DK_MONTA_CLIENT_ID: ${{ secrets.DK_MONTA_CLIENT_ID }}" in workflow
    assert "DK_MONTA_CLIENT_SECRET: ${{ secrets.DK_MONTA_CLIENT_SECRET }}" in workflow
    assert "Missing MONTA_PUBLIC_CLIENT_ID/MONTA_PUBLIC_CLIENT_SECRET or DK_MONTA_CLIENT_ID/DK_MONTA_CLIENT_SECRET" in workflow
    assert "scripts/commercial_fetch_dk_monta.py --country BE --per-page 1000" in workflow
    assert "scripts/commercial_fetch_dk_monta.py --country DK --per-page 1000" in workflow
    assert "scripts/commercial_fetch_eu_public_static.py --source cy,cz,es,gr,lt,lu,mt" in workflow
    assert "Via Lietuva LT DATEX static fetch hit the known Cloudflare challenge" not in workflow
    assert "SI_NAP_PASSWORD: ${{ secrets.SI_NAP_PASSWORD }}" in workflow
    assert "Missing SI_NAP_PASSWORD GitHub Actions secret" in workflow
    assert "scripts/commercial_fetch_si_nap.py --source table" in workflow
    assert "PT_NAP_PASSWORD: ${{ secrets.PT_NAP_PASSWORD }}" in workflow
    assert "scripts/commercial_fetch_pt_mobie.py --source static-datex" in workflow
    assert "HU_NAP_PASSWORD: ${{ secrets.HU_NAP_PASSWORD }}" in workflow
    assert "Missing HU_NAP_PASSWORD GitHub Actions secret" in workflow
    assert "scripts/commercial_fetch_hu_nap.py --source static-real" in workflow
    assert "TRANSPORTDATA_LV_ECO_MOVEMENT_STATIC_API_KEY: ${{ secrets.TRANSPORTDATA_LV_ECO_MOVEMENT_STATIC_API_KEY }}" in workflow
    assert "TRANSPORTDATA_LV_ECO_MOVEMENT_STATUS_PRICE_API_KEY: ${{ secrets.TRANSPORTDATA_LV_ECO_MOVEMENT_STATUS_PRICE_API_KEY }}" in workflow
    assert "TRANSPORTDATA_LV_LVC_EV_CHARGING_STREAM_API_KEY: ${{ secrets.TRANSPORTDATA_LV_LVC_EV_CHARGING_STREAM_API_KEY }}" in workflow
    assert "scripts/commercial_fetch_lv_transportdata.py --source all" in workflow
    assert "reuses its station_amenities.csv, then optionally overlays selected PBF-enriched countries" in workflow
    assert "if: ${{ inputs.amenity_reuse_run_id != '' }}" in workflow
    assert "eu27_ch_static_reused" in workflow
    assert '--input-dir "$merge_input_dir"' in workflow
    assert '"$COUNTRY" == "SI" || "$COUNTRY" == "HU" || "$COUNTRY" == "LV" || "$COUNTRY" == "DK"' in workflow
    assert 'download_flag="--download-osm-pbf"' in workflow
    assert "needs.prepare-normalized-source.result == 'success'" in workflow
    assert "needs.build-country-part.result == 'success'" in workflow
    assert "--allow-empty-expected-country LT" not in workflow
    assert 'default: "open-static-ios-regional-latest"' in workflow
    assert "scripts/build_open_static_regional_release_assets.py" in workflow
    assert "open-static-github-regional-packs" in workflow
    assert "open-static-<GROUP>.sqlite3.zlib" in workflow
    assert "Verify published combined release assets" in workflow
    assert "open_static.sqlite3.zst" in workflow
    assert "missing published release asset" in workflow


def test_onboarded_static_catalog_workflow_is_public_repo_artifact_only():
    workflow = Path(".github/workflows/build-onboarded-static-catalog.yml").read_text(encoding="utf-8")

    assert "Build onboarded static catalog" in workflow
    assert "requirements.txt -r requirements-open-static.txt" in workflow
    assert "actions/upload-artifact@v4" in workflow
    assert "data/onboarded_static" in workflow
    assert "git push" not in workflow
    assert "requirements-commercial.txt" not in workflow


def test_pages_deploy_downloads_open_static_bundle_before_build():
    workflow = Path(".github/workflows/pages-deploy.yml").read_text(encoding="utf-8")

    assert "Download open static SQLite bundle" in workflow
    assert "GITHUB_TOKEN: ${{ github.token }}" in workflow
    assert (
        "python scripts/download_latest_open_static_release.py "
        "--tag open-static-ios-regional-latest --require-checksum"
    ) in workflow
    assert workflow.index("Download open static SQLite bundle") < workflow.index("Build static site bundle")
