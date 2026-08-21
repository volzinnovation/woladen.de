from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_build_site_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "build_site.py"
    spec = importlib.util.spec_from_file_location("build_site_module", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


build_site = _load_build_site_module()


def test_absolute_url_uses_public_origin():
    assert build_site.absolute_url("status.html") == "https://woladen.de/status.html"
    assert build_site.absolute_url("") == "https://woladen.de/"


def test_main_copies_frontend_shell_without_data_bundle(tmp_path: Path, monkeypatch):
    web_dir = tmp_path / "web"
    site_dir = tmp_path / "site"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<h1>woladen</h1>", encoding="utf-8")

    monkeypatch.setattr(build_site, "WEB_DIR", web_dir)
    monkeypatch.setattr(build_site, "SITE_DIR", site_dir)
    build_site.main()

    assert (site_dir / "index.html").read_text(encoding="utf-8") == "<h1>woladen</h1>"
    assert not (site_dir / "data").exists()
    sitemap = (site_dir / "sitemap.xml").read_text(encoding="utf-8")
    assert "https://woladen.de/station.html" in sitemap
    assert "https://woladen.de/status.html" in sitemap
    assert "Sitemap: https://woladen.de/sitemap.xml" in (
        site_dir / "robots.txt"
    ).read_text(encoding="utf-8")
