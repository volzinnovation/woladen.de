from __future__ import annotations

from pathlib import Path

from scripts.live_ingester import bootstrap_loop_if_missing


class DummyService:
    def __init__(self, db_path: Path):
        self.config = type("Config", (), {"db_path": db_path})()
        self.bootstrap_calls = 0

    def bootstrap(self) -> None:
        self.bootstrap_calls += 1


def test_bootstrap_loop_if_missing_bootstraps_when_db_is_missing(tmp_path: Path):
    service = DummyService(tmp_path / "live.sqlite3")
    result = bootstrap_loop_if_missing(service)
    assert result is True
    assert service.bootstrap_calls == 1


def test_bootstrap_loop_if_missing_refreshes_when_db_exists(tmp_path: Path):
    db_path = tmp_path / "live.sqlite3"
    db_path.write_text("", encoding="utf-8")
    service = DummyService(db_path)
    result = bootstrap_loop_if_missing(service)
    assert result is False
    assert service.bootstrap_calls == 1
