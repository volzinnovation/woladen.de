from __future__ import annotations

import csv
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence


def write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_json(path: Path, payload: Any, *, pretty: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if pretty:
        text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    else:
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    path.write_text(text, encoding="utf-8")


def write_csv_atomic(path: Path, fieldnames: Sequence[str], rows: Sequence[dict[str, Any]]) -> None:
    with _atomic_temp_path(path) as temp_path:
        write_csv(temp_path, fieldnames, rows)


def write_text_atomic(path: Path, text: str) -> None:
    with _atomic_temp_path(path) as temp_path:
        write_text(temp_path, text)


def write_json_atomic(path: Path, payload: Any, *, pretty: bool = False) -> None:
    with _atomic_temp_path(path) as temp_path:
        write_json(temp_path, payload, pretty=pretty)


@contextmanager
def staged_output_directory(target_dir: Path) -> Iterator[Path]:
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f".{target_dir.name}-staging-", dir=target_dir.parent) as temp_dir:
        yield Path(temp_dir)


def publish_staged_directory(staged_dir: Path, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    for staged_path in sorted(staged_dir.rglob("*")):
        if staged_path.is_dir():
            continue
        relative_path = staged_path.relative_to(staged_dir)
        target_path = target_dir / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged_path, target_path)


@contextmanager
def _atomic_temp_path(path: Path) -> Iterator[Path]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_handle = tempfile.NamedTemporaryFile(
        prefix=f".{path.name}.tmp-",
        dir=path.parent,
        delete=False,
    )
    temp_path = Path(temp_handle.name)
    temp_handle.close()
    try:
        yield temp_path
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink()
