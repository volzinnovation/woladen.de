#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path

import uvicorn

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.api import create_app
from backend.config import AppConfig


def main() -> None:
    config = AppConfig()
    uvicorn.run(create_app(config), host=config.api_host, port=config.api_port)


if __name__ == "__main__":
    main()
