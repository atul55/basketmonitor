from __future__ import annotations

import os
from pathlib import Path


ENV_PATH = Path(__file__).resolve().parent / ".env"


def load_dotenv(path: str | os.PathLike | None = None) -> bool:
    env_path = Path(path or ENV_PATH)
    if not env_path.exists():
        return False

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if key and key not in os.environ:
            os.environ[key] = value

    return True


def get_env(name: str, default: str | None = None) -> str | None:
    load_dotenv()
    return os.getenv(name, default)


def get_required_env(name: str) -> str:
    value = get_env(name)
    if value is None or value == "":
        raise RuntimeError(
            f"Missing required environment variable '{name}'. Set it in the .env file in the project root."
        )
    return value
