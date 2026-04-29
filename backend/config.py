import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# PostgreSQL connection string — e.g. postgresql://user:pass@localhost/cs2shadowpro
DATABASE_URL: str = os.environ["DATABASE_URL"]
DB_SCHEMA:    str = os.getenv("DB_SCHEMA", "shadowpro")

STEAM_API_KEY:        str = os.getenv("STEAM_API_KEY", "")
RESOLVER_URL:         str = os.getenv("RESOLVER_URL", "http://127.0.0.1:3001")
SYNC_INTERVAL_SECONDS: int = int(os.getenv("SYNC_INTERVAL_SECONDS", "300"))

# Directories for demo storage
_BASE = Path(__file__).parent.parent
DEMOS_PRO_DIR:    Path = Path(os.getenv("DEMOS_PRO_DIR",    str(_BASE / "demos_pro")))
DEMOS_USER_DIR:   Path = Path(os.getenv("DEMOS_USER_DIR",   str(_BASE / "demos_user")))
PARQUET_PRO_DIR:  Path = Path(os.getenv("PARQUET_PRO_DIR",  str(_BASE / "parquet_pro")))
PARQUET_USER_DIR: Path = Path(os.getenv("PARQUET_USER_DIR", str(_BASE / "parquet_user")))

for _d in (
    DEMOS_PRO_DIR,
    DEMOS_USER_DIR,
    PARQUET_PRO_DIR,
    PARQUET_USER_DIR,
):
    _d.mkdir(parents=True, exist_ok=True)


_CONTAINER_PATH_PREFIXES: tuple[tuple[str, Path], ...] = (
    ("/app/parquet_pro",  PARQUET_PRO_DIR),
    ("/app/parquet_user", PARQUET_USER_DIR),
    ("/app/demos_pro",    DEMOS_PRO_DIR),
    ("/app/demos_user",   DEMOS_USER_DIR),
)


def resolve_managed_path(raw_path: str | None) -> str | None:
    """Translate canonical /app paths stored in DB to the current local workspace."""
    if not raw_path:
        return raw_path

    for prefix, local_root in _CONTAINER_PATH_PREFIXES:
        if raw_path == prefix:
            return str(local_root)
        if raw_path.startswith(prefix + "/"):
            suffix = raw_path[len(prefix) + 1:]
            return str(local_root / suffix)
    return raw_path


def to_managed_path(local_path: str | Path) -> str:
    """Normalize a local filesystem path to its canonical /app/... form for DB storage.

    This ensures paths written by host-side scripts and container-side scripts are
    identical in the DB, so resolve_managed_path always works at read time.
    """
    raw = str(local_path)
    for prefix, local_root in _CONTAINER_PATH_PREFIXES:
        root = str(local_root)
        if raw == root:
            return prefix
        if raw.startswith(root + "/"):
            suffix = raw[len(root) + 1:]
            return f"{prefix}/{suffix}"
    return raw
