import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# PostgreSQL connection string — e.g. postgresql://user:pass@localhost/cs2shadowpro
DATABASE_URL: str = os.environ["DATABASE_URL"]

STEAM_API_KEY:        str = os.getenv("STEAM_API_KEY", "")
RESOLVER_URL:         str = os.getenv("RESOLVER_URL", "http://127.0.0.1:3001")
SYNC_INTERVAL_SECONDS: int = int(os.getenv("SYNC_INTERVAL_SECONDS", "300"))

# Directories for demo storage
_BASE = Path(__file__).parent.parent
DEMOS_PRO_DIR:    Path = Path(os.getenv("DEMOS_PRO_DIR",    str(_BASE / "demos_pro")))
DEMOS_USER_DIR:   Path = Path(os.getenv("DEMOS_USER_DIR",   str(_BASE / "demos_user")))
PARQUET_PRO_DIR:  Path = Path(os.getenv("PARQUET_PRO_DIR",  str(_BASE / "parquet_pro")))
PARQUET_USER_DIR: Path = Path(os.getenv("PARQUET_USER_DIR", str(_BASE / "parquet_user")))

for _d in (DEMOS_PRO_DIR, DEMOS_USER_DIR, PARQUET_PRO_DIR, PARQUET_USER_DIR):
    _d.mkdir(parents=True, exist_ok=True)
