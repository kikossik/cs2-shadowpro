"""
Centralised configuration — paths and env vars.
All other modules import from here; nothing reads os.getenv directly.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

DB_PATH = ROOT / "situations.db"
DEMOS_PRO_DIR = ROOT / "demos"
DEMOS_PRO_DECOMPRESSED_DIR = ROOT / "demos_decompressed"
DEMOS_USER_DIR = ROOT / "demos_user"
DEMOS_USER_DIR.mkdir(exist_ok=True)

STEAM_API_KEY: str = os.getenv("STEAM_API_KEY", "")
RESOLVER_URL: str = os.getenv("RESOLVER_URL", "http://127.0.0.1:3001")
