#!/usr/bin/env python3.13
"""
Poll all registered users for new CS2 Premier matches.

Run on a cron or loop — every 15 minutes is a good cadence:

    # One-shot:
    /home/tomyan/Code/VENV/cs2_shadowpro/bin/python pipeline/poll_users.py

    # Loop (bash):
    while true; do
        /home/tomyan/Code/VENV/cs2_shadowpro/bin/python pipeline/poll_users.py
        sleep 900
    done
"""

import sqlite3
import sys
import time
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from backend.db_users import get_all_users, init_users_table  # noqa: E402
from backend.user_matches import sync_user  # noqa: E402

DB_PATH = Path(__file__).resolve().parent.parent / "situations.db"


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    init_users_table(conn)
    users = get_all_users(conn)
    conn.close()

    if not users:
        print("[poll] No users registered yet.")
        return

    print(f"[poll] {len(users)} user(s) to sync")

    for user in users:
        steam_id = user["steam_id"]
        print(f"[poll] Syncing {steam_id} ...", end=" ", flush=True)
        try:
            result = sync_user(steam_id)
            print(result)
        except Exception as exc:
            print(f"ERROR: {exc}")
        time.sleep(2)  # gentle pause between users


if __name__ == "__main__":
    main()
