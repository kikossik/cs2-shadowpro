#!/usr/bin/env python3.13
"""
Poll all registered users for new CS2 Premier matches.

Run on a cron or loop — every 15 minutes is a good cadence:

    # One-shot (from project root):
    /home/tomyan/Code/VENV/cs2_shadowpro/bin/python -m pipeline.poll_users

    # Loop (fish):
    while true; /home/tomyan/Code/VENV/cs2_shadowpro/bin/python -m pipeline.poll_users; sleep 900; end
"""

import time

from backend.config import DB_PATH
from backend.db import connect, init_schema, get_all_users
from backend.sync import sync_user


def main() -> None:
    conn = connect(DB_PATH)
    init_schema(conn)
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
        time.sleep(2)


if __name__ == "__main__":
    main()
