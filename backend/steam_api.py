"""
Steam Web API helpers.

Only used for player profile lookup (name, avatar).  All CS2 match data
comes from parsed demo files, not from the Steam API.
"""

import httpx

STEAM_API_BASE = "https://api.steampowered.com"


async def fetch_profile(steam_id: str, api_key: str) -> dict:
    """Return {'steam_id', 'personaname', 'avatar'} for the given 64-bit Steam ID."""
    url = f"{STEAM_API_BASE}/ISteamUser/GetPlayerSummaries/v2/"
    async with httpx.AsyncClient(timeout=8) as client:
        r = await client.get(url, params={"key": api_key, "steamids": steam_id})
        r.raise_for_status()
        players = r.json()["response"]["players"]
    if not players:
        raise ValueError(f"Steam ID not found: {steam_id}")
    p = players[0]
    return {
        "steam_id": steam_id,
        "personaname": p["personaname"],
        "avatar": p.get("avatarmedium") or p.get("avatar", ""),
    }
