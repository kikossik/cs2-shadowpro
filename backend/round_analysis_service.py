"""Round-analysis cache orchestration.

This module is intentionally thin: cache-aware compute that delegates the
actual user→pro round mapping to `backend.round_mapper`. Callers (FastAPI
route, worker precompute) hit `compute_and_cache_round`.
"""
from __future__ import annotations

from backend import db
from backend.log import get_logger
from backend.round_mapper import map_user_round_to_pro_round

log = get_logger("ROUND_ANALYSIS")

_MAP_DISPLAY: dict[str, str] = {
    "de_ancient": "Ancient", "de_anubis": "Anubis", "de_dust2": "Dust 2",
    "de_inferno": "Inferno", "de_mirage": "Mirage", "de_nuke": "Nuke",
    "de_overpass": "Overpass",
}


def _map_display(map_name: str | None) -> dict:
    name = map_name or "unknown"
    display = _MAP_DISPLAY.get(name, name.replace("de_", "").title())
    return {"key": name.replace("de_", ""), "name": name, "display": display}


def normalize_round_analysis_result(result: dict | None) -> dict | None:
    """Inject map display metadata into a cached payload before returning."""
    if result is None:
        return None
    payload = dict(result)
    best = payload.get("best_match")
    if best is not None and best.get("map") is None:
        best = dict(best)
        best["map"] = _map_display(best.get("map_name"))
        payload["best_match"] = best
    return payload


async def compute_and_cache_round(
    demo_id: str,
    round_num: int,
) -> dict:
    """Compute the user→pro mapping for one round and persist it.

    Raises on error; the caller is responsible for storing an error row if
    needed.
    """
    best = await map_user_round_to_pro_round(demo_id, round_num)
    payload = {
        "best_match": best,
        "query": {"demo_id": demo_id, "round_num": round_num},
    }
    await db.upsert_round_analysis_result(
        demo_id=demo_id,
        round_num=round_num,
        status="done",
        result_payload=payload,
    )
    return payload
