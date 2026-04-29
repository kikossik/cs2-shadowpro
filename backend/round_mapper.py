"""User-round → pro-round mapping.

PLACEHOLDER IMPLEMENTATION. The body of `map_user_round_to_pro_round` returns
the top-1 ANN candidate as the best match. Replace with a real algorithm when
iterating on match quality. The function signature and return shape are the
contract the rest of the system depends on; keep them stable.
"""
from __future__ import annotations

from backend import db
from backend.retrieval import build_pro_round_shortlist


async def map_user_round_to_pro_round(
    demo_id: str,
    round_num: int,
) -> dict | None:
    """Return one pro round most similar to the given user round.

    Returns a dict with the fields the frontend's `best_match` expects, or
    None if no candidate could be found. Score is a 0..1 cosine-derived
    similarity from the ANN index — meaningful for ranking, not calibrated.
    """
    shortlist_result = await build_pro_round_shortlist(demo_id, round_num)
    shortlist = shortlist_result.get("shortlist") or []
    if not shortlist:
        return None

    top = shortlist[0]
    record = await db.get_match_source_record(top["source_match_id"])
    if record is None:
        return None

    match_date = record.get("match_date")
    return {
        "source_match_id":  top["source_match_id"],
        "round_num":        top["round_num"],
        "score":            top["score"],
        "best_window_score": top["best_window_score"],
        "coverage":         top["coverage"],
        "supporting_window_hits": top["supporting_window_hits"],
        "matched_query_windows":  top["matched_query_windows"],
        "top_window":       top["top_window"],
        "map_name":         record.get("map_name"),
        "event_name":       record.get("event_name"),
        "team1_name":       record.get("team1_name"),
        "team2_name":       record.get("team2_name"),
        "team_ct":          record.get("team_ct"),
        "team_t":           record.get("team_t"),
        "match_date":       match_date.isoformat() if match_date else None,
    }
