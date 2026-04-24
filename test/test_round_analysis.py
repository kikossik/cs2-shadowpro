import unittest

from backend.round_analysis import (
    analyze_shortlisted_rounds,
    match_nav_rounds,
    match_original_rounds,
)


TICK_RATE = 64
ORIGIN_TICK = 1000


def _tick(seconds: float) -> int:
    return ORIGIN_TICK + int(seconds * TICK_RATE)


def _segments_from_places(places: list[str], *, duration_s: float = 4.0, alive_start: int = 5) -> list[dict]:
    segments: list[dict] = []
    start_s = 0.0
    for idx, place in enumerate(places):
        end_s = start_s + duration_s
        segments.append({
            "start_tick": _tick(start_s),
            "end_tick": _tick(end_s),
            "place": place,
            "alive_players": max(1, alive_start - max(0, idx - 2)),
            "players": ["p1", "p2", "p3", "p4", "p5"],
        })
        start_s = end_s
    return segments


def _centroid_from_places(places: list[str], *, duration_s: float = 4.0, offset: float = 0.0) -> list[dict]:
    trace: list[dict] = []
    for idx, _ in enumerate(places):
        time_s = idx * duration_s
        trace.append({
            "tick": _tick(time_s),
            "x": (idx * 180.0) + offset,
            "y": (idx * 90.0) + offset,
            "alive_players": max(1, 5 - max(0, idx - 2)),
        })
    trace.append({
        "tick": _tick(len(places) * duration_s),
        "x": (len(places) * 180.0) + offset,
        "y": (len(places) * 90.0) + offset,
        "alive_players": 3,
    })
    return trace


def _nav_sequences_from_places(places: list[str], *, side: str = "t", winner: str = "t") -> dict[str, dict]:
    area_by_place = {place: sum(ord(ch) for ch in place) for place in dict.fromkeys(places)}
    sample_times = [idx * 4.0 for idx, _ in enumerate(places)]
    area_ids = [area_by_place[place] for place in places]
    support_counts = [2 for _ in places]
    enemy_counts = [0 for _ in places]
    bomb_flags = [False for _ in places]
    route_steps = [
        {
            "area_id": area_id,
            "start_sec": time_s,
            "end_sec": time_s,
            "support_count": 2.0,
            "enemy_count": 0.0,
            "bomb_planted": False,
        }
        for time_s, area_id in zip(sample_times, area_ids)
    ]
    return {
        "steam-test": {
            "sample_times": sample_times,
            "sample_ticks": [_tick(time_s) for time_s in sample_times],
            "area_ids": area_ids,
            "support_counts": support_counts,
            "enemy_counts": enemy_counts,
            "nearby_teammate_deaths": [0 for _ in places],
            "bomb_planted_flags": bomb_flags,
            "local_balance": [2 for _ in places],
            "alive_flags": [True for _ in places],
            "route_steps": route_steps,
            "route_step_count": len(route_steps),
            "start_area_id": route_steps[0]["area_id"],
            "death_sec": None,
            "survived_round": True,
            "team_won": winner == side,
            "round_winner": winner,
            "round_reason": "ct_killed",
            "round_end_sec": sample_times[-1] if sample_times else 0.0,
            "round_num": 1,
            "steamid": "steam-test",
            "side": side,
            "name": "test",
        }
    }


def _round_windows(phases: list[str], *, side_to_query: str = "t", site: str = "a") -> list[dict]:
    windows: list[dict] = []
    for idx, phase in enumerate(phases):
        time_s = idx * 4.0
        windows.append({
            "start_tick": _tick(time_s),
            "anchor_tick": _tick(time_s + 2.0),
            "end_tick": _tick(time_s + 4.0),
            "anchor_kind": "time_slice",
            "phase": phase,
            "primary_situation": phase,
            "situation_tags": [phase, f"site_{site}"],
            "site": site,
            "planted": False,
            "side_to_query": side_to_query,
            "focus_weapon_family": None,
            "time_since_freeze_end_s": time_s + 2.0,
            "time_since_bomb_plant_s": None,
            "seconds_remaining_s": 40.0 - time_s,
            "alive_ct": max(1, 5 - idx // 2),
            "alive_t": max(1, 5 - idx // 3),
            "queryable": True,
            "skip_reason": None,
            "window_summary": {
                "ticks_observed": 256,
                "shots_count": idx * 3,
                "smokes_count": 1 if idx >= 1 else 0,
                "infernos_count": 1 if phase == "fight" else 0,
                "flashes_count": 1 if idx >= 2 else 0,
                "he_count": 0,
                "deaths_ct": 1 if idx >= 2 else 0,
                "deaths_t": 1 if idx >= 3 else 0,
            },
        })
    return windows


def _team_windows(phases: list[str], *, site: str = "a", offset: float = 0.0) -> list[dict]:
    windows: list[dict] = []
    for idx, phase in enumerate(phases):
        windows.append({
            "start_tick": _tick(idx * 4.0),
            "anchor_tick": _tick(idx * 4.0 + 2.0),
            "end_tick": _tick(idx * 4.0 + 4.0),
            "anchor_kind": "time_slice",
            "phase": phase,
            "site": site,
            "planted": False,
            "alive_players": max(1, 5 - idx // 2),
            "top_places": [site.upper(), f"Lane{idx}"],
            "place_profile": {site.upper(): 3, f"Lane{idx}": 2},
            "weapon_profile": {"rifle": 5},
            "primary_weapons": ["AK-47"] * 5,
            "centroid_path": [
                [offset + idx * 100.0, offset + idx * 50.0],
                [offset + idx * 100.0 + 30.0, offset + idx * 50.0 + 15.0],
            ],
            "path_distance": 150.0 + idx * 20.0,
        })
    return windows


def _build_artifact(
    *,
    route_places_t: list[str],
    route_places_ct: list[str] | None = None,
    phases: list[str],
    side_to_query: str = "t",
    site: str = "a",
    winner: str = "t",
    reason: str = "ct_killed",
    offset: float = 0.0,
    first_shot_s: float = 8.0,
    first_utility_s: float = 6.0,
    death_times_s: list[float] | None = None,
) -> dict:
    route_places_ct = route_places_ct or ["CTSpawn", "Ticket", "Jungle", "Site"]
    death_times_s = death_times_s or [14.0, 18.0]
    round_end_s = max(len(route_places_t), len(route_places_ct), len(phases)) * 4.0
    return {
        "timing": {
            "tick_rate": TICK_RATE,
            "timeline_origin_tick": ORIGIN_TICK,
            "round_end_tick": _tick(round_end_s),
        },
        "round": {
            "winner": winner,
            "reason": reason,
            "bomb_site": site,
        },
        "map_name": "de_test",
        "user_steamid": "steam-test",
        "events": {
            "first_shot_tick": _tick(first_shot_s),
            "first_utility_tick": _tick(first_utility_s),
            "death_ticks": [_tick(value) for value in death_times_s],
            "bomb_plant_tick": None,
        },
        "windows": _round_windows(phases, side_to_query=side_to_query, site=site),
        "teams": {
            "t": {
                "windows": _team_windows(phases, site=site, offset=offset),
                "nav_trace": {
                    "centroid_trace": _centroid_from_places(route_places_t, offset=offset),
                    "dominant_place_segments": _segments_from_places(route_places_t),
                    "player_place_routes": {},
                    "player_nav_sequences": _nav_sequences_from_places(route_places_t, side="t", winner=winner),
                },
            },
            "ct": {
                "windows": _team_windows(phases, site=site, offset=offset + 30.0),
                "nav_trace": {
                    "centroid_trace": _centroid_from_places(route_places_ct, offset=offset + 40.0),
                    "dominant_place_segments": _segments_from_places(route_places_ct),
                    "player_place_routes": {},
                    "player_nav_sequences": _nav_sequences_from_places(route_places_ct, side="ct", winner=winner),
                },
            },
        },
    }


class RoundAnalysisMatcherTests(unittest.TestCase):
    def test_nav_match_detects_shared_prefix_and_route_split(self) -> None:
        user_artifact = _build_artifact(
            route_places_t=["TSpawn", "Mid", "Catwalk", "BApps"],
            phases=["default", "contact", "fight", "trade_window"],
        )
        pro_artifact = _build_artifact(
            route_places_t=["TSpawn", "Mid", "Catwalk", "ARamp"],
            phases=["default", "contact", "fight", "trade_window"],
            offset=10.0,
        )

        result = match_nav_rounds(
            query={"side_to_query": "t"},
            user_artifact=user_artifact,
            pro_artifact=pro_artifact,
        )

        self.assertGreater(result["score"], 0.5)
        self.assertEqual(result["query_side"], "t")
        self.assertGreaterEqual(result["shared_prefix"]["duration_s"], 8.0)
        self.assertEqual(result["break_event"]["type"], "route_deviation")

    def test_original_match_prefers_structurally_similar_round(self) -> None:
        user_artifact = _build_artifact(
            route_places_t=["TSpawn", "Mid", "Connector", "Site"],
            phases=["default", "contact", "fight", "trade_window"],
            site="a",
        )
        similar_pro = _build_artifact(
            route_places_t=["TSpawn", "Mid", "Connector", "Site"],
            phases=["default", "contact", "fight", "trade_window"],
            site="a",
            offset=8.0,
        )
        dissimilar_pro = _build_artifact(
            route_places_t=["TSpawn", "Palace", "Ramp", "Site"],
            phases=["default", "late_opening", "rotate", "contact"],
            site="b",
            winner="ct",
            reason="t_killed",
            offset=30.0,
            first_shot_s=14.0,
            first_utility_s=2.0,
        )

        similar_result = match_original_rounds(
            query={"side_to_query": "t"},
            user_artifact=user_artifact,
            pro_artifact=similar_pro,
        )
        dissimilar_result = match_original_rounds(
            query={"side_to_query": "t"},
            user_artifact=user_artifact,
            pro_artifact=dissimilar_pro,
        )

        self.assertGreater(similar_result["score"], dissimilar_result["score"])
        self.assertGreater(similar_result["shared_prefix"]["duration_s"], 0.0)

    def test_both_logic_selects_best_deep_match_from_shortlist(self) -> None:
        user_artifact = _build_artifact(
            route_places_t=["TSpawn", "Mid", "Connector", "Site"],
            phases=["default", "contact", "fight", "trade_window"],
        )
        close_candidate = {
            "source_match_id": "pro-close",
            "round_num": 7,
            "map_name": "de_mirage",
            "event_name": "Test Event",
            "team_ct": "A",
            "team_t": "B",
            "match_date": "2026-04-22",
            "score": 0.82,
            "best_window_score": 0.79,
            "coverage": 0.75,
            "supporting_window_hits": 4,
            "matched_query_windows": 3,
            "query_anchor_kinds": ["time_slice"],
            "shortlist_rank": 1,
            "top_window": {
                "window_id": "close-win",
                "source_match_id": "pro-close",
                "map_name": "de_mirage",
                "round_num": 7,
                "anchor_tick": _tick(10.0),
                "start_tick": _tick(8.0),
                "end_tick": _tick(12.0),
                "phase": "fight",
                "site": "a",
                "anchor_kind": "time_slice",
                "score": 0.79,
                "reason": "close window",
                "feature_path": "/tmp/close.json",
            },
            "window_hits": [],
            "artifact": _build_artifact(
                route_places_t=["TSpawn", "Mid", "Connector", "Site"],
                phases=["default", "contact", "fight", "trade_window"],
                offset=5.0,
            ),
        }
        weak_candidate = {
            "source_match_id": "pro-weak",
            "round_num": 11,
            "map_name": "de_mirage",
            "event_name": "Test Event",
            "team_ct": "C",
            "team_t": "D",
            "match_date": "2026-04-22",
            "score": 0.8,
            "best_window_score": 0.76,
            "coverage": 0.7,
            "supporting_window_hits": 4,
            "matched_query_windows": 3,
            "query_anchor_kinds": ["time_slice"],
            "shortlist_rank": 2,
            "top_window": {
                "window_id": "weak-win",
                "source_match_id": "pro-weak",
                "map_name": "de_mirage",
                "round_num": 11,
                "anchor_tick": _tick(10.0),
                "start_tick": _tick(8.0),
                "end_tick": _tick(12.0),
                "phase": "rotate",
                "site": "b",
                "anchor_kind": "time_slice",
                "score": 0.76,
                "reason": "weak window",
                "feature_path": "/tmp/weak.json",
            },
            "window_hits": [],
            "artifact": _build_artifact(
                route_places_t=["TSpawn", "Palace", "Ramp", "A Site"],
                phases=["default", "late_opening", "rotate", "contact"],
                site="b",
                winner="ct",
                reason="t_killed",
                offset=40.0,
                first_shot_s=14.0,
                first_utility_s=2.0,
            ),
        }

        analysis = analyze_shortlisted_rounds(
            query={"side_to_query": "t"},
            user_artifact=user_artifact,
            candidates=[close_candidate, weak_candidate],
            logic="both",
        )

        self.assertEqual(analysis["selected_match"]["source_match_id"], "pro-close")
        self.assertGreater(
            analysis["matches"][0]["logic_scores"]["both"],
            analysis["matches"][1]["logic_scores"]["both"],
        )


if __name__ == "__main__":
    unittest.main()
