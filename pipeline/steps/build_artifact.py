"""Build a single per-match artifact JSON from an existing parquet set.

The artifact contains all rounds keyed by round number. Each round has:
  - windows and team windows (for original matching logic)
  - event timing summary
  - per-player nav sequences (for nav matching logic)
  - team centroid traces and place timelines

This replaces the per-round round_artifacts table from the refactor.
"""
from __future__ import annotations

import json
import math
import bisect
from collections import Counter
from pathlib import Path

import polars as pl

from backend.config import derived_match_dir, to_managed_path
from backend.log import get_logger

log = get_logger("ARTIFACT")
from pipeline.features.extract_windows import list_match_anchor_specs, load_match_frames
from pipeline.features.featurize_windows import FEATURE_VERSION, TICK_RATE, build_window_features

ARTIFACT_VERSION = "clean-v2"
_SIDES = ("ct", "t")
_WEAPON_FAMILIES = ("sniper", "rifle", "smg", "shotgun", "heavy", "pistol")
_WEAPON_PRIORITY = {
    "sniper": 6,
    "rifle": 5,
    "smg": 4,
    "shotgun": 3,
    "heavy": 2,
    "pistol": 1,
}
_SITE_A_TOKENS = {"a", "abombsite", "bombsitea", "asite", "aramp", "ramp", "palace", "heaven", "ticketbooth"}
_SITE_B_TOKENS = {"b", "bbombsite", "bombsiteb", "bsite", "apartments", "apps", "banana", "truck", "shop", "market"}


def _safe_int(value: object | None, default: int | None = None) -> int | None:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: object | None, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _steamid_key(value: object) -> str:
    return str(value or "")


def _normalize_token(value: object | None) -> str | None:
    if not value:
        return None
    normalized = "".join(ch for ch in str(value).lower() if ch.isalnum())
    return normalized or None


def _weapon_family_name(name: object | None) -> str | None:
    token = _normalize_token(name)
    if not token:
        return None
    if token.startswith("weapon"):
        token = token[6:]
    aliases = {
        "uspsilencer": "usps",
        "m4a1silencer": "m4a1s",
        "incendiary": "incendiarygrenade",
    }
    token = aliases.get(token, token)
    if token in {"awp", "ssg08", "scar20", "g3sg1"}:
        return "sniper"
    if token in {"ak47", "m4a4", "m4a1s", "aug", "sg553", "galilar", "galil", "famas"}:
        return "rifle"
    if token in {"mp9", "mac10", "mp7", "mp5sd", "ump45", "p90", "ppbizon", "bizon"}:
        return "smg"
    if token in {"nova", "xm1014", "mag7", "sawedoff"}:
        return "shotgun"
    if token in {"negev", "m249"}:
        return "heavy"
    if token in {"glock18", "usps", "usp", "p2000", "p250", "deagle", "deserteagle", "tec9", "cz75auto", "cz75a", "fiveseven", "elites", "dualberettas", "revolver"}:
        return "pistol"
    return None


def _primary_weapon_family(inventory: object) -> str | None:
    if not isinstance(inventory, list):
        return None
    best_family: str | None = None
    best_priority = -1
    for item in inventory:
        family = _weapon_family_name(item)
        priority = _WEAPON_PRIORITY.get(family or "", 0)
        if priority > best_priority:
            best_priority = priority
            best_family = family
    return best_family


def _top_family(profile: dict[str, int]) -> str | None:
    candidates = [(family, count) for family, count in profile.items() if count > 0]
    if not candidates:
        return None
    candidates.sort(key=lambda item: (_WEAPON_PRIORITY.get(item[0], 0), item[1]), reverse=True)
    return candidates[0][0]


def _infer_site_from_places(places: list[str]) -> str | None:
    votes = Counter()
    for place in places:
        token = _normalize_token(place)
        if token in _SITE_A_TOKENS:
            votes["a"] += 1
        elif token in _SITE_B_TOKENS:
            votes["b"] += 1
    if not votes:
        return None
    return votes.most_common(1)[0][0]


def _round_phase(*, planted: bool, alive_t: int, alive_ct: int, start_sec: float) -> str:
    if planted:
        return "postplant"
    if alive_t <= 2 or alive_ct <= 2:
        return "late_round"
    if start_sec <= 15:
        return "opening"
    return "mid_round"


def _round_rows(frames: dict[str, pl.DataFrame], round_num: int) -> dict[str, pl.DataFrame]:
    rows: dict[str, pl.DataFrame] = {}
    for field, frame in frames.items():
        if frame.height == 0 or "round_num" not in frame.columns:
            rows[field] = pl.DataFrame()
            continue
        rows[field] = frame.filter(pl.col("round_num") == round_num).sort(
            "tick" if "tick" in frame.columns else "round_num"
        )
    return rows


def _round_metadata(round_row: dict) -> dict:
    return {
        "round_num": _safe_int(round_row.get("round_num"), default=0),
        "start_tick": _safe_int(round_row.get("start"), default=0),
        "freeze_end_tick": _safe_int(
            round_row.get("freeze_end"),
            default=_safe_int(round_row.get("start"), default=0),
        ),
        "end_tick": _safe_int(round_row.get("end"), default=0),
        "official_end_tick": _safe_int(
            round_row.get("official_end"),
            default=_safe_int(round_row.get("end"), default=0),
        ),
        "bomb_plant_tick": _safe_int(round_row.get("bomb_plant")),
        "bomb_site": round_row.get("bomb_site"),
        "winner": round_row.get("winner"),
        "reason": round_row.get("reason"),
    }


def _player_entries(round_ticks: pl.DataFrame, side: str) -> list[dict]:
    if round_ticks.height == 0 or "side" not in round_ticks.columns or "steamid" not in round_ticks.columns:
        return []
    filtered = round_ticks.filter(pl.col("side").str.to_lowercase() == side)
    if filtered.height == 0:
        return []
    cols = [col for col in ("steamid", "name") if col in filtered.columns]
    first_rows = (
        filtered
        .select(cols + (["tick"] if "tick" in filtered.columns else []))
        .sort("tick" if "tick" in filtered.columns else "steamid")
        .group_by("steamid", maintain_order=True)
        .first()
    )
    return [
        {"steamid": str(row.get("steamid") or ""), "name": row.get("name") or ""}
        for row in first_rows.iter_rows(named=True)
    ]


def _sample_team_centroid_trace(round_ticks: pl.DataFrame, side: str, max_points: int = 16) -> list[dict]:
    if round_ticks.height == 0 or "tick" not in round_ticks.columns or "side" not in round_ticks.columns:
        return []
    team_rows = round_ticks.filter(pl.col("side").str.to_lowercase() == side)
    if team_rows.height == 0:
        return []
    ticks = sorted(set(team_rows["tick"].to_list()))
    if not ticks:
        return []
    if len(ticks) > max_points:
        step = max(1, len(ticks) // max_points)
        ticks = ticks[::step][:max_points]
        if ticks[-1] != int(team_rows["tick"].max()):
            ticks[-1] = int(team_rows["tick"].max())
    trace: list[dict] = []
    for tick in ticks:
        frame = team_rows.filter(pl.col("tick") == tick)
        if frame.height == 0:
            continue
        alive_count = frame.filter(pl.col("health") > 0).height if "health" in frame.columns else frame.height
        trace.append({
            "tick": int(tick),
            "x": round(float(frame["X"].mean()) if "X" in frame.columns else 0.0, 2),
            "y": round(float(frame["Y"].mean()) if "Y" in frame.columns else 0.0, 2),
            "alive_players": int(alive_count),
        })
    return trace


def _compress_team_place_trace(round_ticks: pl.DataFrame, side: str) -> list[dict]:
    if round_ticks.height == 0 or "tick" not in round_ticks.columns or "side" not in round_ticks.columns or "place" not in round_ticks.columns:
        return []
    team_rows = round_ticks.filter(pl.col("side").str.to_lowercase() == side).sort("tick")
    if team_rows.height == 0:
        return []
    segments: list[dict] = []
    current: dict | None = None
    for tick in sorted(set(team_rows["tick"].to_list())):
        frame = team_rows.filter(pl.col("tick") == tick)
        places = [str(place) for place in frame["place"].to_list() if place]
        place = Counter(places).most_common(1)[0][0] if places else "unknown"
        alive_players = frame.filter(pl.col("health") > 0).height if "health" in frame.columns else frame.height
        player_names = sorted({str(name) for name in frame["name"].to_list() if name}) if "name" in frame.columns else []
        if current and current["place"] == place:
            current["end_tick"] = int(tick)
            current["alive_players"] = max(int(current["alive_players"]), int(alive_players))
            current["players"] = sorted(set(current["players"]) | set(player_names))
            continue
        if current:
            segments.append(current)
        current = {
            "start_tick": int(tick),
            "end_tick": int(tick),
            "place": place,
            "alive_players": int(alive_players),
            "players": player_names,
        }
    if current:
        segments.append(current)
    return segments


def _compress_player_place_routes(round_ticks: pl.DataFrame, side: str) -> dict[str, list[dict]]:
    if round_ticks.height == 0 or "tick" not in round_ticks.columns or "side" not in round_ticks.columns or "steamid" not in round_ticks.columns:
        return {}
    team_rows = round_ticks.filter(pl.col("side").str.to_lowercase() == side).sort(["steamid", "tick"])
    if team_rows.height == 0:
        return {}
    routes: dict[str, list[dict]] = {}
    cols = [col for col in ("steamid", "tick", "place", "name", "health") if col in team_rows.columns]
    for _, player_rows in team_rows.select(cols).group_by("steamid", maintain_order=True):
        player_rows = player_rows.sort("tick")
        steamid = str(player_rows["steamid"][0])
        segments: list[dict] = []
        current: dict | None = None
        for row in player_rows.iter_rows(named=True):
            place = str(row.get("place") or "unknown")
            tick = int(row.get("tick") or 0)
            alive = int(row.get("health") or 0) > 0 if "health" in player_rows.columns else True
            if current and current["place"] == place and current["alive"] == alive:
                current["end_tick"] = tick
                continue
            if current:
                segments.append(current)
            current = {"start_tick": tick, "end_tick": tick, "place": place, "alive": alive}
        if current:
            segments.append(current)
        routes[steamid] = segments
    return routes


def _nearest_tick(target_tick: int, available_ticks: list[int]) -> int:
    idx = bisect.bisect_left(available_ticks, target_tick)
    if idx <= 0:
        return int(available_ticks[0])
    if idx >= len(available_ticks):
        return int(available_ticks[-1])
    before = int(available_ticks[idx - 1])
    after = int(available_ticks[idx])
    return before if abs(target_tick - before) <= abs(after - target_tick) else after


def _sample_ticks(window_start: int, window_end: int, available_ticks: list[int], samples: int) -> list[int]:
    if samples <= 1:
        return [_nearest_tick((window_start + window_end) // 2, available_ticks)]
    sampled: list[int] = []
    seen: set[int] = set()
    span = max(0, window_end - window_start)
    for idx in range(samples):
        target = window_start + round(span * idx / (samples - 1))
        tick = _nearest_tick(int(target), available_ticks)
        if tick not in seen:
            sampled.append(tick)
            seen.add(tick)
    return sampled


def _alive_rows(frame: pl.DataFrame, side: str) -> pl.DataFrame:
    if frame.height == 0 or "side" not in frame.columns:
        return pl.DataFrame()
    rows = frame.filter(pl.col("side").str.to_lowercase() == side)
    if "health" in rows.columns:
        rows = rows.filter(pl.col("health") > 0)
    return rows.sort("steamid") if "steamid" in rows.columns else rows


def _xy(row: dict) -> tuple[float, float]:
    return _safe_float(row.get("X")), _safe_float(row.get("Y"))


def _distance_xy(left: tuple[float, float] | list[float], right: tuple[float, float] | list[float]) -> float:
    return math.hypot(float(left[0]) - float(right[0]), float(left[1]) - float(right[1]))


def _team_shape_summary(rows: pl.DataFrame, previous_rows: pl.DataFrame | None = None) -> dict:
    if rows.height == 0:
        return {
            "centroid_x": 0.0,
            "centroid_y": 0.0,
            "spread": 0.0,
            "pairwise": 0.0,
            "mean_speed": 0.0,
            "radial_signature": [0.0] * 5,
        }

    dict_rows = list(rows.iter_rows(named=True))
    positions = [_xy(row) for row in dict_rows]
    cx = sum(point[0] for point in positions) / len(positions)
    cy = sum(point[1] for point in positions) / len(positions)
    radial = sorted(_distance_xy(point, (cx, cy)) for point in positions)
    pairwise: list[float] = []
    for left_idx, left in enumerate(positions):
        for right in positions[left_idx + 1:]:
            pairwise.append(_distance_xy(left, right))

    speeds: list[float] = []
    if previous_rows is not None and previous_rows.height > 0 and "steamid" in previous_rows.columns:
        previous_by_id = {
            _steamid_key(row.get("steamid")): row
            for row in previous_rows.iter_rows(named=True)
        }
        for row in dict_rows:
            prev = previous_by_id.get(_steamid_key(row.get("steamid")))
            if prev is not None:
                speeds.append(_distance_xy(_xy(row), _xy(prev)))

    radial.extend([0.0] * (5 - len(radial)))
    return {
        "centroid_x": round(cx, 2),
        "centroid_y": round(cy, 2),
        "spread": round(sum(radial) / max(len(positions), 1), 2),
        "pairwise": round(sum(pairwise) / len(pairwise), 2) if pairwise else 0.0,
        "mean_speed": round(sum(speeds) / len(speeds), 2) if speeds else 0.0,
        "radial_signature": [round(value, 2) for value in radial[:5]],
    }


def _track_speeds(positions: list[list[float]], sample_ticks: list[int]) -> list[float]:
    if not positions:
        return []
    speeds = [0.0]
    for idx in range(1, len(positions)):
        dt = max((int(sample_ticks[idx]) - int(sample_ticks[idx - 1])) / TICK_RATE, 1e-6)
        speeds.append(round(_distance_xy(positions[idx], positions[idx - 1]) / dt, 3))
    return speeds


def _path_length(positions: list[list[float]]) -> float:
    if len(positions) < 2:
        return 0.0
    return round(sum(_distance_xy(left, right) for left, right in zip(positions, positions[1:])), 3)


def _build_original_team_windows(
    round_ticks: pl.DataFrame,
    round_meta: dict,
    map_name: str,
    source_type: str,
    source_match_id: str,
    *,
    window_seconds: int = 6,
    stride_seconds: int = 2,
    samples_per_window: int = 8,
    min_alive_t: int = 2,
    min_alive_ct: int = 2,
) -> list[dict]:
    """Build groundup-style rolling team windows with embedded player tracks."""
    required = {"tick", "steamid", "side", "X", "Y"}
    if round_ticks.height == 0 or not required.issubset(set(round_ticks.columns)):
        return []

    available_ticks = sorted(int(tick) for tick in round_ticks["tick"].unique().to_list())
    if not available_ticks:
        return []
    snapshots = {
        int(key[0] if isinstance(key, tuple) else key): value
        for key, value in round_ticks.partition_by("tick", as_dict=True).items()
    }

    freeze_end = int(round_meta.get("freeze_end_tick") or round_meta.get("start_tick") or available_ticks[0])
    end_tick = int(round_meta.get("official_end_tick") or round_meta.get("end_tick") or available_ticks[-1])
    window_ticks = int(window_seconds * TICK_RATE)
    stride_ticks = int(stride_seconds * TICK_RATE)
    if end_tick - freeze_end < window_ticks:
        return []

    windows: list[dict] = []
    for window_start in range(freeze_end, end_tick - window_ticks + 1, stride_ticks):
        window_end = window_start + window_ticks
        sample_ticks = _sample_ticks(window_start, window_end, available_ticks, samples_per_window)
        sample_snapshots = [snapshots[tick] for tick in sample_ticks if tick in snapshots]
        if len(sample_snapshots) < 2:
            continue

        alive_sets: dict[str, list[set[str]]] = {"t": [], "ct": []}
        context_samples: list[dict] = []
        places: dict[str, list[str]] = {"t": [], "ct": []}
        weapon_profiles: dict[str, Counter] = {"t": Counter(), "ct": Counter()}
        previous: pl.DataFrame | None = None
        bomb_flags: list[bool] = []

        for tick, frame in zip(sample_ticks, sample_snapshots):
            planted = round_meta.get("bomb_plant_tick") is not None and int(tick) >= int(round_meta["bomb_plant_tick"])
            bomb_flags.append(planted)
            sample_payload = {"tick": int(tick)}
            for side in _SIDES:
                alive = _alive_rows(frame, side)
                alive_sets[side].append({_steamid_key(value) for value in alive["steamid"].to_list()} if alive.height else set())
                sample_payload[side] = _team_shape_summary(
                    alive,
                    _alive_rows(previous, side) if previous is not None else None,
                )
                if alive.height > 0:
                    if "place" in alive.columns:
                        places[side].extend(str(place) for place in alive["place"].to_list() if place)
                    if "inventory" in alive.columns:
                        for inventory in alive["inventory"].to_list():
                            family = _primary_weapon_family(inventory)
                            if family in _WEAPON_FAMILIES:
                                weapon_profiles[side][family] += 1
            if sample_payload["t"] and sample_payload["ct"]:
                t_shape = sample_payload["t"]
                ct_shape = sample_payload["ct"]
                sample_payload["team_gap"] = _distance_xy(
                    (t_shape["centroid_x"], t_shape["centroid_y"]),
                    (ct_shape["centroid_x"], ct_shape["centroid_y"]),
                )
            else:
                sample_payload["team_gap"] = 0.0
            context_samples.append(sample_payload)
            previous = frame

        stable_t = sorted(set.intersection(*alive_sets["t"])) if alive_sets["t"] else []
        stable_ct = sorted(set.intersection(*alive_sets["ct"])) if alive_sets["ct"] else []
        if len(stable_t) < min_alive_t or len(stable_ct) < min_alive_ct:
            continue

        tracks_by_side: dict[str, dict[str, dict]] = {"t": {}, "ct": {}}
        valid_window = True
        for side, stable_ids in (("t", stable_t), ("ct", stable_ct)):
            for steamid in stable_ids:
                positions: list[list[float]] = []
                yaws: list[float] = []
                relative_to_team: list[list[float]] = []
                player_name = ""
                for frame in sample_snapshots:
                    player_row = frame.filter(pl.col("steamid").cast(pl.String) == steamid)
                    if player_row.height == 0:
                        valid_window = False
                        break
                    row = player_row.row(0, named=True)
                    if "health" in player_row.columns and int(row.get("health") or 0) <= 0:
                        valid_window = False
                        break
                    x = _safe_float(row.get("X"))
                    y = _safe_float(row.get("Y"))
                    positions.append([round(x, 2), round(y, 2)])
                    yaws.append(round(_safe_float(row.get("yaw")), 2))
                    player_name = str(row.get("name") or "")
                    side_alive = _alive_rows(frame, side)
                    cx = _safe_float(side_alive["X"].mean()) if side_alive.height > 0 and "X" in side_alive.columns else 0.0
                    cy = _safe_float(side_alive["Y"].mean()) if side_alive.height > 0 and "Y" in side_alive.columns else 0.0
                    relative_to_team.append([round(x - cx, 2), round(y - cy, 2)])
                if not valid_window:
                    break
                tracks_by_side[side][steamid] = {
                    "steamid": steamid,
                    "name": player_name,
                    "side": side,
                    "positions": positions,
                    "speeds": _track_speeds(positions, sample_ticks),
                    "yaws": yaws,
                    "relative_to_team": relative_to_team,
                    "path_length": _path_length(positions),
                }
            if not valid_window:
                break
        if not valid_window:
            continue

        planted_ratio = sum(1 for flag in bomb_flags if flag) / max(len(bomb_flags), 1)
        planted = planted_ratio >= 0.5
        all_places = places["t"] + places["ct"]
        round_site = str(round_meta.get("bomb_site") or "").lower() or None
        if round_site and "a" in round_site and "b" not in round_site:
            site = "a"
        elif round_site and "b" in round_site and "a" not in round_site:
            site = "b"
        else:
            site = _infer_site_from_places(all_places) if planted else None

        start_sec = (window_start - freeze_end) / TICK_RATE
        end_sec = (window_end - freeze_end) / TICK_RATE
        time_since_plant = None
        if planted and round_meta.get("bomb_plant_tick") is not None:
            time_since_plant = max(0.0, ((window_start + window_end) / 2 - int(round_meta["bomb_plant_tick"])) / TICK_RATE)

        t_weapon_profile = {family: int(weapon_profiles["t"].get(family, 0)) for family in _WEAPON_FAMILIES}
        ct_weapon_profile = {family: int(weapon_profiles["ct"].get(family, 0)) for family in _WEAPON_FAMILIES}
        windows.append({
            "source_type": source_type,
            "source_match_id": source_match_id,
            "map_name": map_name,
            "round_num": int(round_meta.get("round_num") or 0),
            "round_winner": round_meta.get("winner"),
            "round_reason": round_meta.get("reason"),
            "window_start_tick": int(window_start),
            "window_end_tick": int(window_end),
            "window_mid_tick": int((window_start + window_end) // 2),
            "window_duration_sec": round(window_seconds, 3),
            "window_start_sec_from_freeze": round(start_sec, 3),
            "window_end_sec_from_freeze": round(end_sec, 3),
            "round_duration_sec": round(max(0.0, (end_tick - freeze_end) / TICK_RATE), 3),
            "bomb_planted_ratio": round(planted_ratio, 4),
            "planted": planted,
            "site": site,
            "time_since_plant_s": round(time_since_plant, 3) if time_since_plant is not None else None,
            "phase": _round_phase(planted=planted, alive_t=len(stable_t), alive_ct=len(stable_ct), start_sec=start_sec),
            "alive_t": len(stable_t),
            "alive_ct": len(stable_ct),
            "alive_total": len(stable_t) + len(stable_ct),
            "sample_ticks": [int(value) for value in sample_ticks],
            "t_alive_steamids": stable_t,
            "ct_alive_steamids": stable_ct,
            "t_place_profile": dict(Counter(_normalize_token(place) or place for place in places["t"])),
            "ct_place_profile": dict(Counter(_normalize_token(place) or place for place in places["ct"])),
            "t_top_places": [place for place, _ in Counter(_normalize_token(place) or place for place in places["t"]).most_common(3)],
            "ct_top_places": [place for place, _ in Counter(_normalize_token(place) or place for place in places["ct"]).most_common(3)],
            "t_weapon_profile": t_weapon_profile,
            "ct_weapon_profile": ct_weapon_profile,
            "t_focus_weapon_family": _top_family(t_weapon_profile),
            "ct_focus_weapon_family": _top_family(ct_weapon_profile),
            "context_samples": context_samples,
            "tracks_by_side": tracks_by_side,
        })

    return windows


def _window_projection(anchor: dict, features: dict) -> dict:
    return {
        "start_tick": anchor["start_tick"],
        "anchor_tick": anchor["anchor_tick"],
        "end_tick": anchor["end_tick"],
        "anchor_kind": anchor["anchor_kind"],
        "phase": features.get("phase"),
        "primary_situation": features.get("primary_situation"),
        "situation_tags": features.get("situation_tags", []),
        "site": features.get("site"),
        "planted": features.get("planted"),
        "side_to_query": features.get("side_to_query"),
        "focus_weapon_family": features.get("focus_weapon_family"),
        "time_since_freeze_end_s": features.get("time_since_freeze_end_s"),
        "time_since_bomb_plant_s": features.get("time_since_bomb_plant_s"),
        "seconds_remaining_s": features.get("seconds_remaining_s"),
        "alive_ct": features.get("alive_ct"),
        "alive_t": features.get("alive_t"),
        "queryable": features.get("queryable", True),
        "skip_reason": features.get("skip_reason"),
        "window_summary": features.get("window_summary", {}),
    }


def _team_window_projection(side: str, anchor: dict, features: dict) -> dict:
    vector = features.get("vector", {})
    return {
        "start_tick": anchor["start_tick"],
        "anchor_tick": anchor["anchor_tick"],
        "end_tick": anchor["end_tick"],
        "anchor_kind": anchor["anchor_kind"],
        "phase": features.get("phase"),
        "site": features.get("site"),
        "planted": features.get("planted"),
        "alive_players": features.get(f"alive_{side}", 0),
        "top_places": features.get(f"{side}_top_places", []),
        "place_profile": features.get(f"{side}_place_profile", {}),
        "weapon_profile": features.get(f"{side}_weapon_profile", {}),
        "primary_weapons": features.get(f"{side}_primary_weapons", []),
        "centroid_path": features.get(f"{side}_centroid_path", []),
        "path_distance": vector.get(f"{side}_path_distance"),
    }


def _event_summary(round_frames: dict[str, pl.DataFrame], round_meta: dict) -> dict:
    first_shot_tick = None
    if round_frames["shots"].height > 0 and "tick" in round_frames["shots"].columns:
        filtered = round_frames["shots"].filter(pl.col("tick") >= round_meta["freeze_end_tick"])
        if filtered.height > 0:
            first_shot_tick = int(filtered["tick"].min())

    utility_ticks: list[int] = []
    if round_frames["smokes"].height > 0 and "start_tick" in round_frames["smokes"].columns:
        utility_ticks.extend(
            int(tick) for tick in round_frames["smokes"].filter(
                pl.col("start_tick") >= round_meta["freeze_end_tick"]
            )["start_tick"].to_list()
        )
    if round_frames["infernos"].height > 0 and "start_tick" in round_frames["infernos"].columns:
        utility_ticks.extend(
            int(tick) for tick in round_frames["infernos"].filter(
                pl.col("start_tick") >= round_meta["freeze_end_tick"]
            )["start_tick"].to_list()
        )
    if round_frames["flashes"].height > 0 and "tick" in round_frames["flashes"].columns:
        utility_ticks.extend(
            int(tick) for tick in round_frames["flashes"].filter(
                pl.col("tick") >= round_meta["freeze_end_tick"]
            )["tick"].to_list()
        )

    death_ticks: list[int] = []
    ticks = round_frames["ticks"]
    if ticks.height > 0 and "steamid" in ticks.columns and "health" in ticks.columns and "tick" in ticks.columns:
        for _, player_rows in ticks.select(["steamid", "tick", "health"]).group_by("steamid", maintain_order=True):
            player_rows = player_rows.sort("tick")
            prev_alive: bool | None = None
            for row in player_rows.iter_rows(named=True):
                alive = int(row.get("health") or 0) > 0
                if prev_alive is True and not alive:
                    death_ticks.append(int(row["tick"]))
                    break
                prev_alive = alive

    return {
        "first_shot_tick": first_shot_tick,
        "first_utility_tick": min(utility_ticks) if utility_ticks else None,
        "death_ticks": sorted(set(death_ticks)),
        "bomb_plant_tick": round_meta["bomb_plant_tick"],
    }


def build_match_artifact(
    *,
    source_type: str,
    source_match_id: str,
    parquet_dir: Path,
    stem: str,
    map_name: str,
    steam_id: str | None = None,
) -> str:
    """Build a single JSON artifact for the entire match. Returns the artifact path."""
    log.info("building %s (%s)", source_match_id, map_name)
    frames = load_match_frames(parquet_dir, stem)
    rounds = frames["rounds"]
    if rounds.height == 0:
        raise ValueError(f"No rounds found in parquet for {source_match_id}")

    anchors = list_match_anchor_specs(frames=frames)
    anchors_by_round: dict[int, list[dict]] = {}
    for anchor in anchors:
        anchors_by_round.setdefault(int(anchor["round_num"]), []).append(anchor)

    artifact_dir = derived_match_dir(source_type, source_match_id, steam_id=steam_id)
    rounds_data: dict[str, dict] = {}

    for round_row in rounds.iter_rows(named=True):
        round_num = int(round_row["round_num"])
        round_frames = _round_rows(frames, round_num)
        round_ticks = round_frames["ticks"]
        round_meta = _round_metadata(round_row)
        round_anchors = anchors_by_round.get(round_num, [])

        windows: list[dict] = []
        team_windows: dict[str, list[dict]] = {side: [] for side in _SIDES}
        for anchor in round_anchors:
            features = build_window_features(
                ticks=frames["ticks"],
                rounds=frames["rounds"],
                shots=frames["shots"],
                smokes=frames["smokes"],
                infernos=frames["infernos"],
                flashes=frames["flashes"],
                grenade_paths=frames["grenade_paths"],
                round_num=round_num,
                start_tick=anchor["start_tick"],
                anchor_tick=anchor["anchor_tick"],
                end_tick=anchor["end_tick"],
                user_steam_id=steam_id,
                anchor_kind=anchor["anchor_kind"],
            )
            windows.append(_window_projection(anchor, features))
            for side in _SIDES:
                team_windows[side].append(_team_window_projection(side, anchor, features))

        original_team_windows = _build_original_team_windows(
            round_ticks,
            round_meta,
            map_name,
            source_type,
            source_match_id,
        )

        rounds_data[str(round_num)] = {
            "artifact_version": ARTIFACT_VERSION,
            "window_feature_version": FEATURE_VERSION,
            "source_type": source_type,
            "source_match_id": source_match_id,
            "map_name": map_name,
            "round_num": round_num,
            "user_steamid": str(steam_id) if steam_id else None,
            "round": round_meta,
            "timing": {
                "tick_rate": TICK_RATE,
                "timeline_origin": "freeze_end",
                "timeline_origin_tick": round_meta["freeze_end_tick"],
                "round_start_tick": round_meta["start_tick"],
                "round_end_tick": round_meta["official_end_tick"],
            },
            "events": _event_summary(round_frames, round_meta),
            "windows": windows,
            "team_window_catalog": original_team_windows,
            "teams": {
                side: {
                    "players": _player_entries(round_ticks, side),
                    "windows": team_windows[side],
                    "nav_trace": {
                        "centroid_trace": _sample_team_centroid_trace(round_ticks, side),
                        "dominant_place_segments": _compress_team_place_trace(round_ticks, side),
                        "player_place_routes": _compress_player_place_routes(round_ticks, side),
                    },
                }
                for side in _SIDES
            },
        }

    artifact_path = artifact_dir / f"{stem}_{ARTIFACT_VERSION}.json"
    with artifact_path.open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "artifact_version": ARTIFACT_VERSION,
                "window_feature_version": FEATURE_VERSION,
                "source_type": source_type,
                "source_match_id": source_match_id,
                "map_name": map_name,
                "round_count": len(rounds_data),
                "rounds": rounds_data,
            },
            fh,
            ensure_ascii=True,
            indent=2,
        )

    log.info("done %s: %d rounds -> %s", source_match_id, len(rounds_data), artifact_path.name)
    return to_managed_path(artifact_path)
