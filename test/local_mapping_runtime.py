from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from pipeline.features.extract_windows import load_match_frames
from pipeline.features.featurize_windows import (
    DEFAULT_WINDOW_POST_TICKS,
    DEFAULT_WINDOW_PRE_TICKS,
    FEATURE_VERSION,
    MIN_MAPPING_SECONDS,
    TICK_RATE,
    build_window_features,
)


def _radar_path(name: str, *, lower: bool = False) -> Path:
    suffix = "_lower" if lower else ""
    local = Path("web/public/maps") / f"{name}{suffix}.png"
    if local.exists():
        return local
    return Path.home() / ".awpy" / "maps" / f"{name}{suffix}.png"


@dataclass(frozen=True)
class TestMapConfig:
    name: str
    display_name: str
    pos_x: float
    pos_y: float
    scale: float
    radar_path: Path
    has_lower_level: bool = False
    lower_level_max_z: float = -1_000_000.0
    lower_radar_path: Path | None = None

    def world_to_radar_px(self, wx: float, wy: float) -> tuple[float, float]:
        rx = (wx - self.pos_x) / self.scale
        ry = (self.pos_y - wy) / self.scale
        return rx, ry

    def world_r_to_px(self, radius_wu: float, img_w: int, disp_size: int) -> int:
        return max(2, int(radius_wu / self.scale * disp_size / img_w))

    def is_lower(self, z: float) -> bool:
        return self.has_lower_level and z <= self.lower_level_max_z


MAPS: dict[str, TestMapConfig] = {
    "de_ancient": TestMapConfig(
        name="de_ancient",
        display_name="Ancient",
        pos_x=-2953,
        pos_y=2164,
        scale=5.0,
        radar_path=_radar_path("de_ancient"),
    ),
    "de_anubis": TestMapConfig(
        name="de_anubis",
        display_name="Anubis",
        pos_x=-2796,
        pos_y=3328,
        scale=5.22,
        radar_path=_radar_path("de_anubis"),
    ),
    "de_dust2": TestMapConfig(
        name="de_dust2",
        display_name="Dust 2",
        pos_x=-2476,
        pos_y=3239,
        scale=4.4,
        radar_path=_radar_path("de_dust2"),
    ),
    "de_inferno": TestMapConfig(
        name="de_inferno",
        display_name="Inferno",
        pos_x=-2087,
        pos_y=3870,
        scale=4.9,
        radar_path=_radar_path("de_inferno"),
    ),
    "de_mirage": TestMapConfig(
        name="de_mirage",
        display_name="Mirage",
        pos_x=-3230,
        pos_y=1713,
        scale=5.0,
        radar_path=_radar_path("de_mirage"),
    ),
    "de_nuke": TestMapConfig(
        name="de_nuke",
        display_name="Nuke",
        pos_x=-3453,
        pos_y=2887,
        scale=7.0,
        radar_path=_radar_path("de_nuke"),
        lower_radar_path=_radar_path("de_nuke", lower=True),
        has_lower_level=True,
        lower_level_max_z=-495.0,
    ),
    "de_overpass": TestMapConfig(
        name="de_overpass",
        display_name="Overpass",
        pos_x=-4831,
        pos_y=1781,
        scale=5.2,
        radar_path=_radar_path("de_overpass"),
    ),
}


def decode_latest_user_match(parquet_user_dir: Path) -> tuple[str, str | None]:
    round_files = sorted(
        parquet_user_dir.glob("*_rounds.parquet"),
        key=lambda path: path.stat().st_mtime,
    )
    if not round_files:
        raise FileNotFoundError("No user parquet rounds found in parquet_user/")

    latest = round_files[-1]
    demo_id = latest.name.removesuffix("_rounds.parquet")
    steam_id = None
    if demo_id.startswith("user_"):
        parts = demo_id.split("_", 2)
        if len(parts) >= 3:
            steam_id = parts[1]
    return demo_id, steam_id


def read_match_map_name(parquet_dir: Path, stem: str) -> str:
    ticks_path = parquet_dir / f"{stem}_ticks.parquet"
    ticks = pl.read_parquet(ticks_path)
    rounds_path = parquet_dir / f"{stem}_rounds.parquet"
    if not rounds_path.exists():
        raise FileNotFoundError(rounds_path)

    candidate_maps = sorted(MAPS.keys())
    if "_inferno" in stem:
        return "de_inferno"
    if "_mirage" in stem:
        return "de_mirage"
    if "_anubis" in stem:
        return "de_anubis"
    if "_nuke" in stem:
        return "de_nuke"
    if "_dust2" in stem:
        return "de_dust2"
    if "_ancient" in stem:
        return "de_ancient"
    if "_overpass" in stem:
        return "de_overpass"
    if ticks.height == 0:
        raise ValueError(f"Could not infer map for {stem}")

    sample = ticks.select(["X", "Y"]).drop_nulls().head(1000)
    if sample.height == 0:
        raise ValueError(f"Could not infer map for {stem}; no usable positions")

    best_name = None
    best_score = -1
    for map_name, cfg in MAPS.items():
        score = 0
        for row in sample.iter_rows(named=True):
            rx, ry = cfg.world_to_radar_px(float(row["X"]), float(row["Y"]))
            if 0.0 <= rx <= 1024.0 and 0.0 <= ry <= 1024.0:
                score += 1
        if score > best_score:
            best_score = score
            best_name = map_name

    if best_name is None:
        raise ValueError(f"Could not infer map for {stem}; known maps={candidate_maps}")
    return best_name


def load_round_replay_payload(parquet_dir: Path, stem: str, round_num: int, map_name: str) -> dict:
    frames = load_match_frames(parquet_dir, stem)
    ticks_all = frames["ticks"]
    rounds_all = frames["rounds"]
    shots_all = frames["shots"]
    smokes_all = frames["smokes"]
    infernos_all = frames["infernos"]
    flashes_all = frames["flashes"]
    grens_all = frames["grenade_paths"]

    rn = round_num
    r_rounds = rounds_all.filter(pl.col("round_num") == rn)
    raw_ticks = ticks_all.filter(pl.col("round_num") == rn)
    freeze_end = (
        int(r_rounds["freeze_end"][0])
        if r_rounds.height > 0 and "freeze_end" in r_rounds.columns
        else (int(raw_ticks["tick"].min()) if raw_ticks.height > 0 else 0)
    )

    ticks = raw_ticks.filter(pl.col("tick") >= freeze_end).sort("tick")
    shots = shots_all.filter((pl.col("round_num") == rn) & (pl.col("tick") >= freeze_end))
    smokes = smokes_all.filter(
        (pl.col("round_num") == rn)
        & ((pl.col("end_tick").is_null()) | (pl.col("end_tick") >= freeze_end))
    )
    infernos = infernos_all.filter(
        (pl.col("round_num") == rn)
        & ((pl.col("end_tick").is_null()) | (pl.col("end_tick") >= freeze_end))
    )
    flashes = flashes_all.filter((pl.col("round_num") == rn) & (pl.col("tick") >= freeze_end))
    grens = grens_all.filter(
        (pl.col("round_num") == rn) & (pl.col("tick") >= freeze_end)
    ).sort("tick")

    tick_list = sorted(ticks["tick"].unique().to_list())
    ticks_by_tick: dict[int, list[dict]] = {tick: [] for tick in tick_list}
    cols = set(ticks.columns)
    for row in ticks.iter_rows(named=True):
        tick = int(row["tick"])
        ticks_by_tick[tick].append({
            "steamid": str(row.get("steamid") or ""),
            "name": row.get("name") or "",
            "side": (row.get("side") or "ct").lower(),
            "x": float(row.get("X") or 0.0),
            "y": float(row.get("Y") or 0.0),
            "z": float(row.get("Z") or 0.0),
            "yaw": float(row.get("yaw") or 0.0) if "yaw" in cols else 0.0,
            "health": int(row.get("health") or 0),
            "inventory": list(row.get("inventory") or []),
            "flash_duration": float(row.get("flash_duration") or 0.0),
        })
    ticks_payload = [{"tick": tick, "players": ticks_by_tick[tick]} for tick in tick_list]

    shots_payload = [{
        "tick": int(row["tick"]),
        "player_steamid": str(row.get("player_steamid") or ""),
        "weapon": str(row.get("weapon") or ""),
    } for row in shots.iter_rows(named=True)]

    round_last_tick = tick_list[-1] if tick_list else 0
    smokes_payload = [{
        "start_tick": max(int(row["start_tick"]), freeze_end),
        "end_tick": int(row["end_tick"] if row["end_tick"] is not None else round_last_tick),
        "x": float(row.get("X") or 0.0),
        "y": float(row.get("Y") or 0.0),
        "thrower_name": row.get("thrower_name") or "",
    } for row in smokes.iter_rows(named=True)]
    infernos_payload = [{
        "start_tick": max(int(row["start_tick"]), freeze_end),
        "end_tick": int(row["end_tick"] if row["end_tick"] is not None else round_last_tick),
        "x": float(row.get("X") or 0.0),
        "y": float(row.get("Y") or 0.0),
    } for row in infernos.iter_rows(named=True)]
    flashes_payload = [{
        "tick": int(row["tick"]),
        "x": float(row.get("X") or 0.0),
        "y": float(row.get("Y") or 0.0),
    } for row in flashes.iter_rows(named=True)]

    gren_by_entity: dict[int, dict] = {}
    for row in grens.iter_rows(named=True):
        if row.get("X") is None or row.get("Y") is None:
            continue
        entity_id = int(row["entity_id"])
        if entity_id not in gren_by_entity:
            gren_by_entity[entity_id] = {
                "entity_id": entity_id,
                "grenade_type": row["grenade_type"],
                "path": [],
            }
        gren_by_entity[entity_id]["path"].append({
            "tick": int(row["tick"]),
            "x": float(row["X"]),
            "y": float(row["Y"]),
        })

    return {
        "map": map_name,
        "round_num": round_num,
        "freeze_end_tick": freeze_end,
        "tick_list": tick_list,
        "ticks": ticks_payload,
        "shots": shots_payload,
        "smokes": smokes_payload,
        "infernos": infernos_payload,
        "flashes": flashes_payload,
        "grenade_paths": list(gren_by_entity.values()),
    }


def _load_feature_blob(feature_path: Path) -> dict | None:
    if not feature_path.exists():
        return None
    return json.loads(feature_path.read_text(encoding="utf-8"))


def _numeric_similarity(query: dict, candidate: dict, keys: tuple[str, ...]) -> float:
    scores: list[float] = []
    for key in keys:
        if key not in query or key not in candidate:
            continue
        qv = float(query[key])
        cv = float(candidate[key])
        denom = max(abs(qv), abs(cv), 1.0)
        scores.append(max(0.0, 1.0 - abs(qv - cv) / denom))
    return sum(scores) / len(scores) if scores else 0.0


def _time_similarity(query: dict, candidate: dict) -> float:
    q_time = query.get("time_since_freeze_end_s")
    c_time = candidate.get("time_since_freeze_end_s")
    if q_time is None or c_time is None:
        return 0.0

    freeze_score = max(0.0, 1.0 - abs(float(q_time) - float(c_time)) / 25.0)
    q_planted = bool(query.get("planted"))
    c_planted = bool(candidate.get("planted"))
    if q_planted != c_planted:
        return 0.0
    if not q_planted:
        return freeze_score

    q_plant = query.get("time_since_bomb_plant_s")
    c_plant = candidate.get("time_since_bomb_plant_s")
    if q_plant is None or c_plant is None:
        return freeze_score * 0.5
    plant_score = max(0.0, 1.0 - abs(float(q_plant) - float(c_plant)) / 12.0)
    return 0.4 * freeze_score + 0.6 * plant_score


def _path_similarity(query_path: list[list[float]], candidate_path: list[list[float]]) -> float:
    if not query_path or not candidate_path:
        return 0.0
    pairs = zip(query_path, candidate_path)
    distances = [
        math.dist((left[0], left[1]), (right[0], right[1]))
        for left, right in pairs
    ]
    if not distances:
        return 0.0
    mean_distance = sum(distances) / len(distances)
    return 1.0 / (1.0 + mean_distance / 450.0)


def _jaccard_similarity(left: list[str], right: list[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set and not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def _dict_overlap_similarity(left: dict | None, right: dict | None) -> float:
    left = left or {}
    right = right or {}
    keys = set(left) | set(right)
    if not keys:
        return 0.0
    numerator = sum(min(int(left.get(key, 0)), int(right.get(key, 0))) for key in keys)
    denominator = sum(max(int(left.get(key, 0)), int(right.get(key, 0))) for key in keys)
    return numerator / denominator if denominator else 0.0


_WEAPON_COMPATIBILITY = {
    ("sniper", "sniper"): 1.0,
    ("sniper", "rifle"): 0.82,
    ("sniper", "smg"): 0.45,
    ("sniper", "shotgun"): 0.25,
    ("sniper", "heavy"): 0.2,
    ("sniper", "pistol"): 0.2,
    ("rifle", "rifle"): 1.0,
    ("rifle", "smg"): 0.62,
    ("rifle", "shotgun"): 0.32,
    ("rifle", "heavy"): 0.28,
    ("rifle", "pistol"): 0.35,
    ("smg", "smg"): 1.0,
    ("smg", "shotgun"): 0.4,
    ("smg", "heavy"): 0.24,
    ("smg", "pistol"): 0.48,
    ("shotgun", "shotgun"): 1.0,
    ("shotgun", "heavy"): 0.42,
    ("shotgun", "pistol"): 0.28,
    ("heavy", "heavy"): 1.0,
    ("heavy", "pistol"): 0.22,
    ("pistol", "pistol"): 1.0,
}


def _weapon_compatibility(left: str | None, right: str | None) -> float:
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    pair = tuple(sorted((left, right)))
    return _WEAPON_COMPATIBILITY.get(pair, 0.15)


def _weapon_similarity(query: dict, candidate: dict) -> float:
    profile_score = (
        _dict_overlap_similarity(query.get("ct_weapon_profile"), candidate.get("ct_weapon_profile"))
        + _dict_overlap_similarity(query.get("t_weapon_profile"), candidate.get("t_weapon_profile"))
        + _dict_overlap_similarity(query.get("shots_weapon_profile"), candidate.get("shots_weapon_profile"))
    ) / 3.0
    focus_score = _weapon_compatibility(
        query.get("focus_weapon_family"),
        candidate.get("focus_weapon_family"),
    )
    return max(profile_score * 0.8 + focus_score * 0.2, focus_score)


def _situation_similarity(query: dict, candidate: dict) -> float:
    tag_score = _jaccard_similarity(
        list(query.get("situation_tags") or []),
        list(candidate.get("situation_tags") or []),
    )
    primary_match = 1.0 if query.get("primary_situation") == candidate.get("primary_situation") else 0.0
    planted_match = 1.0 if bool(query.get("planted")) == bool(candidate.get("planted")) else 0.0
    return (0.5 * tag_score) + (0.3 * primary_match) + (0.2 * planted_match)


def _state_similarity(query: dict, candidate: dict) -> float:
    return _numeric_similarity(
        query["vector"],
        candidate["vector"],
        (
            "alive_ct",
            "alive_t",
            "alive_diff",
            "hp_ct_sum",
            "hp_t_sum",
            "defuser_ct_count",
            "deaths_ct",
            "deaths_t",
            "shots_ct",
            "shots_t",
            "utility_total",
            "seconds_remaining_s",
        ),
    )


def _spatial_similarity(query: dict, candidate: dict) -> float:
    site_score = 0.0
    if query.get("site") and candidate.get("site"):
        site_score = 1.0 if query["site"] == candidate["site"] else 0.0

    place_score = (
        _dict_overlap_similarity(query.get("ct_place_profile"), candidate.get("ct_place_profile"))
        + _dict_overlap_similarity(query.get("t_place_profile"), candidate.get("t_place_profile"))
        + _jaccard_similarity(query.get("ct_top_places", []), candidate.get("ct_top_places", []))
        + _jaccard_similarity(query.get("t_top_places", []), candidate.get("t_top_places", []))
    ) / 4.0

    centroid_score = _numeric_similarity(
        query["vector"],
        candidate["vector"],
        (
            "ct_centroid_x",
            "ct_centroid_y",
            "t_centroid_x",
            "t_centroid_y",
            "ct_spread",
            "t_spread",
        ),
    )
    return (0.25 * site_score) + (0.4 * place_score) + (0.35 * centroid_score)


def _movement_similarity(query: dict, candidate: dict) -> float:
    path_score = (
        _path_similarity(query.get("ct_centroid_path", []), candidate.get("ct_centroid_path", []))
        + _path_similarity(query.get("t_centroid_path", []), candidate.get("t_centroid_path", []))
    ) / 2.0
    distance_score = _numeric_similarity(
        query["vector"],
        candidate["vector"],
        ("ct_path_distance", "t_path_distance"),
    )
    return (path_score + distance_score) / 2.0


def _situation_weights(query: dict) -> dict[str, float]:
    tags = set(query.get("situation_tags") or [])
    if {"fight", "trade_window", "clutch", "retake", "post_plant"} & tags:
        return {
            "time": 0.12,
            "situation": 0.22,
            "state": 0.24,
            "spatial": 0.18,
            "movement": 0.08,
            "weapons": 0.16,
        }
    if {"setup", "rotate", "default", "mid_round"} & tags:
        return {
            "time": 0.18,
            "situation": 0.18,
            "state": 0.16,
            "spatial": 0.26,
            "movement": 0.12,
            "weapons": 0.10,
        }
    return {
        "time": 0.15,
        "situation": 0.20,
        "state": 0.20,
        "spatial": 0.22,
        "movement": 0.10,
        "weapons": 0.13,
    }


def score_candidate(query: dict, candidate: dict) -> float:
    if bool(query.get("planted")) != bool(candidate.get("planted")):
        return 0.0

    weights = _situation_weights(query)
    parts = {
        "time": _time_similarity(query, candidate),
        "situation": _situation_similarity(query, candidate),
        "state": _state_similarity(query, candidate),
        "spatial": _spatial_similarity(query, candidate),
        "movement": _movement_similarity(query, candidate),
        "weapons": _weapon_similarity(query, candidate),
    }
    return sum(parts[key] * weight for key, weight in weights.items())


def build_reason(query: dict, candidate: dict) -> str:
    parts: list[str] = []
    if query.get("primary_situation") == candidate.get("primary_situation"):
        parts.append(f"same {str(query['primary_situation']).replace('_', ' ')}")
    else:
        overlap = sorted(set(query.get("situation_tags", [])) & set(candidate.get("situation_tags", [])))
        if overlap:
            parts.append(f"shared tags: {', '.join(tag.replace('_', ' ') for tag in overlap[:2])}")
    if query.get("site") and query.get("site") == candidate.get("site"):
        parts.append(f"same {query['site'].upper()} site")
    if candidate["vector"].get("alive_ct") == query["vector"].get("alive_ct") and candidate["vector"].get("alive_t") == query["vector"].get("alive_t"):
        parts.append(f"same alive state {query['vector']['alive_ct']}v{query['vector']['alive_t']}")
    q_weapon = query.get("focus_weapon_family")
    c_weapon = candidate.get("focus_weapon_family")
    if q_weapon and c_weapon and _weapon_compatibility(q_weapon, c_weapon) >= 0.75:
        if q_weapon == c_weapon:
            parts.append(f"same focal weapon family: {q_weapon}")
        else:
            parts.append(f"compatible focal weapons: {q_weapon} to {c_weapon}")
    return "; ".join(parts[:3]) or "closest local situation window"


def load_pro_feature_corpus(parquet_pro_dir: Path, map_name: str) -> list[dict]:
    corpus: list[dict] = []
    feature_glob = f"*_v2.json" if FEATURE_VERSION == "v2" else "*.json"
    for match_dir in sorted(parquet_pro_dir.iterdir()):
        if not match_dir.is_dir():
            continue
        if not match_dir.name.endswith(map_name.removeprefix("de_")) and f"_{map_name.removeprefix('de_')}" not in match_dir.name:
            continue
        feature_dir = match_dir / "window_features"
        if not feature_dir.exists():
            continue
        for feature_path in sorted(feature_dir.glob(feature_glob)):
            blob = _load_feature_blob(feature_path)
            if not blob or not blob.get("queryable", True):
                continue
            corpus.append({
                "feature_path": feature_path,
                "feature": blob,
                "source_match_id": match_dir.name,
                "round_num": int(blob["round_num"]),
                "start_tick": int(blob["start_tick"]),
                "anchor_tick": int(blob["anchor_tick"]),
                "end_tick": int(blob["end_tick"]),
            })
    return corpus


def build_query_feature(
    frames: dict[str, pl.DataFrame],
    *,
    round_num: int,
    anchor_tick: int,
    steam_id: str | None,
) -> dict:
    return build_window_features(
        ticks=frames["ticks"],
        rounds=frames["rounds"],
        shots=frames["shots"],
        smokes=frames["smokes"],
        infernos=frames["infernos"],
        flashes=frames["flashes"],
        grenade_paths=frames["grenade_paths"],
        round_num=round_num,
        start_tick=max(0, anchor_tick - DEFAULT_WINDOW_PRE_TICKS),
        anchor_tick=anchor_tick,
        end_tick=anchor_tick + DEFAULT_WINDOW_POST_TICKS,
        user_steam_id=steam_id,
        anchor_kind="manual_query",
    )


def best_mapping_for_tick(
    *,
    user_frames: dict[str, pl.DataFrame],
    steam_id: str | None,
    round_num: int,
    anchor_tick: int,
    pro_corpus: list[dict],
) -> dict | None:
    query = build_query_feature(
        user_frames,
        round_num=round_num,
        anchor_tick=anchor_tick,
        steam_id=steam_id,
    )
    if not query.get("queryable", True):
        return None

    best: dict | None = None
    for candidate in pro_corpus:
        score = score_candidate(query, candidate["feature"])
        if score <= 0:
            continue
        row = {
            "query": query,
            "source_match_id": candidate["source_match_id"],
            "round_num": candidate["round_num"],
            "start_tick": candidate["start_tick"],
            "anchor_tick": candidate["anchor_tick"],
            "end_tick": candidate["end_tick"],
            "score": round(score, 4),
            "reason": build_reason(query, candidate["feature"]),
            "feature": candidate["feature"],
        }
        if best is None or row["score"] > best["score"]:
            best = row
    return best


def default_anchor_for_round(round_payload: dict) -> int | None:
    freeze_end = int(round_payload["freeze_end_tick"])
    minimum = freeze_end + (MIN_MAPPING_SECONDS * TICK_RATE)
    for tick in round_payload["tick_list"]:
        if tick >= minimum:
            return tick
    return None
