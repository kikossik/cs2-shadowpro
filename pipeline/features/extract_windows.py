"""Extract sliding situation windows from per-match parquet files."""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from .featurize_windows import (
    DEFAULT_SLIDE_STEP_TICKS,
    DEFAULT_WINDOW_POST_TICKS,
    DEFAULT_WINDOW_PRE_TICKS,
    FEATURE_VERSION,
    MIN_MAPPING_SECONDS,
    TICK_RATE,
    build_window_features,
)

_PARQUET_FIELDS = (
    "ticks",
    "rounds",
    "shots",
    "smokes",
    "infernos",
    "flashes",
    "grenade_paths",
)
_ANCHOR_PRIORITY = {
    "time_slice": 0,
    "first_utility_after_cutoff": 1,
    "first_shot_after_cutoff": 1,
    "bomb_plant": 2,
    "death_pulse": 3,
}


def _read_parquet_or_empty(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)


def load_match_frames(parquet_dir: Path, stem: str) -> dict[str, pl.DataFrame]:
    """Load the standard replay parquet set for a match stem."""
    return {
        field: _read_parquet_or_empty(parquet_dir / f"{stem}_{field}.parquet")
        for field in _PARQUET_FIELDS
    }


def _feature_dir(parquet_dir: Path) -> Path:
    feature_dir = parquet_dir / "window_features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    return feature_dir


def _window_id(source_type: str, source_match_id: str, round_num: int, anchor_kind: str, anchor_tick: int) -> str:
    return f"{source_type}_{source_match_id}_r{round_num}_{anchor_kind}_{anchor_tick}_{FEATURE_VERSION}"


def _first_tick(df: pl.DataFrame, tick_column: str) -> int | None:
    if df.height == 0 or tick_column not in df.columns:
        return None
    return int(df.sort(tick_column)[tick_column][0])


def _death_ticks(round_ticks: pl.DataFrame) -> list[int]:
    if round_ticks.height == 0 or "steamid" not in round_ticks.columns or "tick" not in round_ticks.columns:
        return []

    columns = [col for col in ("steamid", "tick", "health") if col in round_ticks.columns]
    death_ticks: list[int] = []
    for _, player_rows in round_ticks.select(columns).group_by("steamid", maintain_order=True):
        ordered = player_rows.sort("tick")
        prev_alive: bool | None = None
        for row in ordered.iter_rows(named=True):
            alive = int(row.get("health") or 0) > 0
            if prev_alive is True and not alive:
                death_ticks.append(int(row["tick"]))
                break
            prev_alive = alive

    return sorted(set(death_ticks))


def _store_anchor(
    anchors: dict[tuple[int, int], dict],
    *,
    round_num: int,
    tick: int | None,
    anchor_kind: str,
    round_start: int,
    round_end: int,
    pre_ticks: int,
    post_ticks: int,
) -> None:
    if tick is None:
        return
    if tick < round_start or tick > round_end:
        return

    key = (round_num, tick)
    anchor = {
        "round_num": round_num,
        "anchor_tick": tick,
        "anchor_kind": anchor_kind,
        "start_tick": max(round_start, tick - pre_ticks),
        "end_tick": min(round_end, tick + post_ticks),
    }
    current = anchors.get(key)
    if current is None or _ANCHOR_PRIORITY.get(anchor_kind, 0) > _ANCHOR_PRIORITY.get(current["anchor_kind"], 0):
        anchors[key] = anchor


def _iter_anchor_specs(
    *,
    ticks: pl.DataFrame,
    rounds: pl.DataFrame,
    shots: pl.DataFrame,
    smokes: pl.DataFrame,
    infernos: pl.DataFrame,
    flashes: pl.DataFrame,
    pre_ticks: int,
    post_ticks: int,
    slide_step_ticks: int,
    min_mapping_seconds: int,
) -> list[dict]:
    anchors: dict[tuple[int, int], dict] = {}

    for round_row in rounds.iter_rows(named=True):
        round_num = int(round_row["round_num"])
        round_start = int(round_row.get("start") or 0)
        round_end = int(round_row.get("official_end") or round_row.get("end") or round_start)
        freeze_end = int(round_row.get("freeze_end") or round_start)
        min_anchor_tick = freeze_end + (min_mapping_seconds * TICK_RATE)
        if min_anchor_tick > round_end:
            continue

        round_ticks = ticks.filter(pl.col("round_num") == round_num) if "round_num" in ticks.columns else pl.DataFrame()
        round_shots = shots.filter(pl.col("round_num") == round_num) if "round_num" in shots.columns else pl.DataFrame()
        round_smokes = smokes.filter(pl.col("round_num") == round_num) if "round_num" in smokes.columns else pl.DataFrame()
        round_infernos = infernos.filter(pl.col("round_num") == round_num) if "round_num" in infernos.columns else pl.DataFrame()
        round_flashes = flashes.filter(pl.col("round_num") == round_num) if "round_num" in flashes.columns else pl.DataFrame()

        regular_tick = min_anchor_tick
        while regular_tick <= round_end:
            _store_anchor(
                anchors,
                round_num=round_num,
                tick=regular_tick,
                anchor_kind="time_slice",
                round_start=round_start,
                round_end=round_end,
                pre_ticks=pre_ticks,
                post_ticks=post_ticks,
            )
            regular_tick += slide_step_ticks

        utility_tick_candidates = [
            _first_tick(round_smokes.filter(pl.col("start_tick") >= min_anchor_tick), "start_tick")
            if round_smokes.height > 0 else None,
            _first_tick(round_infernos.filter(pl.col("start_tick") >= min_anchor_tick), "start_tick")
            if round_infernos.height > 0 else None,
            _first_tick(round_flashes.filter(pl.col("tick") >= min_anchor_tick), "tick")
            if round_flashes.height > 0 else None,
        ]
        utility_tick = min((tick for tick in utility_tick_candidates if tick is not None), default=None)
        _store_anchor(
            anchors,
            round_num=round_num,
            tick=utility_tick,
            anchor_kind="first_utility_after_cutoff",
            round_start=round_start,
            round_end=round_end,
            pre_ticks=pre_ticks,
            post_ticks=post_ticks,
        )

        first_shot = (
            _first_tick(round_shots.filter(pl.col("tick") >= min_anchor_tick), "tick")
            if round_shots.height > 0 else None
        )
        _store_anchor(
            anchors,
            round_num=round_num,
            tick=first_shot,
            anchor_kind="first_shot_after_cutoff",
            round_start=round_start,
            round_end=round_end,
            pre_ticks=pre_ticks,
            post_ticks=post_ticks,
        )

        bomb_plant = round_row.get("bomb_plant")
        bomb_tick = int(bomb_plant) if bomb_plant is not None and int(bomb_plant) >= min_anchor_tick else None
        _store_anchor(
            anchors,
            round_num=round_num,
            tick=bomb_tick,
            anchor_kind="bomb_plant",
            round_start=round_start,
            round_end=round_end,
            pre_ticks=pre_ticks,
            post_ticks=post_ticks,
        )

        for death_tick in _death_ticks(round_ticks):
            if death_tick < min_anchor_tick:
                continue
            _store_anchor(
                anchors,
                round_num=round_num,
                tick=death_tick,
                anchor_kind="death_pulse",
                round_start=round_start,
                round_end=round_end,
                pre_ticks=pre_ticks,
                post_ticks=post_ticks,
            )

    return sorted(anchors.values(), key=lambda row: (row["round_num"], row["anchor_tick"]))


def extract_match_event_windows(
    *,
    source_type: str,
    source_match_id: str,
    parquet_dir: Path,
    stem: str,
    map_name: str,
    steam_id: str | None = None,
    pre_ticks: int = DEFAULT_WINDOW_PRE_TICKS,
    post_ticks: int = DEFAULT_WINDOW_POST_TICKS,
    slide_step_ticks: int = DEFAULT_SLIDE_STEP_TICKS,
    min_mapping_seconds: int = MIN_MAPPING_SECONDS,
) -> list[dict]:
    """Extract situation windows for one user or pro match and persist feature blobs."""
    frames = load_match_frames(parquet_dir, stem)
    rounds = frames["rounds"]
    if rounds.height == 0:
        return []

    feature_dir = _feature_dir(parquet_dir)
    anchors = _iter_anchor_specs(
        ticks=frames["ticks"],
        rounds=rounds,
        shots=frames["shots"],
        smokes=frames["smokes"],
        infernos=frames["infernos"],
        flashes=frames["flashes"],
        pre_ticks=pre_ticks,
        post_ticks=post_ticks,
        slide_step_ticks=slide_step_ticks,
        min_mapping_seconds=min_mapping_seconds,
    )

    windows: list[dict] = []
    for anchor in anchors:
        features = build_window_features(
            ticks=frames["ticks"],
            rounds=frames["rounds"],
            shots=frames["shots"],
            smokes=frames["smokes"],
            infernos=frames["infernos"],
            flashes=frames["flashes"],
            grenade_paths=frames["grenade_paths"],
            round_num=anchor["round_num"],
            start_tick=anchor["start_tick"],
            anchor_tick=anchor["anchor_tick"],
            end_tick=anchor["end_tick"],
            user_steam_id=steam_id,
            anchor_kind=anchor["anchor_kind"],
        )
        if not features.get("queryable", True):
            continue

        window_id = _window_id(
            source_type,
            source_match_id,
            anchor["round_num"],
            anchor["anchor_kind"],
            anchor["anchor_tick"],
        )
        feature_path = feature_dir / f"{window_id}.json"
        with feature_path.open("w", encoding="utf-8") as fh:
            json.dump(features, fh, ensure_ascii=True, indent=2)

        windows.append({
            "window_id": window_id,
            "source_type": source_type,
            "source_match_id": source_match_id,
            "steam_id": steam_id,
            "map_name": map_name,
            "round_num": anchor["round_num"],
            "start_tick": anchor["start_tick"],
            "anchor_tick": anchor["anchor_tick"],
            "end_tick": anchor["end_tick"],
            "side_to_query": features["side_to_query"],
            "phase": features["phase"],
            "site": features["site"],
            "anchor_kind": anchor["anchor_kind"],
            "alive_ct": features["alive_ct"],
            "alive_t": features["alive_t"],
            "feature_version": features["feature_version"],
            "feature_path": str(feature_path),
        })

    return windows
