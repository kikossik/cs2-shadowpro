from __future__ import annotations

import itertools
import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import polars as pl

import awpy.data
import awpy.nav
import awpy.parsers.rounds
import awpy.parsers.utils
from awpy import Demo

from backend import config, db

DEFAULT_EVENTS = [
    "round_freeze_end",
    "round_officially_ended",
    "round_start",
    "round_end",
    "player_spawn",
    "player_given_c4",
    "bomb_pickup",
    "item_pickup",
    "weapon_fire",
    "player_sound",
    "player_hurt",
    "player_death",
    "bomb_dropped",
    "bomb_planted",
    "bomb_defused",
    "bomb_exploded",
    "flashbang_detonate",
    "hegrenade_detonate",
    "smokegrenade_detonate",
    "smokegrenade_expired",
    "inferno_startburn",
    "inferno_expire",
]

FOCUSED_PLAYER_PROPS = sorted(
    {
        "FIRE",
        "RIGHTCLICK",
        "RELOAD",
        "USE",
        "WALK",
        "X",
        "Y",
        "Z",
        "accuracy_penalty",
        "active_weapon_ammo",
        "active_weapon_name",
        "armor_value",
        "current_equip_value",
        "duck_amount",
        "flash_duration",
        "has_defuser",
        "has_helmet",
        "health",
        "in_bomb_zone",
        "in_buy_zone",
        "is_alive",
        "is_defusing",
        "is_scoped",
        "is_walking",
        "last_place_name",
        "pitch",
        "player_name",
        "player_steamid",
        "round_start_equip_value",
        "shots_fired",
        "team_name",
        "total_ammo_left",
        "velocity_X",
        "velocity_Y",
        "velocity_Z",
        "velo_modifier",
        "which_bomb_zone",
        "yaw",
        "zoom_lvl",
    }
)

FOCUSED_WORLD_PROPS = sorted(
    {
        "game_phase",
        "game_time",
        "is_bomb_planted",
        "is_ct_timeout",
        "is_freeze_period",
        "is_match_started",
        "is_technical_timeout",
        "is_terrorist_timeout",
        "is_waiting_for_resume",
        "is_warmup_period",
        "round_in_progress",
        "which_bomb_zone",
    }
)


@dataclass
class DemoArtifacts:
    demo_path: Path
    role: str
    header: dict[str, Any]
    ticks: pl.DataFrame
    rounds: pl.DataFrame
    kills: pl.DataFrame
    damages: pl.DataFrame
    shots: pl.DataFrame
    bomb: pl.DataFrame
    grenades: pl.DataFrame
    demo_id_value: str | None = None
    tick_rate: int = 128

    @property
    def demo_id(self) -> str:
        if self.demo_id_value:
            return self.demo_id_value
        return self.demo_path.stem

    @property
    def map_name(self) -> str:
        return str(self.header.get("map_name"))

    @property
    def tickrate(self) -> int:
        return int(self.tick_rate or 128)

    @property
    def players(self) -> pl.DataFrame:
        return (
            self.ticks.select("steamid", "name", "side")
            .unique()
            .sort(["side", "name"])
        )


# ---------------------------------------------------------------------------
# Semantic feature helpers: weapon family, site, place normalisation.
# ---------------------------------------------------------------------------

_RIFLE_NAMES = {"AK-47", "M4A4", "M4A1-S", "AUG", "SG 553", "FAMAS", "Galil AR"}
_SNIPER_NAMES = {"AWP", "SSG 08", "Scar-20", "G3SG1"}
_SMG_NAMES = {"MP9", "MAC-10", "MP7", "MP5-SD", "UMP-45", "P90", "PP-Bizon"}
_SHOTGUN_NAMES = {"Nova", "XM1014", "MAG-7", "Sawed-Off"}
_HEAVY_NAMES = {"Negev", "M249"}
_PISTOL_NAMES = {
    "Glock-18", "USP-S", "P2000", "P250", "Desert Eagle",
    "Tec-9", "CZ75-Auto", "Five-SeveN", "Dual Berettas", "R8 Revolver",
}

_WEAPON_PRIORITY = {
    "sniper": 6, "rifle": 5, "smg": 4, "shotgun": 3, "heavy": 2, "pistol": 1,
}

_WEAPON_COMPAT: dict[tuple[str, str], float] = {
    ("rifle", "sniper"): 0.82,
    ("rifle", "smg"): 0.62,
    ("rifle", "shotgun"): 0.32,
    ("rifle", "heavy"): 0.28,
    ("pistol", "rifle"): 0.35,
    ("pistol", "sniper"): 0.20,
    ("pistol", "smg"): 0.48,
    ("pistol", "shotgun"): 0.28,
    ("pistol", "heavy"): 0.22,
    ("smg", "sniper"): 0.45,
    ("smg", "shotgun"): 0.40,
    ("heavy", "sniper"): 0.20,
    ("heavy", "smg"): 0.24,
    ("heavy", "shotgun"): 0.42,
    ("shotgun", "sniper"): 0.25,
}

_SITE_A_PLACES = {"abombsite", "bombsitea", "aramp", "palace", "heaven", "ticketbooth"}
_SITE_B_PLACES = {"bbombsite", "bombsiteb", "apartments", "truck", "shop", "marketplace"}


def _weapon_family(name: str | None) -> str | None:
    """Map an awpy active_weapon_name to a coarse family."""
    if not name:
        return None
    if name in _RIFLE_NAMES:
        return "rifle"
    if name in _SNIPER_NAMES:
        return "sniper"
    if name in _SMG_NAMES:
        return "smg"
    if name in _SHOTGUN_NAMES:
        return "shotgun"
    if name in _HEAVY_NAMES:
        return "heavy"
    if name in _PISTOL_NAMES:
        return "pistol"
    return None


def _weapon_compat(left: str | None, right: str | None) -> float:
    """Return a 0..1 compatibility score between two weapon families."""
    if not left or not right:
        return 0.2
    if left == right:
        return 1.0
    key = (left, right) if (left, right) in _WEAPON_COMPAT else (right, left)
    return _WEAPON_COMPAT.get(key, 0.2)


def _top_family(profile: dict[str, int] | None) -> str | None:
    """Return the highest-priority weapon family in a profile."""
    if not profile:
        return None
    candidates = [(f, c) for f, c in profile.items() if c > 0]
    if not candidates:
        return None
    candidates.sort(key=lambda pair: (_WEAPON_PRIORITY.get(pair[0], 0), pair[1]), reverse=True)
    return candidates[0][0]


def _normalize_place(name: str | None) -> str | None:
    """Strip punctuation and lowercase a place name."""
    if not name:
        return None
    normalized = "".join(ch for ch in str(name).lower() if ch.isalnum())
    return normalized or None


def _infer_site_from_rounds(round_row: dict[str, Any]) -> str | None:
    """Return 'a' or 'b' from rounds.bomb_site, when known."""
    bomb_site = round_row.get("bomb_site")
    if not bomb_site:
        return None
    lowered = str(bomb_site).lower()
    if "a" in lowered and "b" not in lowered:
        return "a"
    if "b" in lowered and "a" not in lowered:
        return "b"
    return None


def _infer_site_from_places(places: Sequence[str | None]) -> str | None:
    """Guess A vs B from alive-player place names during the window."""
    a_votes = b_votes = 0
    for p in places:
        n = _normalize_place(p)
        if not n:
            continue
        if n in _SITE_A_PLACES:
            a_votes += 1
        elif n in _SITE_B_PLACES:
            b_votes += 1
    if not (a_votes or b_votes):
        return None
    return "a" if a_votes > b_votes else "b"


def _dict_overlap_similarity(left: dict[str, int] | None, right: dict[str, int] | None) -> float:
    """Return 0..1 symmetric overlap for two integer-count dicts.

    Tolerates None values (treated as 0) and None keys (skipped), which can
    appear when a polars struct-list column is loaded back from parquet.
    """
    left = left or {}
    right = right or {}
    keys = {k for k in (set(left) | set(right)) if k is not None}
    if not keys:
        return 0.0
    def _val(d: dict[str, int], k: str) -> int:
        v = d.get(k, 0)
        return int(v) if v is not None else 0
    numerator = sum(min(_val(left, k), _val(right, k)) for k in keys)
    denominator = sum(max(_val(left, k), _val(right, k)) for k in keys)
    return numerator / denominator if denominator else 0.0


def _jaccard_similarity(left: Sequence[str], right: Sequence[str]) -> float:
    """Return 0..1 Jaccard similarity for two string collections."""
    left_set = set(left or [])
    right_set = set(right or [])
    if not (left_set or right_set):
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


# ---------------------------------------------------------------------------
# Track scoring calibration — scale divisors used to map raw movement gaps
# (position error, path difference, team-relative spacing, speed delta) into
# a comparable 0..~1 range. Defaults are map-agnostic fallbacks; the matcher
# recomputes them per-corpus when enough data is available.
# ---------------------------------------------------------------------------
_DEFAULT_SCALES: dict[str, float] = {
    "position": 900.0,
    "path_relative": 650.0,
    "spacing": 450.0,
    "speed": 240.0,
    "yaw_deg": 90.0,
    "path_length": 700.0,
}

_NAV_CACHE: dict[str, awpy.nav.Nav | None] = {}
_NAV_AREA_CACHE: dict[str, list[dict[str, Any]]] = {}
_NAV_GRID_CACHE: dict[str, dict[tuple[int, int], list[int]]] = {}
_NAV_POINT_CACHE: dict[tuple[str, int, int, int], int | None] = {}
_ROUND_ROUTE_PROFILE_CACHE: dict[tuple[str, int, float, float, float], list[dict[str, Any]]] = {}


def discover_demos(
    user_dir: str | Path = "user_demo",
    pro_dir: str | Path = "pro_demo",
) -> pl.DataFrame:
    """Scan the user and pro demo folders and summarize each demo.

    Args:
        user_dir: Folder containing user demos.
        pro_dir: Folder containing pro demos.
    """
    records: list[dict[str, Any]] = []

    for role, directory in (("user", Path(user_dir)), ("pro", Path(pro_dir))):
        for demo_path in sorted(directory.glob("*.dem")):
            header = Demo(demo_path).header
            records.append(
                {
                    "role": role,
                    "demo_path": str(demo_path.resolve()),
                    "demo_id": demo_path.stem,
                    "file_name": demo_path.name,
                    "file_size_mb": round(demo_path.stat().st_size / (1024 * 1024), 1),
                    "map_name": header.get("map_name"),
                    "server_name": header.get("server_name"),
                    "client_name": header.get("client_name"),
                    "steamid_hint": extract_steamid_hint(demo_path),
                }
            )

    return pl.DataFrame(records).sort(["role", "file_name"])


def extract_steamid_hint(demo_path: str | Path) -> int | None:
    """Extract a SteamID from a user demo filename if one is embedded there.

    Args:
        demo_path: Demo file path or name like `user_<steamid>_...dem`.
    """
    match = re.search(r"user_(\d+)", str(demo_path))
    return int(match.group(1)) if match else None


_DEFAULT_CACHE_ROOT = Path(".cache/awpy")

_CACHE_DATAFRAMES = ("ticks", "rounds", "kills", "damages", "shots", "bomb", "grenades")


def _demo_cache_dir(demo_path: Path, cache_root: Path) -> Path:
    """Return the per-demo cache directory."""
    return cache_root / demo_path.stem


def _demo_cache_valid(demo_path: Path, cache_dir: Path) -> bool:
    """Return True when the cache exists and matches the current demo file."""
    manifest_path = cache_dir / "manifest.json"
    if not manifest_path.exists():
        return False
    try:
        manifest = json.loads(manifest_path.read_text())
        stat = demo_path.stat()
        return (
            manifest.get("mtime_ns") == stat.st_mtime_ns
            and manifest.get("size") == stat.st_size
        )
    except Exception:
        return False


def _save_demo_cache(
    demo_path: Path,
    cache_dir: Path,
    header: dict[str, Any],
    **dataframes: pl.DataFrame,
) -> None:
    """Write parsed demo artifacts to the cache directory."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    stat = demo_path.stat()
    manifest = {"mtime_ns": stat.st_mtime_ns, "size": stat.st_size}
    (cache_dir / "manifest.json").write_text(json.dumps(manifest))
    (cache_dir / "header.json").write_text(
        json.dumps(header, default=str)
    )
    for name, df in dataframes.items():
        df.write_parquet(cache_dir / f"{name}.parquet")


def _load_demo_cache(demo_path: Path, cache_dir: Path, role: str) -> DemoArtifacts:
    """Load previously cached demo artifacts from disk."""
    header = json.loads((cache_dir / "header.json").read_text())
    frames = {
        name: pl.read_parquet(cache_dir / f"{name}.parquet")
        for name in _CACHE_DATAFRAMES
    }
    return DemoArtifacts(
        demo_path=demo_path,
        role=role,
        header=header,
        ticks=frames["ticks"],
        rounds=frames["rounds"],
        kills=frames["kills"],
        damages=frames["damages"],
        shots=frames["shots"],
        bomb=frames["bomb"],
        grenades=frames["grenades"],
    )


def parse_or_load_demo(
    demo_path: str | Path,
    *,
    role: str | None = None,
    cache_root: Path | None = None,
    force: bool = False,
) -> DemoArtifacts:
    """Parse one demo and return its in-memory artifacts.

    Results are cached under ``cache_root`` (default: ``.cache/awpy/``) as
    parquet files alongside a small JSON manifest.  On subsequent calls the
    cache is returned immediately unless ``force=True`` or the ``.dem`` file
    has been modified since the cache was written.

    Args:
        demo_path: Path to the `.dem` file.
        role: Optional label such as `user` or `pro`.
        cache_root: Root folder for the parquet cache.  Defaults to
            ``.cache/awpy`` relative to the current working directory.
        force: When ``True``, ignore any existing cache and re-parse from scratch.
    """
    demo_path = Path(demo_path)
    resolved_role = role or infer_role_from_path(demo_path)
    cache_root = cache_root or _DEFAULT_CACHE_ROOT
    cache_dir = _demo_cache_dir(demo_path, cache_root)

    if not force and _demo_cache_valid(demo_path, cache_dir):
        return _load_demo_cache(demo_path, cache_dir, resolved_role)

    demo = Demo(demo_path)
    demo.parse(
        events=DEFAULT_EVENTS,
        player_props=FOCUSED_PLAYER_PROPS,
        other_props=FOCUSED_WORLD_PROPS,
    )

    ticks = demo.parse_ticks(
        player_props=FOCUSED_PLAYER_PROPS,
        other_props=FOCUSED_WORLD_PROPS,
    )
    ticks = awpy.parsers.utils.fix_common_names(ticks)
    ticks = ticks.join(
        pl.DataFrame({"tick": demo.in_play_ticks}),
        on="tick",
        how="semi",
    )
    ticks = awpy.parsers.rounds.apply_round_num(
        df=ticks,
        rounds_df=demo.rounds,
        tick_col="tick",
    ).filter(pl.col("round_num").is_not_null())

    _save_demo_cache(
        demo_path,
        cache_dir,
        demo.header,
        ticks=ticks,
        rounds=demo.rounds,
        kills=demo.kills,
        damages=demo.damages,
        shots=demo.shots,
        bomb=demo.bomb,
        grenades=demo.grenades,
    )

    return DemoArtifacts(
        demo_path=demo_path,
        role=resolved_role,
        header=demo.header,
        ticks=ticks,
        rounds=demo.rounds,
        kills=demo.kills,
        damages=demo.damages,
        shots=demo.shots,
        bomb=demo.bomb,
        grenades=demo.grenades,
    )


def infer_role_from_path(demo_path: Path) -> str:
    """Infer whether a demo belongs to the user or pro folder.

    Args:
        demo_path: Path to the demo file.
    """
    return "user" if "user_demo" in str(demo_path) else "pro"


def summarize_artifacts(artifacts: Sequence[DemoArtifacts]) -> pl.DataFrame:
    """Create one summary table for a list of parsed demos.

    Args:
        artifacts: Parsed demo bundles returned by `parse_or_load_demo`.
    """
    records = []
    for item in artifacts:
        records.append(
            {
                "role": item.role,
                "demo_id": item.demo_id,
                "map_name": item.map_name,
                "players": item.players["steamid"].n_unique(),
                "rounds": item.rounds.height,
                "ticks": item.ticks.height,
                "kills": item.kills.height,
                "damages": item.damages.height,
                "shots": item.shots.height,
                "bomb_events": item.bomb.height,
                "grenades": item.grenades.height,
            }
        )
    return pl.DataFrame(records).sort(["role", "demo_id"])


def identify_user_player(artifacts: DemoArtifacts) -> dict[str, Any]:
    """Pick the focal user player from one user demo.

    Args:
        artifacts: Parsed data for the user demo.
    """
    hint = extract_steamid_hint(artifacts.demo_path)
    players = artifacts.ticks.select("steamid", "name").unique()
    if hint is not None:
        matched = players.filter(pl.col("steamid") == hint)
        if matched.height >= 1:
            return matched.row(0, named=True)

    counts = (
        artifacts.ticks.group_by(["steamid", "name"])
        .len()
        .sort("len", descending=True)
    )
    return counts.row(0, named=True)


def annotate_ticks_with_clock(artifacts: DemoArtifacts) -> pl.DataFrame:
    """Add round timing columns to the tick table.

    Args:
        artifacts: Parsed demo bundle whose ticks should be annotated.
    """
    round_meta = artifacts.rounds.select(
        "round_num",
        "start",
        "freeze_end",
        "end",
        "winner",
        "reason",
        "bomb_plant",
        "bomb_site",
    )
    return (
        artifacts.ticks.join(round_meta, on="round_num", how="left")
        .with_columns(
            [
                ((pl.col("tick") - pl.col("start")) / artifacts.tickrate).alias("seconds_from_round_start"),
                ((pl.col("tick") - pl.col("freeze_end")) / artifacts.tickrate).alias("seconds_from_freeze_end"),
                ((pl.col("end") - pl.col("tick")) / artifacts.tickrate).alias("seconds_to_round_end"),
            ]
        )
        .sort(["round_num", "tick", "steamid"])
    )


def _sample_ticks(
    *,
    window_start: int,
    window_end: int,
    available_ticks: np.ndarray,
    samples: int,
) -> list[int]:
    """Choose evenly spaced target ticks inside one window.

    Args:
        window_start: First tick in the window.
        window_end: Last tick in the window.
        available_ticks: Real parsed ticks available in that round.
        samples: Number of sample positions to return.
    """
    targets = np.linspace(window_start, window_end, num=samples, dtype=np.int32)
    return [_nearest_tick(int(target), available_ticks) for target in targets]


def _nearest_tick(target: int, available_ticks: np.ndarray) -> int:
    """Return the parsed tick closest to a target tick.

    Args:
        target: Tick we want to sample near.
        available_ticks: Sorted array of parsed ticks.
    """
    idx = int(np.searchsorted(available_ticks, target))
    if idx <= 0:
        return int(available_ticks[0])
    if idx >= available_ticks.size:
        return int(available_ticks[-1])
    before = int(available_ticks[idx - 1])
    after = int(available_ticks[idx])
    return before if abs(target - before) <= abs(after - target) else after


def _round_tick_value(round_row: dict[str, Any], key: str, default: int = 0) -> int:
    value = round_row.get(key)
    if value is None:
        return default
    if isinstance(value, float) and math.isnan(value):
        return default
    return int(value)


def _round_freeze_end_tick(round_row: dict[str, Any], default: int = 0) -> int:
    return _round_tick_value(
        round_row,
        "freeze_end",
        _round_tick_value(round_row, "start", default),
    )


def _distance_xy(a: Sequence[float], b: Sequence[float]) -> float:
    """Compute 2D distance between two `(x, y)` points.

    Args:
        a: First point.
        b: Second point.
    """
    return float(math.sqrt((float(a[0]) - float(b[0])) ** 2 + (float(a[1]) - float(b[1])) ** 2))


def get_window_ticks(artifacts: DemoArtifacts, window_row: dict[str, Any] | pl.DataFrame) -> pl.DataFrame:
    """Return all tick rows that belong to one selected window.

    Args:
        artifacts: Parsed demo bundle for the demo that owns the window.
        window_row: Window row as a dict or one-row DataFrame.
    """
    if isinstance(window_row, pl.DataFrame):
        window_row = window_row.row(0, named=True)
    return annotate_ticks_with_clock(artifacts).filter(
        (pl.col("round_num") == int(window_row["round_num"]))
        & (pl.col("tick") >= int(window_row["window_start_tick"]))
        & (pl.col("tick") <= int(window_row["window_end_tick"]))
    )


def classify_team_window_phase(
    *,
    bomb_planted_ratio: float,
    alive_t: int,
    alive_ct: int,
    start_sec: float,
) -> str:
    """Assign a coarse phase label to one full-team window.

    Args:
        bomb_planted_ratio: Fraction of sampled ticks where the bomb is planted.
        alive_t: Terrorist alive count in the stable window state.
        alive_ct: Counter-Terrorist alive count in the stable window state.
        start_sec: Window start time measured from freeze end.
    """
    if bomb_planted_ratio >= 0.5:
        return "postplant"
    if alive_t <= 2 or alive_ct <= 2:
        return "late_round"
    if start_sec <= 15:
        return "opening"
    return "mid_round"


def build_team_window_catalog(
    artifacts: DemoArtifacts,
    *,
    window_seconds: int = 6,
    stride_seconds: int = 2,
    samples_per_window: int = 8,
    min_alive_t: int = 2,
    min_alive_ct: int = 2,
) -> pl.DataFrame:
    """Build rolling windows that summarize the full alive-player team state.

    A window is kept if at least ``min_alive_t`` terrorists and ``min_alive_ct``
    counter-terrorists remain alive across every sampled tick. Those players
    form the window's stable roster; everyone else is ignored for track-level
    matching but still contributes to the team-shape snapshot features.

    Args:
        artifacts: Parsed demo bundle to sample from.
        window_seconds: Length of each gameplay window in seconds.
        stride_seconds: How far to move before starting the next window.
        samples_per_window: Number of sampled ticks used to summarize the window.
        min_alive_t: Minimum stable terrorists required to keep a window.
        min_alive_ct: Minimum stable counter-terrorists required to keep a window.
    """
    ticks = annotate_ticks_with_clock(artifacts)
    stride_ticks = stride_seconds * artifacts.tickrate
    window_ticks = window_seconds * artifacts.tickrate
    window_rows: list[dict[str, Any]] = []

    for round_row in artifacts.rounds.sort("round_num").iter_rows(named=True):
        round_num = int(round_row["round_num"])
        round_ticks = ticks.filter(pl.col("round_num") == round_num).sort("tick")
        if round_ticks.is_empty():
            continue

        snap_dict = round_ticks.partition_by("tick", as_dict=True)
        snapshots = {
            int(key[0] if isinstance(key, tuple) else key): value
            for key, value in snap_dict.items()
        }
        available_ticks = np.array(sorted(snapshots), dtype=np.int32)
        if available_ticks.size == 0:
            continue

        start_tick = _round_freeze_end_tick(round_row, int(available_ticks[0]))
        end_tick = _round_tick_value(round_row, "end", int(available_ticks[-1]))
        if end_tick - start_tick < window_ticks:
            continue

        round_site = _infer_site_from_rounds(round_row)
        plant_tick = round_row.get("bomb_plant")

        for window_start in range(start_tick, end_tick - window_ticks + 1, stride_ticks):
            window_end = window_start + window_ticks
            sample_ticks = _sample_ticks(
                window_start=window_start,
                window_end=window_end,
                available_ticks=available_ticks,
                samples=samples_per_window,
            )
            feature_row = _build_team_window_features(
                artifacts=artifacts,
                round_row=round_row,
                round_site=round_site,
                plant_tick=plant_tick,
                snapshots=snapshots,
                sample_ticks=sample_ticks,
                window_start=window_start,
                window_end=window_end,
                min_alive_t=min_alive_t,
                min_alive_ct=min_alive_ct,
            )
            if feature_row is None:
                continue
            window_rows.append(feature_row)

    if not window_rows:
        return pl.DataFrame()

    catalog = pl.DataFrame(window_rows, infer_schema_length=None).sort(
        ["role", "demo_id", "round_num", "window_start_tick"]
    )
    return catalog


def _build_team_window_features(
    *,
    artifacts: DemoArtifacts,
    round_row: dict[str, Any],
    round_site: str | None,
    plant_tick: int | None,
    snapshots: dict[int, pl.DataFrame],
    sample_ticks: Sequence[int],
    window_start: int,
    window_end: int,
    min_alive_t: int,
    min_alive_ct: int,
) -> dict[str, Any] | None:
    """Compute one feature row for one full-team window."""
    round_num = int(round_row["round_num"])
    sample_snapshots = [snapshots[int(tick)] for tick in sample_ticks]
    if not sample_snapshots:
        return None

    alive_sets_t: list[set[int]] = []
    alive_sets_ct: list[set[int]] = []
    summaries: list[dict[str, Any]] = []
    bomb_planted_flags: list[bool] = []

    t_places: list[str] = []
    ct_places: list[str] = []
    t_weapon_families: list[str] = []
    ct_weapon_families: list[str] = []
    t_equip_values: list[float] = []
    ct_equip_values: list[float] = []
    ct_defuser_flags: list[int] = []

    for snap in sample_snapshots:
        alive_t = snap.filter((pl.col("side") == "t") & pl.col("is_alive")).sort("steamid")
        alive_ct = snap.filter((pl.col("side") == "ct") & pl.col("is_alive")).sort("steamid")

        alive_sets_t.append({int(v) for v in alive_t["steamid"].to_list()})
        alive_sets_ct.append({int(v) for v in alive_ct["steamid"].to_list()})
        bomb_planted_flags.append(bool(snap["is_bomb_planted"].max()))

        summaries.append(
            {
                "t": _side_shape_summary(alive_t),
                "ct": _side_shape_summary(alive_ct),
                "team_gap": _centroid_gap(alive_t, alive_ct),
            }
        )

        for place in alive_t["place"].to_list():
            n = _normalize_place(place)
            if n:
                t_places.append(n)
        for place in alive_ct["place"].to_list():
            n = _normalize_place(place)
            if n:
                ct_places.append(n)

        for weapon in alive_t["active_weapon_name"].to_list():
            fam = _weapon_family(weapon)
            if fam:
                t_weapon_families.append(fam)
        for weapon in alive_ct["active_weapon_name"].to_list():
            fam = _weapon_family(weapon)
            if fam:
                ct_weapon_families.append(fam)

        if alive_t.height:
            t_equip_values.append(float(alive_t["current_equip_value"].mean()))
        if alive_ct.height:
            ct_equip_values.append(float(alive_ct["current_equip_value"].mean()))
            ct_defuser_flags.append(int(bool(alive_ct["has_defuser"].max())))

    # Stable roster: players alive in EVERY sampled tick. Windows where nobody
    # dies produce the full set; windows with mid-window deaths shrink the set
    # rather than getting dropped outright.
    stable_t = sorted(set.intersection(*alive_sets_t)) if alive_sets_t else []
    stable_ct = sorted(set.intersection(*alive_sets_ct)) if alive_sets_ct else []

    if len(stable_t) < min_alive_t or len(stable_ct) < min_alive_ct:
        return None

    bomb_planted_ratio = float(np.mean(bomb_planted_flags))
    planted = bomb_planted_ratio >= 0.5

    site: str | None = None
    if planted:
        site = round_site or _infer_site_from_places(t_places + ct_places)

    window_mid_tick = int((window_start + window_end) // 2)
    time_since_plant_s: float | None = None
    if planted and plant_tick is not None:
        time_since_plant_s = max(0.0, (window_mid_tick - int(plant_tick)) / artifacts.tickrate)

    t_place_profile = dict(Counter(t_places))
    ct_place_profile = dict(Counter(ct_places))
    t_top_places = [p for p, _ in Counter(t_places).most_common(3)]
    ct_top_places = [p for p, _ in Counter(ct_places).most_common(3)]

    t_weapon_profile = dict(Counter(t_weapon_families))
    ct_weapon_profile = dict(Counter(ct_weapon_families))
    freeze_end_tick = _round_freeze_end_tick(round_row, int(window_start))
    round_end_tick = _round_tick_value(round_row, "end", int(window_end))

    row: dict[str, Any] = {
        "role": artifacts.role,
        "demo_id": artifacts.demo_id,
        "demo_path": str(artifacts.demo_path.resolve()),
        "map_name": artifacts.map_name,
        "round_num": round_num,
        "round_winner": round_row["winner"],
        "round_reason": round_row["reason"],
        "window_start_tick": int(window_start),
        "window_end_tick": int(window_end),
        "window_mid_tick": window_mid_tick,
        "window_duration_sec": (window_end - window_start) / artifacts.tickrate,
        "window_start_sec_from_freeze": (window_start - freeze_end_tick) / artifacts.tickrate,
        "window_end_sec_from_freeze": (window_end - freeze_end_tick) / artifacts.tickrate,
        "round_duration_sec": (round_end_tick - freeze_end_tick) / artifacts.tickrate,
        "bomb_planted_ratio": bomb_planted_ratio,
        "planted": planted,
        "site": site,
        "time_since_plant_s": time_since_plant_s,
        "alive_t": len(stable_t),
        "alive_ct": len(stable_ct),
        "alive_total": len(stable_t) + len(stable_ct),
        "sample_ticks": [int(value) for value in sample_ticks],
        "t_alive_steamids": stable_t,
        "ct_alive_steamids": stable_ct,
        "t_place_profile": t_place_profile,
        "ct_place_profile": ct_place_profile,
        "t_top_places": t_top_places,
        "ct_top_places": ct_top_places,
        "t_weapon_profile": t_weapon_profile,
        "ct_weapon_profile": ct_weapon_profile,
        "t_focus_weapon_family": _top_family(t_weapon_profile),
        "ct_focus_weapon_family": _top_family(ct_weapon_profile),
        "t_mean_equip": float(np.mean(t_equip_values)) if t_equip_values else 0.0,
        "ct_mean_equip": float(np.mean(ct_equip_values)) if ct_equip_values else 0.0,
        "ct_has_defuser": int(max(ct_defuser_flags)) if ct_defuser_flags else 0,
    }
    row["phase"] = classify_team_window_phase(
        bomb_planted_ratio=bomb_planted_ratio,
        alive_t=row["alive_t"],
        alive_ct=row["alive_ct"],
        start_sec=row["window_start_sec_from_freeze"],
    )

    for index, summary in enumerate(summaries):
        for side in ("t", "ct"):
            side_summary = summary[side]
            row[f"s{index}_{side}_cx"] = side_summary["centroid_x"]
            row[f"s{index}_{side}_cy"] = side_summary["centroid_y"]
            row[f"s{index}_{side}_spread"] = side_summary["spread"]
            row[f"s{index}_{side}_pairwise"] = side_summary["pairwise"]
            row[f"s{index}_{side}_mean_speed"] = side_summary["mean_speed"]
            for radial_index, radial_value in enumerate(side_summary["radial_signature"]):
                row[f"s{index}_{side}_radial_{radial_index}"] = radial_value
        row[f"s{index}_team_gap"] = summary["team_gap"]

    return row


def _side_shape_summary(group: pl.DataFrame) -> dict[str, Any]:
    """Summarize one side's alive-player shape in a single snapshot."""
    if group.is_empty():
        return {
            "centroid_x": 0.0,
            "centroid_y": 0.0,
            "spread": 0.0,
            "pairwise": 0.0,
            "mean_speed": 0.0,
            "radial_signature": [0.0] * 5,
        }

    positions = np.column_stack(
        [
            group["X"].to_numpy().astype(float),
            group["Y"].to_numpy().astype(float),
        ]
    )
    centroid = positions.mean(axis=0)
    radial = np.linalg.norm(positions - centroid, axis=1)
    speeds = np.sqrt(
        np.square(group["velocity_X"].fill_null(0).to_numpy().astype(float))
        + np.square(group["velocity_Y"].fill_null(0).to_numpy().astype(float))
    )
    radial_signature = sorted(float(value) for value in radial.tolist())
    radial_signature.extend([0.0] * (5 - len(radial_signature)))
    radial_signature = radial_signature[:5]

    return {
        "centroid_x": float(centroid[0]),
        "centroid_y": float(centroid[1]),
        "spread": float(np.mean(radial)) if radial.size else 0.0,
        "pairwise": _mean_pairwise_distance(positions),
        "mean_speed": float(np.mean(speeds)) if speeds.size else 0.0,
        "radial_signature": radial_signature,
    }


def _centroid_gap(a_group: pl.DataFrame, b_group: pl.DataFrame) -> float:
    """Measure distance between alive-team centroids in one snapshot."""
    if a_group.is_empty() or b_group.is_empty():
        return 0.0
    a_centroid = (float(a_group["X"].mean()), float(a_group["Y"].mean()))
    b_centroid = (float(b_group["X"].mean()), float(b_group["Y"].mean()))
    return _distance_xy(a_centroid, b_centroid)


def _mean_pairwise_distance(positions: np.ndarray) -> float:
    """Return the mean pairwise XY distance inside one position set."""
    if len(positions) <= 1:
        return 0.0
    distances: list[float] = []
    for left_index in range(len(positions)):
        for right_index in range(left_index + 1, len(positions)):
            distances.append(_distance_xy(positions[left_index], positions[right_index]))
    return float(np.mean(distances)) if distances else 0.0


def team_context_feature_columns(catalog: pl.DataFrame) -> list[str]:
    """Return the numeric columns used for full-team context ranking."""
    preferred_scalars = [
        "window_start_sec_from_freeze",
        "window_duration_sec",
        "bomb_planted_ratio",
        "alive_t",
        "alive_ct",
        "alive_total",
    ]
    sample_columns = sorted(
        column
        for column in catalog.columns
        if re.match(
            r"^s\d+_(t|ct)_(cx|cy|spread|pairwise|mean_speed|radial_\d+)$",
            column,
        )
        or re.match(r"^s\d+_team_gap$", column)
    )
    return preferred_scalars + sample_columns


def retrieve_similar_team_windows(
    user_window: dict[str, Any] | pl.DataFrame,
    pro_catalog: pl.DataFrame,
    *,
    top_k: int = 25,
) -> pl.DataFrame:
    """Retrieve pro windows with a similar full-team context.

    Filters on map, alive counts, and plant state, then ranks candidates by a
    mix of geometric team-shape distance, phase mismatch, place-profile
    overlap, weapon-family compatibility, and plant-timing distance.
    """
    if isinstance(user_window, pl.DataFrame):
        if user_window.height != 1:
            raise ValueError("Pass exactly one user team-window row.")
        user_row = user_window.row(0, named=True)
    else:
        user_row = user_window

    candidates = pro_catalog.filter(
        (pl.col("map_name") == user_row["map_name"])
        & (pl.col("alive_t") == int(user_row["alive_t"]))
        & (pl.col("alive_ct") == int(user_row["alive_ct"]))
        & (pl.col("planted") == bool(user_row["planted"]))
    )
    if candidates.is_empty():
        return candidates

    # When both windows are post-plant and the user's site is known, restrict
    # to the same site. Otherwise keep the relaxed pool.
    user_site = user_row.get("site")
    if user_row["planted"] and user_site:
        site_filtered = candidates.filter(pl.col("site") == user_site)
        if site_filtered.height >= 1:
            candidates = site_filtered

    phase_filtered = candidates.filter(pl.col("phase") == user_row["phase"])
    if phase_filtered.height >= min(top_k, 5):
        candidates = phase_filtered

    feature_cols = team_context_feature_columns(candidates)
    matrix = candidates.select(feature_cols).fill_nan(0).fill_null(0).to_numpy()
    user_vector = np.array([float(user_row[column]) for column in feature_cols], dtype=float)

    std = matrix.std(axis=0)
    std[std < 1e-6] = 1.0
    geom_distances = np.sqrt(np.mean(((matrix - user_vector) / std) ** 2, axis=1))

    user_t_profile = user_row.get("t_place_profile") or {}
    user_ct_profile = user_row.get("ct_place_profile") or {}
    user_t_weapons = user_row.get("t_weapon_profile") or {}
    user_ct_weapons = user_row.get("ct_weapon_profile") or {}
    user_t_focus = user_row.get("t_focus_weapon_family")
    user_ct_focus = user_row.get("ct_focus_weapon_family")
    user_plant_time = user_row.get("time_since_plant_s")

    place_penalties = np.zeros(candidates.height, dtype=float)
    weapon_penalties = np.zeros(candidates.height, dtype=float)
    plant_penalties = np.zeros(candidates.height, dtype=float)
    time_penalties = 0.08 * np.abs(
        candidates["window_start_sec_from_freeze"].to_numpy()
        - float(user_row["window_start_sec_from_freeze"])
    )

    t_profiles = candidates["t_place_profile"].to_list()
    ct_profiles = candidates["ct_place_profile"].to_list()
    t_weapons = candidates["t_weapon_profile"].to_list()
    ct_weapons = candidates["ct_weapon_profile"].to_list()
    t_focus = candidates["t_focus_weapon_family"].to_list()
    ct_focus = candidates["ct_focus_weapon_family"].to_list()
    plant_times = candidates["time_since_plant_s"].to_list()

    for i in range(candidates.height):
        place_sim = 0.5 * (
            _dict_overlap_similarity(user_t_profile, t_profiles[i])
            + _dict_overlap_similarity(user_ct_profile, ct_profiles[i])
        )
        place_penalties[i] = 0.6 * (1.0 - place_sim)

        weapon_sim_profile = 0.5 * (
            _dict_overlap_similarity(user_t_weapons, t_weapons[i])
            + _dict_overlap_similarity(user_ct_weapons, ct_weapons[i])
        )
        weapon_sim_focus = 0.5 * (
            _weapon_compat(user_t_focus, t_focus[i])
            + _weapon_compat(user_ct_focus, ct_focus[i])
        )
        weapon_penalties[i] = 0.4 * (1.0 - 0.5 * (weapon_sim_profile + weapon_sim_focus))

        if user_plant_time is not None and plant_times[i] is not None:
            plant_penalties[i] = 0.05 * abs(float(user_plant_time) - float(plant_times[i]))

    total_distance = (
        geom_distances
        + time_penalties
        + place_penalties
        + weapon_penalties
        + plant_penalties
    )

    scored = candidates.with_columns(
        [
            pl.Series("context_feature_distance", geom_distances),
            pl.Series("context_penalty", time_penalties),
            pl.Series("context_place_penalty", place_penalties),
            pl.Series("context_weapon_penalty", weapon_penalties),
            pl.Series("context_plant_penalty", plant_penalties),
            pl.Series("context_distance", total_distance),
        ]
    ).with_columns(
        (
            1.0 / (1.0 + pl.col("context_distance"))
        ).alias("context_match")
    ).sort("context_distance")

    return scored.head(top_k)


def build_window_player_tracks(
    artifacts: DemoArtifacts,
    window_row: dict[str, Any] | pl.DataFrame,
) -> dict[str, Any] | None:
    """Build per-player sampled tracks for one stable team window."""
    if isinstance(window_row, pl.DataFrame):
        window_row = window_row.row(0, named=True)

    sample_ticks = [int(value) for value in window_row["sample_ticks"]]
    window_ticks = get_window_ticks(artifacts, window_row).filter(
        pl.col("tick").is_in(sample_ticks)
    ).sort(["tick", "steamid"])
    if window_ticks.is_empty():
        return None

    snapshots = {
        int(key[0] if isinstance(key, tuple) else key): value
        for key, value in window_ticks.partition_by("tick", as_dict=True).items()
    }
    side_ids = {
        "t": [int(value) for value in window_row["t_alive_steamids"]],
        "ct": [int(value) for value in window_row["ct_alive_steamids"]],
    }
    tracks_by_side: dict[str, dict[int, dict[str, Any]]] = {"t": {}, "ct": {}}

    for side, steamids in side_ids.items():
        for steamid in steamids:
            player_rows = window_ticks.filter(pl.col("steamid") == steamid).sort("tick")
            if player_rows.height != len(sample_ticks):
                return None

            positions = np.column_stack(
                [
                    player_rows["X"].to_numpy().astype(float),
                    player_rows["Y"].to_numpy().astype(float),
                ]
            )
            speeds = np.sqrt(
                np.square(player_rows["velocity_X"].fill_null(0).to_numpy().astype(float))
                + np.square(player_rows["velocity_Y"].fill_null(0).to_numpy().astype(float))
            )
            yaws = player_rows["yaw"].fill_null(0).to_numpy().astype(float)

            rel_team: list[tuple[float, float]] = []
            for tick in sample_ticks:
                snap = snapshots[int(tick)]
                side_group = snap.filter((pl.col("side") == side) & pl.col("is_alive"))
                centroid = (
                    float(side_group["X"].mean()) if side_group.height else 0.0,
                    float(side_group["Y"].mean()) if side_group.height else 0.0,
                )
                player_row = snap.filter(pl.col("steamid") == steamid)
                if player_row.is_empty():
                    return None
                rel_team.append(
                    (
                        float(player_row["X"][0]) - centroid[0],
                        float(player_row["Y"][0]) - centroid[1],
                    )
                )

            tracks_by_side[side][steamid] = {
                "steamid": steamid,
                "name": str(player_rows["name"][0]),
                "side": side,
                "positions": positions,
                "speeds": speeds,
                "yaws": yaws,
                "relative_to_team": np.array(rel_team, dtype=float),
                "path_length": float(np.linalg.norm(np.diff(positions, axis=0), axis=1).sum())
                if len(positions) > 1
                else 0.0,
            }

    return {
        "sample_ticks": sample_ticks,
        "tracks_by_side": tracks_by_side,
    }


def _player_side_for_window(window_row: dict[str, Any], player_steamid: int) -> str | None:
    """Return which side a player belongs to inside one team window."""
    if int(player_steamid) in [int(value) for value in window_row["t_alive_steamids"]]:
        return "t"
    if int(player_steamid) in [int(value) for value in window_row["ct_alive_steamids"]]:
        return "ct"
    return None


def _best_track_assignment(
    user_tracks: dict[int, dict[str, Any]],
    pro_tracks: dict[int, dict[str, Any]],
) -> tuple[list[dict[str, Any]], float]:
    """Solve the minimum-cost player assignment for two same-sized track sets."""
    if len(user_tracks) != len(pro_tracks):
        raise ValueError("Track assignment requires equal player counts.")
    if not user_tracks:
        return [], 0.0

    user_ids = list(user_tracks)
    pro_ids = list(pro_tracks)
    best_pairs: list[dict[str, Any]] | None = None
    best_cost = float("inf")

    for permutation in itertools.permutations(pro_ids, len(user_ids)):
        pairs: list[dict[str, Any]] = []
        costs: list[float] = []
        for user_id, pro_id in zip(user_ids, permutation, strict=False):
            cost = _player_alignment_cost(user_tracks[user_id], pro_tracks[pro_id])
            costs.append(cost)
            pairs.append(
                {
                    "user_steamid": int(user_id),
                    "user_name": user_tracks[user_id]["name"],
                    "pro_steamid": int(pro_id),
                    "pro_name": pro_tracks[pro_id]["name"],
                    "side": user_tracks[user_id]["side"],
                    "alignment_cost": float(cost),
                }
            )
        mean_cost = float(np.mean(costs)) if costs else 0.0
        if mean_cost < best_cost:
            best_cost = mean_cost
            best_pairs = pairs

    return best_pairs or [], best_cost


def _player_alignment_cost(
    user_track: dict[str, Any],
    pro_track: dict[str, Any],
    *,
    scales: dict[str, float] | None = None,
) -> float:
    """Score how similarly two matched players move through a window."""
    s = scales or _DEFAULT_SCALES
    positions_user = user_track["positions"]
    positions_pro = pro_track["positions"]
    relative_user = positions_user - positions_user[0]
    relative_pro = positions_pro - positions_pro[0]
    spacing_user = user_track["relative_to_team"]
    spacing_pro = pro_track["relative_to_team"]

    position_cost = float(np.mean(np.linalg.norm(positions_user - positions_pro, axis=1))) / s["position"]
    path_cost = float(np.mean(np.linalg.norm(relative_user - relative_pro, axis=1))) / s["path_relative"]
    spacing_cost = float(np.mean(np.linalg.norm(spacing_user - spacing_pro, axis=1))) / s["spacing"]
    speed_cost = float(np.mean(np.abs(user_track["speeds"] - pro_track["speeds"]))) / s["speed"]

    return (
        0.35 * position_cost
        + 0.35 * path_cost
        + 0.20 * spacing_cost
        + 0.10 * speed_cost
    )


def _player_difference_components(
    user_track: dict[str, Any],
    pro_track: dict[str, Any],
    *,
    scales: dict[str, float] | None = None,
) -> dict[str, float]:
    """Break a matched player comparison into interpretable movement components."""
    s = scales or _DEFAULT_SCALES
    positions_user = user_track["positions"]
    positions_pro = pro_track["positions"]
    relative_user = positions_user - positions_user[0]
    relative_pro = positions_pro - positions_pro[0]
    spacing_user = user_track["relative_to_team"]
    spacing_pro = pro_track["relative_to_team"]

    position_gap = float(np.mean(np.linalg.norm(positions_user - positions_pro, axis=1))) / s["position"]
    path_gap = float(np.mean(np.linalg.norm(relative_user - relative_pro, axis=1))) / s["path_relative"]
    spacing_gap = float(np.mean(np.linalg.norm(spacing_user - spacing_pro, axis=1))) / s["spacing"]
    speed_gap = float(np.mean(np.abs(user_track["speeds"] - pro_track["speeds"]))) / s["speed"]
    yaw_gap = float(np.mean(_angular_difference_degrees(user_track["yaws"], pro_track["yaws"]))) / s["yaw_deg"]
    path_length_gap = abs(float(user_track["path_length"]) - float(pro_track["path_length"])) / s["path_length"]

    total = (
        0.25 * position_gap
        + 0.25 * path_gap
        + 0.20 * spacing_gap
        + 0.15 * speed_gap
        + 0.10 * yaw_gap
        + 0.05 * path_length_gap
    )
    return {
        "position_gap": position_gap,
        "path_gap": path_gap,
        "spacing_gap": spacing_gap,
        "speed_gap": speed_gap,
        "yaw_gap": yaw_gap,
        "path_length_gap": path_length_gap,
        "total": total,
    }


def _angular_difference_degrees(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Return elementwise shortest-path angle differences in degrees."""
    diff = np.abs(a - b) % 360.0
    return np.minimum(diff, 360.0 - diff)


def _score_all_player_assignments(
    user_artifact: DemoArtifacts,
    pro_artifact: DemoArtifacts,
    user_window: dict[str, Any],
    pro_window: dict[str, Any],
    *,
    user_steamid: int,
) -> list[dict[str, Any]]:
    """Return one scored row for every candidate pro player as the user's counterpart.

    Unlike `score_team_window_match`, this does not pick a single winner. Instead it
    returns all candidate assignments so callers can aggregate across rounds and pick
    the best player by round-level similarity rather than per-window divergence.

    Args:
        user_artifact: Parsed bundle for the user demo.
        pro_artifact: Parsed bundle for the candidate pro demo.
        user_window: One user team window row.
        pro_window: One candidate pro team window row, already annotated with
            ``context_match`` and ``context_distance`` by
            ``retrieve_similar_team_windows``.
        user_steamid: SteamID of the focal user player.
    """
    user_side = _player_side_for_window(user_window, user_steamid)
    if user_side is None:
        return []
    other_side = "ct" if user_side == "t" else "t"

    if int(user_window["alive_t"]) != int(pro_window["alive_t"]):
        return []
    if int(user_window["alive_ct"]) != int(pro_window["alive_ct"]):
        return []

    user_tracks = build_window_player_tracks(user_artifact, user_window)
    pro_tracks = build_window_player_tracks(pro_artifact, pro_window)
    if user_tracks is None or pro_tracks is None:
        return []

    user_side_tracks = user_tracks["tracks_by_side"][user_side]
    pro_side_tracks = pro_tracks["tracks_by_side"][user_side]
    if int(user_steamid) not in user_side_tracks:
        return []

    enemy_pairs, _ = _best_track_assignment(
        user_tracks["tracks_by_side"][other_side],
        pro_tracks["tracks_by_side"][other_side],
    )

    context_match = float(pro_window.get("context_match", 0.0))
    results: list[dict[str, Any]] = []

    for candidate_pro_steamid, candidate_pro_track in pro_side_tracks.items():
        teammate_user_tracks = {
            sid: track
            for sid, track in user_side_tracks.items()
            if sid != int(user_steamid)
        }
        teammate_pro_tracks = {
            sid: track
            for sid, track in pro_side_tracks.items()
            if sid != int(candidate_pro_steamid)
        }
        teammate_pairs, _ = _best_track_assignment(teammate_user_tracks, teammate_pro_tracks)

        non_user_pairs = teammate_pairs + enemy_pairs
        non_user_cost = float(np.mean([p["alignment_cost"] for p in non_user_pairs])) if non_user_pairs else 0.0
        non_user_alignment = 1.0 / (1.0 + non_user_cost)

        user_components = _player_difference_components(
            user_side_tracks[int(user_steamid)],
            candidate_pro_track,
        )
        user_pro_difference = user_components["total"] / (1.0 + user_components["total"])

        # Round similarity scores how well ALL players match (not just non-user).
        # Including the user–counterpart alignment ensures the selected pro round
        # is genuinely similar to the user round, not just similar in the
        # supporting cast while the matched player does something completely different.
        user_alignment = 1.0 / (1.0 + user_components["total"])
        n_non_user = len(non_user_pairs)
        if n_non_user > 0:
            all_player_alignment = (non_user_alignment * n_non_user + user_alignment) / (n_non_user + 1)
        else:
            all_player_alignment = user_alignment

        results.append(
            {
                "user_round_num": int(user_window["round_num"]),
                "user_window_start_tick": int(user_window["window_start_tick"]),
                "user_window_end_tick": int(user_window["window_end_tick"]),
                "user_window_start_sec_from_freeze": float(user_window["window_start_sec_from_freeze"]),
                "user_window_end_sec_from_freeze": float(user_window["window_end_sec_from_freeze"]),
                "user_phase": str(user_window["phase"]),
                "user_side": user_side,
                "match_demo_id": str(pro_window["demo_id"]),
                "match_round_num": int(pro_window["round_num"]),
                "match_window_start_tick": int(pro_window["window_start_tick"]),
                "match_window_end_tick": int(pro_window["window_end_tick"]),
                "match_window_start_sec_from_freeze": float(pro_window["window_start_sec_from_freeze"]),
                "match_window_end_sec_from_freeze": float(pro_window["window_end_sec_from_freeze"]),
                "matched_pro_steamid": int(candidate_pro_steamid),
                "matched_pro_player": str(candidate_pro_track["name"]),
                "context_match": context_match,
                "context_distance": float(pro_window.get("context_distance", 0.0)),
                "non_user_alignment": non_user_alignment,
                "non_user_cost": non_user_cost,
                "user_pro_difference": user_pro_difference,
                "user_difference_cost": float(user_components["total"]),
                "user_alignment": user_alignment,
                "round_similarity": context_match * all_player_alignment,
            }
        )

    return results


def select_best_pro_round_match(
    user_artifact: DemoArtifacts,
    pro_artifacts: Sequence[DemoArtifacts],
    *,
    user_steamid: int,
    user_round_num: int,
    start_after_freeze_sec: float = 20.0,
    window_seconds: int = 6,
    stride_seconds: int = 2,
    samples_per_window: int = 8,
    candidate_pool: int = 20,
    min_windows: int = 2,
) -> dict[str, Any] | None:
    """Find the single best matching pro round for one user round.

    Implements the full matching strategy from the plan:

    1. Build team windows for the user round after ``freeze_end + start_after_freeze_sec``.
    2. Retrieve similar pro team windows by team context.
    3. Score every candidate player assignment across those windows.
    4. Aggregate by ``(pro_demo, pro_round, pro_player)`` using
       ``mean(context_match * non_user_alignment)`` as the round-level similarity.
    5. Return the single best combination together with its per-window scores for
       downstream divergence computation.

    Returns ``None`` when no match satisfies ``min_windows``.

    Args:
        user_artifact: Parsed bundle for the user demo.
        pro_artifacts: Parsed bundles for all candidate pro demos.
        user_steamid: SteamID of the focal user player.
        user_round_num: Round number inside the user demo to analyse.
        start_after_freeze_sec: Skip the first N seconds after ``freeze_end``.
        window_seconds: Length of each team window in seconds.
        stride_seconds: Stride between consecutive window starts.
        samples_per_window: Number of sampled ticks per window.
        candidate_pool: Maximum pro windows to retrieve per user window.
        min_windows: Minimum matched window count required to accept a round pair.
    """
    user_team_catalog = build_team_window_catalog(
        user_artifact,
        window_seconds=window_seconds,
        stride_seconds=stride_seconds,
        samples_per_window=samples_per_window,
    )

    round_windows_with_user = user_team_catalog.filter(
        (pl.col("round_num") == user_round_num)
        & (
            pl.col("t_alive_steamids").list.contains(user_steamid)
            | pl.col("ct_alive_steamids").list.contains(user_steamid)
        )
    )
    if round_windows_with_user.is_empty():
        return None

    # Preferred: use only windows after the start_after cutoff. For short rounds
    # where the user dies early, progressively relax the cutoff so we still get
    # at least ``min_windows`` windows to compare.
    user_round_windows = round_windows_with_user.filter(
        pl.col("window_start_sec_from_freeze") >= start_after_freeze_sec
    )
    if user_round_windows.height < min_windows:
        relaxed_cutoffs = [10.0, 5.0, 0.0]
        for cutoff in relaxed_cutoffs:
            if cutoff >= start_after_freeze_sec:
                continue
            candidate = round_windows_with_user.filter(
                pl.col("window_start_sec_from_freeze") >= cutoff
            )
            if candidate.height >= min_windows:
                user_round_windows = candidate
                break
        else:
            user_round_windows = round_windows_with_user
    if user_round_windows.is_empty():
        return None

    pro_team_catalogs = [
        build_team_window_catalog(
            artifact,
            window_seconds=window_seconds,
            stride_seconds=stride_seconds,
            samples_per_window=samples_per_window,
        )
        for artifact in pro_artifacts
    ]
    non_empty = [c for c in pro_team_catalogs if not c.is_empty()]
    if not non_empty:
        return None
    pro_team_catalog = pl.concat(non_empty, how="diagonal_relaxed")
    pro_by_demo_id = {artifact.demo_id: artifact for artifact in pro_artifacts}

    all_scored: list[dict[str, Any]] = []
    for user_row in user_round_windows.iter_rows(named=True):
        retrieved = retrieve_similar_team_windows(user_row, pro_team_catalog, top_k=candidate_pool)
        if retrieved.is_empty():
            continue
        for pro_row in retrieved.iter_rows(named=True):
            pro_artifact = pro_by_demo_id.get(str(pro_row["demo_id"]))
            if pro_artifact is None:
                continue
            scored = _score_all_player_assignments(
                user_artifact,
                pro_artifact,
                user_row,
                pro_row,
                user_steamid=user_steamid,
            )
            all_scored.extend(scored)

    if not all_scored:
        return None

    scores_df = pl.DataFrame(all_scored)

    # Dedup: each scored row came from one (user_window, retrieved_pro_window, candidate_pro_player)
    # triple. Multiple retrieved pro windows from the same pro (demo, round) inflate the count for
    # the same (pro_round, player) candidate. Collapse to one row per
    # (user_window, pro_demo, pro_round, pro_player), keeping the best-scoring pro window.
    scores_df = (
        scores_df
        .sort("round_similarity", descending=True)
        .unique(
            subset=[
                "user_window_start_tick",
                "match_demo_id",
                "match_round_num",
                "matched_pro_steamid",
            ],
            keep="first",
            maintain_order=True,
        )
    )

    total_user_windows = user_round_windows.height
    stride_ticks = stride_seconds * user_artifact.tickrate

    # Longest run of consecutive user windows (stride apart) where the same (pro_round, player)
    # is present. Rewards rounds that match continuously, not just in isolated hits.
    streak_df = _longest_consecutive_streaks(
        scores_df,
        group_cols=["match_demo_id", "match_round_num", "matched_pro_steamid"],
        order_col="user_window_start_tick",
        expected_step=stride_ticks,
    )

    grouped = (
        scores_df.group_by(
            ["match_demo_id", "match_round_num", "matched_pro_steamid", "matched_pro_player"]
        )
        .agg(
            [
                pl.col("round_similarity").mean().alias("mean_round_similarity"),
                pl.col("non_user_alignment").mean().alias("mean_non_user_alignment"),
                pl.col("context_match").mean().alias("mean_context_match"),
                pl.col("user_pro_difference").mean().alias("mean_user_pro_difference"),
                pl.col("user_round_num").first().alias("user_round_num"),
                pl.len().alias("window_count"),
            ]
        )
        .with_columns((pl.col("window_count") / total_user_windows).alias("coverage"))
        .join(
            streak_df,
            on=["match_demo_id", "match_round_num", "matched_pro_steamid"],
            how="left",
        )
        .with_columns(
            pl.col("longest_streak").fill_null(1).alias("longest_streak"),
        )
        .with_columns(
            # Composite score: mean similarity, boosted by continuity and coverage.
            (
                pl.col("mean_round_similarity")
                * (1.0 + 0.15 * (pl.col("longest_streak") - 1).clip(0, 6))
                * (0.70 + 0.30 * pl.col("coverage").clip(0.0, 1.0))
            ).alias("round_score")
        )
        .filter(pl.col("window_count") >= min_windows)
        .sort("round_score", descending=True)
    )

    if grouped.is_empty():
        return None

    best = grouped.row(0, named=True)

    best_window_scores = (
        scores_df.filter(
            (pl.col("match_demo_id") == best["match_demo_id"])
            & (pl.col("match_round_num") == best["match_round_num"])
            & (pl.col("matched_pro_steamid") == best["matched_pro_steamid"])
        )
        .sort("user_window_start_sec_from_freeze")
    )

    return {
        "user_round_num": int(best["user_round_num"]),
        "match_demo_id": str(best["match_demo_id"]),
        "match_round_num": int(best["match_round_num"]),
        "matched_pro_steamid": int(best["matched_pro_steamid"]),
        "matched_pro_player": str(best["matched_pro_player"]),
        "mean_round_similarity": float(best["mean_round_similarity"]),
        "mean_non_user_alignment": float(best["mean_non_user_alignment"]),
        "mean_context_match": float(best["mean_context_match"]),
        "mean_user_pro_difference": float(best["mean_user_pro_difference"]),
        "window_count": int(best["window_count"]),
        "coverage": float(best["coverage"]),
        "longest_streak": int(best["longest_streak"]),
        "round_score": float(best["round_score"]),
        "all_round_rankings": grouped.to_dicts(),
        "window_scores": best_window_scores.to_dicts(),
    }


def _longest_consecutive_streaks(
    df: pl.DataFrame,
    *,
    group_cols: Sequence[str],
    order_col: str,
    expected_step: int,
) -> pl.DataFrame:
    """Per-group longest run of consecutive values spaced by ``expected_step``."""
    if df.is_empty():
        return pl.DataFrame(
            schema={**{col: df.schema[col] for col in group_cols}, "longest_streak": pl.Int64}
        )

    ordered = df.sort([*group_cols, order_col]).with_columns(
        pl.col(order_col).diff().over(list(group_cols)).alias("_step")
    ).with_columns(
        (pl.col("_step") != expected_step).fill_null(True).alias("_break")
    ).with_columns(
        pl.col("_break").cast(pl.Int64).cum_sum().over(list(group_cols)).alias("_run_id")
    )
    return (
        ordered
        .group_by([*group_cols, "_run_id"])
        .agg(pl.len().alias("_run_len"))
        .group_by(list(group_cols))
        .agg(pl.col("_run_len").max().alias("longest_streak"))
    )


def compute_round_divergence_timeline(
    window_scores: Sequence[dict[str, Any]],
    *,
    divergence_threshold: float = 0.15,
    min_consecutive: int = 2,
) -> dict[str, Any]:
    """Compute per-window divergence signal and detect the first sustained divergence.

    The divergence signal for each window is:

        ``divergence_signal = user_pro_difference * non_user_alignment``

    High values mean the user moved differently from the matched pro player **and**
    the surrounding players were still well-aligned—so the divergence belongs to the
    user, not a different game state.

    Divergence start is defined as the first run of ``min_consecutive`` consecutive
    windows above ``divergence_threshold``.

    Args:
        window_scores: Per-window scored rows from ``select_best_pro_round_match``.
        divergence_threshold: Minimum signal value to count as a divergence window.
        min_consecutive: Minimum consecutive windows that must exceed the threshold.
    """
    sorted_windows = sorted(
        window_scores,
        key=lambda row: float(row["user_window_start_sec_from_freeze"]),
    )

    timeline: list[dict[str, Any]] = []
    for row in sorted_windows:
        divergence_signal = float(row["user_pro_difference"]) * float(row["non_user_alignment"])
        timeline.append(
            {
                "user_window_start_sec_from_freeze": float(row["user_window_start_sec_from_freeze"]),
                "user_window_end_sec_from_freeze": float(row["user_window_end_sec_from_freeze"]),
                "user_window_start_tick": int(row["user_window_start_tick"]),
                "user_window_end_tick": int(row["user_window_end_tick"]),
                "divergence_signal": divergence_signal,
                "user_pro_difference": float(row["user_pro_difference"]),
                "non_user_alignment": float(row["non_user_alignment"]),
                "context_match": float(row["context_match"]),
            }
        )

    divergence_start_sec: float | None = None
    divergence_start_tick: int | None = None
    divergence_end_sec: float | None = timeline[-1]["user_window_end_sec_from_freeze"] if timeline else None

    consecutive = 0
    candidate_sec: float | None = None
    candidate_tick: int | None = None

    for entry in timeline:
        if entry["divergence_signal"] >= divergence_threshold:
            if consecutive == 0:
                candidate_sec = entry["user_window_start_sec_from_freeze"]
                candidate_tick = entry["user_window_start_tick"]
            consecutive += 1
            if consecutive >= min_consecutive and divergence_start_sec is None:
                divergence_start_sec = candidate_sec
                divergence_start_tick = candidate_tick
        else:
            consecutive = 0
            candidate_sec = None
            candidate_tick = None

    return {
        "timeline": timeline,
        "divergence_start_sec": divergence_start_sec,
        "divergence_start_tick": divergence_start_tick,
        "divergence_end_sec": divergence_end_sec,
        "threshold": divergence_threshold,
        "min_consecutive": min_consecutive,
    }


# ---------------------------------------------------------------------------
# Clean-repo API adapter.
# ---------------------------------------------------------------------------

_FRAME_NAMES = ("ticks", "rounds", "kills", "damages", "shots", "bomb", "grenades")


def _json_default(value: Any) -> str:
    return str(value)


def _empty_frame() -> pl.DataFrame:
    return pl.DataFrame()


def _read_header(parquet_dir: Path, demo_id: str, map_name: str | None) -> dict[str, Any]:
    for candidate in (
        parquet_dir / f"{demo_id}_header.json",
        parquet_dir / "header.json",
    ):
        if candidate.exists():
            with candidate.open("r", encoding="utf-8") as fh:
                header = json.load(fh)
            if map_name and not header.get("map_name"):
                header["map_name"] = map_name
            return header
    return {"map_name": map_name or "unknown"}


def _read_frame(parquet_dir: Path, demo_id: str, name: str, *, required: bool = False) -> pl.DataFrame:
    candidates = (
        parquet_dir / f"{demo_id}_{name}.parquet",
        parquet_dir / f"{Path(demo_id).stem}_{name}.parquet",
        parquet_dir / f"{name}.parquet",
    )
    for candidate in candidates:
        if candidate.exists():
            return pl.read_parquet(candidate)
    if required:
        raise FileNotFoundError(f"missing {name} parquet in {parquet_dir}")
    return _empty_frame()


def _ensure_columns(df: pl.DataFrame, defaults: dict[str, Any]) -> pl.DataFrame:
    exprs = []
    for name, default in defaults.items():
        if name not in df.columns:
            exprs.append(pl.lit(default).alias(name))
    return df.with_columns(exprs) if exprs else df


def _coerce_ticks_for_matching(ticks: pl.DataFrame) -> pl.DataFrame:
    rename_map: dict[str, str] = {}
    if "player_steamid" in ticks.columns and "steamid" not in ticks.columns:
        rename_map["player_steamid"] = "steamid"
    if "player_name" in ticks.columns and "name" not in ticks.columns:
        rename_map["player_name"] = "name"
    if "last_place_name" in ticks.columns and "place" not in ticks.columns:
        rename_map["last_place_name"] = "place"
    if rename_map:
        ticks = ticks.rename(rename_map)

    ticks = _ensure_columns(
        ticks,
        {
            "steamid": 0,
            "name": "",
            "side": "",
            "X": 0.0,
            "Y": 0.0,
            "Z": 0.0,
            "velocity_X": 0.0,
            "velocity_Y": 0.0,
            "velocity_Z": 0.0,
            "yaw": 0.0,
            "health": 0,
            "is_bomb_planted": False,
            "place": None,
            "active_weapon_name": None,
            "current_equip_value": 0,
            "has_defuser": False,
        },
    )
    if "is_alive" not in ticks.columns:
        ticks = ticks.with_columns((pl.col("health").fill_null(0) > 0).alias("is_alive"))
    return ticks.with_columns(
        [
            pl.col("steamid").cast(pl.Int64, strict=False),
            pl.col("round_num").cast(pl.Int64, strict=False),
            pl.col("tick").cast(pl.Int64, strict=False),
            pl.col("side").cast(pl.Utf8, strict=False).str.to_lowercase(),
        ]
    ).filter(pl.col("round_num").is_not_null())


def _coerce_rounds_for_matching(rounds: pl.DataFrame) -> pl.DataFrame:
    if "end" not in rounds.columns and "official_end" in rounds.columns:
        rounds = rounds.rename({"official_end": "end"})
    rounds = _ensure_columns(
        rounds,
        {
            "round_num": 0,
            "start": 0,
            "freeze_end": 0,
            "end": 0,
            "winner": None,
            "reason": None,
            "bomb_plant": None,
            "bomb_site": None,
        },
    )
    return rounds.with_columns(
        [
            pl.col("round_num").cast(pl.Int64, strict=False),
            pl.col("start").cast(pl.Int64, strict=False),
            pl.col("freeze_end").cast(pl.Int64, strict=False),
            pl.col("end").cast(pl.Int64, strict=False),
        ]
    ).filter(pl.col("round_num").is_not_null())


def _tick_rate_from_record(record: dict[str, Any], header: dict[str, Any]) -> int:
    for value in (
        record.get("tick_rate"),
        header.get("tick_rate"),
        header.get("tickrate"),
        header.get("network_protocol_tickrate"),
    ):
        if value:
            try:
                parsed = int(float(value))
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                return parsed
    return 128


@lru_cache(maxsize=8)
def _load_demo_artifacts_cached(
    parquet_dir_raw: str,
    demo_id: str,
    role: str,
    tick_rate: int,
    map_name: str | None,
) -> DemoArtifacts:
    parquet_dir = Path(config.resolve_managed_path(parquet_dir_raw) or parquet_dir_raw)
    header = _read_header(parquet_dir, demo_id, map_name)
    header.setdefault("map_name", map_name or "unknown")
    frames = {
        name: _read_frame(parquet_dir, demo_id, name, required=name in {"ticks", "rounds"})
        for name in _FRAME_NAMES
    }
    return DemoArtifacts(
        demo_path=Path(demo_id),
        role=role,
        header=header,
        ticks=_coerce_ticks_for_matching(frames["ticks"]),
        rounds=_coerce_rounds_for_matching(frames["rounds"]),
        kills=frames["kills"],
        damages=frames["damages"],
        shots=frames["shots"],
        bomb=frames["bomb"],
        grenades=frames["grenades"],
        demo_id_value=demo_id,
        tick_rate=tick_rate,
    )


def _load_demo_artifacts(record: dict[str, Any], *, role: str) -> DemoArtifacts:
    parquet_dir = record.get("parquet_dir")
    if not parquet_dir:
        raise ValueError(f"game {record.get('source_match_id') or record.get('game_id')} has no parquet_dir")
    demo_id = str(record.get("source_match_id") or record.get("game_id"))
    header = _read_header(Path(config.resolve_managed_path(parquet_dir) or parquet_dir), demo_id, record.get("map_name"))
    tick_rate = _tick_rate_from_record(record, header)
    return _load_demo_artifacts_cached(
        str(parquet_dir),
        demo_id,
        role,
        tick_rate,
        record.get("map_name"),
    )


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _compact_match(
    match: dict[str, Any] | None,
    *,
    logic: str,
    source_record: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if match is None:
        return None
    record = source_record or {}
    if logic == "nav":
        divergence = compute_route_divergence_timeline(match.get("window_scores") or [])
    else:
        divergence = compute_round_divergence_timeline(match.get("window_scores") or [])

    payload: dict[str, Any] = {
        "logic": logic,
        "source_match_id": str(match["match_demo_id"]),
        "round_num": int(match["match_round_num"]),
        "score": float(match["round_score"]),
        "matched_pro_steamid": int(match["matched_pro_steamid"]),
        "matched_pro_player": str(match["matched_pro_player"]),
        "map_name": record.get("map_name"),
        "event_name": record.get("event_name"),
        "team1_name": record.get("team1_name"),
        "team2_name": record.get("team2_name"),
        "team_ct": record.get("team_ct"),
        "team_t": record.get("team_t"),
        "match_date": _iso_date(record.get("match_date")),
        "coverage": float(match.get("coverage", 0.0)),
        "longest_streak": int(match.get("longest_streak", 0)),
        "window_count": int(match.get("window_count", 0)),
        "mean_round_similarity": float(match.get("mean_round_similarity", 0.0)),
        "mean_non_user_alignment": float(match.get("mean_non_user_alignment", 0.0)),
        "mean_context_match": float(match.get("mean_context_match", 0.0)),
        "mean_user_pro_difference": float(match.get("mean_user_pro_difference", 0.0)),
        "divergence_start_sec": divergence.get("divergence_start_sec"),
        "divergence_start_tick": divergence.get("divergence_start_tick"),
        "divergence_end_sec": divergence.get("divergence_end_sec"),
        "divergence_timeline": divergence.get("timeline", []),
    }
    for key in (
        "shared_route_steps",
        "matched_prefix_duration_sec",
        "break_event_label",
        "break_event_type",
        "break_time_sec",
        "route_timing_alignment",
        "local_context_alignment",
        "bomb_state_alignment",
        "prefix_score",
        "coach_value",
        "survival_gap_sec",
    ):
        if key in match:
            value = match[key]
            payload[key] = int(value) if key == "shared_route_steps" else value
    return json.loads(json.dumps(payload, default=_json_default))


async def _load_same_map_pro_artifacts(map_name: str) -> tuple[list[DemoArtifacts], dict[str, dict]]:
    matches = await db.get_pro_matches()
    records = {
        str(row["match_id"]): row
        for row in matches
        if row.get("map_name") == map_name and row.get("parquet_dir")
    }
    artifacts = [
        _load_demo_artifacts(
            {
                **record,
                "source_match_id": match_id,
                "game_id": match_id,
            },
            role="pro",
        )
        for match_id, record in records.items()
    ]
    return artifacts, records


async def map_user_round_to_pro_round(
    demo_id: str,
    round_num: int,
) -> dict | None:
    """Return original and nav matches for one user round, plus the higher-score best match."""
    user_record = await db.get_match_source_record(demo_id)
    if user_record is None or not user_record.get("parquet_dir"):
        return None

    user_artifact = _load_demo_artifacts(user_record, role="user")
    pro_artifacts, pro_records = await _load_same_map_pro_artifacts(user_artifact.map_name)
    if not pro_artifacts:
        return {
            "query": {"demo_id": demo_id, "round_num": round_num},
            "best_match": None,
            "original": None,
            "nav": None,
        }

    steam_id = user_record.get("steam_id")
    if steam_id is None:
        user_player = identify_user_player(user_artifact)
        steam_id = user_player["steamid"]
    user_steamid = int(steam_id)

    original_raw = select_best_pro_round_match(
        user_artifact,
        pro_artifacts,
        user_steamid=user_steamid,
        user_round_num=round_num,
    )
    if original_raw is not None:
        original_raw["logic"] = "original"
        original_raw["user_steamid"] = user_steamid

    nav_raw = select_best_pro_route_match(
        user_artifact,
        pro_artifacts,
        user_steamid=user_steamid,
        user_round_num=round_num,
    )
    if nav_raw is not None:
        nav_raw["user_steamid"] = user_steamid

    needed_ids = {
        str(raw["match_demo_id"])
        for raw in (original_raw, nav_raw)
        if raw is not None
    }
    records = {
        match_id: {
            **pro_records.get(match_id, {}),
            **(await db.get_match_source_record(match_id) or {}),
        }
        for match_id in needed_ids
    }

    original = _compact_match(
        original_raw,
        logic="original",
        source_record=records.get(str(original_raw["match_demo_id"])) if original_raw else None,
    )
    nav = _compact_match(
        nav_raw,
        logic="nav",
        source_record=records.get(str(nav_raw["match_demo_id"])) if nav_raw else None,
    )
    candidates = [item for item in (original, nav) if item is not None]
    best_match = max(candidates, key=lambda item: float(item.get("score") or 0.0)) if candidates else None

    return {
        "query": {"demo_id": demo_id, "round_num": round_num},
        "best_match": best_match,
        "original": original,
        "nav": nav,
    }


# ---------------------------------------------------------------------------
# Route-based matching using AWPy nav meshes.
# ---------------------------------------------------------------------------

def _load_nav_mesh(map_name: str) -> awpy.nav.Nav | None:
    """Load one map nav mesh from the local AWPy data directory."""
    cached = _NAV_CACHE.get(map_name)
    if cached is not None or map_name in _NAV_CACHE:
        return cached

    nav_path = awpy.data.NAVS_DIR / f"{map_name}.json"
    if not nav_path.exists():
        _NAV_CACHE[map_name] = None
        return None

    nav_mesh = awpy.nav.Nav.from_json(nav_path)
    _NAV_CACHE[map_name] = nav_mesh
    return nav_mesh


def _nav_area_records(map_name: str) -> list[dict[str, Any]]:
    """Prepare lightweight nav-area records for point lookup."""
    if map_name in _NAV_AREA_CACHE:
        return _NAV_AREA_CACHE[map_name]

    nav_mesh = _load_nav_mesh(map_name)
    if nav_mesh is None:
        _NAV_AREA_CACHE[map_name] = []
        return []

    records: list[dict[str, Any]] = []
    grid: dict[tuple[int, int], list[int]] = {}
    cell_size = 512.0
    for area in nav_mesh.areas.values():
        points_xy = [(float(c.x), float(c.y)) for c in area.corners]
        if not points_xy:
            continue
        xs = [point[0] for point in points_xy]
        ys = [point[1] for point in points_xy]
        z_mean = float(sum(float(c.z) for c in area.corners) / len(area.corners))
        centroid = area.centroid
        record = (
            {
                "area_id": int(area.area_id),
                "points_xy": points_xy,
                "min_x": min(xs),
                "max_x": max(xs),
                "min_y": min(ys),
                "max_y": max(ys),
                "z_mean": z_mean,
                "centroid_x": float(centroid.x),
                "centroid_y": float(centroid.y),
            }
        )
        record_index = len(records)
        records.append(record)
        min_cell_x = int(math.floor(record["min_x"] / cell_size))
        max_cell_x = int(math.floor(record["max_x"] / cell_size))
        min_cell_y = int(math.floor(record["min_y"] / cell_size))
        max_cell_y = int(math.floor(record["max_y"] / cell_size))
        for cell_x in range(min_cell_x, max_cell_x + 1):
            for cell_y in range(min_cell_y, max_cell_y + 1):
                grid.setdefault((cell_x, cell_y), []).append(record_index)

    _NAV_AREA_CACHE[map_name] = records
    _NAV_GRID_CACHE[map_name] = grid
    return records


def _point_in_polygon_xy(x: float, y: float, polygon: Sequence[tuple[float, float]]) -> bool:
    """Return True when a 2D point lies inside a polygon."""
    inside = False
    n = len(polygon)
    if n < 3:
        return False

    for i in range(n):
        x1, y1 = polygon[i]
        x2, y2 = polygon[(i + 1) % n]
        intersects = ((y1 > y) != (y2 > y))
        if not intersects:
            continue
        slope_x = (x2 - x1) * (y - y1) / ((y2 - y1) or 1e-9) + x1
        if x < slope_x:
            inside = not inside
    return inside


def _lookup_nav_area_id(
    map_name: str,
    x: float,
    y: float,
    z: float,
    *,
    z_tolerance: float = 130.0,
) -> int | None:
    """Map a world position to the closest containing nav area."""
    cache_key = (
        map_name,
        int(round(x / 16.0)),
        int(round(y / 16.0)),
        int(round(z / 32.0)),
    )
    if cache_key in _NAV_POINT_CACHE:
        return _NAV_POINT_CACHE[cache_key]

    best_id: int | None = None
    best_distance = float("inf")
    fallback_id: int | None = None
    fallback_distance = float("inf")
    records = _nav_area_records(map_name)
    grid = _NAV_GRID_CACHE.get(map_name, {})
    cell_size = 512.0
    center_cell = (int(math.floor(x / cell_size)), int(math.floor(y / cell_size)))
    candidate_indexes: set[int] = set()
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            candidate_indexes.update(grid.get((center_cell[0] + dx, center_cell[1] + dy), []))
    if not candidate_indexes:
        candidate_indexes = set(range(len(records)))

    for index in candidate_indexes:
        area = records[index]
        if x < area["min_x"] - 8.0 or x > area["max_x"] + 8.0:
            continue
        if y < area["min_y"] - 8.0 or y > area["max_y"] + 8.0:
            continue

        z_gap = abs(z - area["z_mean"])
        centroid_distance = _distance_xy((x, y), (area["centroid_x"], area["centroid_y"]))
        if z_gap <= z_tolerance and centroid_distance < fallback_distance:
            fallback_distance = centroid_distance
            fallback_id = int(area["area_id"])

        if z_gap > z_tolerance:
            continue
        if not _point_in_polygon_xy(x, y, area["points_xy"]):
            continue
        if centroid_distance < best_distance:
            best_distance = centroid_distance
            best_id = int(area["area_id"])

    resolved = best_id if best_id is not None else fallback_id
    _NAV_POINT_CACHE[cache_key] = resolved
    return resolved


def _sample_round_ticks(
    artifacts: DemoArtifacts,
    round_num: int,
    *,
    sample_step_sec: float,
) -> list[tuple[int, float, pl.DataFrame]]:
    """Return sampled round snapshots as `(tick, t_sec, snapshot)` rows."""
    ticks = annotate_ticks_with_clock(artifacts)
    round_ticks = ticks.filter(pl.col("round_num") == round_num).sort("tick")
    if round_ticks.is_empty():
        return []

    round_row = artifacts.rounds.filter(pl.col("round_num") == round_num)
    if round_row.is_empty():
        return []
    meta = round_row.row(0, named=True)

    freeze_end_tick = _round_freeze_end_tick(meta)
    end_tick = _round_tick_value(meta, "end", freeze_end_tick)
    available_ticks = np.array(sorted(set(int(v) for v in round_ticks["tick"].to_list())), dtype=np.int32)
    if available_ticks.size == 0:
        return []

    step_ticks = max(1, int(round(sample_step_sec * artifacts.tickrate)))
    target_ticks = list(range(freeze_end_tick, end_tick + 1, step_ticks))
    if not target_ticks or target_ticks[-1] != end_tick:
        target_ticks.append(end_tick)

    sampled_ticks: list[int] = []
    seen: set[int] = set()
    for target_tick in target_ticks:
        sampled = _nearest_tick(int(target_tick), available_ticks)
        if sampled in seen:
            continue
        seen.add(sampled)
        sampled_ticks.append(sampled)

    snapshots_raw = round_ticks.partition_by("tick", as_dict=True)
    snapshots = {
        int(key[0] if isinstance(key, tuple) else key): value
        for key, value in snapshots_raw.items()
    }

    sampled_rows: list[tuple[int, float, pl.DataFrame]] = []
    for tick in sampled_ticks:
        snapshot = snapshots.get(int(tick))
        if snapshot is None:
            continue
        t_sec = (int(tick) - freeze_end_tick) / artifacts.tickrate
        sampled_rows.append((int(tick), float(t_sec), snapshot))
    return sampled_rows


def _teammate_support_count(
    snapshot: pl.DataFrame,
    *,
    steamid: int,
    side: str,
    teammate_radius: float,
) -> int:
    """Count nearby alive teammates for one player inside one snapshot."""
    player_row = snapshot.filter(pl.col("steamid") == steamid)
    if player_row.is_empty() or not bool(player_row["is_alive"][0]):
        return 0

    origin = (float(player_row["X"][0]), float(player_row["Y"][0]))
    teammates = snapshot.filter(
        (pl.col("side") == side)
        & pl.col("is_alive")
        & (pl.col("steamid") != steamid)
    )
    count = 0
    for teammate in teammates.iter_rows(named=True):
        dist = _distance_xy(origin, (float(teammate["X"]), float(teammate["Y"])))
        if dist <= teammate_radius:
            count += 1
    return count


def _enemy_pressure_count(
    snapshot: pl.DataFrame,
    *,
    steamid: int,
    side: str,
    enemy_radius: float,
) -> int:
    """Count nearby alive enemies for one player inside one snapshot."""
    player_row = snapshot.filter(pl.col("steamid") == steamid)
    if player_row.is_empty() or not bool(player_row["is_alive"][0]):
        return 0

    origin = (float(player_row["X"][0]), float(player_row["Y"][0]))
    enemies = snapshot.filter(
        (pl.col("side") != side)
        & pl.col("is_alive")
    )
    count = 0
    for enemy in enemies.iter_rows(named=True):
        dist = _distance_xy(origin, (float(enemy["X"]), float(enemy["Y"])))
        if dist <= enemy_radius:
            count += 1
    return count


def _nearby_teammate_deaths(
    previous_snapshot: pl.DataFrame,
    current_snapshot: pl.DataFrame,
    *,
    steamid: int,
    side: str,
    teammate_radius: float,
) -> int:
    """Count nearby teammates who were alive and died between two sampled snapshots."""
    player_row = previous_snapshot.filter(pl.col("steamid") == steamid)
    if player_row.is_empty() or not bool(player_row["is_alive"][0]):
        return 0

    origin = (float(player_row["X"][0]), float(player_row["Y"][0]))
    teammates = previous_snapshot.filter(
        (pl.col("side") == side)
        & pl.col("is_alive")
        & (pl.col("steamid") != steamid)
    )
    death_count = 0
    for teammate in teammates.iter_rows(named=True):
        dist = _distance_xy(origin, (float(teammate["X"]), float(teammate["Y"])))
        if dist > teammate_radius:
            continue
        current_row = current_snapshot.filter(pl.col("steamid") == int(teammate["steamid"]))
        if current_row.is_empty():
            continue
        if not bool(current_row["is_alive"][0]):
            death_count += 1
    return death_count


def _infer_bomb_planted(snapshot: pl.DataFrame, tick: int) -> bool:
    """Infer whether the bomb is planted at one sampled tick."""
    if "bomb_plant" not in snapshot.columns or snapshot.is_empty():
        return False

    plant_tick = snapshot["bomb_plant"][0]
    if plant_tick is None:
        return False
    if isinstance(plant_tick, float) and math.isnan(plant_tick):
        return False
    return int(tick) >= int(plant_tick)


def _collapse_route_steps(
    sample_times: Sequence[float],
    area_ids: Sequence[int | None],
    support_counts: Sequence[int],
    enemy_counts: Sequence[int],
    bomb_planted_flags: Sequence[bool],
) -> list[dict[str, Any]]:
    """Collapse repeated nav areas into route steps."""
    steps: list[dict[str, Any]] = []
    for idx, area_id in enumerate(area_ids):
        if area_id is None:
            continue
        t_sec = float(sample_times[idx])
        support_count = int(support_counts[idx])
        enemy_count = int(enemy_counts[idx])
        bomb_planted = bool(bomb_planted_flags[idx])
        if steps and steps[-1]["area_id"] == int(area_id):
            steps[-1]["end_sec"] = t_sec
            steps[-1]["support_values"].append(support_count)
            steps[-1]["enemy_values"].append(enemy_count)
            steps[-1]["bomb_values"].append(1.0 if bomb_planted else 0.0)
            continue
        steps.append(
            {
                "area_id": int(area_id),
                "start_sec": t_sec,
                "end_sec": t_sec,
                "support_values": [support_count],
                "enemy_values": [enemy_count],
                "bomb_values": [1.0 if bomb_planted else 0.0],
            }
        )

    for step in steps:
        support_values = step.pop("support_values")
        enemy_values = step.pop("enemy_values")
        bomb_values = step.pop("bomb_values")
        step["support_count"] = float(np.mean(support_values)) if support_values else 0.0
        step["enemy_count"] = float(np.mean(enemy_values)) if enemy_values else 0.0
        step["bomb_planted"] = float(np.mean(bomb_values)) >= 0.5 if bomb_values else False
    return steps


def _build_round_route_profiles(
    artifacts: DemoArtifacts,
    round_num: int,
    *,
    sample_step_sec: float = 1.0,
    teammate_radius: float = 900.0,
    enemy_radius: float = 1000.0,
) -> list[dict[str, Any]]:
    """Build route profiles for every player in one round."""
    cache_key = (
        artifacts.demo_id,
        int(round_num),
        float(sample_step_sec),
        float(teammate_radius),
        float(enemy_radius),
    )
    if cache_key in _ROUND_ROUTE_PROFILE_CACHE:
        return _ROUND_ROUTE_PROFILE_CACHE[cache_key]

    round_samples = _sample_round_ticks(artifacts, round_num, sample_step_sec=sample_step_sec)
    if not round_samples:
        _ROUND_ROUTE_PROFILE_CACHE[cache_key] = []
        return []

    round_row = artifacts.rounds.filter(pl.col("round_num") == round_num)
    if round_row.is_empty():
        _ROUND_ROUTE_PROFILE_CACHE[cache_key] = []
        return []
    round_meta = round_row.row(0, named=True)

    player_meta = (
        annotate_ticks_with_clock(artifacts)
        .filter(pl.col("round_num") == round_num)
        .select("steamid", "name", "side")
        .unique()
    )
    if player_meta.is_empty():
        _ROUND_ROUTE_PROFILE_CACHE[cache_key] = []
        return []

    profiles: list[dict[str, Any]] = []
    for player in player_meta.iter_rows(named=True):
        steamid = int(player["steamid"])
        name = str(player["name"])
        side = str(player["side"])
        sample_times: list[float] = []
        sample_ticks: list[int] = []
        area_ids: list[int | None] = []
        support_counts: list[int] = []
        enemy_counts: list[int] = []
        nearby_teammate_deaths: list[int] = []
        bomb_planted_flags: list[bool] = []
        alive_flags: list[bool] = []
        previous_snapshot: pl.DataFrame | None = None

        for tick, t_sec, snapshot in round_samples:
            player_row = snapshot.filter(pl.col("steamid") == steamid)
            if player_row.is_empty():
                previous_snapshot = snapshot
                continue
            alive = bool(player_row["is_alive"][0])
            area_id: int | None = None
            if alive:
                area_id = _lookup_nav_area_id(
                    artifacts.map_name,
                    float(player_row["X"][0]),
                    float(player_row["Y"][0]),
                    float(player_row["Z"][0]),
                )
            sample_times.append(float(t_sec))
            sample_ticks.append(int(tick))
            area_ids.append(area_id)
            support_counts.append(
                _teammate_support_count(
                    snapshot,
                    steamid=steamid,
                    side=side,
                    teammate_radius=teammate_radius,
                )
            )
            enemy_counts.append(
                _enemy_pressure_count(
                    snapshot,
                    steamid=steamid,
                    side=side,
                    enemy_radius=enemy_radius,
                )
            )
            nearby_teammate_deaths.append(
                0 if previous_snapshot is None else _nearby_teammate_deaths(
                    previous_snapshot,
                    snapshot,
                    steamid=steamid,
                    side=side,
                    teammate_radius=teammate_radius,
                )
            )
            bomb_planted_flags.append(_infer_bomb_planted(snapshot, int(tick)))
            alive_flags.append(alive)
            previous_snapshot = snapshot

        route_steps = _collapse_route_steps(
            sample_times,
            area_ids,
            support_counts,
            enemy_counts,
            bomb_planted_flags,
        )
        if not route_steps:
            continue

        death_sec: float | None = None
        seen_alive = False
        for t_sec, alive in zip(sample_times, alive_flags, strict=False):
            seen_alive = seen_alive or alive
            if seen_alive and not alive:
                death_sec = float(t_sec)
                break

        profiles.append(
            {
                "demo_id": artifacts.demo_id,
                "demo_path": str(artifacts.demo_path.resolve()),
                "map_name": artifacts.map_name,
                "round_num": int(round_num),
                "steamid": steamid,
                "name": name,
                "side": side,
                "sample_times": sample_times,
                "sample_ticks": sample_ticks,
                "area_ids": area_ids,
                "support_counts": support_counts,
                "enemy_counts": enemy_counts,
                "nearby_teammate_deaths": nearby_teammate_deaths,
                "bomb_planted_flags": bomb_planted_flags,
                "local_balance": [
                    int(support) - int(enemy)
                    for support, enemy in zip(support_counts, enemy_counts, strict=False)
                ],
                "alive_flags": alive_flags,
                "route_steps": route_steps,
                "route_step_count": len(route_steps),
                "start_area_id": int(route_steps[0]["area_id"]),
                "death_sec": death_sec,
                "survived_round": bool(alive_flags[-1]) if alive_flags else False,
                "team_won": str(round_meta["winner"]) == side,
                "round_winner": str(round_meta["winner"]),
                "round_reason": str(round_meta["reason"]),
                "round_end_sec": float(sample_times[-1]) if sample_times else 0.0,
            }
        )

    _ROUND_ROUTE_PROFILE_CACHE[cache_key] = profiles
    return profiles


def _nav_area_similarity(
    nav_mesh: awpy.nav.Nav,
    left_area_id: int | None,
    right_area_id: int | None,
) -> float:
    """Return a soft 0..1 similarity between two nav areas."""
    if left_area_id is None or right_area_id is None:
        return 0.0
    if int(left_area_id) == int(right_area_id):
        return 1.0

    left_area = nav_mesh.areas.get(int(left_area_id))
    right_area = nav_mesh.areas.get(int(right_area_id))
    if left_area is None or right_area is None:
        return 0.0
    if int(right_area_id) in left_area.connected_areas or int(left_area_id) in right_area.connected_areas:
        return 0.82

    centroid_gap = _distance_xy(
        (float(left_area.centroid.x), float(left_area.centroid.y)),
        (float(right_area.centroid.x), float(right_area.centroid.y)),
    )
    if centroid_gap <= 250.0:
        return 0.75
    if centroid_gap <= 500.0:
        return 0.55
    if centroid_gap <= 900.0:
        return 0.30
    return 0.0


def _shared_route_prefix(
    user_profile: dict[str, Any],
    pro_profile: dict[str, Any],
    *,
    nav_mesh: awpy.nav.Nav,
    min_step_similarity: float = 0.60,
    max_timing_gap_sec: float = 5.0,
    min_context_alignment: float = 0.38,
) -> dict[str, float]:
    """Measure how long two players follow the same route prefix."""
    user_times = user_profile["sample_times"]
    pro_times = pro_profile["sample_times"]
    n = min(len(user_times), len(pro_times))
    if n == 0:
        return {
            "shared_steps": 0.0,
            "prefix_similarity": 0.0,
            "timing_alignment": 0.0,
            "support_alignment": 0.0,
            "local_context_alignment": 0.0,
            "bomb_state_alignment": 0.0,
            "prefix_duration_sec": 0.0,
            "user_prefix_end_sec": 0.0,
            "pro_prefix_end_sec": 0.0,
            "matched_sample_count": 0.0,
            "break_index": 0.0,
        }

    matched: list[dict[str, float]] = []
    break_index = n
    for idx in range(n):
        timing_gap = abs(float(user_times[idx]) - float(pro_times[idx]))
        step_similarity = _nav_area_similarity(
            nav_mesh,
            user_profile["area_ids"][idx],
            pro_profile["area_ids"][idx],
        )
        support_alignment = 1.0 / (
            1.0
            + abs(float(user_profile["support_counts"][idx]) - float(pro_profile["support_counts"][idx]))
        )
        enemy_alignment = 1.0 / (
            1.0
            + abs(float(user_profile["enemy_counts"][idx]) - float(pro_profile["enemy_counts"][idx]))
        )
        bomb_alignment = 1.0 if (
            bool(user_profile["bomb_planted_flags"][idx]) == bool(pro_profile["bomb_planted_flags"][idx])
        ) else 0.0
        local_context_alignment = (
            0.40 * support_alignment
            + 0.35 * enemy_alignment
            + 0.25 * bomb_alignment
        )
        if (
            step_similarity < min_step_similarity
            or timing_gap > max_timing_gap_sec
            or local_context_alignment < min_context_alignment
        ):
            break_index = idx
            break
        matched.append(
            {
                "index": float(idx),
                "step_similarity": step_similarity,
                "timing_gap": timing_gap,
                "support_alignment": support_alignment,
                "enemy_alignment": enemy_alignment,
                "bomb_alignment": bomb_alignment,
                "local_context_alignment": local_context_alignment,
            }
        )

    if not matched:
        return {
            "shared_steps": 0.0,
            "prefix_similarity": 0.0,
            "timing_alignment": 0.0,
            "support_alignment": 0.0,
            "local_context_alignment": 0.0,
            "bomb_state_alignment": 0.0,
            "prefix_duration_sec": 0.0,
            "user_prefix_end_sec": 0.0,
            "pro_prefix_end_sec": 0.0,
            "matched_sample_count": 0.0,
            "break_index": float(break_index),
        }

    last_index = int(matched[-1]["index"])
    user_prefix_end_sec = float(user_times[last_index])
    pro_prefix_end_sec = float(pro_times[last_index])
    return {
        "shared_steps": float(
            min(
                _count_route_steps_through(user_profile["route_steps"], user_prefix_end_sec),
                _count_route_steps_through(pro_profile["route_steps"], pro_prefix_end_sec),
            )
        ),
        "prefix_similarity": float(np.mean([row["step_similarity"] for row in matched])),
        "timing_alignment": 1.0 / (1.0 + float(np.mean([row["timing_gap"] for row in matched])) / 4.0),
        "support_alignment": float(np.mean([row["support_alignment"] for row in matched])),
        "local_context_alignment": float(np.mean([row["local_context_alignment"] for row in matched])),
        "bomb_state_alignment": float(np.mean([row["bomb_alignment"] for row in matched])),
        "prefix_duration_sec": float(min(user_prefix_end_sec, pro_prefix_end_sec)),
        "user_prefix_end_sec": user_prefix_end_sec,
        "pro_prefix_end_sec": pro_prefix_end_sec,
        "matched_sample_count": float(len(matched)),
        "break_index": float(break_index),
    }


def _count_route_steps_through(route_steps: Sequence[dict[str, Any]], t_sec: float) -> int:
    """Count collapsed route steps reached by time ``t_sec``."""
    count = 0
    for step in route_steps:
        if float(step["start_sec"]) <= float(t_sec) + 1e-6:
            count += 1
    return count


def _post_break_local_conversion(
    user_profile: dict[str, Any],
    pro_profile: dict[str, Any],
    *,
    break_index: int,
    lookahead_samples: int = 3,
) -> float:
    """Estimate whether the pro's local group converts better after the shared prefix."""
    n = min(len(user_profile["sample_times"]), len(pro_profile["sample_times"]))
    if n == 0:
        return 0.0

    start = max(0, min(int(break_index), n - 1))
    stop = min(n, start + int(lookahead_samples))

    def _segment_mean(values: Sequence[int]) -> float:
        segment = [float(v) for v in values[start:stop]]
        return float(np.mean(segment)) if segment else 0.0

    user_local_score = (
        _segment_mean(user_profile["support_counts"])
        - _segment_mean(user_profile["enemy_counts"])
        - 0.75 * _segment_mean(user_profile["nearby_teammate_deaths"])
    )
    pro_local_score = (
        _segment_mean(pro_profile["support_counts"])
        - _segment_mean(pro_profile["enemy_counts"])
        - 0.75 * _segment_mean(pro_profile["nearby_teammate_deaths"])
    )
    return float(np.clip((pro_local_score - user_local_score + 2.0) / 4.0, 0.0, 1.0))


def _classify_route_break(
    user_profile: dict[str, Any],
    pro_profile: dict[str, Any],
    *,
    nav_mesh: awpy.nav.Nav,
    prefix: dict[str, float],
) -> dict[str, Any]:
    """Classify the first meaningful break after the shared nav-mesh prefix."""
    labels = {
        "route_deviation": "route deviation",
        "user_death": "user death",
        "local_teammate_collapse": "local teammate collapse",
        "bomb_state_divergence": "bomb-state divergence",
        "round_outcome_collapse": "round outcome collapse",
        "local_context_divergence": "local context divergence",
        "shared_prefix_complete": "shared path complete",
    }

    n = min(len(user_profile["sample_times"]), len(pro_profile["sample_times"]))
    if n == 0:
        return {
            "break_event_type": "shared_prefix_complete",
            "break_event_label": labels["shared_prefix_complete"],
            "break_time_sec": 0.0,
            "survival_gap_sec": 0.0,
        }

    break_index = min(max(int(prefix.get("break_index", n)), 0), n)
    user_survival_end = (
        float(user_profile["death_sec"])
        if user_profile["death_sec"] is not None
        else float(user_profile["round_end_sec"])
    )
    pro_survival_end = (
        float(pro_profile["death_sec"])
        if pro_profile["death_sec"] is not None
        else float(pro_profile["round_end_sec"])
    )
    survival_gap_sec = max(0.0, pro_survival_end - user_survival_end)

    if break_index >= n:
        event_type = "shared_prefix_complete"
        break_time_sec = float(prefix.get("prefix_duration_sec", min(user_survival_end, pro_survival_end)))
        if not bool(user_profile["team_won"]) and bool(pro_profile["team_won"]):
            event_type = "round_outcome_collapse"
        elif survival_gap_sec > 0.5 and user_profile["death_sec"] is not None:
            event_type = "user_death"
            break_time_sec = float(user_profile["death_sec"])
        return {
            "break_event_type": event_type,
            "break_event_label": labels[event_type],
            "break_time_sec": float(break_time_sec),
            "survival_gap_sec": float(survival_gap_sec),
        }

    break_time_sec = float(user_profile["sample_times"][break_index])
    user_alive = bool(user_profile["alive_flags"][break_index])
    pro_alive = bool(pro_profile["alive_flags"][break_index])
    bomb_mismatch = bool(user_profile["bomb_planted_flags"][break_index]) != bool(
        pro_profile["bomb_planted_flags"][break_index]
    )
    user_local_collapse = int(user_profile["nearby_teammate_deaths"][break_index]) > int(
        pro_profile["nearby_teammate_deaths"][break_index]
    ) and float(user_profile["local_balance"][break_index]) < float(pro_profile["local_balance"][break_index])
    route_similarity = _nav_area_similarity(
        nav_mesh,
        user_profile["area_ids"][break_index],
        pro_profile["area_ids"][break_index],
    )

    if (not user_alive) and pro_alive:
        event_type = "user_death"
    elif user_local_collapse:
        event_type = "local_teammate_collapse"
    elif bomb_mismatch:
        event_type = "bomb_state_divergence"
    elif route_similarity < 0.60:
        event_type = "route_deviation"
    elif not bool(user_profile["team_won"]) and bool(pro_profile["team_won"]):
        event_type = "round_outcome_collapse"
    else:
        event_type = "local_context_divergence"

    return {
        "break_event_type": event_type,
        "break_event_label": labels[event_type],
        "break_time_sec": break_time_sec,
        "survival_gap_sec": float(survival_gap_sec),
    }


def _build_route_window_scores(
    user_profile: dict[str, Any],
    pro_profile: dict[str, Any],
    *,
    nav_mesh: awpy.nav.Nav,
) -> list[dict[str, Any]]:
    """Build per-sample route comparison rows for divergence reporting."""
    user_times = user_profile["sample_times"]
    pro_times = pro_profile["sample_times"]
    n = min(len(user_times), len(pro_times))
    if n == 0:
        return []

    window_scores: list[dict[str, Any]] = []
    for idx in range(n):
        user_start = float(user_times[idx])
        user_end = float(user_times[idx + 1]) if idx + 1 < n else float(user_profile["round_end_sec"])
        pro_start = float(pro_times[idx])
        pro_end = float(pro_times[idx + 1]) if idx + 1 < n else float(pro_profile["round_end_sec"])

        route_similarity = _nav_area_similarity(
            nav_mesh,
            user_profile["area_ids"][idx],
            pro_profile["area_ids"][idx],
        )
        route_difference = 1.0 - route_similarity
        enemy_difference = min(
            1.0,
            abs(float(user_profile["enemy_counts"][idx]) - float(pro_profile["enemy_counts"][idx])) / 3.0,
        )
        support_difference = min(
            1.0,
            abs(float(user_profile["support_counts"][idx]) - float(pro_profile["support_counts"][idx])) / 3.0,
        )
        bomb_state_difference = float(
            bool(user_profile["bomb_planted_flags"][idx]) != bool(pro_profile["bomb_planted_flags"][idx])
        )
        local_collapse_difference = min(
            1.0,
            max(
                0.0,
                float(user_profile["nearby_teammate_deaths"][idx])
                - float(pro_profile["nearby_teammate_deaths"][idx]),
            ),
        )
        support_alignment = 1.0 / (
            1.0
            + abs(float(user_profile["support_counts"][idx]) - float(pro_profile["support_counts"][idx]))
        )
        enemy_alignment = 1.0 / (
            1.0
            + abs(float(user_profile["enemy_counts"][idx]) - float(pro_profile["enemy_counts"][idx]))
        )
        local_context_alignment = (
            0.40 * support_alignment
            + 0.35 * enemy_alignment
            + 0.25 * (1.0 - bomb_state_difference)
        )
        local_context_difference = 1.0 - local_context_alignment
        survival_difference = float(
            (not bool(user_profile["alive_flags"][idx])) and bool(pro_profile["alive_flags"][idx])
        )
        user_pro_difference = min(
            1.0,
            0.45 * route_difference
            + 0.20 * local_context_difference
            + 0.15 * local_collapse_difference
            + 0.10 * bomb_state_difference
            + 0.10 * survival_difference,
        )
        non_user_alignment = max(
            0.0,
            1.0
            - (
                0.50 * local_context_difference
                + 0.25 * local_collapse_difference
                + 0.25 * bomb_state_difference
            ),
        )

        window_scores.append(
            {
                "user_round_num": int(user_profile["round_num"]),
                "user_window_start_tick": int(user_profile["sample_ticks"][idx]),
                "user_window_end_tick": int(user_profile["sample_ticks"][idx]),
                "user_window_start_sec_from_freeze": user_start,
                "user_window_end_sec_from_freeze": max(user_start, user_end),
                "user_phase": "route",
                "user_side": str(user_profile["side"]),
                "match_demo_id": str(pro_profile["demo_id"]),
                "match_round_num": int(pro_profile["round_num"]),
                "match_window_start_tick": int(pro_profile["sample_ticks"][idx]),
                "match_window_end_tick": int(pro_profile["sample_ticks"][idx]),
                "match_window_start_sec_from_freeze": pro_start,
                "match_window_end_sec_from_freeze": max(pro_start, pro_end),
                "matched_pro_steamid": int(pro_profile["steamid"]),
                "matched_pro_player": str(pro_profile["name"]),
                "context_match": 0.60 * route_similarity + 0.40 * local_context_alignment,
                "context_distance": route_difference,
                "non_user_alignment": non_user_alignment,
                "non_user_cost": 1.0 - non_user_alignment,
                "user_pro_difference": user_pro_difference,
                "user_difference_cost": user_pro_difference,
                "user_alignment": 1.0 - user_pro_difference,
                "round_similarity": max(0.0, 1.0 - user_pro_difference) * local_context_alignment,
                "route_difference": route_difference,
                "support_difference": support_difference,
                "enemy_difference": enemy_difference,
                "bomb_state_difference": bomb_state_difference,
                "local_context_alignment": local_context_alignment,
                "local_context_difference": local_context_difference,
                "local_collapse_difference": local_collapse_difference,
                "survival_difference": survival_difference,
            }
        )

    return window_scores


def select_best_pro_route_match(
    user_artifact: DemoArtifacts,
    pro_artifacts: Sequence[DemoArtifacts],
    *,
    user_steamid: int,
    user_round_num: int,
    sample_step_sec: float = 1.0,
    teammate_radius: float = 900.0,
    enemy_radius: float = 1000.0,
    min_shared_steps: int = 2,
) -> dict[str, Any] | None:
    """Find a pro match by shared nav-mesh prefix plus local-context guard rails."""
    nav_mesh = _load_nav_mesh(user_artifact.map_name)
    if nav_mesh is None:
        return None

    user_profiles = _build_round_route_profiles(
        user_artifact,
        user_round_num,
        sample_step_sec=sample_step_sec,
        teammate_radius=teammate_radius,
        enemy_radius=enemy_radius,
    )
    user_profile = next((p for p in user_profiles if int(p["steamid"]) == int(user_steamid)), None)
    if user_profile is None:
        return None

    all_rankings: list[dict[str, Any]] = []
    for pro_artifact in pro_artifacts:
        round_nums = [
            int(value)
            for value in pro_artifact.rounds["round_num"].drop_nulls().to_list()
        ]
        for round_num in round_nums:
            pro_profiles = _build_round_route_profiles(
                pro_artifact,
                round_num,
                sample_step_sec=sample_step_sec,
                teammate_radius=teammate_radius,
                enemy_radius=enemy_radius,
            )
            for pro_profile in pro_profiles:
                if str(pro_profile["side"]) != str(user_profile["side"]):
                    continue

                start_similarity = _nav_area_similarity(
                    nav_mesh,
                    user_profile["start_area_id"],
                    pro_profile["start_area_id"],
                )
                if start_similarity < 0.55:
                    continue

                start_support_alignment = 1.0 / (
                    1.0
                    + abs(float(user_profile["support_counts"][0]) - float(pro_profile["support_counts"][0]))
                )
                start_enemy_alignment = 1.0 / (
                    1.0
                    + abs(float(user_profile["enemy_counts"][0]) - float(pro_profile["enemy_counts"][0]))
                )
                start_context_alignment = 0.55 * start_support_alignment + 0.45 * start_enemy_alignment
                if start_context_alignment < 0.34:
                    continue
                if bool(user_profile["bomb_planted_flags"][0]) != bool(pro_profile["bomb_planted_flags"][0]):
                    continue

                prefix = _shared_route_prefix(user_profile, pro_profile, nav_mesh=nav_mesh)
                shared_steps = int(prefix["shared_steps"])
                if shared_steps < int(min_shared_steps):
                    continue

                break_event = _classify_route_break(
                    user_profile,
                    pro_profile,
                    nav_mesh=nav_mesh,
                    prefix=prefix,
                )
                prefix_duration_norm = min(
                    1.0,
                    float(prefix["prefix_duration_sec"]) / max(float(user_profile["round_end_sec"]), 1.0),
                )
                shared_steps_norm = min(
                    1.0,
                    float(shared_steps) / max(float(user_profile["route_step_count"]), 1.0),
                )
                survived_longer = min(1.0, float(break_event["survival_gap_sec"]) / 12.0)
                round_outcome_advantage = 1.0 if (
                    bool(pro_profile["team_won"]) and not bool(user_profile["team_won"])
                ) else (0.5 if bool(pro_profile["team_won"]) else 0.0)
                post_break_local_conversion = _post_break_local_conversion(
                    user_profile,
                    pro_profile,
                    break_index=int(prefix["break_index"]),
                )

                prefix_score = (
                    0.35 * prefix_duration_norm
                    + 0.25 * shared_steps_norm
                    + 0.15 * float(prefix["prefix_similarity"])
                    + 0.10 * float(prefix["timing_alignment"])
                    + 0.15 * float(prefix["local_context_alignment"])
                )
                coach_value = (
                    0.40 * survived_longer
                    + 0.35 * round_outcome_advantage
                    + 0.25 * post_break_local_conversion
                )
                round_score = 0.75 * prefix_score + 0.25 * coach_value
                mean_user_pro_difference = 1.0 - (
                    0.55 * float(prefix["prefix_similarity"])
                    + 0.45 * float(prefix["local_context_alignment"])
                )

                all_rankings.append(
                    {
                        "user_round_num": int(user_round_num),
                        "match_demo_id": str(pro_profile["demo_id"]),
                        "match_round_num": int(pro_profile["round_num"]),
                        "matched_pro_steamid": int(pro_profile["steamid"]),
                        "matched_pro_player": str(pro_profile["name"]),
                        "mean_round_similarity": prefix_score,
                        "mean_non_user_alignment": float(prefix["local_context_alignment"]),
                        "mean_context_match": float(prefix["prefix_similarity"]),
                        "mean_user_pro_difference": mean_user_pro_difference,
                        "window_count": int(prefix["matched_sample_count"]),
                        "coverage": prefix_duration_norm,
                        "longest_streak": shared_steps,
                        "shared_route_steps": shared_steps,
                        "user_route_steps": int(user_profile["route_step_count"]),
                        "pro_route_steps": int(pro_profile["route_step_count"]),
                        "matched_prefix_duration_sec": float(prefix["prefix_duration_sec"]),
                        "route_timing_alignment": float(prefix["timing_alignment"]),
                        "local_context_alignment": float(prefix["local_context_alignment"]),
                        "bomb_state_alignment": float(prefix["bomb_state_alignment"]),
                        "prefix_score": float(prefix_score),
                        "coach_value": float(coach_value),
                        "survived_longer": survived_longer,
                        "round_outcome_advantage": round_outcome_advantage,
                        "post_break_local_conversion": float(post_break_local_conversion),
                        "survival_gap_sec": float(break_event["survival_gap_sec"]),
                        "break_event_type": str(break_event["break_event_type"]),
                        "break_event_label": str(break_event["break_event_label"]),
                        "break_time_sec": float(break_event["break_time_sec"]),
                        "start_context_alignment": float(start_context_alignment),
                        "round_score": round_score,
                    }
                )

    if not all_rankings:
        return None

    all_rankings.sort(key=lambda row: float(row["round_score"]), reverse=True)
    best = all_rankings[0]
    best_artifact = next(
        artifact for artifact in pro_artifacts if artifact.demo_id == best["match_demo_id"]
    )
    best_profiles = _build_round_route_profiles(
        best_artifact,
        int(best["match_round_num"]),
        sample_step_sec=sample_step_sec,
        teammate_radius=teammate_radius,
        enemy_radius=enemy_radius,
    )
    best_profile = next(
        profile for profile in best_profiles if int(profile["steamid"]) == int(best["matched_pro_steamid"])
    )
    window_scores = _build_route_window_scores(user_profile, best_profile, nav_mesh=nav_mesh)
    if window_scores:
        best["mean_round_similarity"] = float(
            np.mean([float(row["round_similarity"]) for row in window_scores])
        )
        best["mean_non_user_alignment"] = float(
            np.mean([float(row["non_user_alignment"]) for row in window_scores])
        )
        best["mean_context_match"] = float(
            np.mean([float(row["context_match"]) for row in window_scores])
        )
        best["mean_user_pro_difference"] = float(
            np.mean([float(row["user_pro_difference"]) for row in window_scores])
        )

    result = {
        "logic": "nav",
        "user_round_num": int(best["user_round_num"]),
        "match_demo_id": str(best["match_demo_id"]),
        "match_round_num": int(best["match_round_num"]),
        "matched_pro_steamid": int(best["matched_pro_steamid"]),
        "matched_pro_player": str(best["matched_pro_player"]),
        "mean_round_similarity": float(best["mean_round_similarity"]),
        "mean_non_user_alignment": float(best["mean_non_user_alignment"]),
        "mean_context_match": float(best["mean_context_match"]),
        "mean_user_pro_difference": float(best["mean_user_pro_difference"]),
        "window_count": int(best["window_count"]),
        "coverage": float(best["coverage"]),
        "longest_streak": int(best["longest_streak"]),
        "shared_route_steps": int(best["shared_route_steps"]),
        "user_route_steps": int(best["user_route_steps"]),
        "pro_route_steps": int(best["pro_route_steps"]),
        "matched_prefix_duration_sec": float(best["matched_prefix_duration_sec"]),
        "route_timing_alignment": float(best["route_timing_alignment"]),
        "local_context_alignment": float(best["local_context_alignment"]),
        "bomb_state_alignment": float(best["bomb_state_alignment"]),
        "prefix_score": float(best["prefix_score"]),
        "coach_value": float(best["coach_value"]),
        "survived_longer": float(best["survived_longer"]),
        "round_outcome_advantage": float(best["round_outcome_advantage"]),
        "post_break_local_conversion": float(best["post_break_local_conversion"]),
        "survival_gap_sec": float(best["survival_gap_sec"]),
        "break_event_type": str(best["break_event_type"]),
        "break_event_label": str(best["break_event_label"]),
        "break_time_sec": float(best["break_time_sec"]),
        "round_score": float(best["round_score"]),
        "all_round_rankings": all_rankings,
        "window_scores": window_scores,
    }
    return result


def compute_route_divergence_timeline(
    window_scores: Sequence[dict[str, Any]],
    *,
    divergence_threshold: float = 0.30,
    min_consecutive: int = 2,
) -> dict[str, Any]:
    """Detect divergence for route-based matches using route/outcome gaps."""
    sorted_windows = sorted(
        window_scores,
        key=lambda row: float(row["user_window_start_sec_from_freeze"]),
    )

    timeline: list[dict[str, Any]] = []
    for row in sorted_windows:
        route_difference = float(row.get("route_difference", row["user_pro_difference"]))
        support_difference = float(row.get("support_difference", 0.0))
        enemy_difference = float(row.get("enemy_difference", 0.0))
        bomb_state_difference = float(row.get("bomb_state_difference", 0.0))
        local_context_difference = float(row.get("local_context_difference", 0.0))
        local_collapse_difference = float(row.get("local_collapse_difference", 0.0))
        survival_difference = float(row.get("survival_difference", 0.0))
        divergence_signal = min(
            1.0,
            0.40 * route_difference
            + 0.20 * local_context_difference
            + 0.15 * local_collapse_difference
            + 0.10 * bomb_state_difference
            + 0.10 * survival_difference
            + 0.05 * support_difference,
        )
        timeline.append(
            {
                "user_window_start_sec_from_freeze": float(row["user_window_start_sec_from_freeze"]),
                "user_window_end_sec_from_freeze": float(row["user_window_end_sec_from_freeze"]),
                "user_window_start_tick": int(row["user_window_start_tick"]),
                "user_window_end_tick": int(row["user_window_end_tick"]),
                "divergence_signal": divergence_signal,
                "user_pro_difference": route_difference,
                "non_user_alignment": 1.0 - local_context_difference,
                "context_match": float(row.get("context_match", 0.0)),
                "route_difference": route_difference,
                "support_difference": support_difference,
                "enemy_difference": enemy_difference,
                "bomb_state_difference": bomb_state_difference,
                "local_context_difference": local_context_difference,
                "local_collapse_difference": local_collapse_difference,
                "survival_difference": survival_difference,
            }
        )

    divergence_start_sec: float | None = None
    divergence_start_tick: int | None = None
    divergence_end_sec: float | None = timeline[-1]["user_window_end_sec_from_freeze"] if timeline else None

    consecutive = 0
    candidate_sec: float | None = None
    candidate_tick: int | None = None
    for entry in timeline:
        if entry["divergence_signal"] >= divergence_threshold:
            if consecutive == 0:
                candidate_sec = entry["user_window_start_sec_from_freeze"]
                candidate_tick = entry["user_window_start_tick"]
            consecutive += 1
            if consecutive >= min_consecutive and divergence_start_sec is None:
                divergence_start_sec = candidate_sec
                divergence_start_tick = candidate_tick
        else:
            consecutive = 0
            candidate_sec = None
            candidate_tick = None

    return {
        "timeline": timeline,
        "divergence_start_sec": divergence_start_sec,
        "divergence_start_tick": divergence_start_tick,
        "divergence_end_sec": divergence_end_sec,
        "threshold": divergence_threshold,
        "min_consecutive": min_consecutive,
    }
