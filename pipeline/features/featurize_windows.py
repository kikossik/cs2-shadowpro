"""Build retrieval-friendly features from replay slices."""
from __future__ import annotations

import math
from collections import Counter

import polars as pl

FEATURE_VERSION = "v2"
MIN_MAPPING_SECONDS = 20
DEFAULT_WINDOW_PRE_TICKS = 320
DEFAULT_WINDOW_POST_TICKS = 320
DEFAULT_SLIDE_STEP_TICKS = 128
TICK_RATE = 64

# Situations where the CT side has the key decisions to make (retake, hold)
_CT_SITUATIONS = frozenset({"retake", "site_hold", "post_plant"})
# Situations where the T side has the key decisions to make (execute, setup)
_T_SITUATIONS = frozenset({"exec_like", "setup"})


def _infer_pro_side_to_query(primary_situation: str, alive_ct: int, alive_t: int) -> str:
    """Derive side_to_query for pro windows that have no specific user.

    Uses the primary situation label to pick the side with agency in that
    scenario, falling back to the underdog (fewer alive) for neutral situations.
    """
    if primary_situation in _CT_SITUATIONS:
        return "ct"
    if primary_situation in _T_SITUATIONS:
        return "t"
    if alive_ct < alive_t:
        return "ct"
    if alive_t < alive_ct:
        return "t"
    return "ct"


_SITE_A_TOKENS = {
    "a", "abombsite", "bombsitea", "asite", "aramp", "ramp", "palace", "heaven",
}
_SITE_B_TOKENS = {
    "b", "bbombsite", "bombsiteb", "bsite", "apartments", "apps", "banana",
}

_UTILITY_FAMILIES = {
    "flashbang",
    "hegrenade",
    "smokegrenade",
    "molotov",
    "incendiarygrenade",
    "incgrenade",
    "decoy",
}
_PISTOL_FAMILIES = {
    "glock18", "usps", "usp", "p2000", "p250", "deagle", "deserteagle",
    "tec9", "cz75auto", "cz75a", "fiveseven", "elites", "dualberettas",
    "revolver",
}
_RIFLE_FAMILIES = {
    "ak47", "m4a4", "m4a1s", "m4a1silencer", "aug", "sg553",
    "galilar", "galil", "famas",
}
_SNIPER_FAMILIES = {
    "awp", "ssg08", "scar20", "g3sg1",
}
_SMG_FAMILIES = {
    "mp9", "mac10", "mp7", "mp5sd", "ump45", "p90", "ppbizon", "bizon",
}
_SHOTGUN_FAMILIES = {
    "nova", "xm1014", "mag7", "sawedoff",
}
_HEAVY_FAMILIES = {
    "negev", "m249",
}
_WEAPON_FAMILIES = (
    "sniper",
    "rifle",
    "smg",
    "shotgun",
    "heavy",
    "pistol",
)
_WEAPON_PRIORITY = {
    "sniper": 6,
    "rifle": 5,
    "smg": 4,
    "shotgun": 3,
    "heavy": 2,
    "pistol": 1,
}


def _safe_int(value: object, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_token(value: str | None) -> str | None:
    if not value:
        return None
    return "".join(ch for ch in str(value).lower() if ch.isalnum())


def _normalize_place(place: str | None) -> str | None:
    return _normalize_token(place)


def _normalize_weapon_name(name: str | None) -> str | None:
    if not name:
        return None

    normalized = _normalize_token(name)
    if not normalized:
        return None

    if normalized.startswith("weapon"):
        normalized = normalized[6:]
    if normalized in {"knife", "knifet", "taser", "c4"}:
        return normalized
    if normalized == "uspsilencer":
        return "usps"
    if normalized == "m4a1silencer":
        return "m4a1s"
    if normalized == "incendiary":
        return "incendiarygrenade"
    if normalized == "smoke":
        return "smokegrenade"
    return normalized


def _weapon_family(name: str | None) -> str | None:
    normalized = _normalize_weapon_name(name)
    if not normalized:
        return None
    if normalized in {"knife", "knifet", "taser", "c4"}:
        return None
    if normalized in _UTILITY_FAMILIES:
        return "utility"
    if normalized in _SNIPER_FAMILIES:
        return "sniper"
    if normalized in _RIFLE_FAMILIES:
        return "rifle"
    if normalized in _SMG_FAMILIES:
        return "smg"
    if normalized in _SHOTGUN_FAMILIES:
        return "shotgun"
    if normalized in _HEAVY_FAMILIES:
        return "heavy"
    if normalized in _PISTOL_FAMILIES:
        return "pistol"
    return "unknown"


def _infer_primary_weapon(inventory: object) -> str | None:
    if not isinstance(inventory, list):
        return None

    best_name: str | None = None
    best_priority = -1
    for item in inventory:
        family = _weapon_family(str(item))
        if family in {None, "utility", "unknown"}:
            continue
        priority = _WEAPON_PRIORITY.get(family, 0)
        if priority > best_priority:
            best_priority = priority
            best_name = str(item)

    if best_name:
        return best_name

    for item in inventory:
        family = _weapon_family(str(item))
        if family == "pistol":
            return str(item)
    return None


def _infer_site(place_values: list[str], bomb_site: str | None = None) -> str | None:
    if bomb_site:
        lowered = bomb_site.lower()
        if "a" in lowered and "not" not in lowered:
            return "a"
        if "b" in lowered and "not" not in lowered:
            return "b"

    counts = Counter()
    for place in place_values:
        normalized = _normalize_place(place)
        if normalized in _SITE_A_TOKENS:
            counts["a"] += 1
        elif normalized in _SITE_B_TOKENS:
            counts["b"] += 1

    if not counts:
        return None
    return counts.most_common(1)[0][0]


def _alive_rows(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0:
        return frame
    if "health" not in frame.columns:
        return frame
    return frame.filter(pl.col("health") > 0)


def _team_rows(frame: pl.DataFrame, side: str) -> pl.DataFrame:
    if frame.height == 0 or "side" not in frame.columns:
        return pl.DataFrame()

    rows = frame.filter(
        pl.col("side").cast(pl.String).str.to_lowercase() == side
    )
    return _alive_rows(rows)


def _nearest_tick_frame(round_ticks: pl.DataFrame, anchor_tick: int) -> pl.DataFrame:
    if round_ticks.height == 0 or "tick" not in round_ticks.columns:
        return pl.DataFrame()

    nearest_tick = (
        round_ticks
        .select("tick")
        .with_columns((pl.col("tick") - anchor_tick).abs().alias("_distance"))
        .sort(["_distance", "tick"])
        .item(0, 0)
    )
    return round_ticks.filter(pl.col("tick") == nearest_tick)


def _spread_from_rows(rows: list[dict], centroid_x: float, centroid_y: float) -> float:
    if not rows:
        return 0.0
    distances = [
        math.dist(
            (centroid_x, centroid_y),
            (_safe_float(row.get("X")), _safe_float(row.get("Y"))),
        )
        for row in rows
    ]
    return round(sum(distances) / len(distances), 2)


def _counts_dict(counter: Counter, keys: tuple[str, ...]) -> dict[str, int]:
    return {key: int(counter.get(key, 0)) for key in keys}


def _team_state(frame: pl.DataFrame, side: str) -> dict:
    rows = _team_rows(frame, side)
    if rows.height == 0:
        return {
            "alive": 0,
            "hp_sum": 0,
            "defuser_count": 0,
            "centroid_x": 0.0,
            "centroid_y": 0.0,
            "spread": 0.0,
            "top_places": [],
            "place_profile": {},
            "weapon_profile": _counts_dict(Counter(), _WEAPON_FAMILIES),
            "primary_weapons": [],
        }

    list_rows = list(rows.iter_rows(named=True))
    centroid_x = round(
        sum(_safe_float(row.get("X")) for row in list_rows) / len(list_rows),
        2,
    )
    centroid_y = round(
        sum(_safe_float(row.get("Y")) for row in list_rows) / len(list_rows),
        2,
    )
    place_counts = Counter(
        str(row.get("place"))
        for row in list_rows
        if row.get("place")
    )

    primary_weapons: list[str] = []
    weapon_counter: Counter = Counter()
    for row in list_rows:
        weapon_name = _infer_primary_weapon(row.get("inventory"))
        if weapon_name:
            primary_weapons.append(weapon_name)
            family = _weapon_family(weapon_name)
            if family in _WEAPON_FAMILIES:
                weapon_counter[family] += 1

    return {
        "alive": rows.height,
        "hp_sum": _safe_int(rows["health"].sum()) if "health" in rows.columns else 0,
        "defuser_count": (
            _safe_int(rows["has_defuser"].cast(pl.Int8).sum())
            if "has_defuser" in rows.columns
            else 0
        ),
        "centroid_x": centroid_x,
        "centroid_y": centroid_y,
        "spread": _spread_from_rows(list_rows, centroid_x, centroid_y),
        "top_places": [name for name, _ in place_counts.most_common(4)],
        "place_profile": dict(place_counts.most_common(8)),
        "weapon_profile": _counts_dict(weapon_counter, _WEAPON_FAMILIES),
        "primary_weapons": sorted(primary_weapons),
    }


def _sample_centroid_path(window_ticks: pl.DataFrame, side: str, max_points: int = 8) -> list[list[float]]:
    rows = _team_rows(window_ticks, side)
    if rows.height == 0 or "tick" not in rows.columns:
        return []

    ticks = sorted(set(rows["tick"].to_list()))
    if not ticks:
        return []

    if len(ticks) > max_points:
        step = max(1, len(ticks) // max_points)
        ticks = ticks[::step][:max_points]

    path: list[list[float]] = []
    for tick in ticks:
        frame = rows.filter(pl.col("tick") == tick)
        dict_rows = list(frame.iter_rows(named=True))
        if not dict_rows:
            continue
        cx = round(sum(_safe_float(row.get("X")) for row in dict_rows) / len(dict_rows), 2)
        cy = round(sum(_safe_float(row.get("Y")) for row in dict_rows) / len(dict_rows), 2)
        path.append([cx, cy])
    return path


def _path_distance(path: list[list[float]]) -> float:
    if len(path) < 2:
        return 0.0
    distance = 0.0
    for left, right in zip(path, path[1:]):
        distance += math.dist((left[0], left[1]), (right[0], right[1]))
    return round(distance, 2)


def _round_side_lookup(round_ticks: pl.DataFrame) -> dict[int, str]:
    if round_ticks.height == 0 or "steamid" not in round_ticks.columns or "side" not in round_ticks.columns:
        return {}

    lookup: dict[int, str] = {}
    for row in round_ticks.select(["steamid", "side"]).iter_rows(named=True):
        steamid = row.get("steamid")
        side = row.get("side")
        if steamid is None or not side:
            continue
        lookup[int(steamid)] = str(side).lower()
    return lookup


def _death_events(round_ticks: pl.DataFrame) -> list[dict]:
    if round_ticks.height == 0 or "steamid" not in round_ticks.columns or "tick" not in round_ticks.columns:
        return []

    columns = [col for col in ("steamid", "tick", "health", "side") if col in round_ticks.columns]
    events: list[dict] = []

    for _, player_rows in round_ticks.select(columns).group_by("steamid", maintain_order=True):
        ordered = player_rows.sort("tick")
        prev_alive: bool | None = None
        side_value = None

        for row in ordered.iter_rows(named=True):
            side_value = side_value or row.get("side")
            alive = _safe_int(row.get("health"), default=100) > 0
            if prev_alive is True and not alive:
                events.append({
                    "tick": _safe_int(row.get("tick")),
                    "side": str(side_value).lower() if side_value else None,
                    "steamid": _safe_int(row.get("steamid")),
                })
                break
            prev_alive = alive

    events.sort(key=lambda row: row["tick"])
    return events


def _count_deaths(
    death_events: list[dict],
    *,
    start_tick: int,
    end_tick: int,
) -> dict[str, int]:
    counter: Counter = Counter()
    for event in death_events:
        tick = _safe_int(event.get("tick"), default=-1)
        if tick < start_tick or tick > end_tick:
            continue
        side = event.get("side")
        if side in {"ct", "t"}:
            counter[str(side)] += 1

    return {
        "ct": int(counter.get("ct", 0)),
        "t": int(counter.get("t", 0)),
    }


def _count_shots_by_side(window_shots: pl.DataFrame, side_lookup: dict[int, str]) -> dict[str, int]:
    if window_shots.height == 0 or "player_steamid" not in window_shots.columns:
        return {"ct": 0, "t": 0}

    counter: Counter = Counter()
    for row in window_shots.select(["player_steamid"]).iter_rows(named=True):
        steamid = row.get("player_steamid")
        if steamid is None:
            continue
        side = side_lookup.get(int(steamid))
        if side in {"ct", "t"}:
            counter[side] += 1
    return {
        "ct": int(counter.get("ct", 0)),
        "t": int(counter.get("t", 0)),
    }


def _grenade_entity_counts(window_grenades: pl.DataFrame) -> dict[str, int]:
    if window_grenades.height == 0 or "entity_id" not in window_grenades.columns:
        return {}

    rows = (
        window_grenades
        .group_by("entity_id")
        .agg(pl.first("grenade_type").alias("grenade_type"))
        .iter_rows(named=True)
    )
    counter: Counter = Counter()
    for row in rows:
        token = _normalize_token(row.get("grenade_type"))
        if not token or token == "cdecoyprojectile":
            continue
        counter[token] += 1
    return dict(counter)


def _shots_weapon_profile(window_shots: pl.DataFrame) -> dict[str, int]:
    counter: Counter = Counter()
    if window_shots.height == 0 or "weapon" not in window_shots.columns:
        return _counts_dict(counter, _WEAPON_FAMILIES)

    for row in window_shots.select("weapon").iter_rows(named=True):
        family = _weapon_family(row.get("weapon"))
        if family in _WEAPON_FAMILIES:
            counter[family] += 1
    return _counts_dict(counter, _WEAPON_FAMILIES)


def _lookup_user_side(anchor_frame: pl.DataFrame, steam_id: str | None) -> str | None:
    if not steam_id or anchor_frame.height == 0 or "steamid" not in anchor_frame.columns:
        return None

    try:
        steamid_int = int(steam_id)
    except ValueError:
        return None

    rows = anchor_frame.filter(pl.col("steamid") == steamid_int)
    if rows.height == 0 or "side" not in rows.columns:
        return None
    side = rows["side"][0]
    return str(side).lower() if side else None


def _lookup_user_weapon_family(anchor_frame: pl.DataFrame, steam_id: str | None) -> str | None:
    if not steam_id or anchor_frame.height == 0 or "steamid" not in anchor_frame.columns:
        return None

    try:
        steamid_int = int(steam_id)
    except ValueError:
        return None

    rows = anchor_frame.filter(pl.col("steamid") == steamid_int)
    if rows.height == 0 or "inventory" not in rows.columns:
        return None
    inventory = rows["inventory"][0]
    return _weapon_family(_infer_primary_weapon(inventory))


def _seconds_between(left_tick: int | None, right_tick: int | None) -> float | None:
    if left_tick is None or right_tick is None:
        return None
    return round((left_tick - right_tick) / TICK_RATE, 2)


def _build_situation_labels(
    *,
    planted: bool,
    site: str | None,
    time_since_freeze_end_s: float,
    time_since_bomb_plant_s: float | None,
    alive_ct: int,
    alive_t: int,
    shots_total: int,
    utility_total: int,
    deaths_ct: int,
    deaths_t: int,
    ct_path_distance: float,
    t_path_distance: float,
) -> tuple[str, list[str]]:
    tags: list[str] = []

    if planted:
        tags.append("post_plant")
        if time_since_bomb_plant_s is not None and time_since_bomb_plant_s <= 12:
            tags.append("fresh_plant")
        if ct_path_distance >= max(250.0, t_path_distance * 0.75):
            tags.append("retake")
        else:
            tags.append("site_hold")

    if shots_total >= 4:
        tags.append("fight")
    elif shots_total >= 1:
        tags.append("contact")

    if utility_total >= 5 and shots_total >= 2 and not planted:
        tags.append("exec_like")
    elif utility_total >= 4 and shots_total <= 2:
        tags.append("setup")

    if deaths_ct + deaths_t >= 1:
        tags.append("kill_event")
    if deaths_ct >= 1 and deaths_t >= 1:
        tags.append("trade_window")

    if min(alive_ct, alive_t) == 1 and max(alive_ct, alive_t) >= 2:
        tags.append("clutch")
    if abs(alive_ct - alive_t) >= 2:
        tags.append("man_advantage")

    if max(ct_path_distance, t_path_distance) >= 700 and shots_total <= 2:
        tags.append("rotate")

    if not tags:
        if time_since_freeze_end_s <= 28:
            tags.append("late_opening")
        else:
            tags.append("mid_round")
        if utility_total <= 1 and shots_total == 0:
            tags.append("default")

    if site:
        tags.append(f"site_{site}")

    primary = "default"
    for candidate in (
        "retake",
        "post_plant",
        "trade_window",
        "clutch",
        "exec_like",
        "fight",
        "setup",
        "rotate",
        "contact",
        "mid_round",
        "late_opening",
        "default",
    ):
        if candidate in tags:
            primary = candidate
            break

    return primary, tags


def build_window_features(
    *,
    ticks: pl.DataFrame,
    rounds: pl.DataFrame,
    shots: pl.DataFrame,
    smokes: pl.DataFrame,
    infernos: pl.DataFrame,
    flashes: pl.DataFrame,
    grenade_paths: pl.DataFrame,
    round_num: int,
    start_tick: int,
    anchor_tick: int,
    end_tick: int,
    user_steam_id: str | None = None,
    anchor_kind: str | None = None,
) -> dict:
    """Build a retrieval feature blob for one time window."""
    round_ticks = ticks.filter(pl.col("round_num") == round_num) if "round_num" in ticks.columns else pl.DataFrame()
    anchor_frame = _nearest_tick_frame(round_ticks, anchor_tick)
    window_ticks = (
        round_ticks.filter((pl.col("tick") >= start_tick) & (pl.col("tick") <= end_tick))
        if round_ticks.height > 0
        else pl.DataFrame()
    )

    round_row = (
        rounds.filter(pl.col("round_num") == round_num).row(0, named=True)
        if rounds.height > 0 and rounds.filter(pl.col("round_num") == round_num).height > 0
        else {}
    )

    window_shots = shots.filter(pl.col("round_num") == round_num) if "round_num" in shots.columns else pl.DataFrame()
    if window_shots.height > 0 and "tick" in window_shots.columns:
        window_shots = window_shots.filter(
            (pl.col("tick") >= start_tick) & (pl.col("tick") <= end_tick)
        )

    window_smokes = smokes.filter(pl.col("round_num") == round_num) if "round_num" in smokes.columns else pl.DataFrame()
    if window_smokes.height > 0 and "start_tick" in window_smokes.columns:
        window_smokes = window_smokes.filter(
            (pl.col("start_tick") >= start_tick) & (pl.col("start_tick") <= end_tick)
        )

    window_infernos = infernos.filter(pl.col("round_num") == round_num) if "round_num" in infernos.columns else pl.DataFrame()
    if window_infernos.height > 0 and "start_tick" in window_infernos.columns:
        window_infernos = window_infernos.filter(
            (pl.col("start_tick") >= start_tick) & (pl.col("start_tick") <= end_tick)
        )

    window_flashes = flashes.filter(pl.col("round_num") == round_num) if "round_num" in flashes.columns else pl.DataFrame()
    if window_flashes.height > 0 and "tick" in window_flashes.columns:
        window_flashes = window_flashes.filter(
            (pl.col("tick") >= start_tick) & (pl.col("tick") <= end_tick)
        )

    window_grenades = (
        grenade_paths.filter(pl.col("round_num") == round_num)
        if "round_num" in grenade_paths.columns
        else pl.DataFrame()
    )
    if window_grenades.height > 0 and "tick" in window_grenades.columns:
        window_grenades = window_grenades.filter(
            (pl.col("tick") >= start_tick) & (pl.col("tick") <= end_tick)
        )

    ct_state = _team_state(anchor_frame, "ct")
    t_state = _team_state(anchor_frame, "t")
    ct_path = _sample_centroid_path(window_ticks, "ct")
    t_path = _sample_centroid_path(window_ticks, "t")

    round_end = _safe_int(round_row.get("official_end") or round_row.get("end"), default=end_tick)
    bomb_plant_tick = _safe_int(round_row.get("bomb_plant"), default=-1)
    if bomb_plant_tick < 0:
        bomb_plant_tick = None

    freeze_end = _safe_int(round_row.get("freeze_end"), default=-1)
    if freeze_end < 0:
        freeze_end = None

    planted = bomb_plant_tick is not None and anchor_tick >= bomb_plant_tick
    bomb_site = round_row.get("bomb_site")
    place_values = ct_state["top_places"] + t_state["top_places"]
    site = _infer_site(place_values, bomb_site=bomb_site)

    death_events = _death_events(round_ticks)
    deaths_in_window = _count_deaths(
        death_events,
        start_tick=start_tick,
        end_tick=end_tick,
    )
    shots_by_side = _count_shots_by_side(window_shots, _round_side_lookup(round_ticks))
    grenade_entity_counts = _grenade_entity_counts(window_grenades)
    he_count = int(grenade_entity_counts.get("chegrenadeprojectile", 0))
    utility_total = (
        window_smokes.height
        + window_infernos.height
        + window_flashes.height
        + he_count
    )

    time_since_freeze_end_s = _seconds_between(anchor_tick, freeze_end)
    time_since_bomb_plant_s = _seconds_between(anchor_tick, bomb_plant_tick) if planted else None
    seconds_remaining_s = round(max(0.0, (round_end - anchor_tick) / TICK_RATE), 2)

    primary_situation, situation_tags = _build_situation_labels(
        planted=planted,
        site=site,
        time_since_freeze_end_s=time_since_freeze_end_s or 0.0,
        time_since_bomb_plant_s=time_since_bomb_plant_s,
        alive_ct=ct_state["alive"],
        alive_t=t_state["alive"],
        shots_total=window_shots.height,
        utility_total=utility_total,
        deaths_ct=deaths_in_window["ct"],
        deaths_t=deaths_in_window["t"],
        ct_path_distance=_path_distance(ct_path),
        t_path_distance=_path_distance(t_path),
    )

    user_side = _lookup_user_side(anchor_frame, user_steam_id)
    side_to_query = user_side if user_side is not None else _infer_pro_side_to_query(
        primary_situation, ct_state["alive"], t_state["alive"]
    )
    focus_weapon_family = _lookup_user_weapon_family(anchor_frame, user_steam_id)
    queryable = (time_since_freeze_end_s is None or time_since_freeze_end_s >= MIN_MAPPING_SECONDS)
    skip_reason = None if queryable else f"anchor is only {time_since_freeze_end_s:.2f}s after freeze end"

    vector = {
        "alive_ct": ct_state["alive"],
        "alive_t": t_state["alive"],
        "alive_diff": ct_state["alive"] - t_state["alive"],
        "hp_ct_sum": ct_state["hp_sum"],
        "hp_t_sum": t_state["hp_sum"],
        "defuser_ct_count": ct_state["defuser_count"],
        "ct_centroid_x": ct_state["centroid_x"],
        "ct_centroid_y": ct_state["centroid_y"],
        "t_centroid_x": t_state["centroid_x"],
        "t_centroid_y": t_state["centroid_y"],
        "ct_spread": ct_state["spread"],
        "t_spread": t_state["spread"],
        "shots_ct": shots_by_side["ct"],
        "shots_t": shots_by_side["t"],
        "shots_total": window_shots.height,
        "deaths_ct": deaths_in_window["ct"],
        "deaths_t": deaths_in_window["t"],
        "deaths_total": deaths_in_window["ct"] + deaths_in_window["t"],
        "smokes_count": window_smokes.height,
        "infernos_count": window_infernos.height,
        "flashes_count": window_flashes.height,
        "he_count": he_count,
        "utility_total": utility_total,
        "ct_path_distance": _path_distance(ct_path),
        "t_path_distance": _path_distance(t_path),
        "time_since_freeze_end_s": time_since_freeze_end_s or 0.0,
        "time_since_bomb_plant_s": time_since_bomb_plant_s if time_since_bomb_plant_s is not None else -1.0,
        "seconds_remaining_s": seconds_remaining_s,
        "planted": 1 if planted else 0,
    }

    for side_name, state in (("ct", ct_state), ("t", t_state)):
        for family, count in state["weapon_profile"].items():
            vector[f"{side_name}_{family}_count"] = count

    return {
        "feature_version": FEATURE_VERSION,
        "round_num": round_num,
        "start_tick": start_tick,
        "anchor_tick": anchor_tick,
        "end_tick": end_tick,
        "anchor_kind": anchor_kind,
        "phase": primary_situation,
        "primary_situation": primary_situation,
        "situation_tags": situation_tags,
        "site": site,
        "planted": planted,
        "side_to_query": side_to_query,
        "focus_weapon_family": focus_weapon_family,
        "time_since_freeze_end_s": time_since_freeze_end_s,
        "time_since_bomb_plant_s": time_since_bomb_plant_s,
        "seconds_remaining_s": seconds_remaining_s,
        "queryable": queryable,
        "skip_reason": skip_reason,
        "alive_ct": ct_state["alive"],
        "alive_t": t_state["alive"],
        "vector": vector,
        "ct_top_places": ct_state["top_places"],
        "t_top_places": t_state["top_places"],
        "ct_place_profile": ct_state["place_profile"],
        "t_place_profile": t_state["place_profile"],
        "ct_weapon_profile": ct_state["weapon_profile"],
        "t_weapon_profile": t_state["weapon_profile"],
        "shots_weapon_profile": _shots_weapon_profile(window_shots),
        "ct_primary_weapons": ct_state["primary_weapons"],
        "t_primary_weapons": t_state["primary_weapons"],
        "ct_centroid_path": ct_path,
        "t_centroid_path": t_path,
        "window_summary": {
            "ticks_observed": window_ticks["tick"].n_unique() if window_ticks.height > 0 and "tick" in window_ticks.columns else 0,
            "shots_count": window_shots.height,
            "smokes_count": window_smokes.height,
            "infernos_count": window_infernos.height,
            "flashes_count": window_flashes.height,
            "he_count": he_count,
            "deaths_ct": deaths_in_window["ct"],
            "deaths_t": deaths_in_window["t"],
        },
    }
