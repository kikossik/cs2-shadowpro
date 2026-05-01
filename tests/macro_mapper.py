"""Macro-scenario-first round mapper.

Scoring stack (per the proposed redesign):
    final = 0.45 * enemy_macro + 0.25 * ally_macro + 0.20 * focal_route + 0.10 * outcome

A "macro signature" for one round is:
    - user_side, enemy_side
    - plant_site (a / b / not_planted)
    - first_contact_zone  (where the first kill happens)
    - enemy zone histogram, computed over the early window 0..40s of the round
    - ally  zone histogram, same window
    - enemy site_intent (a / b / mid / unclear) and a confidence in [0,1]
    - outcome (won/lost), round time bucket, alive-counts at end of window

When both sides confidently know enemy_intent_site and they differ, the
candidate is hard-rejected. This is the key behavioral change: the route can
look identical, but if the enemy story is "B pressure" vs "A pressure" the pro
round is the wrong coaching analogue.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable, Sequence

import numpy as np
import polars as pl

from tests.data_loader import DemoArtifacts
from tests.zones import ALL_ZONES, zone_for

# Window over which we compute "where is each team going / pressuring"
EARLY_WINDOW_SEC: float = 40.0
SAMPLE_HZ: float = 4.0  # 4 samples / sec is plenty for macro intent

# Sub-windows for the windowed-min comparison. The min across these windows is
# what enters scoring, so a round that "starts similar then diverges" loses
# more points than a round that stays aligned the whole way through.
WINDOWS_SEC: tuple[tuple[float, float], ...] = (
    (0.0, 10.0),
    (10.0, 25.0),
    (25.0, 40.0),
)

# Scoring weights — match the user's proposal verbatim.
W_ENEMY = 0.45
W_ALLY  = 0.25
W_FOCAL = 0.20
W_COACH = 0.10

# Hard-reject when both sides confidently know the enemy bombsite intent and they differ.
SITE_CONF_GATE = 0.7

# Economy buckets — derived from focal player's `round_start_equip_value`.
# Allowed inter-bucket distance is 1; pistol↔full_buy gets hard-rejected.
ECON_BUCKETS = ("pistol", "eco", "force", "full_buy")
# upper exclusive bound for each bucket (last one is +inf)
ECON_THRESHOLDS = (1500.0, 2500.0, 3800.0)
ECON_MAX_DISTANCE = 1


def _econ_bucket(equip_value: float) -> str:
    for bucket, hi in zip(ECON_BUCKETS, ECON_THRESHOLDS):
        if equip_value < hi:
            return bucket
    return ECON_BUCKETS[-1]


def _econ_distance(a: str, b: str) -> int:
    return abs(ECON_BUCKETS.index(a) - ECON_BUCKETS.index(b))


# ---------------------------------------------------------------------------
# Round signature
# ---------------------------------------------------------------------------

@dataclass
class RoundSignature:
    demo_id: str
    round_num: int
    user_side: str            # 'ct' | 't' (the side the focal player is on; for pro this is "team_side")
    enemy_side: str           # opposite of user_side
    plant_site: str           # 'a' | 'b' | 'not_planted'
    won: bool                 # did the user/focal team win
    end_window_alive: tuple[int, int]   # (allies_alive_at_end_of_window, enemies_alive_at_end_of_window)
    first_contact_zone: str   # 'a_site', 'b_site', etc.
    first_contact_side: str   # 'a' | 'b' | 'mid' | 'spawn'
    enemy_zone_hist: dict[str, float]
    ally_zone_hist:  dict[str, float]
    # Per-window histograms for the windowed-min comparison (one entry per WINDOWS_SEC bucket).
    enemy_zone_hist_windows: list[dict[str, float]]
    ally_zone_hist_windows:  list[dict[str, float]]
    focal_zone_hist_windows: list[dict[str, float]]
    enemy_site_intent: str    # 'a' | 'b' | 'mid' | 'unclear'
    enemy_site_conf:   float  # [0,1]
    enemy_centroid_end: tuple[float, float]
    focal_steamid: int        # for user: the focal user; for pro: the player whose route we'll attempt to match later
    focal_track:   list[tuple[float, float, float]]   # [(t_sec_from_freeze_end, x, y)] sampled at SAMPLE_HZ
    freeze_end_tick: int
    end_tick: int
    tick_rate: int
    econ_bucket: str = "full_buy"
    econ_value:  float = 0.0
    round_outcome_reason: str = ""

    def macro_features(self) -> tuple[str, str, dict[str, float], dict[str, float], str]:
        return (
            self.plant_site,
            self.first_contact_zone,
            self.enemy_zone_hist,
            self.ally_zone_hist,
            self.enemy_site_intent,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_hist(counts: dict[str, float]) -> dict[str, float]:
    total = sum(counts.values()) or 1.0
    return {z: counts.get(z, 0.0) / total for z in ALL_ZONES}


def _hist_similarity(a: dict[str, float], b: dict[str, float]) -> float:
    """1 - 0.5 * L1 over normalized histograms (== histogram intersection on probabilities)."""
    if not a or not b:
        return 0.0
    keys = set(a) | set(b)
    inter = sum(min(a.get(k, 0.0), b.get(k, 0.0)) for k in keys)
    return float(inter)  # already in [0,1] for normalized hists


def _infer_site_intent(enemy_zone_hist: dict[str, float]) -> tuple[str, float]:
    """Aggregate enemy time spent in side-aligned zones into A/B/mid mass."""
    side_mass = {"a": 0.0, "b": 0.0, "mid": 0.0}
    for zone, mass in enemy_zone_hist.items():
        # Find a representative `place` for this zone via the reverse map; quick:
        if zone in ("a_site", "palace", "a_ramp", "jungle"):
            side_mass["a"] += mass
        elif zone in ("b_site", "b_apps", "b_short", "market", "underpass"):
            side_mass["b"] += mass
        elif zone in ("mid", "catwalk", "connector"):
            side_mass["mid"] += mass
        # spawns / other / ladder ignored
    label = max(side_mass, key=side_mass.get)
    confidence = side_mass[label]
    if confidence < 0.35:
        return "unclear", confidence
    return label, confidence


def _first_contact_zone(art: DemoArtifacts, round_num: int) -> tuple[str, str]:
    if art.kills.height == 0:
        return ("other", "spawn")
    rk = art.kills.filter(pl.col("round_num") == round_num).sort("tick")
    if rk.height == 0:
        return ("other", "spawn")
    place = rk["victim_place"][0] if "victim_place" in rk.columns else None
    return zone_for(place)


def _round_window_ticks(art: DemoArtifacts, round_row: dict, window_sec: float) -> tuple[int, int, int] | None:
    """(window_start_tick, window_end_tick, tick_rate). Returns None if the round has no usable ticks."""
    tr = art.tick_rate
    freeze_end = round_row.get("freeze_end") or round_row.get("start")
    if freeze_end is None:
        return None
    freeze_end = int(freeze_end)
    end_tick = round_row.get("official_end") or round_row.get("end") or freeze_end
    end_tick = int(end_tick)
    return freeze_end, min(end_tick, freeze_end + int(window_sec * tr)), tr


def _team_side(art: DemoArtifacts, round_num: int, steamid: int) -> str | None:
    sub = art.ticks.filter(
        (pl.col("round_num") == round_num) & (pl.col("steamid") == steamid)
    ).select("side").drop_nulls().head(1)
    if sub.height == 0:
        return None
    val = sub["side"][0]
    return str(val).lower() if val else None


# ---------------------------------------------------------------------------
# Build a round signature
# ---------------------------------------------------------------------------

def build_signature(
    art: DemoArtifacts,
    round_row: dict,
    focal_steamid: int,
) -> RoundSignature | None:
    rn = int(round_row["round_num"])
    side = _team_side(art, rn, focal_steamid)
    if side not in ("ct", "t"):
        return None
    enemy_side = "t" if side == "ct" else "ct"

    window = _round_window_ticks(art, round_row, EARLY_WINDOW_SEC)
    if window is None:
        return None
    win_start, win_end, tr = window

    rt = art.ticks.filter(
        (pl.col("round_num") == rn)
        & (pl.col("tick") >= win_start)
        & (pl.col("tick") <= win_end)
    )
    if rt.height == 0:
        return None

    # Down-sample: keep one sample every (tick_rate / SAMPLE_HZ) ticks; tag a
    # second-from-freeze-end column for the windowed comparisons.
    step = max(1, int(tr / SAMPLE_HZ))
    rt = rt.with_columns(
        ((pl.col("tick") - win_start) // step).alias("_bin"),
        ((pl.col("tick") - win_start).cast(pl.Float64) / float(tr)).alias("_t_sec"),
    )

    ally = rt.filter(pl.col("side") == side)
    enemy = rt.filter(pl.col("side") == enemy_side)

    # Zone histograms over the window — only count alive players.

    def hist(df: pl.DataFrame, t_lo: float | None = None, t_hi: float | None = None) -> dict[str, float]:
        if df.height == 0:
            return {z: 0.0 for z in ALL_ZONES}
        sub = df
        if t_lo is not None:
            sub = sub.filter((pl.col("_t_sec") >= t_lo) & (pl.col("_t_sec") < t_hi))
        if sub.height == 0:
            return {z: 0.0 for z in ALL_ZONES}
        alive = sub.filter(pl.col("is_alive"))
        counts: dict[str, float] = {z: 0.0 for z in ALL_ZONES}
        for place in alive["place"].to_list():
            zone, _ = zone_for(place)
            counts[zone] = counts.get(zone, 0.0) + 1.0
        return _normalize_hist(counts)

    ally_hist = hist(ally)
    enemy_hist = hist(enemy)
    focal_df = rt.filter(pl.col("steamid") == focal_steamid)

    enemy_hist_windows = [hist(enemy, lo, hi) for lo, hi in WINDOWS_SEC]
    ally_hist_windows  = [hist(ally,  lo, hi) for lo, hi in WINDOWS_SEC]
    focal_hist_windows = [hist(focal_df, lo, hi) for lo, hi in WINDOWS_SEC]

    intent, intent_conf = _infer_site_intent(enemy_hist)

    # Economy bucket from the focal player's actual loadout once buys settle
    # (round_start_equip_value excludes freeze-time purchases; current_equip_value
    # at ~5s after freeze-end captures the rifle/utility they actually have).
    equip_val = 0.0
    if focal_df.height > 0 and "current_equip_value" in focal_df.columns:
        target = win_start + 5 * tr
        sample = focal_df.filter(pl.col("tick") >= target).select("current_equip_value").drop_nulls().head(1)
        if sample.height == 0:
            sample = focal_df.select("current_equip_value").drop_nulls().tail(1)
        if sample.height > 0:
            equip_val = float(sample.item())
    econ_bucket = _econ_bucket(equip_val)

    # Enemy centroid at the end of the window (last bin, alive only)
    last_bin = rt["_bin"].max()
    enemy_end = enemy.filter((pl.col("_bin") == last_bin) & pl.col("is_alive"))
    if enemy_end.height > 0:
        cx = float(enemy_end["X"].mean())
        cy = float(enemy_end["Y"].mean())
    else:
        cx = cy = 0.0

    # Alive counts at end of window
    end_bin_df = rt.filter(pl.col("_bin") == last_bin)
    allies_alive = int(end_bin_df.filter((pl.col("side") == side) & pl.col("is_alive")).height)
    enemies_alive = int(end_bin_df.filter((pl.col("side") == enemy_side) & pl.col("is_alive")).height)

    # Plant site
    plant_val = round_row.get("bomb_site")
    if plant_val is None or str(plant_val).lower() in ("not_planted", "none", ""):
        plant_site = "not_planted"
    else:
        s = str(plant_val).lower()
        plant_site = "a" if "a" in s else ("b" if "b" in s else "not_planted")

    # First contact (over the full round, not just the window)
    fc_zone, fc_side = _first_contact_zone(art, rn)

    # Focal player track over the window
    focal = rt.filter(pl.col("steamid") == focal_steamid).sort("tick")
    track: list[tuple[float, float, float]] = []
    if focal.height > 0:
        for tick, x, y in zip(focal["tick"].to_list(), focal["X"].to_list(), focal["Y"].to_list()):
            track.append(((int(tick) - win_start) / float(tr), float(x), float(y)))

    won = bool(round_row.get("winner") and str(round_row["winner"]).lower() == side)

    return RoundSignature(
        demo_id=art.demo_id,
        round_num=rn,
        user_side=side,
        enemy_side=enemy_side,
        plant_site=plant_site,
        won=won,
        end_window_alive=(allies_alive, enemies_alive),
        first_contact_zone=fc_zone,
        first_contact_side=fc_side,
        enemy_zone_hist=enemy_hist,
        ally_zone_hist=ally_hist,
        enemy_zone_hist_windows=enemy_hist_windows,
        ally_zone_hist_windows=ally_hist_windows,
        focal_zone_hist_windows=focal_hist_windows,
        enemy_site_intent=intent,
        enemy_site_conf=float(intent_conf),
        enemy_centroid_end=(cx, cy),
        focal_steamid=int(focal_steamid),
        focal_track=track,
        freeze_end_tick=int(win_start),
        end_tick=int(round_row.get("official_end") or round_row.get("end") or win_end),
        tick_rate=int(tr),
        econ_bucket=econ_bucket,
        econ_value=equip_val,
        round_outcome_reason=str(round_row.get("reason") or ""),
    )


def build_all_signatures(
    arts: Sequence[DemoArtifacts],
    *,
    focal_steamid: int | None,
    pick_focal_per_round: bool = False,
) -> list[RoundSignature]:
    """For pros we pick a focal player per round (one per side) since user_steamid won't appear."""
    sigs: list[RoundSignature] = []
    for art in arts:
        for row in art.rounds.iter_rows(named=True):
            if pick_focal_per_round:
                # Build one signature per side using a representative player as focal.
                for side in ("ct", "t"):
                    sub = art.ticks.filter(
                        (pl.col("round_num") == row["round_num"])
                        & (pl.col("side") == side)
                        & pl.col("is_alive")
                    ).select("steamid").drop_nulls().head(1)
                    if sub.height == 0:
                        continue
                    sig = build_signature(art, row, int(sub["steamid"][0]))
                    if sig is not None:
                        sigs.append(sig)
            else:
                if focal_steamid is None:
                    continue
                sig = build_signature(art, row, int(focal_steamid))
                if sig is not None:
                    sigs.append(sig)
    return sigs


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _hist_is_empty(h: dict[str, float]) -> bool:
    return sum(h.values()) <= 1e-9


def _windowed_min_similarity(
    a_windows: Sequence[dict[str, float]],
    b_windows: Sequence[dict[str, float]],
) -> tuple[float, list[float]]:
    """Min similarity across paired sub-windows.

    Windows in which either side has no alive data (e.g. the focal player is
    already dead) are skipped — otherwise an empty histogram zeros out the min
    across the board and every candidate looks equally bad. If every window is
    empty on at least one side, fall back to 0.
    """
    per: list[float] = []
    for a, b in zip(a_windows, b_windows):
        if _hist_is_empty(a) or _hist_is_empty(b):
            continue
        per.append(_hist_similarity(a, b))
    if not per:
        return 0.0, []
    return float(min(per)), per


def _outcome_match(user: RoundSignature, pro: RoundSignature) -> float:
    """Coaching value: prefer pro WON when user LOST, also reward shared plant-state."""
    out = 0.5 if pro.won else 0.0
    if pro.won and not user.won:
        out = 1.0
    plant_match = 1.0 if user.plant_site == pro.plant_site else 0.0
    return 0.7 * out + 0.3 * plant_match


def score_pair(user: RoundSignature, pro: RoundSignature) -> dict[str, float]:
    """Return per-component scores plus the final score and a hard_reject flag.

    Macro components are min-pooled across WINDOWS_SEC sub-windows, so a round
    that "starts the same and diverges" cannot ride a high early-window score
    into a top match.
    """
    # Side must match: a CT round must be coached against a CT pro round.
    if user.user_side != pro.user_side:
        return {"final": 0.0, "hard_reject": 1.0, "reason_side": 1.0,
                "enemy": 0.0, "ally": 0.0, "focal": 0.0, "coach": 0.0,
                "econ_distance": float(_econ_distance(user.econ_bucket, pro.econ_bucket))}

    # Economy gate: pistol↔full_buy and any 2-step jump are rejected.
    econ_dist = _econ_distance(user.econ_bucket, pro.econ_bucket)
    if econ_dist > ECON_MAX_DISTANCE:
        return {"final": 0.0, "hard_reject": 1.0, "reason_econ": 1.0,
                "enemy": 0.0, "ally": 0.0, "focal": 0.0, "coach": 0.0,
                "econ_distance": float(econ_dist)}

    # Hard gate on confident enemy-site mismatch.
    site_gate = (
        user.enemy_site_intent in ("a", "b")
        and pro.enemy_site_intent in ("a", "b")
        and user.enemy_site_conf >= SITE_CONF_GATE
        and pro.enemy_site_conf >= SITE_CONF_GATE
        and user.enemy_site_intent != pro.enemy_site_intent
    )

    enemy, enemy_per = _windowed_min_similarity(user.enemy_zone_hist_windows, pro.enemy_zone_hist_windows)
    ally,  ally_per  = _windowed_min_similarity(user.ally_zone_hist_windows,  pro.ally_zone_hist_windows)
    focal, focal_per = _windowed_min_similarity(user.focal_zone_hist_windows, pro.focal_zone_hist_windows)
    coach = _outcome_match(user, pro)

    final = W_ENEMY * enemy + W_ALLY * ally + W_FOCAL * focal + W_COACH * coach

    if site_gate:
        return {"final": 0.0, "hard_reject": 1.0, "reason_intent": 1.0,
                "enemy": enemy, "ally": ally, "focal": focal, "coach": coach,
                "econ_distance": float(econ_dist)}

    # Soft penalty when first-contact side disagrees in non-spawn cases.
    if user.first_contact_side in ("a", "b") and pro.first_contact_side in ("a", "b") \
            and user.first_contact_side != pro.first_contact_side:
        final *= 0.7

    # Soft penalty for a 1-step economy gap (full_buy vs force still scores, just discounted).
    if econ_dist == 1:
        final *= 0.92

    return {
        "final": float(final),
        "hard_reject": 0.0,
        "enemy": float(enemy),
        "ally": float(ally),
        "focal": float(focal),
        "coach": float(coach),
        "econ_distance": float(econ_dist),
        "enemy_per_window": enemy_per,
        "ally_per_window": ally_per,
        "focal_per_window": focal_per,
    }


def best_pro_match(
    user_sig: RoundSignature,
    pro_sigs: Iterable[RoundSignature],
) -> tuple[RoundSignature | None, dict[str, float]]:
    best: tuple[RoundSignature | None, dict[str, float]] = (None, {"final": -1.0, "hard_reject": 0.0,
                                                                    "enemy": 0.0, "ally": 0.0, "focal": 0.0, "coach": 0.0})
    for pro in pro_sigs:
        s = score_pair(user_sig, pro)
        if s["hard_reject"]:
            continue
        if s["final"] > best[1]["final"]:
            best = (pro, s)
    return best
