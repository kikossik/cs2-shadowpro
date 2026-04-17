#!/usr/bin/env python3.13
"""
Extract per-player situation snapshots from a parsed demo.

Reads Parquet files under parsed_sample/ (produced by parse_one_demo.py),
samples decision-moment ticks per round, joins per-player state at each
sampled tick, derives alive counts / phase / time remaining / utility counts
/ economy bucket, and writes situations_sample.parquet.

Usage:
    python extract_situations.py
"""

from pathlib import Path

import polars as pl

PARSED = Path("parsed_sample")
OUT = Path("situations_sample.parquet")

TICKRATE = 64              # confirmed: (rounds.freeze_end - rounds.start).median() == 1280 = 20s * 64
ROUND_TIME_S = 115         # CS2 MR12 round playtime
BOMB_TIME_S = 40           # bomb ticking duration after plant
SAMPLE_INTERVAL_S = 5      # heartbeat cadence
DEDUP_WINDOW_S = 1         # collapse samples within this window per player

# Economy classification from `ticks.inventory` (human-readable weapon names).
# Anything not listed here (plus pistols) = eco.
RIFLES = {
    "AK-47", "M4A4", "M4A1-S", "AUG", "SG 553",
    "AWP", "SSG 08", "SCAR-20", "G3SG1",
}
SMGS_SHOTGUNS = {
    "MP9", "MAC-10", "MP7", "MP5-SD", "UMP-45", "P90", "PP-Bizon",
    "Galil AR", "FAMAS",
    "Nova", "XM1014", "MAG-7", "Sawed-Off",
    "Negev", "M249",
}

EVENT_PRIORITY = {"bomb": 0, "kill": 1, "heartbeat": 2}


DECISION_BOMB_EVENTS = {"plant", "defuse", "detonate"}  # skip pickup/drop (administrative)


def build_sample_ticks(rounds: pl.DataFrame, kills: pl.DataFrame, bomb: pl.DataFrame) -> pl.DataFrame:
    rows: list[tuple[int, int, str]] = []
    interval = SAMPLE_INTERVAL_S * TICKRATE

    # Round window lookup for clamping event samples to the actual playing period.
    # Skip rounds with null freeze_end/end (mid-match disconnects / surrenders).
    window = {
        r["round_num"]: (r["freeze_end"], r["end"])
        for r in rounds.iter_rows(named=True)
        if r["freeze_end"] is not None and r["end"] is not None
    }

    for rn, (fe, end) in window.items():
        t = fe
        while t <= end:
            rows.append((rn, t, "heartbeat"))
            t += interval

    for rn, t in kills.select("round_num", "tick").iter_rows():
        if rn in window and window[rn][0] <= t <= window[rn][1]:
            rows.append((rn, t, "kill"))

    bomb_ev = bomb.filter(pl.col("event").is_in(list(DECISION_BOMB_EVENTS)))
    for rn, t in bomb_ev.select("round_num", "tick").iter_rows():
        if rn in window and window[rn][0] <= t <= window[rn][1]:
            rows.append((rn, t, "bomb"))

    return pl.DataFrame(
        rows,
        schema={"round_num": pl.UInt32, "tick": pl.Int32, "source_event": pl.Utf8},
        orient="row",
    )


def extract(
    rounds: pl.DataFrame,
    ticks: pl.DataFrame,
    kills: pl.DataFrame,
    bomb: pl.DataFrame,
    smokes: pl.DataFrame,
    infernos: pl.DataFrame,
    *,
    source: str,
    demo_id: str,
    verbose: bool = False,
) -> pl.DataFrame:
    samples = build_sample_ticks(rounds, kills, bomb)
    if verbose:
        print(f"sample ticks: {samples.height}")

    # Join per-player tick state at each sampled tick (alive only; drop null-side bad rows).
    alive = ticks.filter((pl.col("health") > 0) & pl.col("side").is_not_null())
    merged = samples.join(alive, on=["round_num", "tick"], how="inner")
    if verbose:
        print(f"alive player-samples: {merged.height}")

    # Per-sample alive counts by side.
    alive_counts = (
        merged.group_by(["round_num", "tick"])
        .agg(
            (pl.col("side") == "ct").sum().cast(pl.Int32).alias("alive_ct"),
            (pl.col("side") == "t").sum().cast(pl.Int32).alias("alive_t"),
        )
    )
    merged = merged.join(alive_counts, on=["round_num", "tick"])

    # Round metadata (phase + time_remaining).
    merged = merged.join(
        rounds.select("round_num", "freeze_end", "end", "bomb_plant"),
        on="round_num",
    )

    merged = merged.with_columns(
        pl.when(pl.col("tick") <= pl.col("freeze_end"))
        .then(pl.lit("freeze"))
        .when(pl.col("bomb_plant").is_not_null() & (pl.col("tick") >= pl.col("bomb_plant")))
        .then(pl.lit("post_plant"))
        .otherwise(pl.lit("pre_plant"))
        .alias("phase"),
    )

    round_end_tick = pl.col("freeze_end") + ROUND_TIME_S * TICKRATE
    post_plant_end = pl.col("bomb_plant") + BOMB_TIME_S * TICKRATE
    merged = merged.with_columns(
        pl.when(pl.col("phase") == "freeze")
        .then((pl.col("freeze_end") - pl.col("tick")) / TICKRATE)
        .when(pl.col("phase") == "post_plant")
        .then((post_plant_end - pl.col("tick")) / TICKRATE)
        .otherwise((round_end_tick - pl.col("tick")) / TICKRATE)
        .cast(pl.Float32)
        .alias("time_remaining_s"),
    )

    # Active utility counts (smokes / infernos whose [start_tick, end_tick] covers the sample tick).
    sample_keys = merged.select("round_num", "tick").unique()

    def utility_counts(util: pl.DataFrame, out_col: str) -> pl.DataFrame:
        # polars join_where would be ideal; use plain inner-join-on-round + filter for portability.
        u = util.select(
            pl.col("round_num").cast(pl.UInt32),
            pl.col("start_tick").cast(pl.Int32),
            pl.col("end_tick").cast(pl.Int32),
        )
        joined = sample_keys.join(u, on="round_num", how="left")
        active = joined.with_columns(
            (
                pl.col("start_tick").is_not_null()
                & (pl.col("start_tick") <= pl.col("tick"))
                & (pl.col("tick") <= pl.col("end_tick"))
            ).alias("is_active")
        )
        return (
            active.group_by(["round_num", "tick"])
            .agg(pl.col("is_active").sum().cast(pl.Int32).alias(out_col))
        )

    merged = merged.join(utility_counts(smokes, "smokes_active"), on=["round_num", "tick"], how="left")
    merged = merged.join(utility_counts(infernos, "mollies_active"), on=["round_num", "tick"], how="left")
    merged = merged.with_columns(
        pl.col("smokes_active").fill_null(0),
        pl.col("mollies_active").fill_null(0),
    )

    # Economy bucket from inventory (polars-native; list[str] set intersection).
    inv = pl.col("inventory").fill_null([])
    merged = merged.with_columns(
        pl.when(inv.list.set_intersection(list(RIFLES)).list.len() > 0)
        .then(pl.lit("full"))
        .when(inv.list.set_intersection(list(SMGS_SHOTGUNS)).list.len() > 0)
        .then(pl.lit("semi"))
        .otherwise(pl.lit("eco"))
        .alias("economy_bucket")
    )

    # Clip window (for radar playback later).
    merged = merged.with_columns(
        (pl.col("tick") - 3 * TICKRATE).alias("clip_start_tick"),
        (pl.col("tick") + 12 * TICKRATE).alias("clip_end_tick"),
    )

    # Dedup: one row per (round, player, second-bucket). Keep highest-priority event.
    merged = merged.with_columns(
        (pl.col("tick") // (DEDUP_WINDOW_S * TICKRATE)).alias("_bucket"),
        pl.col("source_event")
        .replace_strict(EVENT_PRIORITY, default=3, return_dtype=pl.Int32)
        .alias("_prio"),
    )
    dedup = (
        merged.sort(["round_num", "steamid", "_bucket", "_prio"])
        .unique(subset=["round_num", "steamid", "_bucket"], keep="first")
        .drop("_bucket", "_prio")
    )

    # Final select.
    result = dedup.select(
        pl.lit(source).alias("source"),
        pl.lit(demo_id).alias("demo_id"),
        pl.col("round_num"),
        pl.col("tick"),
        pl.col("source_event"),
        pl.col("steamid").alias("player_steamid"),
        pl.col("name").alias("player_name"),
        pl.col("side").alias("player_side"),
        pl.col("place").alias("player_place"),
        pl.col("X").alias("player_x"),
        pl.col("Y").alias("player_y"),
        pl.col("Z").alias("player_z"),
        pl.col("balance"),
        pl.col("active_weapon"),
        pl.col("economy_bucket"),
        pl.col("alive_ct"),
        pl.col("alive_t"),
        pl.col("phase"),
        pl.col("time_remaining_s"),
        pl.col("smokes_active"),
        pl.col("mollies_active"),
        pl.col("clip_start_tick"),
        pl.col("clip_end_tick"),
    ).sort(["round_num", "tick", "player_steamid"])

    # Sanity filter: drop rows from round-boundary noise (>5 alive per side, empty place).
    before = result.height
    result = result.filter(
        (pl.col("alive_ct") <= 5)
        & (pl.col("alive_t") <= 5)
        & (pl.col("player_place") != "")
    )
    if verbose:
        print(f"sanity filter: {before} → {result.height} ({before - result.height} dropped)")
    return result


def main() -> None:
    rounds = pl.read_parquet(PARSED / "rounds.parquet")
    ticks = pl.read_parquet(PARSED / "ticks.parquet")
    kills = pl.read_parquet(PARSED / "kills.parquet")
    bomb = pl.read_parquet(PARSED / "bomb.parquet")
    smokes = pl.read_parquet(PARSED / "smokes.parquet")
    infernos = pl.read_parquet(PARSED / "infernos.parquet")

    result = extract(
        rounds, ticks, kills, bomb, smokes, infernos,
        source="pro", demo_id="sample", verbose=True,
    )

    result.write_parquet(OUT)
    print(f"\nWrote {OUT}: {result.height} situations")
    print("\nby source_event:")
    print(result.group_by("source_event").len().sort("len", descending=True))
    print("\nby phase:")
    print(result.group_by("phase").len().sort("len", descending=True))
    print("\nby economy_bucket × side:")
    print(result.group_by(["economy_bucket", "player_side"]).len().sort("len", descending=True))
    print("\nalive counts distribution:")
    print(result.group_by(["alive_ct", "alive_t"]).len().sort("len", descending=True).head(10))


if __name__ == "__main__":
    main()
