#!/usr/bin/env python3.13
"""
Parse one decompressed CS2 demo with awpy and dump what's available.

Exploratory: prints each DataFrame awpy produces (rounds, ticks, kills,
grenades, ...) with its columns and a small sample, and saves full
DataFrames as Parquet under parsed_sample/ for off-line inspection.

Usage:
    python parse_one_demo.py demos_decompressed/<file>.dem
"""

import sys
from pathlib import Path

import polars as pl
from awpy import Demo

OUT_DIR = Path("parsed_sample")

# Extra per-tick player fields beyond awpy's default (X/Y/Z/health/side/place/name/steamid).
# Needed to classify economy tier and equipment for situation matching.
PLAYER_PROPS = [
    "balance",
    "active_weapon",
    "armor_value",
    "has_defuser",
    "flash_duration",
    "inventory",
]


def main(path: Path) -> None:
    print(f"Parsing: {path}")
    dem = Demo(path=str(path))
    dem.parse(player_props=PLAYER_PROPS)

    header = getattr(dem, "header", None)
    print("\n=== header ===")
    print(header)

    df_attrs: dict[str, pl.DataFrame] = {}
    for name in dir(dem):
        if name.startswith("_"):
            continue
        try:
            val = getattr(dem, name)
        except Exception:
            continue
        if isinstance(val, pl.DataFrame):
            df_attrs[name] = val

    print(f"\nDataFrame attributes: {sorted(df_attrs)}")

    OUT_DIR.mkdir(exist_ok=True)
    for name, df in df_attrs.items():
        print(f"\n=== {name}  (rows={len(df)}, cols={len(df.columns)}) ===")
        print("columns:", df.columns)
        if len(df):
            print(df.head(3))
        df.write_parquet(OUT_DIR / f"{name}.parquet")

    print(f"\nSaved full DataFrames → {OUT_DIR}/")


if len(sys.argv) != 2:
    sys.exit("usage: parse_one_demo.py <path/to/file.dem>")
main(Path(sys.argv[1]))
