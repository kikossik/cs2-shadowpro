"""Load parsed-demo artifacts from parquet, or parse on demand."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class DemoArtifacts:
    demo_id: str
    role: str             # "user" | "pro"
    map_name: str
    rounds: pl.DataFrame
    ticks: pl.DataFrame
    bomb: pl.DataFrame
    kills: pl.DataFrame
    tick_rate: int = 64   # user demos are 64-tick CSGO/CS2 MM; pro HLTV are 64. close enough.


def _read_parquet_dir(parquet_dir: Path, demo_id: str) -> dict[str, pl.DataFrame | dict]:
    """Read a parquet bundle written by processing.py / pipeline.steps.ingest."""
    out: dict[str, pl.DataFrame | dict] = {}
    for kind in ("rounds", "ticks", "bomb", "kills"):
        candidates = list(parquet_dir.glob(f"*_{kind}.parquet"))
        if not candidates:
            raise FileNotFoundError(f"missing {kind}.parquet in {parquet_dir}")
        out[kind] = pl.read_parquet(candidates[0])
    hdr_path = next(parquet_dir.glob("*_header.json"))
    out["header"] = json.loads(hdr_path.read_text())
    return out


def load_user_artifacts(steam_id: str, map_filter: str | None = None) -> list[DemoArtifacts]:
    """Each user demo is parsed into its own subfolder under parquet_user/<steam_id>/."""
    base = REPO_ROOT / "parquet_user" / steam_id
    out: list[DemoArtifacts] = []
    for d in sorted(base.iterdir()):
        if not d.is_dir():
            continue
        try:
            bundle = _read_parquet_dir(d, d.name)
        except FileNotFoundError:
            continue
        map_name = bundle["header"].get("map_name", "")
        if map_filter and map_name != map_filter:
            continue
        out.append(
            DemoArtifacts(
                demo_id=d.name,
                role="user",
                map_name=map_name,
                rounds=bundle["rounds"],
                ticks=bundle["ticks"],
                bomb=bundle["bomb"],
                kills=bundle["kills"],
            )
        )
    return out


def load_pro_artifacts(map_filter: str | None = None) -> list[DemoArtifacts]:
    """Pro parquet bundles live one level deep under parquet_pro/<match>/."""
    base = REPO_ROOT / "parquet_pro"
    out: list[DemoArtifacts] = []
    for d in sorted(base.iterdir()):
        if not d.is_dir():
            continue
        try:
            bundle = _read_parquet_dir(d, d.name)
        except (FileNotFoundError, StopIteration):
            continue
        map_name = bundle["header"].get("map_name", "")
        if map_filter and map_name != map_filter:
            continue
        out.append(
            DemoArtifacts(
                demo_id=d.name,
                role="pro",
                map_name=map_name,
                rounds=bundle["rounds"],
                ticks=bundle["ticks"],
                bomb=bundle["bomb"],
                kills=bundle["kills"],
            )
        )
    return out


def detect_user_steamid(artifacts: list[DemoArtifacts]) -> int | None:
    """Pick the steamid that appears in every user demo. Falls back to the most common."""
    if not artifacts:
        return None
    sets: list[set[int]] = []
    counts: dict[int, int] = {}
    for art in artifacts:
        ids = set(int(x) for x in art.ticks["steamid"].drop_nulls().unique().to_list())
        sets.append(ids)
        for sid in ids:
            counts[sid] = counts.get(sid, 0) + 1
    intersection = set.intersection(*sets) if sets else set()
    if intersection:
        return next(iter(intersection))
    return max(counts, key=counts.get) if counts else None
