"""Extract .dem files from HLTV RAR archives.

HLTV archives are named *.dem.bz2 but are actually multi-map RAR bundles.
Only maps in the current 7-map competitive pool are extracted.
"""
from __future__ import annotations

import re
import time
from pathlib import Path

import rarfile

from backend.log import get_logger

log = get_logger("DECOMPRESS")

KNOWN_MAPS = ("mirage", "dust2", "inferno", "nuke", "anubis", "ancient", "overpass")


def _detect_map(member_name: str) -> str:
    low = member_name.lower()
    for m in KNOWN_MAPS:
        if m in low:
            return m
    stem = Path(member_name).stem
    return re.sub(r"[^a-z0-9]+", "-", stem.lower()).strip("-") or "unknown"


def extract_all_dems(archive: Path, dst_dir: Path) -> list[Path]:
    """Extract every .dem member from a RAR archive. Returns list of extracted Paths."""
    dst_dir.mkdir(parents=True, exist_ok=True)
    stem = archive.name.removesuffix(".dem.bz2").removesuffix(".bz2").removesuffix(".dem")

    extracted: list[Path] = []
    with rarfile.RarFile(archive) as rf:
        dem_members = [m for m in rf.namelist() if m.lower().endswith(".dem")]
        if not dem_members:
            raise ValueError(f"archive {archive.name} contains no .dem members")

        for member in dem_members:
            tag = _detect_map(member)
            dst = dst_dir / f"{stem}_{tag}.dem"

            if dst.exists():
                log.info("SKIP (exists): %s", dst.name)
                extracted.append(dst)
                continue

            dst_part = dst.with_name(dst.name + ".part")
            dst_part.unlink(missing_ok=True)

            start = time.monotonic()
            with rf.open(member) as fin, open(dst_part, "wb") as fout:
                while chunk := fin.read(1 << 20):
                    fout.write(chunk)
            dst_part.rename(dst)
            elapsed = int(time.monotonic() - start)
            size_mb = dst.stat().st_size / 1_048_576
            log.info("-> %s (%.0f MB, %ds) [from: %s]", dst.name, size_mb, elapsed, member)
            extracted.append(dst)

    return extracted
