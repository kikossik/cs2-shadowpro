#!/usr/bin/env python3.13
"""
Extract Mirage demos from HLTV RAR archives (*.dem.bz2) → demos_decompressed/*.dem.

HLTV packages demos as RAR archives despite the .dem.bz2 extension. Each archive
may contain multiple map demos (BO3/BO5); this script extracts only the Mirage one.

Resume-safe: skips if the target .dem already exists.

Usage:
    python decompress_demos.py
"""

import time
from pathlib import Path

import rarfile

SRC_DIR = Path("demos")
DST_DIR = Path("demos_decompressed")


def extract_mirage(src: Path, dst: Path) -> None:
    dst_part = dst.with_name(dst.name + ".part")
    dst_part.unlink(missing_ok=True)

    with rarfile.RarFile(src) as rf:
        members = rf.namelist()
        mirage = next(
            (m for m in members if "mirage" in m.lower() and m.endswith(".dem")),
            None,
        )
        if mirage is None:
            raise ValueError(f"No mirage .dem found in archive. Contents: {members}")

        start = time.monotonic()
        with rf.open(mirage) as fin, open(dst_part, "wb") as fout:
            while chunk := fin.read(1 << 20):
                fout.write(chunk)

    dst_part.rename(dst)
    elapsed = int(time.monotonic() - start)
    dst_mb = dst.stat().st_size / 1_048_576
    print(f"  → {dst.name}  ({dst_mb:.0f} MB, {elapsed}s)  [from: {mirage}]")


def main() -> None:
    DST_DIR.mkdir(exist_ok=True)
    # Remove stale .part files from previous failed runs
    for stale in DST_DIR.glob("*.part"):
        stale.unlink()
        print(f"  removed stale: {stale.name}")

    sources = sorted(SRC_DIR.glob("*.dem.bz2"))
    if not sources:
        print(f"No .dem.bz2 files in {SRC_DIR}/")
        return

    ok = 0
    for i, src in enumerate(sources, 1):
        dst = DST_DIR / src.name.removesuffix(".bz2")
        print(f"[{i:2}/{len(sources)}] {src.name}", flush=True)

        if dst.exists():
            print(f"  SKIP (exists): {dst.name}")
            ok += 1
            continue

        try:
            extract_mirage(src, dst)
            ok += 1
        except Exception as e:
            print(f"  ERROR: {e}")

    print(f"\nDone: {ok}/{len(sources)} extracted → {DST_DIR}/")


main()
