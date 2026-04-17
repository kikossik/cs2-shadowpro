#!/usr/bin/env python3.13
"""
Decompress demos/*.dem.bz2 → demos_decompressed/*.dem.

Streaming, chunked, resume-safe (skips if target exists).

Usage:
    python decompress_demos.py
"""

import bz2
import time
from pathlib import Path

SRC_DIR = Path("demos")
DST_DIR = Path("demos_decompressed")
CHUNK = 1 << 20  # 1 MiB


def decompress_one(src: Path, dst: Path) -> None:
    dst_part = dst.with_name(dst.name + ".part")
    dst_part.unlink(missing_ok=True)

    start = time.monotonic()
    with bz2.open(src, "rb") as fin, open(dst_part, "wb") as fout:
        while True:
            chunk = fin.read(CHUNK)
            if not chunk:
                break
            fout.write(chunk)

    dst_part.rename(dst)
    elapsed = int(time.monotonic() - start)
    src_mb = src.stat().st_size / 1_048_576
    dst_mb = dst.stat().st_size / 1_048_576
    print(f"  → {dst.name}  ({src_mb:.0f} → {dst_mb:.0f} MB, {elapsed}s)")


def main() -> None:
    DST_DIR.mkdir(exist_ok=True)
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
            decompress_one(src, dst)
            ok += 1
        except Exception as e:
            print(f"  ERROR: {e}")

    print(f"\nDone: {ok}/{len(sources)} decompressed → {DST_DIR}/")


main()
