"""Entrypoint: macro-first round mapping over Mirage demos + side-by-side pygame.

Usage:
    python -m tests.run_test --steam-id 76561198857367828 --map de_mirage
    python -m tests.run_test --no-viz                 # print mapping table only
    python -m tests.run_test --user-round 5           # inspect one round only
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from tests.data_loader import (
    detect_user_steamid,
    load_pro_artifacts,
    load_user_artifacts,
)
from tests.macro_mapper import (
    best_pro_match,
    build_all_signatures,
    build_signature,
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--steam-id", required=False, default="76561198857367828")
    p.add_argument("--map", default="de_mirage")
    p.add_argument("--no-viz", action="store_true", help="skip pygame visualization")
    p.add_argument("--user-round", type=int, default=None,
                   help="optional: only show this round (1-indexed within whichever user demo)")
    p.add_argument("--user-demo", type=str, default=None,
                   help="optional: only use this user demo id")
    p.add_argument("--limit", type=int, default=None,
                   help="optional: cap on the number of pairs visualized")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    print(f"Loading user demos for steam_id={args.steam_id}, map={args.map}")
    user_arts = load_user_artifacts(args.steam_id, map_filter=args.map)
    if not user_arts:
        print(f"No user demos found for steam_id={args.steam_id} on {args.map}.")
        return 1
    if args.user_demo:
        # accept a substring so the long parquet folder name doesn't have to be typed in full
        match = [a for a in user_arts if args.user_demo in a.demo_id]
        if not match:
            print(f"--user-demo='{args.user_demo}' matched none of the available {args.map} demos:")
            for a in user_arts:
                print(f"  {a.demo_id}")
            return 1
        user_arts = match
    print(f"  found {len(user_arts)} user demo(s).")

    print(f"Loading pro demos for map={args.map}")
    pro_arts = load_pro_artifacts(map_filter=args.map)
    if not pro_arts:
        print(f"No pro demos found for {args.map}.")
        return 1
    print(f"  found {len(pro_arts)} pro demo(s).")

    # Prefer the explicit --steam-id when it actually plays in the demos; otherwise auto-detect.
    explicit = int(args.steam_id) if args.steam_id and args.steam_id.isdigit() else None
    if explicit is not None and any(
        explicit in set(int(x) for x in art.ticks["steamid"].drop_nulls().unique().to_list())
        for art in user_arts
    ):
        user_steamid = explicit
        print(f"User steamid (explicit): {user_steamid}")
    else:
        user_steamid = detect_user_steamid(user_arts)
        if user_steamid is None:
            print("Could not detect a user steamid that appears in every user demo.")
            return 2
        print(f"User steamid detected: {user_steamid}")

    print("Building pro round signatures (one per side per round) …")
    pro_sigs = build_all_signatures(pro_arts, focal_steamid=None, pick_focal_per_round=True)
    print(f"  built {len(pro_sigs)} pro signatures")

    print("Building user round signatures …")
    user_sigs = []
    for art in user_arts:
        for row in art.rounds.iter_rows(named=True):
            if args.user_round is not None and int(row["round_num"]) != args.user_round:
                continue
            sig = build_signature(art, row, int(user_steamid))
            if sig is not None:
                user_sigs.append((art, sig))
    print(f"  built {len(user_sigs)} user signatures")

    # Map each user round to its best pro round.
    pairs = []
    print()
    print(f"{'demo':32s} {'rd':>3s} {'side':>4s} {'econ':>9s} {'intent':>8s} {'plant':>10s}  →  "
          f"{'pro_demo':40s} {'rd':>3s} {'p_econ':>9s} {'final':>6s}  e/a/f/c")
    for user_art, user_sig in user_sigs:
        pro_sig, score = best_pro_match(user_sig, pro_sigs)
        if pro_sig is None:
            print(f"{user_art.demo_id[:32]:32s} {user_sig.round_num:>3d} "
                  f"{user_sig.user_side:>4s} {user_sig.econ_bucket:>9s} {user_sig.enemy_site_intent:>8s} "
                  f"{user_sig.plant_site:>10s}  →  no candidate")
            continue
        pro_art = next(a for a in pro_arts if a.demo_id == pro_sig.demo_id)
        print(
            f"{user_art.demo_id[:32]:32s} {user_sig.round_num:>3d} "
            f"{user_sig.user_side:>4s} {user_sig.econ_bucket:>9s} {user_sig.enemy_site_intent:>8s} "
            f"{user_sig.plant_site:>10s}  →  "
            f"{pro_sig.demo_id[:40]:40s} {pro_sig.round_num:>3d} "
            f"{pro_sig.econ_bucket:>9s} {score['final']:>6.3f}  "
            f"{score['enemy']:.2f}/{score['ally']:.2f}/{score['focal']:.2f}/{score['coach']:.2f}"
        )
        pairs.append((user_art, user_sig, pro_art, pro_sig, score))

    if args.no_viz or not pairs:
        return 0

    # Lazy-load pygame & radar so --no-viz works without a display.
    from tests.visualizer import PanelSource, _load_map_meta, _load_radar, PANEL, play_paired_rounds

    map_meta = _load_map_meta(args.map)
    radar = _load_radar_safely(args.map)

    panel_pairs = []
    for user_art, user_sig, pro_art, pro_sig, score in pairs[: args.limit]:
        panel_pairs.append((
            PanelSource(title="USER", art=user_art, sig=user_sig, radar=radar, map_meta=map_meta),
            PanelSource(title="PRO",  art=pro_art,  sig=pro_sig,  radar=radar, map_meta=map_meta),
            score,
        ))

    play_paired_rounds(panel_pairs, map_name=args.map)
    return 0


def _load_radar_safely(map_name: str):
    """pygame must be initialized before image.load — defer until display is up."""
    import pygame
    pygame.init()
    pygame.display.set_mode((1, 1), flags=pygame.HIDDEN)
    from tests.visualizer import _load_radar, PANEL
    return _load_radar(map_name, PANEL)


if __name__ == "__main__":
    raise SystemExit(main())
