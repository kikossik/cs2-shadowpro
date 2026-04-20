#!/usr/bin/env python3.13
"""CS2 2D Round Replayer — map-agnostic, supports all 7 competitive maps.

Usage:
    python viewer/main.py <demo.dem> [--map de_mirage] [--cache-dir /tmp/cache]

Controls:
    Space          — pause / play
    ← / →          — step ±1 second
    Shift + ← / →  — step ±5 seconds
    [ / ]           — previous / next round
    Home            — restart current round
    L               — toggle upper/lower level  (Nuke only)
    Q / Esc         — quit
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from viewer.cache import DemoCache, DemoData
from viewer.maps import MAPS, MapConfig, detect_map
from viewer.renderer import (
    BG_COLOR, DIM_COLOR,
    decode_weapon, draw_hud, draw_player, draw_utilities, world_to_screen,
)


# ── Window / playback constants ────────────────────────────────────────────────
WINDOW_W       = 1280
WINDOW_H       =  880
HUD_H          =   80
FPS            =   60
TICKRATE       =   64
PLAYBACK_SPEED = 1.0


# ── Round state ────────────────────────────────────────────────────────────────
@dataclass
class RoundState:
    ticks: pl.DataFrame
    tick_list: list[int]
    freeze_end: int
    weapon_map: dict[str, list[str | None]]
    smokes: pl.DataFrame
    infernos: pl.DataFrame
    flashes: pl.DataFrame
    grenade_paths: dict[int, dict] = field(default_factory=dict)


def _build_weapon_map(
    shots_df: pl.DataFrame,
    round_num: int,
    tick_list: list[int],
) -> dict[str, list[str | None]]:
    r_shots = shots_df.filter(pl.col('round_num') == round_num).sort('tick')
    by_player: dict[str, list[tuple[int, str]]] = {}
    for row in r_shots.iter_rows(named=True):
        sid = str(row['player_steamid'])
        wpn = decode_weapon(str(row['weapon']))
        by_player.setdefault(sid, []).append((int(row['tick']), wpn))

    result: dict[str, list[str | None]] = {}
    for sid, events in by_player.items():
        arr: list[str | None] = []
        ei = 0
        for t in tick_list:
            while ei + 1 < len(events) and events[ei + 1][0] <= t:
                ei += 1
            arr.append(events[ei][1] if events[ei][0] <= t else None)
        result[sid] = arr
    return result


def _build_grenade_paths(grenade_paths_all: pl.DataFrame, round_num: int) -> dict[int, dict]:
    raw = grenade_paths_all.filter(pl.col('round_num') == round_num).sort('tick')
    r_gp: dict[int, dict] = {}
    for row in raw.iter_rows(named=True):
        if row['X'] is None or row['Y'] is None:
            continue
        eid = int(row['entity_id'])
        if eid not in r_gp:
            r_gp[eid] = {'type': row['grenade_type'], 'ticks': [], 'xs': [], 'ys': []}
        r_gp[eid]['ticks'].append(int(row['tick']))
        r_gp[eid]['xs'].append(float(row['X']))
        r_gp[eid]['ys'].append(float(row['Y']))
    return r_gp


# ── Replayer ───────────────────────────────────────────────────────────────────
class Replayer:
    def __init__(
        self,
        demo_path: Path,
        map_cfg: MapConfig,
        cache_dir: Path | None = None,
    ) -> None:
        self.map_cfg      = map_cfg
        self.data: DemoData = DemoCache(demo_path, cache_dir).get()
        self._showing_lower = False

    # ── Public ─────────────────────────────────────────────────────────────────
    def run(self) -> None:
        try:
            import pygame
        except ImportError:
            print("pygame not found — install it with:\n"
                  "  pip install pygame")
            sys.exit(1)

        pygame.init()
        screen = pygame.display.set_mode((WINDOW_W, WINDOW_H))
        pygame.display.set_caption(
            f"CS2 Replayer — {self.map_cfg.display_name}"
        )
        clock = pygame.time.Clock()

        font    = pygame.font.SysFont("monospace", 18, bold=True)
        font_sm = pygame.font.SysFont("monospace", 12)
        font_xs = pygame.font.SysFont("monospace", 11)

        # Load radar image(s)
        map_raw       = pygame.image.load(str(self.map_cfg.radar_path)).convert()
        lower_raw     = None
        if self.map_cfg.has_lower_level and self.map_cfg.lower_radar_path:
            lower_raw = pygame.image.load(str(self.map_cfg.lower_radar_path)).convert()

        img_w, img_h  = map_raw.get_size()
        map_area_h    = WINDOW_H - HUD_H
        disp_size     = min(WINDOW_W, map_area_h)
        off_x         = (WINDOW_W  - disp_size) // 2
        off_y         = (map_area_h - disp_size) // 2
        map_surf      = pygame.transform.smoothscale(map_raw, (disp_size, disp_size))
        lower_surf    = (
            pygame.transform.smoothscale(lower_raw, (disp_size, disp_size))
            if lower_raw else None
        )

        BAR_H = 14
        BAR_X = 14
        BAR_Y = WINDOW_H - HUD_H + 54
        BAR_W = WINDOW_W - 28

        has_yaw   = 'yaw'            in set(self.data.ticks.columns)
        has_flash = 'flash_duration' in set(self.data.ticks.columns)

        round_nums = sorted(self.data.ticks['round_num'].unique().to_list())
        rnd_idx    = 0
        state      = self._load_round(round_nums[rnd_idx])
        tick_idx   = 0
        paused     = False
        accum      = 0.0
        scrubbing  = False

        running = True
        while running:
            dt = clock.tick(FPS) / 1000.0

            # ── Events ─────────────────────────────────────────────────────────
            for ev in pygame.event.get():
                if ev.type == pygame.QUIT:
                    running = False

                elif ev.type == pygame.KEYDOWN:
                    mods = pygame.key.get_mods()
                    k    = ev.key

                    if k in (pygame.K_q, pygame.K_ESCAPE):
                        running = False
                    elif k == pygame.K_SPACE:
                        paused = not paused; accum = 0.0
                    elif k == pygame.K_HOME:
                        tick_idx = 0; accum = 0.0
                    elif k == pygame.K_l and self.map_cfg.has_lower_level:
                        self._showing_lower = not self._showing_lower
                    else:
                        step = int((5.0 if mods & pygame.KMOD_SHIFT else 1.0) * TICKRATE)
                        if k == pygame.K_RIGHT:
                            tick_idx = min(len(state.tick_list) - 1, tick_idx + step); accum = 0.0
                        elif k == pygame.K_LEFT:
                            tick_idx = max(0, tick_idx - step); accum = 0.0
                        elif k == pygame.K_RIGHTBRACKET:
                            rnd_idx  = min(len(round_nums) - 1, rnd_idx + 1)
                            state    = self._load_round(round_nums[rnd_idx])
                            tick_idx = 0; accum = 0.0; paused = False
                        elif k == pygame.K_LEFTBRACKET:
                            rnd_idx  = max(0, rnd_idx - 1)
                            state    = self._load_round(round_nums[rnd_idx])
                            tick_idx = 0; accum = 0.0; paused = False

                elif ev.type == pygame.MOUSEBUTTONDOWN and ev.button == 1:
                    mx, my = ev.pos
                    if BAR_X <= mx <= BAR_X + BAR_W and BAR_Y - 8 <= my <= BAR_Y + BAR_H + 8:
                        scrubbing = True
                        prog      = (mx - BAR_X) / BAR_W
                        tick_idx  = max(0, min(len(state.tick_list) - 1,
                                               int(prog * (len(state.tick_list) - 1))))
                elif ev.type == pygame.MOUSEMOTION and scrubbing:
                    mx, _    = ev.pos
                    prog     = (mx - BAR_X) / BAR_W
                    tick_idx = max(0, min(len(state.tick_list) - 1,
                                          int(prog * (len(state.tick_list) - 1))))
                elif ev.type == pygame.MOUSEBUTTONUP and ev.button == 1:
                    scrubbing = False

            # ── Playback advance ───────────────────────────────────────────────
            if not paused and not scrubbing:
                accum   += dt * TICKRATE * PLAYBACK_SPEED
                steps    = int(accum)
                accum   -= steps
                tick_idx += steps
                if tick_idx >= len(state.tick_list):
                    tick_idx = len(state.tick_list) - 1
                    paused   = True

            # ── Render ─────────────────────────────────────────────────────────
            screen.fill(BG_COLOR)

            active_surf = lower_surf if (self._showing_lower and lower_surf) else map_surf
            screen.blit(active_surf, (off_x, off_y))

            cur_tick = state.tick_list[tick_idx]

            draw_utilities(
                screen,
                state.smokes, state.infernos, state.flashes, state.grenade_paths,
                cur_tick, self.map_cfg,
                img_w, img_h, disp_size, off_x, off_y,
                font_xs,
                window_w=WINDOW_W,
                window_h_map=WINDOW_H - HUD_H,
            )

            snap = state.ticks.filter(pl.col('tick') == cur_tick)
            for row in snap.iter_rows(named=True):
                wx = row.get('X') or 0.0
                wy = row.get('Y') or 0.0
                wz = row.get('Z') or 0.0
                px, py = world_to_screen(
                    wx, wy, self.map_cfg, img_w, img_h, disp_size, off_x, off_y,
                )
                sid       = str(row.get('steamid') or '')
                wpn_list  = state.weapon_map.get(sid, [])
                active_wpn = wpn_list[tick_idx] if tick_idx < len(wpn_list) else None

                # Ghost players on the other level (Nuke)
                ghost = (
                    self.map_cfg.has_lower_level
                    and self.map_cfg.is_lower(wz) != self._showing_lower
                )
                draw_player(
                    screen, px, py, row,
                    font_name=font_xs,
                    font_wpn=font_xs,
                    has_yaw=has_yaw,
                    has_flash=has_flash,
                    active_wpn=active_wpn or '',
                    ghost=ghost,
                )

            level_badge = None
            if self.map_cfg.has_lower_level:
                level_badge = "[LOWER]" if self._showing_lower else "[UPPER]"

            draw_hud(
                screen, font, font_sm,
                rnd=round_nums[rnd_idx],
                cur_tick=cur_tick,
                freeze_end_tick=state.freeze_end,
                tick_idx=tick_idx,
                n_ticks=len(state.tick_list),
                paused=paused,
                bar_x=BAR_X, bar_y=BAR_Y, bar_w=BAR_W, bar_h=BAR_H,
                window_w=WINDOW_W,
                window_h=WINDOW_H,
                hud_h=HUD_H,
                tickrate=TICKRATE,
                level_badge=level_badge,
            )

            pygame.display.flip()

        pygame.quit()
        sys.exit(0)

    # ── Private ────────────────────────────────────────────────────────────────
    def _load_round(self, rnum: int) -> RoundState:
        rt   = self.data.ticks.filter(pl.col('round_num') == rnum).sort('tick')
        tl   = sorted(rt['tick'].unique().to_list())
        r_r  = self.data.rounds.filter(pl.col('round_num') == rnum)
        fe   = int(r_r['freeze_end'][0]) if r_r.height > 0 else (tl[0] if tl else 0)
        wmap = _build_weapon_map(self.data.shots, rnum, tl)
        return RoundState(
            ticks        = rt,
            tick_list    = tl,
            freeze_end   = fe,
            weapon_map   = wmap,
            smokes       = self.data.smokes.filter(pl.col('round_num') == rnum),
            infernos     = self.data.infernos.filter(pl.col('round_num') == rnum),
            flashes      = self.data.flashes.filter(pl.col('round_num') == rnum),
            grenade_paths= _build_grenade_paths(self.data.grenade_paths, rnum),
        )


# ── CLI ────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="CS2 2D round replayer")
    parser.add_argument("demo", type=Path, help="Path to .dem file")
    parser.add_argument("--map", dest="map_name", default=None,
                        help="Override map name (e.g. de_mirage)")
    parser.add_argument("--cache-dir", type=Path, default=None,
                        help="Directory to store Parquet cache files")
    args = parser.parse_args()

    demo_path: Path = args.demo
    if not demo_path.exists():
        print(f"Demo not found: {demo_path}")
        sys.exit(1)

    # Resolve map config
    if args.map_name:
        map_cfg = MAPS.get(args.map_name)
        if map_cfg is None:
            print(f"Unknown map: {args.map_name}. Available: {', '.join(MAPS)}")
            sys.exit(1)
    else:
        print("Detecting map from demo header …", flush=True)
        map_cfg = detect_map(demo_path)
        if map_cfg is None:
            print("Could not auto-detect map. Use --map <map_name>")
            sys.exit(1)
        print(f"  Detected: {map_cfg.display_name} ({map_cfg.name})", flush=True)

    if not map_cfg.radar_path.exists():
        print(f"Radar image not found: {map_cfg.radar_path}\n"
              "Run: python -c \"import awpy.data; awpy.data.download()\"")
        sys.exit(1)

    Replayer(demo_path, map_cfg, cache_dir=args.cache_dir).run()


if __name__ == "__main__":
    main()
