"""Side-by-side pygame visualizer for user vs mapped pro rounds."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import numpy as np
import polars as pl
import pygame

from tests.data_loader import DemoArtifacts, REPO_ROOT
from tests.macro_mapper import RoundSignature, EARLY_WINDOW_SEC

PANEL = 720           # pixel size of each radar panel
HUD_H = 110           # info bar above each panel
PAD = 12

PLAYBACK_HZ = 16      # sim ticks shown per real second
SECONDS_PER_ROUND_CAP = 95.0   # safety cap so dragged-out rounds don't stall

# Map metadata loaded once from awpy
MAP_DATA_PATH = Path.home() / ".awpy" / "maps" / "map-data.json"


@dataclass
class PanelSource:
    title: str
    art: DemoArtifacts
    sig: RoundSignature
    radar: pygame.Surface
    map_meta: dict


def _load_map_meta(map_name: str) -> dict:
    md = json.loads(MAP_DATA_PATH.read_text())
    return md[map_name]


def _load_radar(map_name: str, size: int) -> pygame.Surface:
    radar_path = Path.home() / ".awpy" / "maps" / f"{map_name}.png"
    if not radar_path.exists():
        # fall back to repo radar
        radar_path = REPO_ROOT / "web" / "public" / "maps" / f"{map_name}.png"
    img = pygame.image.load(str(radar_path)).convert()
    return pygame.transform.smoothscale(img, (size, size))


def _world_to_panel(x: float, y: float, meta: dict, panel_size: int, image_size: int = 1024) -> tuple[int, int]:
    rx = (x - meta["pos_x"]) / meta["scale"]
    ry = (meta["pos_y"] - y) / meta["scale"]
    sx = rx * (panel_size / image_size)
    sy = ry * (panel_size / image_size)
    return int(sx), int(sy)


def _round_window_ticks(art: DemoArtifacts, round_num: int) -> tuple[int, int, int]:
    row = art.rounds.filter(pl.col("round_num") == round_num).head(1).to_dicts()[0]
    tr = art.tick_rate
    start = int(row["freeze_end"])
    end = int(row["official_end"] or row["end"] or start)
    return start, end, tr


def _ticks_for_render(art: DemoArtifacts, round_num: int) -> pl.DataFrame:
    """All players' positions for this round, columns: tick, steamid, side, X, Y, is_alive, name."""
    return (
        art.ticks.filter(pl.col("round_num") == round_num)
        .select(["tick", "steamid", "side", "X", "Y", "is_alive", "name", "place"])
        .sort("tick")
    )


def _samples_at_hz(df: pl.DataFrame, start_tick: int, end_tick: int, tick_rate: int, hz: int) -> list[int]:
    step = max(1, tick_rate // hz)
    return list(range(start_tick, end_tick + 1, step))


def _interp_positions(player_df: pl.DataFrame, sample_ticks: list[int]) -> dict[int, tuple[float, float, bool]]:
    """For one player's slice (already filtered), return tick -> (x, y, alive) by nearest-prev fill."""
    if player_df.height == 0:
        return {}
    ticks = player_df["tick"].to_list()
    xs = player_df["X"].to_list()
    ys = player_df["Y"].to_list()
    alive_col = player_df["is_alive"].to_list()
    ticks_arr = np.array(ticks)

    out: dict[int, tuple[float, float, bool]] = {}
    for st in sample_ticks:
        idx = np.searchsorted(ticks_arr, st, side="right") - 1
        if idx < 0:
            idx = 0
        out[st] = (float(xs[idx]), float(ys[idx]), bool(alive_col[idx]))
    return out


def _build_player_streams(art: DemoArtifacts, round_num: int, sample_ticks: list[int]) -> list[dict]:
    """One dict per player with positions over the sampled ticks."""
    rt = _ticks_for_render(art, round_num)
    out: list[dict] = []
    for sid in rt["steamid"].drop_nulls().unique().to_list():
        sub = rt.filter(pl.col("steamid") == sid).sort("tick")
        if sub.height == 0:
            continue
        side_vals = sub["side"].drop_nulls().to_list()
        if not side_vals:
            continue
        out.append({
            "steamid": int(sid),
            "name": str(sub["name"][0] or ""),
            "side": str(side_vals[0]).lower(),
            "positions": _interp_positions(sub, sample_ticks),
        })
    return out


def _render_panel(
    surface: pygame.Surface,
    panel_source: PanelSource,
    sample_tick: int,
    focal_steamid: int,
    streams: list[dict],
    fonts: dict[str, pygame.font.Font],
    panel_origin: tuple[int, int],
) -> None:
    px, py = panel_origin
    surface.blit(panel_source.radar, (px, py))

    user_side = panel_source.sig.user_side
    enemy_side = panel_source.sig.enemy_side

    color_ally = (60, 175, 250)   # blue
    color_enemy = (240, 90, 80)   # red
    color_focal = (255, 220, 60)  # yellow ring
    color_dead = (90, 90, 90)

    for stream in streams:
        pos = stream["positions"].get(sample_tick)
        if not pos:
            continue
        x, y, alive = pos
        sx, sy = _world_to_panel(x, y, panel_source.map_meta, PANEL)
        sx += px
        sy += py
        if not alive:
            pygame.draw.circle(surface, color_dead, (sx, sy), 4, 1)
            continue
        col = color_ally if stream["side"] == user_side else color_enemy
        pygame.draw.circle(surface, col, (sx, sy), 6)
        if stream["steamid"] == focal_steamid:
            pygame.draw.circle(surface, color_focal, (sx, sy), 10, 2)


def _draw_hud(
    surface: pygame.Surface,
    panel_source: PanelSource,
    fonts: dict[str, pygame.font.Font],
    origin: tuple[int, int],
    score: dict[str, float] | None = None,
) -> None:
    sig = panel_source.sig
    x, y = origin
    bg = pygame.Rect(x, y, PANEL, HUD_H)
    pygame.draw.rect(surface, (20, 22, 28), bg)
    pygame.draw.rect(surface, (60, 64, 72), bg, 1)

    title = f"{panel_source.title}  —  {sig.demo_id[:34]}  R{sig.round_num}"
    surface.blit(fonts["title"].render(title, True, (230, 230, 230)), (x + 10, y + 6))

    won = "WON" if sig.won else "LOST"
    color_won = (90, 220, 120) if sig.won else (220, 110, 110)
    surface.blit(fonts["body"].render(
        f"side={sig.user_side.upper()}  enemy_intent={sig.enemy_site_intent.upper()} "
        f"(conf={sig.enemy_site_conf:.2f})  plant={sig.plant_site}  result={won}",
        True, color_won), (x + 10, y + 32))

    surface.blit(fonts["body"].render(
        f"first_contact={sig.first_contact_zone}/{sig.first_contact_side}  "
        f"end_alive={sig.end_window_alive[0]}v{sig.end_window_alive[1]}  "
        f"reason={sig.round_outcome_reason}",
        True, (200, 200, 200)), (x + 10, y + 54))

    if score is not None:
        surface.blit(fonts["body"].render(
            f"score={score.get('final', 0):.3f}  enemy={score.get('enemy',0):.2f} "
            f"ally={score.get('ally',0):.2f}  focal={score.get('focal',0):.2f} "
            f"coach={score.get('coach',0):.2f}",
            True, (255, 220, 130)), (x + 10, y + 78))


def play_paired_rounds(
    pairs: Sequence[tuple[PanelSource, PanelSource, dict[str, float]]],
    *,
    map_name: str = "de_mirage",
) -> None:
    pygame.init()
    screen_w = PAD * 3 + PANEL * 2
    screen_h = PAD * 3 + HUD_H + PANEL
    screen = pygame.display.set_mode((screen_w, screen_h))
    pygame.display.set_caption("Macro mapper — user (left) vs mapped pro (right)")
    clock = pygame.time.Clock()
    fonts = {
        "title": pygame.font.SysFont("monospace", 16, bold=True),
        "body":  pygame.font.SysFont("monospace", 13),
        "small": pygame.font.SysFont("monospace", 11),
    }

    idx = 0
    paused = False
    speed = 1.0

    while idx < len(pairs):
        user_src, pro_src, score = pairs[idx]

        u_start, u_end, u_tr = _round_window_ticks(user_src.art, user_src.sig.round_num)
        p_start, p_end, p_tr = _round_window_ticks(pro_src.art,  pro_src.sig.round_num)

        u_end = min(u_end, u_start + int(SECONDS_PER_ROUND_CAP * u_tr))
        p_end = min(p_end, p_start + int(SECONDS_PER_ROUND_CAP * p_tr))

        u_samples = _samples_at_hz(None, u_start, u_end, u_tr, PLAYBACK_HZ)
        p_samples = _samples_at_hz(None, p_start, p_end, p_tr, PLAYBACK_HZ)
        n_frames = max(len(u_samples), len(p_samples))

        u_streams = _build_player_streams(user_src.art, user_src.sig.round_num, u_samples)
        p_streams = _build_player_streams(pro_src.art,  pro_src.sig.round_num,  p_samples)

        frame = 0
        advance = False
        quit_app = False
        while not advance and not quit_app:
            for ev in pygame.event.get():
                if ev.type == pygame.QUIT:
                    quit_app = True
                elif ev.type == pygame.KEYDOWN:
                    if ev.key in (pygame.K_ESCAPE, pygame.K_q):
                        quit_app = True
                    elif ev.key == pygame.K_SPACE:
                        advance = True
                    elif ev.key == pygame.K_LEFT:
                        idx = max(0, idx - 2)
                        advance = True
                    elif ev.key == pygame.K_p:
                        paused = not paused
                    elif ev.key == pygame.K_UP:
                        speed = min(4.0, speed * 1.5)
                    elif ev.key == pygame.K_DOWN:
                        speed = max(0.25, speed / 1.5)
                    elif ev.key == pygame.K_r:
                        frame = 0

            if quit_app:
                break

            if not paused:
                frame = min(n_frames - 1, frame + 1)

            screen.fill((10, 12, 16))
            left_origin = (PAD, PAD)
            right_origin = (PAD * 2 + PANEL, PAD)

            _draw_hud(screen, user_src, fonts, left_origin)
            _draw_hud(screen, pro_src,  fonts, right_origin, score=score)

            u_tick = u_samples[min(frame, len(u_samples) - 1)]
            p_tick = p_samples[min(frame, len(p_samples) - 1)]

            _render_panel(
                screen, user_src, u_tick,
                user_src.sig.focal_steamid, u_streams, fonts,
                (left_origin[0], left_origin[1] + HUD_H + PAD),
            )
            _render_panel(
                screen, pro_src, p_tick,
                pro_src.sig.focal_steamid, p_streams, fonts,
                (right_origin[0], right_origin[1] + HUD_H + PAD),
            )

            footer_y = screen_h - PAD - 14
            screen.blit(fonts["small"].render(
                f"[SPACE] next  [←] prev  [P] pause  [↑/↓] speed={speed:.2f}x  "
                f"[R] restart  [Q/ESC] quit   round {idx+1}/{len(pairs)}  frame {frame}/{n_frames}",
                True, (170, 170, 170)),
                (PAD, footer_y))

            pygame.display.flip()
            clock.tick(int(PLAYBACK_HZ * speed))

            if frame >= n_frames - 1 and not paused:
                # Hold the last frame for a beat, then auto-advance.
                pygame.time.delay(700)
                advance = True

        if quit_app:
            break
        idx += 1

    pygame.quit()
