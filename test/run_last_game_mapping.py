#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl

from test.local_mapping_runtime import (
    FEATURE_VERSION,
    MAPS,
    TestMapConfig,
    best_mapping_for_tick,
    decode_latest_user_match,
    default_anchor_for_round,
    load_match_frames,
    load_pro_feature_corpus,
    load_round_replay_payload,
    read_match_map_name,
)
from viewer.renderer import (
    BG_COLOR,
    DIM_COLOR,
    decode_weapon,
    draw_hud,
    draw_player,
    draw_utilities,
    world_to_screen,
)


WINDOW_W = 1680
WINDOW_H = 920
HUD_H = 110
FPS = 60
TICKRATE = 64
PLAYBACK_SPEED = 1.0
PANE_GAP = 16


@dataclass
class ReplayState:
    payload: dict
    tick_list: list[int]
    tick_to_index: dict[int, int]
    weapon_map: dict[str, list[str | None]]
    grenade_paths: dict[int, dict]


class PilFontAdapter:
    def __init__(self, *, size: int, bold: bool = False) -> None:
        self.size = size
        self.bold = bold
        self._font = None

    def render(self, text: str, antialias: bool, color: tuple[int, int, int]) -> "pygame.Surface":
        import pygame._freetype as ft

        if self._font is None:
            if not ft.was_init():
                ft.init()
            self._font = ft.Font(None, self.size)
            self._font.strong = self.bold
        sample = text or " "
        surface, _ = self._font.render(sample, fgcolor=color, bgcolor=None)
        return surface.convert_alpha()


def _convert_png_to_bmp(source: Path, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / f"{source.stem}.bmp"
    if target.exists() and target.stat().st_mtime >= source.stat().st_mtime:
        return target

    script = (
        "from pathlib import Path; "
        "from PIL import Image; "
        f"src=Path({source.as_posix()!r}); "
        f"dst=Path({target.as_posix()!r}); "
        "dst.parent.mkdir(parents=True, exist_ok=True); "
        "Image.open(src).convert('RGBA').save(dst, format='BMP')"
    )
    try:
        subprocess.run(
            ["python3", "-c", script],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else str(exc)
        raise RuntimeError(
            f"Failed to convert radar image {source} to BMP via system python3/Pillow: {stderr}"
        ) from exc
    return target


def _load_radar_image(image_path: Path, *, convert_alpha: bool = True):
    import pygame

    try:
        surface = pygame.image.load(str(image_path))
    except pygame.error:
        cache_dir = ROOT / "test" / ".cache" / "bmp_radar"
        bmp_path = _convert_png_to_bmp(image_path, cache_dir)
        surface = pygame.image.load(str(bmp_path))
    return surface.convert_alpha() if convert_alpha else surface.convert()


def _build_weapon_map(shots_payload: list[dict], tick_list: list[int]) -> dict[str, list[str | None]]:
    by_player: dict[str, list[tuple[int, str]]] = {}
    for row in sorted(shots_payload, key=lambda item: item["tick"]):
        sid = str(row["player_steamid"])
        weapon = decode_weapon(str(row["weapon"]))
        by_player.setdefault(sid, []).append((int(row["tick"]), weapon))

    result: dict[str, list[str | None]] = {}
    for sid, events in by_player.items():
        values: list[str | None] = []
        event_index = 0
        for tick in tick_list:
            while event_index + 1 < len(events) and events[event_index + 1][0] <= tick:
                event_index += 1
            values.append(events[event_index][1] if events[event_index][0] <= tick else None)
        result[sid] = values
    return result


def _build_grenade_paths(paths_payload: list[dict]) -> dict[int, dict]:
    result: dict[int, dict] = {}
    for row in paths_payload:
        entity_id = int(row["entity_id"])
        path = row.get("path") or []
        result[entity_id] = {
            "type": row["grenade_type"],
            "ticks": [int(item["tick"]) for item in path],
            "xs": [float(item["x"]) for item in path],
            "ys": [float(item["y"]) for item in path],
        }
    return result


def _make_replay_state(payload: dict) -> ReplayState:
    tick_list = list(payload["tick_list"])
    return ReplayState(
        payload=payload,
        tick_list=tick_list,
        tick_to_index={tick: index for index, tick in enumerate(tick_list)},
        weapon_map=_build_weapon_map(payload["shots"], tick_list),
        grenade_paths=_build_grenade_paths(payload["grenade_paths"]),
    )


def _utility_frame(rows: list[dict], kind: str) -> pl.DataFrame:
    if not rows:
        if kind == "smokes":
            return pl.DataFrame(schema={
                "start_tick": pl.Int64,
                "end_tick": pl.Int64,
                "X": pl.Float64,
                "Y": pl.Float64,
                "thrower_name": pl.String,
            })
        if kind == "infernos":
            return pl.DataFrame(schema={
                "start_tick": pl.Int64,
                "end_tick": pl.Int64,
                "X": pl.Float64,
                "Y": pl.Float64,
            })
        if kind == "flashes":
            return pl.DataFrame(schema={
                "tick": pl.Int64,
                "X": pl.Float64,
                "Y": pl.Float64,
            })
        return pl.DataFrame()

    frame = pl.DataFrame(rows)
    rename_map = {}
    if "x" in frame.columns:
        rename_map["x"] = "X"
    if "y" in frame.columns:
        rename_map["y"] = "Y"
    return frame.rename(rename_map) if rename_map else frame


def _round_numbers(frames: dict[str, pl.DataFrame]) -> list[int]:
    rounds = frames["rounds"]
    if rounds.height == 0:
        return []
    return sorted(int(value) for value in rounds["round_num"].to_list())


def _load_round_state(parquet_dir: Path, stem: str, map_name: str, round_num: int) -> ReplayState:
    return _make_replay_state(load_round_replay_payload(parquet_dir, stem, round_num, map_name))


def _find_tick_index(tick_list: list[int], target_tick: int) -> int:
    if not tick_list:
        return 0
    best_index = 0
    best_distance = sys.maxsize
    for index, tick in enumerate(tick_list):
        distance = abs(tick - target_tick)
        if distance < best_distance:
            best_distance = distance
            best_index = index
    return best_index


def _tick_for_index(state: ReplayState, tick_index: int) -> int:
    if not state.tick_list:
        return 0
    tick_index = max(0, min(len(state.tick_list) - 1, tick_index))
    return state.tick_list[tick_index]


def _derive_pro_tick(user_tick: int, mapping: dict | None, pro_state: ReplayState | None) -> int:
    if mapping is None or pro_state is None or not pro_state.tick_list:
        return 0
    target_tick = int(mapping["anchor_tick"]) + (int(user_tick) - int(mapping["query"]["anchor_tick"]))
    return _find_tick_index(pro_state.tick_list, target_tick)


def _pane_rects() -> tuple[tuple[int, int, int, int], tuple[int, int, int, int]]:
    map_area_h = WINDOW_H - HUD_H
    pane_w = (WINDOW_W - (PANE_GAP * 3)) // 2
    pane_h = map_area_h - (PANE_GAP * 2)
    left = (PANE_GAP, PANE_GAP, pane_w, pane_h)
    right = (PANE_GAP * 2 + pane_w, PANE_GAP, pane_w, pane_h)
    return left, right


def _draw_replay_pane(
    *,
    screen,
    pane_rect: tuple[int, int, int, int],
    state: ReplayState | None,
    map_cfg: TestMapConfig,
    radar_image,
    lower_image,
    tick_index: int,
    show_lower: bool,
    title: str,
    subtitle: str,
    font,
    font_sm,
    font_xs,
) -> None:
    import pygame

    x, y, width, height = pane_rect
    pygame.draw.rect(screen, (22, 22, 30), pane_rect, border_radius=8)
    pygame.draw.rect(screen, (55, 55, 72), pane_rect, width=1, border_radius=8)

    title_surface = font.render(title, True, (235, 235, 245))
    subtitle_surface = font_sm.render(subtitle, True, (170, 170, 190))
    screen.blit(title_surface, (x + 12, y + 10))
    screen.blit(subtitle_surface, (x + 12, y + 34))

    if state is None:
        empty_surface = font.render("No replay loaded", True, DIM_COLOR)
        screen.blit(empty_surface, (x + 20, y + 80))
        return

    map_margin_top = 64
    map_size = min(width - 24, height - map_margin_top - 12)
    off_x = x + (width - map_size) // 2
    off_y = y + map_margin_top

    active_image = lower_image if (show_lower and lower_image is not None) else radar_image
    img_w, img_h = active_image.get_size()
    scaled = pygame.transform.smoothscale(active_image, (map_size, map_size))
    screen.blit(scaled, (off_x, off_y))

    if not state.tick_list:
        return

    tick_index = max(0, min(len(state.tick_list) - 1, tick_index))
    cur_tick = state.tick_list[tick_index]
    ticks_payload = state.payload["ticks"][tick_index]

    draw_utilities(
        screen,
        _utility_frame(state.payload["smokes"], "smokes"),
        _utility_frame(state.payload["infernos"], "infernos"),
        _utility_frame(state.payload["flashes"], "flashes"),
        state.grenade_paths,
        cur_tick,
        map_cfg,
        img_w,
        img_h,
        map_size,
        off_x,
        off_y,
        font_xs,
        window_w=WINDOW_W,
        window_h_map=WINDOW_H - HUD_H,
    )

    for row in ticks_payload["players"]:
        px, py = world_to_screen(
            row["x"],
            row["y"],
            map_cfg,
            img_w,
            img_h,
            map_size,
            off_x,
            off_y,
        )
        sid = row["steamid"]
        weapon_list = state.weapon_map.get(sid, [])
        active_weapon = weapon_list[tick_index] if tick_index < len(weapon_list) else ""
        ghost = map_cfg.has_lower_level and map_cfg.is_lower(row["z"]) != show_lower
        draw_player(
            screen,
            px,
            py,
            {
                "health": row["health"],
                "side": row["side"],
                "name": row["name"],
                "yaw": row["yaw"],
                "inventory": row["inventory"],
                "flash_duration": row["flash_duration"],
            },
            font_name=font_xs,
            font_wpn=font_xs,
            has_yaw=True,
            has_flash=True,
            active_wpn=active_weapon,
            ghost=ghost,
        )


def _best_initial_round(round_numbers: list[int], frames: dict[str, pl.DataFrame], map_name: str, stem: str, parquet_dir: Path) -> tuple[int, ReplayState]:
    for round_num in round_numbers:
        state = _load_round_state(parquet_dir, stem, map_name, round_num)
        if default_anchor_for_round(state.payload) is not None:
            return round_num, state
    if not round_numbers:
        raise ValueError("No rounds found")
    return round_numbers[0], _load_round_state(parquet_dir, stem, map_name, round_numbers[0])


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone pygame side-by-side mapping test for the latest local user match.")
    parser.add_argument("--demo-id", help="Explicit user demo id stem from parquet_user.")
    parser.add_argument("--round", type=int, help="Optional starting round.")
    parser.add_argument("--parquet-user-dir", default="parquet_user")
    parser.add_argument("--parquet-pro-dir", default="parquet_pro")
    args = parser.parse_args()

    parquet_user_dir = Path(args.parquet_user_dir)
    parquet_pro_dir = Path(args.parquet_pro_dir)

    user_demo_id, steam_id = (args.demo_id, None) if args.demo_id else decode_latest_user_match(parquet_user_dir)
    if args.demo_id and user_demo_id.startswith("user_"):
        parts = user_demo_id.split("_", 2)
        steam_id = parts[1] if len(parts) >= 3 else None

    user_map_name = read_match_map_name(parquet_user_dir, user_demo_id)
    map_cfg = MAPS[user_map_name]
    if not map_cfg.radar_path.exists():
        raise FileNotFoundError(f"Radar PNG not found: {map_cfg.radar_path}")

    user_frames = load_match_frames(parquet_user_dir, user_demo_id)
    round_numbers = _round_numbers(user_frames)
    if not round_numbers:
        raise ValueError(f"No rounds found for {user_demo_id}")

    if args.round:
        current_round = args.round
        user_state = _load_round_state(parquet_user_dir, user_demo_id, user_map_name, current_round)
    else:
        current_round, user_state = _best_initial_round(round_numbers, user_frames, user_map_name, user_demo_id, parquet_user_dir)

    pro_corpus = load_pro_feature_corpus(parquet_pro_dir, user_map_name)
    if not pro_corpus:
        raise FileNotFoundError(
            f"No {FEATURE_VERSION} pro feature blobs found for {user_map_name} in {parquet_pro_dir}. "
            "Run the pro corpus backfill first."
        )

    try:
        import pygame
    except ImportError as exc:
        raise SystemExit("pygame is required. Install it with `pip install pygame`.") from exc

    pygame.init()
    screen = pygame.display.set_mode((WINDOW_W, WINDOW_H))
    pygame.display.set_caption("ShadowPro Test Viewer")
    clock = pygame.time.Clock()

    font = PilFontAdapter(size=20, bold=True)
    font_sm = PilFontAdapter(size=14)
    font_xs = PilFontAdapter(size=12)

    user_radar = _load_radar_image(map_cfg.radar_path)
    user_lower = _load_radar_image(map_cfg.lower_radar_path) if map_cfg.lower_radar_path and map_cfg.lower_radar_path.exists() else None

    left_rect, right_rect = _pane_rects()
    user_tick_index = 0
    paused = False
    accum = 0.0
    show_lower = False

    mapping: dict | None = None
    pro_state: ReplayState | None = None
    pro_map_cfg: TestMapConfig | None = None
    pro_radar = None
    pro_lower = None

    def remap_current_tick() -> None:
        nonlocal mapping, pro_state, pro_map_cfg, pro_radar, pro_lower
        if not user_state.tick_list:
            return
        user_tick = _tick_for_index(user_state, user_tick_index)
        mapping = best_mapping_for_tick(
            user_frames=user_frames,
            steam_id=steam_id,
            round_num=current_round,
            anchor_tick=user_tick,
            pro_corpus=pro_corpus,
        )
        if mapping is None:
            pro_state = None
            pro_map_cfg = None
            pro_radar = None
            pro_lower = None
            return

        pro_match_id = mapping["source_match_id"]
        pro_dir = parquet_pro_dir / pro_match_id
        pro_map_name = read_match_map_name(pro_dir, pro_match_id)
        pro_map_cfg = MAPS[pro_map_name]
        pro_state = _load_round_state(pro_dir, pro_match_id, pro_map_name, mapping["round_num"])
        pro_radar = _load_radar_image(pro_map_cfg.radar_path)
        pro_lower = (
            _load_radar_image(pro_map_cfg.lower_radar_path)
            if pro_map_cfg.lower_radar_path and pro_map_cfg.lower_radar_path.exists()
            else None
        )

    initial_anchor = default_anchor_for_round(user_state.payload)
    if initial_anchor is not None:
        user_tick_index = _find_tick_index(user_state.tick_list, initial_anchor)
        remap_current_tick()

    running = True
    while running:
        dt = clock.tick(FPS) / 1000.0
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.KEYDOWN:
                mods = pygame.key.get_mods()
                step = 5 * TICKRATE if mods & pygame.KMOD_SHIFT else TICKRATE
                if event.key in (pygame.K_q, pygame.K_ESCAPE):
                    running = False
                elif event.key == pygame.K_SPACE:
                    paused = not paused
                    accum = 0.0
                elif event.key == pygame.K_HOME:
                    user_tick_index = 0
                    accum = 0.0
                elif event.key == pygame.K_l and map_cfg.has_lower_level:
                    show_lower = not show_lower
                elif event.key == pygame.K_m:
                    remap_current_tick()
                elif event.key == pygame.K_RETURN:
                    remap_current_tick()
                elif event.key == pygame.K_RIGHT:
                    user_tick_index = min(len(user_state.tick_list) - 1, user_tick_index + step)
                    accum = 0.0
                elif event.key == pygame.K_LEFT:
                    user_tick_index = max(0, user_tick_index - step)
                    accum = 0.0
                elif event.key == pygame.K_RIGHTBRACKET:
                    round_pos = min(len(round_numbers) - 1, round_numbers.index(current_round) + 1)
                    current_round = round_numbers[round_pos]
                    user_state = _load_round_state(parquet_user_dir, user_demo_id, user_map_name, current_round)
                    user_tick_index = 0
                    accum = 0.0
                    mapping = None
                    pro_state = None
                    initial_anchor = default_anchor_for_round(user_state.payload)
                    if initial_anchor is not None:
                        user_tick_index = _find_tick_index(user_state.tick_list, initial_anchor)
                        remap_current_tick()
                elif event.key == pygame.K_LEFTBRACKET:
                    round_pos = max(0, round_numbers.index(current_round) - 1)
                    current_round = round_numbers[round_pos]
                    user_state = _load_round_state(parquet_user_dir, user_demo_id, user_map_name, current_round)
                    user_tick_index = 0
                    accum = 0.0
                    mapping = None
                    pro_state = None
                    initial_anchor = default_anchor_for_round(user_state.payload)
                    if initial_anchor is not None:
                        user_tick_index = _find_tick_index(user_state.tick_list, initial_anchor)
                        remap_current_tick()

        if not paused and user_state.tick_list:
            accum += dt * TICKRATE * PLAYBACK_SPEED
            steps = int(accum)
            if steps > 0:
                accum -= steps
                user_tick_index = min(len(user_state.tick_list) - 1, user_tick_index + steps)
                if user_tick_index >= len(user_state.tick_list) - 1:
                    paused = True

        screen.fill(BG_COLOR)

        user_tick = _tick_for_index(user_state, user_tick_index)
        pro_tick_index = _derive_pro_tick(user_tick, mapping, pro_state)

        _draw_replay_pane(
            screen=screen,
            pane_rect=left_rect,
            state=user_state,
            map_cfg=map_cfg,
            radar_image=user_radar,
            lower_image=user_lower,
            tick_index=user_tick_index,
            show_lower=show_lower,
            title=f"USER · {user_demo_id}",
            subtitle=f"{map_cfg.display_name} · round {current_round}",
            font=font,
            font_sm=font_sm,
            font_xs=font_xs,
        )

        if pro_state and pro_map_cfg and pro_radar:
            pro_title = f"PRO · {mapping['source_match_id']}"
            pro_subtitle = (
                f"{pro_map_cfg.display_name} · round {mapping['round_num']} · "
                f"{mapping['feature'].get('primary_situation', 'mapped event')}"
            )
            _draw_replay_pane(
                screen=screen,
                pane_rect=right_rect,
                state=pro_state,
                map_cfg=pro_map_cfg,
                radar_image=pro_radar,
                lower_image=pro_lower,
                tick_index=pro_tick_index,
                show_lower=show_lower and pro_map_cfg.has_lower_level,
                title=pro_title,
                subtitle=pro_subtitle,
                font=font,
                font_sm=font_sm,
                font_xs=font_xs,
            )
        else:
            _draw_replay_pane(
                screen=screen,
                pane_rect=right_rect,
                state=None,
                map_cfg=map_cfg,
                radar_image=user_radar,
                lower_image=user_lower,
                tick_index=0,
                show_lower=False,
                title="PRO · no match",
                subtitle="Press M or Enter to remap the current tick",
                font=font,
                font_sm=font_sm,
                font_xs=font_xs,
            )

        hud_bar_x = 16
        hud_bar_w = WINDOW_W - 32
        hud_bar_y = WINDOW_H - 34
        draw_hud(
            screen,
            font,
            font_sm,
            rnd=current_round,
            cur_tick=user_tick,
            freeze_end_tick=int(user_state.payload["freeze_end_tick"]),
            tick_idx=user_tick_index,
            n_ticks=len(user_state.tick_list),
            paused=paused,
            bar_x=hud_bar_x,
            bar_y=hud_bar_y,
            bar_w=hud_bar_w,
            bar_h=12,
            window_w=WINDOW_W,
            window_h=WINDOW_H,
            hud_h=HUD_H,
            tickrate=TICKRATE,
            level_badge="LOWER" if show_lower else None,
        )

        status_parts = [
            f"map={map_cfg.display_name}",
            f"tick={user_tick}",
            "controls: space play/pause, [ ] round, arrows scrub, m remap",
        ]
        if mapping:
            status_parts.insert(1, f"score={mapping['score']:.3f}")
            status_parts.insert(2, mapping["reason"])
        else:
            status_parts.insert(1, "no valid mapping at this tick")
        status_text = "  |  ".join(status_parts)
        status_surface = font_sm.render(status_text[:220], True, (210, 210, 225))
        screen.blit(status_surface, (16, WINDOW_H - HUD_H + 10))

        pygame.display.flip()

    pygame.quit()


if __name__ == "__main__":
    main()
