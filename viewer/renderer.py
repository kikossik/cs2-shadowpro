"""Pure drawing functions for the 2D CS2 round replayer.

No global state — every function receives what it needs via arguments.
map_cfg (MapConfig) carries all coordinate transform constants.
"""
from __future__ import annotations

import math

import polars as pl

from viewer.maps import MapConfig


# ── Colours ────────────────────────────────────────────────────────────────────
CT_COLOR   = ( 70, 160, 255)
T_COLOR    = (255,  90,  70)
DEAD_COLOR = ( 80,  80,  92)
BG_COLOR   = ( 18,  18,  26)
HUD_BG     = ( 12,  12,  20)
WHITE      = (255, 255, 255)
TEXT_COLOR = (210, 210, 225)
DIM_COLOR  = (100, 100, 115)

# ── Player geometry ────────────────────────────────────────────────────────────
PLAYER_R  = 9
ARROW_LEN = PLAYER_R + 13

# ── Utility constants (world units / ticks) ────────────────────────────────────
SMOKE_RADIUS_WU   = 144.0
INFERNO_RADIUS_WU = 100.0
FLASH_VIS_TICKS   =  48
TRAIL_FADE_TICKS  =  64

# ── Grenade type → trail colour ────────────────────────────────────────────────
_GREN_COLORS: dict[str, tuple[int, int, int]] = {
    'CSmokeGrenadeProjectile':      (175, 182, 210),
    'CMolotovProjectile':           (255, 120,  30),
    'CIncendiaryGrenadeProjectile': (255, 120,  30),
    'CFlashbangProjectile':         (255, 240, 110),
    'CHEGrenadeProjectile':         ( 80, 210,  90),
}

# ── Weapon priority pools ──────────────────────────────────────────────────────
_RIFLES   = {'AWP', 'AK-47', 'M4A4', 'M4A1-S', 'AUG', 'SG 553',
             'SSG 08', 'SCAR-20', 'G3SG1', 'Galil AR', 'FAMAS'}
_SMGS     = {'MP9', 'MAC-10', 'MP7', 'MP5-SD', 'UMP-45', 'P90', 'PP-Bizon'}
_SHOTGUNS = {'Nova', 'XM1014', 'MAG-7', 'Sawed-Off'}
_HEAVY    = {'Negev', 'M249'}
_UTILITY  = {'C4', 'Flashbang', 'HE Grenade', 'Smoke Grenade',
             'Molotov', 'Incendiary Grenade', 'Decoy Grenade'}

# ── CS2 weapon classname → display name ───────────────────────────────────────
_WPN_CLASS: dict[str, str] = {
    'weapon_ak47': 'AK-47', 'weapon_m4a1': 'M4A4', 'weapon_m4a1_silencer': 'M4A1-S',
    'weapon_awp': 'AWP', 'weapon_sg556': 'SG 553', 'weapon_aug': 'AUG',
    'weapon_ssg08': 'SSG 08', 'weapon_scar20': 'SCAR-20', 'weapon_g3sg1': 'G3SG1',
    'weapon_galil': 'Galil AR', 'weapon_galilar': 'Galil AR', 'weapon_famas': 'FAMAS',
    'weapon_mp9': 'MP9', 'weapon_mac10': 'MAC-10', 'weapon_mp7': 'MP7',
    'weapon_mp5sd': 'MP5-SD', 'weapon_ump45': 'UMP-45', 'weapon_p90': 'P90',
    'weapon_bizon': 'PP-Bizon', 'weapon_nova': 'Nova', 'weapon_xm1014': 'XM1014',
    'weapon_mag7': 'MAG-7', 'weapon_sawedoff': 'Sawed-Off',
    'weapon_negev': 'Negev', 'weapon_m249': 'M249',
    'weapon_deagle': 'Desert Eagle', 'weapon_glock': 'Glock-18',
    'weapon_usp_silencer': 'USP-S', 'weapon_p250': 'P250',
    'weapon_tec9': 'Tec-9', 'weapon_cz75a': 'CZ75-Auto',
    'weapon_fiveseven': 'Five-SeveN', 'weapon_elite': 'Dual Berettas',
    'weapon_revolver': 'R8 Revolver', 'weapon_p2000': 'P2000',
    'weapon_flashbang': 'Flashbang', 'weapon_hegrenade': 'HE Grenade',
    'weapon_smokegrenade': 'Smoke Grenade', 'weapon_molotov': 'Molotov',
    'weapon_incgrenade': 'Incendiary Grenade', 'weapon_decoy': 'Decoy Grenade',
    'weapon_c4': 'C4', 'weapon_knife': 'Knife', 'weapon_taser': 'Zeus x27',
}


# ── Public helpers ─────────────────────────────────────────────────────────────

def best_weapon(inv: list[str] | None) -> str:
    if not inv:
        return ''
    for pool in (_RIFLES, _SMGS, _SHOTGUNS, _HEAVY):
        for w in inv:
            if w in pool:
                return w
    for w in inv:
        if w not in _UTILITY:
            return w
    return ''


def decode_weapon(raw: str) -> str:
    wpn = _WPN_CLASS.get(raw)
    if wpn is None:
        base = '_'.join(raw.split('_')[:2])
        wpn = _WPN_CLASS.get(base, raw.replace('weapon_', '').replace('_', ' ').title())
    return wpn


def world_to_screen(
    wx: float, wy: float,
    map_cfg: MapConfig,
    img_w: int, img_h: int,
    disp_size: int,
    off_x: int, off_y: int,
) -> tuple[int, int]:
    rx, ry = map_cfg.world_to_radar_px(wx, wy)
    sx = off_x + rx * disp_size / img_w
    sy = off_y + ry * disp_size / img_h
    return int(sx), int(sy)


# ── Internal helpers ───────────────────────────────────────────────────────────

def _draw_alpha_circle(surf, rgba: tuple, cx: int, cy: int, r: int) -> None:
    import pygame
    r = max(1, r)
    s = pygame.Surface((r * 2 + 2, r * 2 + 2), pygame.SRCALPHA)
    pygame.draw.circle(s, rgba, (r + 1, r + 1), r)
    surf.blit(s, (cx - r - 1, cy - r - 1))


# ── Main draw calls ────────────────────────────────────────────────────────────

def draw_utilities(
    surf,
    smokes_r: pl.DataFrame,
    infernos_r: pl.DataFrame,
    flashes_r: pl.DataFrame,
    grenade_paths_r: dict,
    cur_tick: int,
    map_cfg: MapConfig,
    img_w: int, img_h: int,
    disp_size: int, off_x: int, off_y: int,
    font_xs,
    window_w: int,
    window_h_map: int,
) -> None:
    import pygame

    smoke_px   = map_cfg.world_r_to_px(SMOKE_RADIUS_WU,   img_w, disp_size)
    inferno_px = map_cfg.world_r_to_px(INFERNO_RADIUS_WU, img_w, disp_size)

    # ── Grenade trajectories ──────────────────────────────────────────────────
    if grenade_paths_r:
        trail_surf = pygame.Surface((window_w, window_h_map), pygame.SRCALPHA)
        for path in grenade_paths_r.values():
            tks       = path['ticks']
            xs        = path['xs']
            ys        = path['ys']
            gtype     = path['type']
            col       = _GREN_COLORS.get(gtype, (200, 200, 200))
            first_t   = tks[0]
            last_t    = tks[-1]
            if cur_tick < first_t or cur_tick > last_t + TRAIL_FADE_TICKS:
                continue
            pts: list[tuple[int, int]] = []
            for i, t in enumerate(tks):
                if t <= cur_tick:
                    sx, sy = world_to_screen(xs[i], ys[i], map_cfg, img_w, img_h, disp_size, off_x, off_y)
                    pts.append((sx, sy))
                else:
                    break
            if not pts:
                continue
            in_flight = cur_tick <= last_t
            if in_flight:
                trail_alpha = 210
            else:
                age_after   = cur_tick - last_t
                trail_alpha = max(0, int(210 * (1 - age_after / TRAIL_FADE_TICKS)))
            if len(pts) >= 2:
                pygame.draw.lines(trail_surf, (*col, trail_alpha), False, pts, 2)
            if in_flight:
                cx, cy = pts[-1]
                pygame.draw.circle(trail_surf, (*col, 255), (cx, cy), 4)
                pygame.draw.circle(trail_surf, (255, 255, 255, 230), (cx, cy), 4, 1)
        surf.blit(trail_surf, (0, 0))

    # ── Active smokes ─────────────────────────────────────────────────────────
    active_smokes = smokes_r.filter(
        (pl.col('start_tick') <= cur_tick) & (pl.col('end_tick') >= cur_tick)
    )
    for row in active_smokes.iter_rows(named=True):
        px, py   = world_to_screen(row['X'], row['Y'], map_cfg, img_w, img_h, disp_size, off_x, off_y)
        duration = max(1, row['end_tick'] - row['start_tick'])
        age      = cur_tick - row['start_tick']
        fade     = min(1.0, min(age, duration - age) / (64 * 2))
        alpha    = int(155 * min(1.0, fade + 0.3))
        _draw_alpha_circle(surf, (155, 162, 182, alpha), px, py, smoke_px)
        pygame.draw.circle(surf, (195, 202, 225), (px, py), smoke_px, 2)
        rem   = max(0.0, 1.0 - age / duration)
        bar_w = smoke_px * 2
        bx, by = px - smoke_px, py + smoke_px + 4
        pygame.draw.rect(surf, (52, 55, 70), (bx, by, bar_w, 3))
        pygame.draw.rect(surf, (175, 182, 215), (bx, by, max(1, int(bar_w * rem)), 3))
        tname = row.get('thrower_name') or ''
        if tname:
            short = (tname.split()[-1] if ' ' in tname else tname)[:10]
            lbl = font_xs.render(short, True, (155, 162, 195))
            surf.blit(lbl, (px - lbl.get_width() // 2, by + 5))

    # ── Active infernos ───────────────────────────────────────────────────────
    active_fires = infernos_r.filter(
        (pl.col('start_tick') <= cur_tick) & (pl.col('end_tick') >= cur_tick)
    )
    for row in active_fires.iter_rows(named=True):
        px, py   = world_to_screen(row['X'], row['Y'], map_cfg, img_w, img_h, disp_size, off_x, off_y)
        duration = max(1, row['end_tick'] - row['start_tick'])
        age      = cur_tick - row['start_tick']
        rem      = max(0.0, 1.0 - age / duration)
        pulse    = int(155 + 45 * math.sin(cur_tick * 0.25))
        _draw_alpha_circle(surf, (228, 72, 12, pulse),       px, py, inferno_px)
        _draw_alpha_circle(surf, (255, 148, 28, pulse // 2), px, py, max(2, inferno_px - 5))
        pygame.draw.circle(surf, (255, 108, 22), (px, py), inferno_px, 2)
        bar_w = inferno_px * 2
        bx, by = px - inferno_px, py + inferno_px + 4
        pygame.draw.rect(surf, (58, 36, 22), (bx, by, bar_w, 3))
        pygame.draw.rect(surf, (255, 138, 32), (bx, by, max(1, int(bar_w * rem)), 3))

    # ── Flash detonations ─────────────────────────────────────────────────────
    for row in flashes_r.iter_rows(named=True):
        age = cur_tick - int(row['tick'])
        if 0 <= age <= FLASH_VIS_TICKS:
            px, py  = world_to_screen(row['X'], row['Y'], map_cfg, img_w, img_h, disp_size, off_x, off_y)
            t       = age / FLASH_VIS_TICKS
            alpha   = int(100 * (1 - t))
            r_ring  = max(4, int(smoke_px * 2.60 * (1 + t * 1.2)))
            r_core  = max(3, int(smoke_px * 0.88 * (1 + t * 0.4)))
            _draw_alpha_circle(surf, (255, 255, 200, alpha // 3), px, py, r_ring)
            pygame.draw.circle(surf, (255, 255, 160), (px, py), r_ring,
                               max(1, int(3 * (1 - t)) + 1))
            _draw_alpha_circle(surf, (255, 255, 100, alpha), px, py, r_core)


def draw_player(
    surf,
    px: int, py: int,
    row: dict,
    font_name,
    font_wpn,
    has_yaw: bool,
    has_flash: bool,
    active_wpn: str,
    ghost: bool = False,
) -> None:
    import pygame

    health  = int(row.get('health') or 0)
    alive   = health > 0
    side    = row.get('side') or 'ct'
    name    = row.get('name') or '?'
    color   = (CT_COLOR if side == 'ct' else T_COLOR) if alive else DEAD_COLOR
    r       = PLAYER_R if alive else max(4, PLAYER_R - 3)

    flashed = False
    if alive and has_flash:
        flashed = (row.get('flash_duration') or 0.0) > 0.0

    alpha = 64 if ghost else 255

    # Directional arrow
    if alive and has_yaw and not ghost:
        yaw = row.get('yaw')
        if yaw is not None:
            rad = math.radians(yaw)
            dx  =  math.cos(rad) * ARROW_LEN
            dy  = -math.sin(rad) * ARROW_LEN
            pygame.draw.line(surf, color, (px, py), (int(px + dx), int(py + dy)), 2)

    # Body circle
    if ghost:
        _draw_alpha_circle(surf, (*color, alpha), px, py, r)
        pygame.draw.circle(surf, (*WHITE, alpha // 2), (px, py), r, 1)
    else:
        pygame.draw.circle(surf, color, (px, py), r)
        border = (255, 255, 140) if flashed else WHITE
        pygame.draw.circle(surf, border, (px, py), r, 1)

    # Name tag
    short = (name.split()[-1] if ' ' in name else name)[:12]
    nl    = font_name.render(short, True, WHITE if not ghost else DIM_COLOR)
    surf.blit(nl, (px - nl.get_width() // 2, py - r - 15))

    if alive and not ghost:
        # HP bar
        bw, bh = 30, 3
        bx, by = px - bw // 2, py + r + 3
        pygame.draw.rect(surf, (45, 45, 55), (bx, by, bw, bh))
        hc = (65, 200, 65) if health > 50 else (200, 165, 40) if health > 25 else (205, 55, 55)
        pygame.draw.rect(surf, hc, (bx, by, max(1, int(bw * health / 100)), bh))

        wpn = active_wpn or best_weapon(row.get('inventory') or [])
        if wpn:
            wl = font_wpn.render(wpn[:14], True, (170, 170, 200))
            surf.blit(wl, (px - wl.get_width() // 2, by + bh + 1))


def draw_hud(
    surf,
    font,
    font_sm,
    rnd: int,
    cur_tick: int,
    freeze_end_tick: int,
    tick_idx: int,
    n_ticks: int,
    paused: bool,
    bar_x: int, bar_y: int, bar_w: int, bar_h: int,
    window_w: int,
    window_h: int,
    hud_h: int,
    tickrate: int,
    level_badge: str | None = None,
) -> None:
    import pygame

    hy = window_h - hud_h
    pygame.draw.rect(surf, HUD_BG, (0, hy, window_w, hud_h))
    pygame.draw.line(surf, (50, 50, 68), (0, hy), (window_w, hy), 1)

    elapsed = max(0.0, (cur_tick - freeze_end_tick) / tickrate)
    mins, secs = divmod(elapsed, 60)

    rl = font.render(f"Round {rnd:2d}", True, TEXT_COLOR)
    surf.blit(rl, (14, hy + 8))

    play_str = "⏸ PAUSED" if paused else "▶ PLAYING"
    play_col = (210, 210, 100) if paused else (90, 210, 90)
    pl_lbl = font_sm.render(play_str, True, play_col)
    surf.blit(pl_lbl, (14, hy + 30))

    ts = font.render(f"{int(mins):01d}:{secs:05.2f}", True, WHITE)
    surf.blit(ts, (window_w // 2 - ts.get_width() // 2, hy + 8))

    controls = "Space=pause  ←/→=±1s  Shift=±5s  [/]=round  Home=restart"
    if level_badge:
        controls += "  L=level"
    hint = font_sm.render(controls, True, DIM_COLOR)
    surf.blit(hint, (window_w // 2 - hint.get_width() // 2, hy + 30))

    # Scrubber bar
    pygame.draw.rect(surf, (48, 48, 64), (bar_x, bar_y, bar_w, bar_h), border_radius=4)
    prog = tick_idx / max(1, n_ticks - 1)
    fw   = max(0, int(bar_w * prog))
    if fw:
        pygame.draw.rect(surf, (110, 130, 210), (bar_x, bar_y, fw, bar_h), border_radius=4)
    hx = max(bar_x, bar_x + fw)
    pygame.draw.circle(surf, WHITE, (hx, bar_y + bar_h // 2), bar_h // 2 + 2)

    tl = font_sm.render(f"tick {cur_tick}", True, DIM_COLOR)
    surf.blit(tl, (window_w - tl.get_width() - 14, hy + 8))

    if level_badge:
        badge = font_sm.render(level_badge, True, (200, 200, 255))
        surf.blit(badge, (window_w - badge.get_width() - 14, hy + 30))
