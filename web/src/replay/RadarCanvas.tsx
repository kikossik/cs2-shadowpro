/**
 * RadarCanvas — Canvas-based 2D round replayer.
 *
 * Direct port of viewer/renderer.py drawing logic to CanvasRenderingContext2D.
 * All coordinates from the API are world units; this component applies the
 * map-specific transform (pos_x / pos_y / scale) to place them on screen.
 *
 * The map image fills a centred square inside the canvas. The HUD / controls
 * are rendered as React JSX outside this component (RoundReplayPage).
 */
import { useEffect, useRef } from "react";
import type { MapConfig, RoundReplayData, WeaponMap } from "./types";
import {
  CT_COLOR, T_COLOR, DEAD_COLOR, WHITE, DIM_COLOR,
  SMOKE_RADIUS_WU, INFERNO_RADIUS_WU,
  FLASH_VIS_TICKS, TRAIL_FADE_TICKS,
  GREN_COLORS, TICKRATE,
  bestWeapon, decodeWeapon,
} from "./constants";

// ── Geometry constants ─────────────────────────────────────────────────────────
const PLAYER_R  = 9;
const ARROW_LEN = PLAYER_R + 13;

// ── Coordinate transform ───────────────────────────────────────────────────────
function worldToScreen(
  wx: number, wy: number,
  cfg: MapConfig,
  imgW: number, imgH: number,
  dispSize: number,
  offX: number, offY: number,
): [number, number] {
  const rx = (wx - cfg.pos_x) / cfg.scale;
  const ry = (cfg.pos_y - wy) / cfg.scale;
  return [offX + rx * dispSize / imgW, offY + ry * dispSize / imgH];
}

function worldRToPx(r: number, cfg: MapConfig, imgW: number, dispSize: number): number {
  return Math.max(2, (r / cfg.scale) * (dispSize / imgW));
}

// ── Alpha fill circle ──────────────────────────────────────────────────────────
function fillCircleAlpha(
  ctx: CanvasRenderingContext2D,
  cx: number, cy: number, r: number,
  color: string, alpha: number,
) {
  ctx.save();
  ctx.globalAlpha = alpha;
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.arc(cx, cy, r, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();
}

function strokeCircle(
  ctx: CanvasRenderingContext2D,
  cx: number, cy: number, r: number,
  color: string, lw = 1,
) {
  ctx.strokeStyle = color;
  ctx.lineWidth = lw;
  ctx.beginPath();
  ctx.arc(cx, cy, r, 0, Math.PI * 2);
  ctx.stroke();
}

// ── Drawing ────────────────────────────────────────────────────────────────────

function drawGrenadeTrails(
  ctx: CanvasRenderingContext2D,
  paths: RoundReplayData["grenade_paths"],
  curTick: number,
  cfg: MapConfig, imgW: number, imgH: number, dispSize: number, offX: number, offY: number,
) {
  for (const gren of paths) {
    const { path, grenade_type } = gren;
    if (!path.length) continue;
    const firstT = path[0].tick;
    const lastT  = path[path.length - 1].tick;
    if (curTick < firstT || curTick > lastT + TRAIL_FADE_TICKS) continue;

    const colorBase = GREN_COLORS[grenade_type] ?? "rgba(200,200,200,";

    // Collect points up to curTick
    const pts: [number, number][] = [];
    for (const pt of path) {
      if (pt.tick <= curTick) {
        pts.push(worldToScreen(pt.x, pt.y, cfg, imgW, imgH, dispSize, offX, offY));
      } else break;
    }
    if (!pts.length) continue;

    const inFlight = curTick <= lastT;
    const trailAlpha = inFlight
      ? 0.82
      : Math.max(0, (1 - (curTick - lastT) / TRAIL_FADE_TICKS) * 0.82);

    if (pts.length >= 2) {
      ctx.save();
      ctx.globalAlpha = trailAlpha;
      ctx.strokeStyle = colorBase + "1)";
      ctx.lineWidth = 2;
      ctx.lineJoin = "round";
      ctx.beginPath();
      ctx.moveTo(pts[0][0], pts[0][1]);
      for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i][0], pts[i][1]);
      ctx.stroke();
      ctx.restore();
    }

    // Dot at current position while in flight
    if (inFlight) {
      const [gx, gy] = pts[pts.length - 1];
      ctx.save();
      ctx.fillStyle = colorBase + "1)";
      ctx.beginPath(); ctx.arc(gx, gy, 4, 0, Math.PI * 2); ctx.fill();
      strokeCircle(ctx, gx, gy, 4, "rgba(255,255,255,0.9)", 1);
      ctx.restore();
    }
  }
}

function drawSmokes(
  ctx: CanvasRenderingContext2D,
  smokes: RoundReplayData["smokes"],
  curTick: number,
  cfg: MapConfig, imgW: number, imgH: number, dispSize: number, offX: number, offY: number,
) {
  const smokePx = worldRToPx(SMOKE_RADIUS_WU, cfg, imgW, dispSize);
  for (const s of smokes) {
    if (curTick < s.start_tick || curTick > s.end_tick) continue;
    const [px, py] = worldToScreen(s.x, s.y, cfg, imgW, imgH, dispSize, offX, offY);
    const duration = Math.max(1, s.end_tick - s.start_tick);
    const age      = curTick - s.start_tick;
    const fade     = Math.min(1, Math.min(age, duration - age) / (TICKRATE * 2));
    const alpha    = Math.min(1, fade + 0.3) * 0.61;

    fillCircleAlpha(ctx, px, py, smokePx, "#9ba6b6", alpha);
    strokeCircle(ctx, px, py, smokePx, "#c3cae1", 2);

    // Duration bar
    const rem  = Math.max(0, 1 - age / duration);
    const barW = smokePx * 2;
    const bx   = px - smokePx;
    const by   = py + smokePx + 4;
    ctx.fillStyle = "#343746"; ctx.fillRect(bx, by, barW, 3);
    ctx.fillStyle = "#afb6d7"; ctx.fillRect(bx, by, Math.max(1, barW * rem), 3);

    // Thrower name
    if (s.thrower_name) {
      const short = s.thrower_name.includes(" ")
        ? s.thrower_name.split(" ").pop()!.slice(0, 10)
        : s.thrower_name.slice(0, 10);
      ctx.fillStyle = "#9ba2c3";
      ctx.font = "10px monospace";
      ctx.textAlign = "center";
      ctx.fillText(short, px, by + 12);
    }
  }
}

function drawInfernos(
  ctx: CanvasRenderingContext2D,
  infernos: RoundReplayData["infernos"],
  curTick: number,
  cfg: MapConfig, imgW: number, imgH: number, dispSize: number, offX: number, offY: number,
) {
  const infernoPx = worldRToPx(INFERNO_RADIUS_WU, cfg, imgW, dispSize);
  for (const inf of infernos) {
    if (curTick < inf.start_tick || curTick > inf.end_tick) continue;
    const [px, py] = worldToScreen(inf.x, inf.y, cfg, imgW, imgH, dispSize, offX, offY);
    const duration = Math.max(1, inf.end_tick - inf.start_tick);
    const age      = curTick - inf.start_tick;
    const rem      = Math.max(0, 1 - age / duration);
    const pulse    = 0.61 + 0.18 * Math.sin(curTick * 0.25);

    fillCircleAlpha(ctx, px, py, infernoPx, "#e4480c", pulse);
    fillCircleAlpha(ctx, px, py, Math.max(2, infernoPx - 5), "#ff941c", pulse * 0.5);
    strokeCircle(ctx, px, py, infernoPx, "#ff6c16", 2);

    // Duration bar
    const barW = infernoPx * 2;
    const bx   = px - infernoPx;
    const by   = py + infernoPx + 4;
    ctx.fillStyle = "#3a2416"; ctx.fillRect(bx, by, barW, 3);
    ctx.fillStyle = "#ff8a20"; ctx.fillRect(bx, by, Math.max(1, barW * rem), 3);
  }
}

function drawFlashes(
  ctx: CanvasRenderingContext2D,
  flashes: RoundReplayData["flashes"],
  curTick: number,
  cfg: MapConfig, imgW: number, imgH: number, dispSize: number, offX: number, offY: number,
) {
  const smokePx = worldRToPx(SMOKE_RADIUS_WU, cfg, imgW, dispSize);
  for (const fl of flashes) {
    const age = curTick - fl.tick;
    if (age < 0 || age > FLASH_VIS_TICKS) continue;
    const [px, py] = worldToScreen(fl.x, fl.y, cfg, imgW, imgH, dispSize, offX, offY);
    const t      = age / FLASH_VIS_TICKS;
    const alpha  = (1 - t) * 0.39;
    const rRing  = Math.max(4, smokePx * 2.60 * (1 + t * 1.2));
    const rCore  = Math.max(3, smokePx * 0.88 * (1 + t * 0.4));
    fillCircleAlpha(ctx, px, py, rRing, "#ffffc8", alpha / 3);
    strokeCircle(ctx, px, py, rRing, "#ffff9a", Math.max(1, Math.round(3 * (1 - t)) + 1));
    fillCircleAlpha(ctx, px, py, rCore, "#ffff64", alpha);
  }
}

function drawPlayer(
  ctx: CanvasRenderingContext2D,
  player: import("./types").PlayerTick,
  tickIdx: number,
  weaponMap: WeaponMap,
  cfg: MapConfig, imgW: number, imgH: number, dispSize: number, offX: number, offY: number,
  ghost = false,
) {
  const { x, y, yaw, health, side, name, inventory, flash_duration } = player;
  const alive   = health > 0;
  const color   = alive ? (side === "ct" ? CT_COLOR : T_COLOR) : DEAD_COLOR;
  const r       = alive ? PLAYER_R : Math.max(4, PLAYER_R - 3);
  const [px, py] = worldToScreen(x, y, cfg, imgW, imgH, dispSize, offX, offY);
  const flashed  = alive && flash_duration > 0;

  // Directional arrow (alive, not ghost)
  if (alive && !ghost) {
    const rad = (yaw * Math.PI) / 180;
    const dx  =  Math.cos(rad) * ARROW_LEN;
    const dy  = -Math.sin(rad) * ARROW_LEN;
    ctx.strokeStyle = color;
    ctx.lineWidth   = 2;
    ctx.beginPath();
    ctx.moveTo(px, py);
    ctx.lineTo(px + dx, py + dy);
    ctx.stroke();
  }

  // Body circle
  if (ghost) {
    fillCircleAlpha(ctx, px, py, r, color, 0.25);
    strokeCircle(ctx, px, py, r, "rgba(255,255,255,0.12)", 1);
  } else {
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.arc(px, py, r, 0, Math.PI * 2); ctx.fill();
    strokeCircle(ctx, px, py, r, flashed ? "#ffff8c" : WHITE, 1);
  }

  // Name tag
  const short = name.includes(" ") ? name.split(" ").pop()!.slice(0, 12) : name.slice(0, 12);
  ctx.font      = "10px monospace";
  ctx.textAlign = "center";
  ctx.fillStyle = ghost ? DIM_COLOR : WHITE;
  ctx.fillText(short, px, py - r - 5);

  if (alive && !ghost) {
    // HP bar
    const bw = 30, bh = 3;
    const bx = px - bw / 2, by = py + r + 3;
    ctx.fillStyle = "#2d2d37"; ctx.fillRect(bx, by, bw, bh);
    const hc = health > 50 ? "#41c841" : health > 25 ? "#c8a528" : "#cd3737";
    ctx.fillStyle = hc;
    ctx.fillRect(bx, by, Math.max(1, (bw * health) / 100), bh);

    // Weapon label
    const sid    = player.steamid;
    const wpnMap = weaponMap[sid];
    const raw    = wpnMap?.[tickIdx] ?? null;
    const wpn    = raw ? decodeWeapon(raw) : bestWeapon(inventory);
    if (wpn) {
      ctx.font      = "9px monospace";
      ctx.textAlign = "center";
      ctx.fillStyle = "#aaaac8";
      ctx.fillText(wpn.slice(0, 14), px, by + bh + 9);
    }
  }
}

// ── Component ──────────────────────────────────────────────────────────────────

export interface RadarCanvasProps {
  mapConfig: MapConfig;
  radarImage: HTMLImageElement;
  lowerRadarImage?: HTMLImageElement;
  data: RoundReplayData;
  tickIdx: number;
  weaponMap: WeaponMap;
  showLower?: boolean;
  width: number;
  height: number;
}

export function RadarCanvas({
  mapConfig, radarImage, lowerRadarImage, data, tickIdx, weaponMap,
  showLower = false, width, height,
}: RadarCanvasProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = "#12121a";
    ctx.fillRect(0, 0, width, height);

    // Compute centered-square layout
    const dispSize = Math.min(width, height);
    const offX     = Math.floor((width  - dispSize) / 2);
    const offY     = Math.floor((height - dispSize) / 2);

    const img    = (showLower && lowerRadarImage) ? lowerRadarImage : radarImage;
    const imgW   = img.naturalWidth  || 1024;
    const imgH   = img.naturalHeight || 1024;

    // Radar image
    ctx.drawImage(img, offX, offY, dispSize, dispSize);

    if (!data.tick_list.length) return;
    const curTick = data.tick_list[tickIdx] ?? data.tick_list[0];

    // Grenade trails (under players)
    drawGrenadeTrails(ctx, data.grenade_paths, curTick, mapConfig, imgW, imgH, dispSize, offX, offY);

    // Smokes
    drawSmokes(ctx, data.smokes, curTick, mapConfig, imgW, imgH, dispSize, offX, offY);

    // Infernos
    drawInfernos(ctx, data.infernos, curTick, mapConfig, imgW, imgH, dispSize, offX, offY);

    // Flash detonations
    drawFlashes(ctx, data.flashes, curTick, mapConfig, imgW, imgH, dispSize, offX, offY);

    // Players
    const frame = data.ticks[tickIdx];
    if (frame) {
      for (const player of frame.players) {
        const ghost = mapConfig.has_lower_level
          && (player.z <= mapConfig.lower_level_max_z) !== showLower;
        drawPlayer(ctx, player, tickIdx, weaponMap, mapConfig, imgW, imgH, dispSize, offX, offY, ghost);
      }
    }
  }, [data, tickIdx, weaponMap, mapConfig, radarImage, lowerRadarImage, showLower, width, height]);

  return (
    <canvas
      ref={canvasRef}
      width={width}
      height={height}
      style={{ display: "block", width: "100%", height: "100%" }}
    />
  );
}
