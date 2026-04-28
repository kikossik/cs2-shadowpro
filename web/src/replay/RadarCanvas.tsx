/**
 * RadarCanvas — Canvas-based 2D round replayer.
 *
 * Two-tier visual hierarchy: the focal player(s) get full HUD detail
 * (name, HP, weapon, halo) so the viewer can study them; all other actors
 * stay context-only (dot + facing arrow). Utility (smokes/molotovs/flashes)
 * is rendered as flat shapes without duration bars or thrower labels.
 */
import { useEffect, useRef } from "react";
import type { MapConfig, PlayerTick, RoundReplayData, WeaponMap } from "./types";
import {
  CT_COLOR, T_COLOR, DEAD_COLOR, WHITE,
  SMOKE_RADIUS_WU, INFERNO_RADIUS_WU,
  FLASH_VIS_TICKS,
  GREN_COLORS, TICKRATE,
  bestWeapon, decodeWeapon,
} from "./constants";

// ── Visual constants ───────────────────────────────────────────────────────────
const PLAYER_R           = 7;
const FOCAL_R            = 9;
const ARROW_LEN          = 14;
const FOCAL_ARROW_LEN    = 18;
const FOCAL_COLOR        = "#facc15";
const FLASHED_COLOR      = "#ffe066";

// In-flight trails fade quickly after detonation/landing.
const TRAIL_AFTER_TICKS  = 12;

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

// ── Primitive helpers ──────────────────────────────────────────────────────────
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

// ── Utility (smokes / molotovs / flashes / grenades) ───────────────────────────

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
    // Soft fade at the very start and end so smokes don't pop in/out.
    const duration = Math.max(1, s.end_tick - s.start_tick);
    const age      = curTick - s.start_tick;
    const fade     = Math.min(1, Math.min(age, duration - age) / TICKRATE);
    fillCircleAlpha(ctx, px, py, smokePx, "#b9bfcc", 0.42 * fade + 0.05);
    strokeCircle(ctx, px, py, smokePx, "rgba(220,224,236,0.55)", 1);
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
    fillCircleAlpha(ctx, px, py, infernoPx, "#f57e2c", 0.55);
    strokeCircle(ctx, px, py, infernoPx, "rgba(255,138,32,0.85)", 1);
  }
}

function drawFlashes(
  ctx: CanvasRenderingContext2D,
  flashes: RoundReplayData["flashes"],
  curTick: number,
  cfg: MapConfig, imgW: number, imgH: number, dispSize: number, offX: number, offY: number,
) {
  // Compact flicker: small fading dot, no expanding ring.
  const visTicks = Math.round(FLASH_VIS_TICKS * 0.5);
  for (const fl of flashes) {
    const age = curTick - fl.tick;
    if (age < 0 || age > visTicks) continue;
    const [px, py] = worldToScreen(fl.x, fl.y, cfg, imgW, imgH, dispSize, offX, offY);
    const t     = age / visTicks;
    const alpha = (1 - t) * 0.55;
    const r     = 6 + 4 * t;
    fillCircleAlpha(ctx, px, py, r, "#ffeb6b", alpha);
  }
}

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
    if (curTick < firstT || curTick > lastT + TRAIL_AFTER_TICKS) continue;

    const colorBase = GREN_COLORS[grenade_type] ?? "rgba(200,200,200,";

    // Collect points up to curTick.
    const pts: [number, number][] = [];
    for (const pt of path) {
      if (pt.tick <= curTick) {
        pts.push(worldToScreen(pt.x, pt.y, cfg, imgW, imgH, dispSize, offX, offY));
      } else break;
    }
    if (!pts.length) continue;

    const inFlight   = curTick <= lastT;
    const trailAlpha = inFlight ? 0.55 : Math.max(0, (1 - (curTick - lastT) / TRAIL_AFTER_TICKS) * 0.4);

    if (pts.length >= 2 && trailAlpha > 0) {
      ctx.save();
      ctx.globalAlpha = trailAlpha;
      ctx.strokeStyle = colorBase + "1)";
      ctx.lineWidth   = 1.5;
      ctx.lineJoin    = "round";
      ctx.beginPath();
      ctx.moveTo(pts[0][0], pts[0][1]);
      for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i][0], pts[i][1]);
      ctx.stroke();
      ctx.restore();
    }

    if (inFlight) {
      const [gx, gy] = pts[pts.length - 1];
      ctx.save();
      ctx.globalAlpha = 0.95;
      ctx.fillStyle = colorBase + "1)";
      ctx.beginPath();
      ctx.arc(gx, gy, 3, 0, Math.PI * 2);
      ctx.fill();
      ctx.restore();
    }
  }
}

// ── Player rendering ───────────────────────────────────────────────────────────

function drawPlayer(
  ctx: CanvasRenderingContext2D,
  player: PlayerTick,
  tickIdx: number,
  weaponMap: WeaponMap,
  cfg: MapConfig, imgW: number, imgH: number, dispSize: number, offX: number, offY: number,
  ghost: boolean,
  highlighted: boolean,
) {
  const { x, y, yaw, health, side, inventory, flash_duration } = player;
  const alive   = health > 0;
  const sideCol = side === "ct" ? CT_COLOR : T_COLOR;
  const [px, py] = worldToScreen(x, y, cfg, imgW, imgH, dispSize, offX, offY);
  const flashed = alive && flash_duration > 0;

  // Dead: small × marker, dim color, no decoration.
  if (!alive) {
    const arm = 4;
    ctx.save();
    ctx.globalAlpha = ghost ? 0.35 : 0.7;
    ctx.strokeStyle = highlighted ? FOCAL_COLOR : DEAD_COLOR;
    ctx.lineWidth = highlighted ? 2 : 1.5;
    ctx.lineCap = "round";
    ctx.beginPath();
    ctx.moveTo(px - arm, py - arm); ctx.lineTo(px + arm, py + arm);
    ctx.moveTo(px + arm, py - arm); ctx.lineTo(px - arm, py + arm);
    ctx.stroke();
    ctx.restore();
    return;
  }

  const r = highlighted ? FOCAL_R : PLAYER_R;
  const arrowLen = highlighted ? FOCAL_ARROW_LEN : ARROW_LEN;

  // Facing arrow.
  const rad = (yaw * Math.PI) / 180;
  const dx  =  Math.cos(rad) * arrowLen;
  const dy  = -Math.sin(rad) * arrowLen;
  ctx.save();
  ctx.globalAlpha = ghost ? 0.35 : 1.0;
  ctx.strokeStyle = sideCol;
  ctx.lineWidth   = highlighted ? 2.5 : 1.75;
  ctx.lineCap     = "round";
  ctx.beginPath();
  ctx.moveTo(px, py);
  ctx.lineTo(px + dx, py + dy);
  ctx.stroke();
  ctx.restore();

  // Focal halo (single soft ring, not three layers).
  if (highlighted && !ghost) {
    fillCircleAlpha(ctx, px, py, r + 7, FOCAL_COLOR, 0.18);
    strokeCircle(ctx, px, py, r + 5, FOCAL_COLOR, 2);
  }

  // Body.
  ctx.save();
  ctx.globalAlpha = ghost ? 0.30 : 1.0;
  ctx.fillStyle = sideCol;
  ctx.beginPath();
  ctx.arc(px, py, r, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();

  // Outline. Subtle for context players, bright for focal, warm for flashed.
  const outlineColor = highlighted
    ? FOCAL_COLOR
    : flashed
      ? FLASHED_COLOR
      : "rgba(255,255,255,0.45)";
  strokeCircle(ctx, px, py, r, outlineColor, highlighted ? 2 : 1);

  // Focal-only HUD: name + HP bar + weapon label.
  if (highlighted && !ghost) {
    const name = player.name;
    const short = name.includes(" ") ? name.split(" ").pop()!.slice(0, 12) : name.slice(0, 12);
    ctx.font         = "bold 11px monospace";
    ctx.textAlign    = "center";
    ctx.textBaseline = "alphabetic";
    ctx.fillStyle    = FOCAL_COLOR;
    ctx.fillText(short, px, py - r - 7);

    // Slim HP bar.
    const bw = 32, bh = 3;
    const bx = px - bw / 2, by = py + r + 5;
    ctx.fillStyle = "rgba(0,0,0,0.55)"; ctx.fillRect(bx - 1, by - 1, bw + 2, bh + 2);
    ctx.fillStyle = "#2d2d37";          ctx.fillRect(bx, by, bw, bh);
    const hc = health > 50 ? "#41c841" : health > 25 ? "#c8a528" : "#cd3737";
    ctx.fillStyle = hc;
    ctx.fillRect(bx, by, Math.max(1, (bw * health) / 100), bh);

    // Weapon label.
    const raw = weaponMap[player.steamid]?.[tickIdx] ?? null;
    const wpn = raw ? decodeWeapon(raw) : bestWeapon(inventory);
    if (wpn) {
      ctx.font      = "10px monospace";
      ctx.textAlign = "center";
      ctx.fillStyle = WHITE;
      ctx.fillText(wpn.slice(0, 14), px, by + bh + 11);
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
  highlightedSteamIds?: string[];
  showLower?: boolean;
  width: number;
  height: number;
}

export function RadarCanvas({
  mapConfig, radarImage, lowerRadarImage, data, tickIdx, weaponMap,
  highlightedSteamIds, showLower = false, width, height,
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

    const dispSize = Math.min(width, height);
    const offX     = Math.floor((width  - dispSize) / 2);
    const offY     = Math.floor((height - dispSize) / 2);

    const img    = (showLower && lowerRadarImage) ? lowerRadarImage : radarImage;
    const imgW   = img.naturalWidth  || 1024;
    const imgH   = img.naturalHeight || 1024;

    ctx.drawImage(img, offX, offY, dispSize, dispSize);

    if (!data.tick_list.length) return;
    const curTick = data.tick_list[tickIdx] ?? data.tick_list[0];

    // Draw order: utility under players. Focal players drawn last so their HUD
    // sits on top of any neighboring player's body.
    drawSmokes  (ctx, data.smokes,        curTick, mapConfig, imgW, imgH, dispSize, offX, offY);
    drawInfernos(ctx, data.infernos,      curTick, mapConfig, imgW, imgH, dispSize, offX, offY);
    drawGrenadeTrails(ctx, data.grenade_paths, curTick, mapConfig, imgW, imgH, dispSize, offX, offY);
    drawFlashes (ctx, data.flashes,       curTick, mapConfig, imgW, imgH, dispSize, offX, offY);

    const frame = data.ticks[tickIdx];
    if (frame) {
      const focal = new Set((highlightedSteamIds ?? []).map(String));
      const context: PlayerTick[] = [];
      const focalPlayers: PlayerTick[] = [];
      for (const player of frame.players) {
        if (focal.has(String(player.steamid))) focalPlayers.push(player);
        else context.push(player);
      }
      const renderPlayer = (player: PlayerTick, highlighted: boolean) => {
        const ghost = mapConfig.has_lower_level
          && (player.z <= mapConfig.lower_level_max_z) !== showLower;
        drawPlayer(
          ctx, player, tickIdx, weaponMap,
          mapConfig, imgW, imgH, dispSize, offX, offY,
          ghost, highlighted,
        );
      };
      for (const p of context) renderPlayer(p, false);
      for (const p of focalPlayers) renderPlayer(p, true);
    }
  }, [data, tickIdx, weaponMap, highlightedSteamIds, mapConfig, radarImage, lowerRadarImage, showLower, width, height]);

  return (
    <canvas
      ref={canvasRef}
      width={width}
      height={height}
      style={{ display: "block", width: "100%", height: "100%" }}
    />
  );
}
