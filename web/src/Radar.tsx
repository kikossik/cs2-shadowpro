// Single radar pane rendered to a <canvas>.
// Time model: t = progress 0..1 across [-3s .. +12s] playback window.

import { useEffect, useRef } from "react";
import type { Density, RadarIntensity, Side, SituationPane, Theme } from "./types";

function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}

type Props = {
  side: "user" | "pro";
  data: SituationPane;
  progress: number;
  theme: Theme;
  density: Density;
  bg: HTMLImageElement | null;
  intensity?: RadarIntensity;
};

export function Radar({ side, data, progress, theme, density, bg, intensity = "full" }: Props) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const dpr = Math.min(window.devicePixelRatio || 1, 2);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const W = rect.width, H = rect.height;
    canvas.width = W * dpr;
    canvas.height = H * dpr;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, W, H);

    if (bg && bg.complete) {
      if (intensity === "dim") ctx.globalAlpha = 0.55;
      if (intensity === "wire") ctx.globalAlpha = 0.22;
      ctx.drawImage(bg, 0, 0, W, H);
      ctx.globalAlpha = 1;
    }

    const sideColor = (s: Side) => (s === "CT" ? theme.ct : theme.tside);
    const px = (p: number) => p * W;
    const py = (p: number) => p * H;

    // Vignette border
    ctx.save();
    ctx.strokeStyle = theme.grid;
    ctx.lineWidth = 1;
    ctx.strokeRect(0.5, 0.5, W - 1, H - 1);
    ctx.restore();

    const tSec = -3 + progress * 15;

    // Smokes
    if (density.smokes) {
      for (const s of data.smokes ?? []) {
        const r = s.r * W;
        const cx = px(s.x), cy = py(s.y);
        const grad = ctx.createRadialGradient(cx, cy, r * 0.2, cx, cy, r);
        grad.addColorStop(0, theme.smoke + "cc");
        grad.addColorStop(1, theme.smoke + "00");
        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.arc(cx, cy, r, 0, Math.PI * 2);
        ctx.fill();
        ctx.strokeStyle = theme.smoke + "66";
        ctx.setLineDash([3, 4]);
        ctx.lineWidth = 1;
        ctx.stroke();
        ctx.setLineDash([]);
      }
    }

    // Molotovs
    if (density.molotovs) {
      for (const m of data.molotovs ?? []) {
        const r = m.r * W;
        const cx = px(m.x), cy = py(m.y);
        ctx.save();
        ctx.fillStyle = theme.fire + "55";
        ctx.beginPath();
        ctx.arc(cx, cy, r, 0, Math.PI * 2);
        ctx.fill();
        ctx.strokeStyle = theme.fire;
        ctx.lineWidth = 1.25;
        ctx.beginPath();
        ctx.arc(cx, cy, r, 0, Math.PI * 2);
        ctx.stroke();
        for (let i = 0; i < 6; i++) {
          const a = (i / 6) * Math.PI * 2 + progress * 6;
          const rr = r * 0.5 * (0.6 + 0.3 * Math.sin(progress * 10 + i));
          ctx.fillStyle = theme.fire;
          ctx.beginPath();
          ctx.arc(cx + Math.cos(a) * rr, cy + Math.sin(a) * rr, 1.4, 0, Math.PI * 2);
          ctx.fill();
        }
        ctx.restore();
      }
    }

    // Bomb plant marker
    if (data.bomb?.planted) {
      const cx = px(data.bomb.x), cy = py(data.bomb.y);
      ctx.save();
      ctx.strokeStyle = theme.accent;
      ctx.fillStyle = theme.accent + "22";
      ctx.lineWidth = 1.25;
      const s = 10;
      ctx.beginPath();
      ctx.moveTo(cx, cy - s);
      ctx.lineTo(cx + s, cy);
      ctx.lineTo(cx, cy + s);
      ctx.lineTo(cx - s, cy);
      ctx.closePath();
      ctx.fill();
      ctx.stroke();
      const pulse = (Math.sin(progress * Math.PI * 4) + 1) * 0.5;
      ctx.globalAlpha = 0.3 + pulse * 0.5;
      ctx.beginPath();
      ctx.arc(cx, cy, s + 4 + pulse * 6, 0, Math.PI * 2);
      ctx.stroke();
      ctx.globalAlpha = 1;
      ctx.fillStyle = theme.accent;
      ctx.font = "600 9px 'JetBrains Mono', ui-monospace, monospace";
      ctx.textAlign = "center";
      ctx.fillText("C4", cx, cy + 3);
      ctx.restore();
    }

    // Kill X markers — appear at event t, then persist with fade
    if (density.kills) {
      for (const k of data.kills ?? []) {
        if (tSec < k.t) continue;
        const age = tSec - k.t;
        const alpha = Math.max(0.25, 1 - age / 20);
        const cx = px(k.x), cy = py(k.y);
        ctx.save();
        ctx.strokeStyle = theme.ink;
        ctx.globalAlpha = alpha;
        ctx.lineWidth = 2;
        ctx.lineCap = "round";
        const s = 6;
        ctx.beginPath();
        ctx.moveTo(cx - s, cy - s); ctx.lineTo(cx + s, cy + s);
        ctx.moveTo(cx + s, cy - s); ctx.lineTo(cx - s, cy + s);
        ctx.stroke();
        if (density.labels) {
          ctx.globalAlpha = alpha * 0.7;
          ctx.fillStyle = theme.ink;
          ctx.font = "9px 'JetBrains Mono', ui-monospace, monospace";
          ctx.textAlign = "left";
          ctx.fillText(k.victim, cx + 9, cy - 6);
        }
        ctx.restore();
      }
    }

    // Focal player trail
    if (density.trails && data.trail?.length > 1) {
      const trail = data.trail;
      const n = trail.length;
      const idxF = progress * (n - 1);
      const i0 = Math.floor(idxF);
      const i1 = Math.min(n - 1, i0 + 1);
      const ft = idxF - i0;
      const curX = lerp(trail[i0][0], trail[i1][0], ft);
      const curY = lerp(trail[i0][1], trail[i1][1], ft);

      ctx.save();
      ctx.lineWidth = 2;
      ctx.lineCap = "round";
      ctx.strokeStyle = sideColor(data.focal.side);
      for (let i = Math.max(0, i0 - 4); i < i0; i++) {
        const a = (i - (i0 - 4)) / 4;
        ctx.globalAlpha = 0.1 + a * 0.35;
        ctx.beginPath();
        ctx.moveTo(px(trail[i][0]), py(trail[i][1]));
        ctx.lineTo(px(trail[i + 1][0]), py(trail[i + 1][1]));
        ctx.stroke();
      }
      ctx.globalAlpha = 0.6;
      ctx.beginPath();
      ctx.moveTo(px(trail[i0][0]), py(trail[i0][1]));
      ctx.lineTo(px(curX), py(curY));
      ctx.stroke();
      ctx.restore();

      data.focal.__x = curX;
      data.focal.__y = curY;
    } else {
      data.focal.__x = data.focal.x;
      data.focal.__y = data.focal.y;
    }

    // Other players
    for (const p of data.players ?? []) {
      const cx = px(p.x), cy = py(p.y);
      const color = sideColor(p.side);
      ctx.save();
      ctx.fillStyle = color + "33";
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.25;
      ctx.beginPath();
      ctx.arc(cx, cy, 5.5, 0, Math.PI * 2);
      ctx.fill();
      ctx.stroke();
      if (density.labels) {
        ctx.fillStyle = theme.dim;
        ctx.font = "9px 'JetBrains Mono', ui-monospace, monospace";
        ctx.textAlign = "left";
        ctx.fillText(p.name, cx + 9, cy + 3);
      }
      ctx.restore();
    }

    // Focal player
    {
      const f = data.focal;
      const cx = px(f.__x ?? f.x), cy = py(f.__y ?? f.y);
      const color = sideColor(f.side);
      ctx.save();
      const halo = ctx.createRadialGradient(cx, cy, 4, cx, cy, 22);
      halo.addColorStop(0, color + "66");
      halo.addColorStop(1, color + "00");
      ctx.fillStyle = halo;
      ctx.beginPath();
      ctx.arc(cx, cy, 22, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = color;
      ctx.strokeStyle = theme.paper;
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.arc(cx, cy, 7.5, 0, Math.PI * 2);
      ctx.fill();
      ctx.stroke();
      ctx.strokeStyle = color;
      ctx.lineWidth = 1;
      ctx.globalAlpha = 0.5;
      ctx.beginPath();
      ctx.arc(cx, cy, 14, 0, Math.PI * 2);
      ctx.stroke();
      ctx.globalAlpha = 1;
      const label = f.name.toUpperCase();
      ctx.font = "600 10px 'JetBrains Mono', ui-monospace, monospace";
      const w = ctx.measureText(label).width + 10;
      ctx.fillStyle = theme.bg + "ee";
      ctx.strokeStyle = color;
      ctx.lineWidth = 1;
      ctx.fillRect(cx + 14, cy - 8, w, 16);
      ctx.strokeRect(cx + 14, cy - 8, w, 16);
      ctx.fillStyle = color;
      ctx.textAlign = "left";
      ctx.fillText(label, cx + 19, cy + 3);
      ctx.restore();
    }

    // Side indicator top-left
    ctx.save();
    ctx.font = "600 10px 'JetBrains Mono', ui-monospace, monospace";
    ctx.fillStyle = theme.dim;
    ctx.textAlign = "left";
    ctx.fillText(side === "user" ? "// YOU" : "// PRO", 10, 18);
    ctx.restore();
  }, [data, progress, theme, density, bg, intensity, side, dpr]);

  return (
    <canvas
      ref={canvasRef}
      className="radar-canvas"
      style={{ width: "100%", height: "100%", display: "block" }}
    />
  );
}
