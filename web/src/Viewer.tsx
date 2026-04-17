import { useEffect, useMemo, useRef, useState } from "react";
import { Radar } from "./Radar";
import { THEMES } from "./themes";
import { SITUATION_DATA } from "./mockData";
import type {
  Density, MatchBreakdownRow, Situation, SituationPane,
  Theme, TweakState,
} from "./types";

const TWEAK_DEFAULTS: TweakState = {
  theme: "tactical",
  layout: "side-by-side",
  radarIntensity: "full",
  showTrails: true,
  showSmokes: true,
  showMolotovs: true,
  showKills: true,
  showLabels: true,
  showCallouts: true,
};

// 15-second playback window, scaled by speed.
function usePlayback() {
  const [progress, setProgress] = useState(0.4);
  const [playing, setPlaying] = useState(true);
  const [speed, setSpeed] = useState(1);
  const rafRef = useRef<number | null>(null);
  const lastRef = useRef<number>(performance.now());

  useEffect(() => {
    if (!playing) return;
    const tick = (now: number) => {
      const dt = (now - lastRef.current) / 1000;
      lastRef.current = now;
      setProgress((p) => {
        const next = p + (dt * speed) / 15;
        if (next >= 1) return 0;
        return next;
      });
      rafRef.current = requestAnimationFrame(tick);
    };
    lastRef.current = performance.now();
    rafRef.current = requestAnimationFrame(tick);
    return () => {
      if (rafRef.current != null) cancelAnimationFrame(rafRef.current);
    };
  }, [playing, speed]);

  return { progress, setProgress, playing, setPlaying, speed, setSpeed };
}

type PaneHeaderProps = {
  kind: "user" | "pro";
  theme: Theme;
  label: string;
  sub: string;
  meta: string;
  score?: number;
};
function PaneHeader({ kind, theme, label, sub, meta, score }: PaneHeaderProps) {
  const tone = kind === "pro" ? theme.accent : theme.ink;
  return (
    <div className="pane-head" style={{ borderColor: theme.border }}>
      <div className="pane-head-left">
        <div className="pane-tag" style={{ color: theme.dim }}>
          {kind === "user" ? "SOURCE · FACEIT" : "MATCH · HLTV CORPUS"}
        </div>
        <div className="pane-label" style={{ color: tone, fontFamily: theme.fontHead }}>
          {label}
        </div>
        <div className="pane-sub" style={{ color: theme.dim }}>{sub}</div>
      </div>
      <div className="pane-head-right">
        {score != null && (
          <div className="score-chip" style={{ borderColor: theme.borderHi, color: theme.accent }}>
            <span style={{ color: theme.dim }}>MATCH</span>
            <span style={{ fontVariantNumeric: "tabular-nums" }}>{(score * 100).toFixed(0)}</span>
            <span style={{ color: theme.dim }}>%</span>
          </div>
        )}
        <div className="pane-meta" style={{ color: theme.dim }}>{meta}</div>
      </div>
    </div>
  );
}

function FeatureStrip({ theme, features }: { theme: Theme; features: Situation["features"] }) {
  const items = [
    { k: "AREA",  v: features.area },
    { k: "SIDE",  v: features.side },
    { k: "COUNT", v: features.playerCount },
    { k: "ECON",  v: features.economy },
    { k: "PHASE", v: features.phase },
    { k: "TIME",  v: `0:${String(features.timeRemaining).padStart(2, "0")}` },
    { k: "UTIL",  v: `${features.utility.smokes}s · ${features.utility.molotovs}m · ${features.utility.flashes}f` },
  ];
  return (
    <div className="feature-strip" style={{ borderColor: theme.border, background: theme.panelSoft }}>
      {items.map((it, i) => (
        <div key={it.k} className="feature-cell"
          style={{ borderColor: i === items.length - 1 ? "transparent" : theme.border }}>
          <div className="feature-k" style={{ color: theme.dim }}>{it.k}</div>
          <div className="feature-v" style={{ color: theme.ink, fontFamily: theme.fontMono }}>{it.v}</div>
        </div>
      ))}
    </div>
  );
}

type ScrubberProps = {
  theme: Theme;
  progress: number;
  setProgress: (n: number) => void;
  playing: boolean;
  setPlaying: (b: boolean) => void;
  speed: number;
  setSpeed: (n: number) => void;
  tick: Situation["tick"];
};
function Scrubber({ theme, progress, setProgress, playing, setPlaying, speed, setSpeed, tick }: ScrubberProps) {
  const trackRef = useRef<HTMLDivElement | null>(null);
  const onTrack = (e: React.MouseEvent<HTMLDivElement>) => {
    if (!trackRef.current) return;
    const r = trackRef.current.getBoundingClientRect();
    const x = Math.max(0, Math.min(1, (e.clientX - r.left) / r.width));
    setProgress(x);
  };
  const tSec = -3 + progress * 15;
  const currentTick = Math.round(tick.start + progress * (tick.end - tick.start));

  const marks = useMemo(() => {
    type Mark = { t: number; side: "user" | "pro"; label: string; pct: number };
    const ev: Omit<Mark, "pct">[] = [];
    const d = SITUATION_DATA;
    for (const k of d.user.kills) ev.push({ t: k.t, side: "user", label: k.victim });
    for (const k of d.pro.kills)  ev.push({ t: k.t, side: "pro",  label: k.victim });
    if (d.user.bomb?.planted) ev.push({ t: d.user.bomb.plantedAt, side: "user", label: "C4" });
    if (d.pro.bomb?.planted)  ev.push({ t: d.pro.bomb.plantedAt,  side: "pro",  label: "C4" });
    return ev
      .map((e): Mark => ({ ...e, pct: Math.max(0, Math.min(1, (e.t + 3) / 15)) }))
      .filter((e) => e.pct >= 0 && e.pct <= 1);
  }, []);

  return (
    <div className="scrubber-row" style={{ borderColor: theme.border, background: theme.panel }}>
      <div className="transport">
        <button className="btn-xs"
          onClick={() => setProgress(Math.max(0, progress - 1 / 15))}
          style={{ color: theme.ink, borderColor: theme.border }}>‹‹</button>
        <button className="btn-play"
          onClick={() => setPlaying(!playing)}
          style={{ color: theme.paper, background: theme.accent }}>
          {playing ? "❚❚" : "▶"}
        </button>
        <button className="btn-xs"
          onClick={() => setProgress(Math.min(1, progress + 1 / 15))}
          style={{ color: theme.ink, borderColor: theme.border }}>››</button>
      </div>

      <div className="timeline" ref={trackRef}
        onClick={onTrack}
        onMouseMove={(e) => { if (e.buttons === 1) onTrack(e); }}>
        <div className="timeline-bg" style={{ background: theme.panelSoft, borderColor: theme.border }} />
        <div className="timeline-grid">
          {Array.from({ length: 16 }).map((_, i) => (
            <div key={i} className="tl-grid"
              style={{ left: `${(i / 15) * 100}%`, background: theme.border }} />
          ))}
        </div>
        <div className="tl-zero" style={{ left: `${(3 / 15) * 100}%`, background: theme.accent }}>
          <div className="tl-zero-label"
            style={{ color: theme.accent, background: theme.panel, borderColor: theme.border, fontFamily: theme.fontMono }}>
            T=0
          </div>
        </div>
        {marks.map((m, i) => (
          <div key={i} className={`tl-mark ${m.side}`}
            style={{
              left: `${m.pct * 100}%`,
              background: m.side === "user" ? theme.ct : theme.accent2,
            }}
            title={`${m.side}: ${m.label}`} />
        ))}
        <div className="timeline-fill"
          style={{
            width: `${progress * 100}%`,
            background: theme.accent + "33",
            borderRight: `1px solid ${theme.accent}`,
          }} />
        <div className="playhead" style={{ left: `${progress * 100}%`, background: theme.accent }} />
      </div>

      <div className="readouts" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
        <div className="readout-row">
          <span style={{ color: theme.dim }}>T</span>
          <span style={{ color: theme.ink }}>{tSec >= 0 ? "+" : ""}{tSec.toFixed(2)}s</span>
        </div>
        <div className="readout-row">
          <span style={{ color: theme.dim }}>TICK</span>
          <span style={{ color: theme.ink }}>{currentTick}</span>
        </div>
      </div>

      <div className="speed-group">
        {[0.5, 1, 2].map((s) => (
          <button key={s}
            onClick={() => setSpeed(s)}
            className="speed-btn"
            style={{
              color: speed === s ? theme.paper : theme.dim,
              background: speed === s ? theme.accent : "transparent",
              borderColor: theme.border,
              fontFamily: theme.fontMono,
            }}>
            {s}x
          </button>
        ))}
      </div>
    </div>
  );
}

function WhyMatched({ theme, match }: { theme: Theme; match: Situation["match"] }) {
  const score = match.score;
  return (
    <aside className="why" style={{ background: theme.panel, borderColor: theme.border }}>
      <div className="why-head">
        <div className="why-tag" style={{ color: theme.dim }}>/// WHY IT MATCHED</div>
        <div className="why-score">
          <svg viewBox="0 0 80 80" width="80" height="80">
            <circle cx="40" cy="40" r="34" fill="none" stroke={theme.border} strokeWidth="4" />
            <circle cx="40" cy="40" r="34" fill="none"
              stroke={theme.accent} strokeWidth="4" strokeLinecap="round"
              strokeDasharray={`${2 * Math.PI * 34 * score} ${2 * Math.PI * 34}`}
              transform="rotate(-90 40 40)" />
            <text x="40" y="44" textAnchor="middle"
              fill={theme.ink} fontFamily={theme.fontMono} fontSize="18" fontWeight="600">
              {(score * 100).toFixed(0)}
            </text>
            <text x="40" y="57" textAnchor="middle" fill={theme.dim}
              fontFamily={theme.fontMono} fontSize="7">MATCH</text>
          </svg>
          <div className="why-score-label" style={{ color: theme.ink, fontFamily: theme.fontHead }}>
            Strong mirror
            <div style={{ color: theme.dim, fontSize: 11, fontFamily: theme.fontMono, marginTop: 2 }}>
              top-1 of 147 candidates
            </div>
          </div>
        </div>
      </div>

      <div className="why-break">
        {match.breakdown.map((b: MatchBreakdownRow, i) => (
          <div key={i} className="why-row" style={{ borderColor: theme.border }}>
            <div className="why-row-top">
              <span className="why-label" style={{ color: theme.dim, fontFamily: theme.fontMono }}>{b.label}</span>
              <span className="why-weight" style={{ color: theme.dimmer, fontFamily: theme.fontMono }}>×{b.weight.toFixed(1)}</span>
            </div>
            <div className="why-row-val" style={{ color: theme.ink, fontFamily: theme.fontMono }}>
              <span className={b.matched ? "dot ok" : "dot no"}
                style={{ background: b.matched ? theme.accent : theme.dimmer }} />
              {b.value}
            </div>
            {b.note && <div className="why-note" style={{ color: theme.dim }}>{b.note}</div>}
          </div>
        ))}
      </div>

      <div className="why-note-box" style={{ borderColor: theme.borderHi, background: theme.panelSoft }}>
        <div className="why-note-tag" style={{ color: theme.accent, fontFamily: theme.fontMono }}>/// READ</div>
        <div className="why-note-body" style={{ color: theme.ink, fontFamily: theme.fontHead }}>
          {match.note}
        </div>
        <div className="why-cta" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
          ↳ FaZe vs. G2 · IEM Katowice 2026 · R19
        </div>
      </div>

      <div className="why-actions">
        <button className="act primary"
          style={{ background: theme.accent, color: theme.paper, fontFamily: theme.fontMono }}>
          NEXT SITUATION →
        </button>
        <button className="act"
          style={{ borderColor: theme.border, color: theme.ink, fontFamily: theme.fontMono }}>
          PIN
        </button>
        <button className="act"
          style={{ borderColor: theme.border, color: theme.ink, fontFamily: theme.fontMono }}>
          DISMISS
        </button>
      </div>
    </aside>
  );
}

function TopBar({ theme, data }: { theme: Theme; data: Situation }) {
  return (
    <header className="topbar" style={{ borderColor: theme.border, background: theme.panel }}>
      <div className="tb-left">
        <div className="brand" style={{ fontFamily: theme.fontHead, color: theme.ink }}>
          <span className="brand-mark" style={{ background: theme.accent, color: theme.paper }}>SP</span>
          <span>ShadowPro</span>
          <span className="brand-slash" style={{ color: theme.dimmer }}>/</span>
          <span style={{ color: theme.dim, fontFamily: theme.fontMono, fontSize: 12 }}>situation viewer</span>
        </div>
      </div>
      <nav className="tb-crumbs" style={{ fontFamily: theme.fontMono, color: theme.dim }}>
        <span>matches</span>
        <span style={{ color: theme.dimmer }}>/</span>
        <span>ESEA · mir_2026_04_12_2203</span>
        <span style={{ color: theme.dimmer }}>/</span>
        <span style={{ color: theme.ink }}>round {data.round}</span>
        <span style={{ color: theme.dimmer }}>/</span>
        <span style={{ color: theme.accent }}>situation 3 of 5</span>
      </nav>
      <div className="tb-right">
        <div className="scoreboard" style={{ fontFamily: theme.fontMono, color: theme.dim }}>
          <span style={{ color: theme.ct }}>CT {data.score.ct}</span>
          <span style={{ color: theme.dimmer }}>—</span>
          <span style={{ color: theme.tside }}>T {data.score.t}</span>
        </div>
        <div className="user-chip" style={{ borderColor: theme.border, color: theme.ink, fontFamily: theme.fontMono }}>
          <span className="dot ok" style={{ background: theme.accent }} />
          k1llj0y · L9
        </div>
      </div>
    </header>
  );
}

function RoundRail({ theme, data }: { theme: Theme; data: Situation }) {
  const counts = [0, 2, 1, 0, 3, 1, 0, 1, 2, 0, 4, 1, 2, 3, 0, 1, 2, 0, 1, 1, 0, 2, 0, 1];
  const rounds = counts.map((found, i) => ({ n: i + 1, found, current: i + 1 === data.round }));
  return (
    <div className="round-rail" style={{ borderColor: theme.border, background: theme.panel }}>
      <div className="rail-tag" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
        ROUNDS <span style={{ color: theme.ink }}>24</span>
        <span style={{ color: theme.dimmer }}> · </span>
        <span style={{ color: theme.accent }}>18 situations</span>
      </div>
      <div className="rail-track">
        {rounds.map((r) => (
          <button key={r.n}
            className={`rail-dot ${r.current ? "current" : ""} ${r.found === 0 ? "empty" : ""}`}
            style={{
              borderColor: r.current ? theme.accent : theme.border,
              background: r.current ? theme.accent + "22" : r.found ? theme.panelSoft : "transparent",
              color: r.current ? theme.accent : r.found ? theme.ink : theme.dimmer,
              fontFamily: theme.fontMono,
            }}
            title={r.found ? `${r.found} situation${r.found > 1 ? "s" : ""}` : "no strong match"}>
            <span className="rn">{r.n}</span>
            {r.found > 0 && <span className="cnt" style={{ color: r.current ? theme.accent : theme.dim }}>{r.found}</span>}
          </button>
        ))}
      </div>
    </div>
  );
}

type TweaksPanelProps = {
  theme: Theme;
  state: TweakState;
  set: (patch: Partial<TweakState>) => void;
  open: boolean;
  onClose: () => void;
};
function TweaksPanel({ theme, state, set, open, onClose }: TweaksPanelProps) {
  if (!open) return null;
  const Row = ({ label, children }: { label: string; children: React.ReactNode }) => (
    <div className="tw-row">
      <div className="tw-label" style={{ color: theme.dim, fontFamily: theme.fontMono }}>{label}</div>
      <div className="tw-ctrl">{children}</div>
    </div>
  );
  type Opt<T extends string> = { v: T; l: string };
  function Seg<T extends string>({ options, value, onChange }: { options: Opt<T>[]; value: T; onChange: (v: T) => void }) {
    return (
      <div className="tw-seg" style={{ borderColor: theme.border }}>
        {options.map((o) => (
          <button key={o.v}
            onClick={() => onChange(o.v)}
            style={{
              background: value === o.v ? theme.accent : "transparent",
              color: value === o.v ? theme.paper : theme.dim,
              fontFamily: theme.fontMono,
              borderColor: theme.border,
            }}>
            {o.l}
          </button>
        ))}
      </div>
    );
  }
  const Toggle = ({ v, onChange }: { v: boolean; onChange: (b: boolean) => void }) => (
    <button className="tw-toggle"
      onClick={() => onChange(!v)}
      style={{
        background: v ? theme.accent : "transparent",
        borderColor: v ? theme.accent : theme.border,
      }}>
      <span style={{
        background: v ? theme.paper : theme.dim,
        transform: v ? "translateX(18px)" : "translateX(2px)",
      }} />
    </button>
  );
  return (
    <div className="tweaks" style={{ background: theme.panel, borderColor: theme.borderHi, color: theme.ink }}>
      <div className="tw-head" style={{ borderColor: theme.border }}>
        <span style={{ fontFamily: theme.fontMono, color: theme.dim }}>/// TWEAKS</span>
        <button className="tw-close" onClick={onClose} style={{ color: theme.dim }}>×</button>
      </div>
      <Row label="THEME">
        <Seg
          options={[{ v: "tactical", l: "Tactical" }, { v: "editorial", l: "Editorial" }, { v: "broadcast", l: "Broadcast" }]}
          value={state.theme}
          onChange={(v) => set({ theme: v })} />
      </Row>
      <Row label="LAYOUT">
        <Seg
          options={[{ v: "side-by-side", l: "Side" }, { v: "stacked", l: "Stack" }, { v: "overlay", l: "Overlay" }]}
          value={state.layout}
          onChange={(v) => set({ layout: v })} />
      </Row>
      <Row label="MAP">
        <Seg
          options={[{ v: "full", l: "Full" }, { v: "dim", l: "Dim" }, { v: "wire", l: "Wire" }]}
          value={state.radarIntensity}
          onChange={(v) => set({ radarIntensity: v })} />
      </Row>
      <div className="tw-sep" style={{ borderColor: theme.border }} />
      <Row label="TRAILS"><Toggle v={state.showTrails} onChange={(v) => set({ showTrails: v })} /></Row>
      <Row label="SMOKES"><Toggle v={state.showSmokes} onChange={(v) => set({ showSmokes: v })} /></Row>
      <Row label="MOLOTOVS"><Toggle v={state.showMolotovs} onChange={(v) => set({ showMolotovs: v })} /></Row>
      <Row label="KILL X"><Toggle v={state.showKills} onChange={(v) => set({ showKills: v })} /></Row>
      <Row label="LABELS"><Toggle v={state.showLabels} onChange={(v) => set({ showLabels: v })} /></Row>
    </div>
  );
}

function PaneOverlay({ theme, data }: { theme: Theme; side: "user" | "pro"; data: SituationPane; progress: number }) {
  const alive = {
    ct: data.players.filter((p) => p.side === "CT" && p.alive).length,
    t:  data.players.filter((p) => p.side === "T"  && p.alive).length + 1, // include focal
  };
  return (
    <div className="pane-ov">
      <div className="ov-br"
        style={{ background: theme.panel + "cc", borderColor: theme.border, color: theme.dim, fontFamily: theme.fontMono }}>
        <span><span style={{ color: theme.ct }}>CT</span> {alive.ct}</span>
        <span style={{ color: theme.dimmer }}>vs</span>
        <span><span style={{ color: theme.tside }}>T</span> {alive.t}</span>
      </div>
      {data.bomb?.planted && (
        <div className="ov-tr"
          style={{ background: theme.panel + "cc", borderColor: theme.accent + "88", color: theme.accent, fontFamily: theme.fontMono }}>
          <span className="bomb-dot" style={{ background: theme.accent }} /> C4 PLANTED
        </div>
      )}
    </div>
  );
}

export function Viewer() {
  const [state, setState] = useState<TweakState>(TWEAK_DEFAULTS);
  const [tweaksOpen, setTweaksOpen] = useState(true);
  const theme = THEMES[state.theme];
  const data = SITUATION_DATA;
  const { progress, setProgress, playing, setPlaying, speed, setSpeed } = usePlayback();

  const [bg, setBg] = useState<HTMLImageElement | null>(null);
  useEffect(() => {
    const img = new Image();
    img.src = "mirage-placeholder.svg";
    img.onload = () => setBg(img);
  }, []);

  const update = (patch: Partial<TweakState>) => {
    setState((s) => ({ ...s, ...patch }));
  };

  const density: Density = {
    trails: state.showTrails,
    smokes: state.showSmokes,
    molotovs: state.showMolotovs,
    kills: state.showKills,
    labels: state.showLabels,
    callouts: state.showCallouts,
  };

  // Sync theme tokens to CSS variables for any non-inline style consumers.
  useEffect(() => {
    const r = document.documentElement.style;
    r.setProperty("--bg", theme.bg);
    r.setProperty("--panel", theme.panel);
    r.setProperty("--panelSoft", theme.panelSoft);
    r.setProperty("--border", theme.border);
    r.setProperty("--ink", theme.ink);
    r.setProperty("--dim", theme.dim);
    r.setProperty("--accent", theme.accent);
    r.setProperty("--fontHead", theme.fontHead);
    r.setProperty("--fontMono", theme.fontMono);
  }, [theme]);

  const radarProps = (s: "user" | "pro") => ({
    side: s,
    data: s === "user" ? data.user : data.pro,
    progress, theme, density, bg,
    intensity: state.radarIntensity,
  });

  return (
    <div className="app" style={{ background: theme.bg, color: theme.ink, fontFamily: theme.fontHead }}>
      <TopBar theme={theme} data={data} />
      <RoundRail theme={theme} data={data} />

      <main className={`stage layout-${state.layout}`}>
        <section className="radars">
          <div className="pane user">
            <PaneHeader kind="user" theme={theme}
              label={data.user.label} sub={data.user.sub} meta={data.user.matchMeta} />
            <div className="radar-hold" style={{ background: theme.paper, borderColor: theme.border }}>
              <Radar {...radarProps("user")} />
              <PaneOverlay theme={theme} side="user" data={data.user} progress={progress} />
            </div>
          </div>
          <div className="pane pro">
            <PaneHeader kind="pro" theme={theme}
              label={data.pro.label} sub={data.pro.sub} meta={data.pro.matchMeta}
              score={data.match.score} />
            <div className="radar-hold" style={{ background: theme.paper, borderColor: theme.border }}>
              <Radar {...radarProps("pro")} />
              <PaneOverlay theme={theme} side="pro" data={data.pro} progress={progress} />
            </div>
          </div>
        </section>
        <WhyMatched theme={theme} match={data.match} />
      </main>

      <FeatureStrip theme={theme} features={data.features} />
      <Scrubber
        theme={theme}
        progress={progress} setProgress={setProgress}
        playing={playing} setPlaying={setPlaying}
        speed={speed} setSpeed={setSpeed}
        tick={data.tick} />

      <TweaksPanel
        theme={theme} state={state} set={update}
        open={tweaksOpen}
        onClose={() => setTweaksOpen(false)} />

      {!tweaksOpen && (
        <button className="tweaks-fab"
          onClick={() => setTweaksOpen(true)}
          style={{
            background: theme.panel, borderColor: theme.borderHi,
            color: theme.dim, fontFamily: theme.fontMono,
          }}>
          /// TWEAKS
        </button>
      )}
    </div>
  );
}
