import { useEffect, useMemo, useRef, useState } from "react";
import { Radar } from "./Radar";
import { THEMES } from "./themes";
import { SITUATION_DATA } from "./mockData";
import type {
  Density, MatchBreakdownRow, Situation, SituationPane,
  Theme, TweakState, Side,
} from "./types";

// ── API types ──────────────────────────────────────────────────────────────────

type ApiSituation = {
  id: number;
  round_num: number;
  tick: number;
  clip_start_tick: number;
  clip_end_tick: number;
  player_side: string;
  player_place: string;
  player_name: string | null;
  economy_bucket: string;
  alive_ct: number;
  alive_t: number;
  phase: string;
  time_remaining_s: number;
  smokes_active: number;
  mollies_active: number;
  player_x_norm: number;
  player_y_norm: number;
};

type ApiMatch = {
  demo_id: string;
  map: { key: string; name: string; display: string };
  score_ct: number | null;
  score_t: number | null;
  round_count: number | null;
};

type PlayerSample = { tick: number; x: number; y: number };

type PlayerTrack = {
  steamid: string;
  name: string;
  side: string;   // "ct" | "t"
  is_focal: boolean;
  samples: PlayerSample[];
};

type RoundEvent = { tick: number; type: string; steamid: string; side: string };

type RoundData = {
  round_num: number;
  tick_start: number;
  tick_end: number;
  players: PlayerTrack[];
  events: RoundEvent[];
};

// ── Helpers ────────────────────────────────────────────────────────────────────

const ECON_LABELS: Record<string, string> = {
  full: "Full buy", semi: "Semi buy", eco: "Eco",
};
const PHASE_LABELS: Record<string, string> = {
  post_plant: "Post-plant", pre_plant: "Pre-plant", freeze: "Freeze",
};

function lerpPos(samples: PlayerSample[], tick: number): { x: number; y: number } {
  if (!samples.length) return { x: 0.5, y: 0.5 };
  if (tick <= samples[0].tick) return { x: samples[0].x, y: samples[0].y };
  const last = samples[samples.length - 1];
  if (tick >= last.tick) return { x: last.x, y: last.y };
  let lo = 0, hi = samples.length - 1;
  while (lo < hi - 1) {
    const mid = (lo + hi) >> 1;
    if (samples[mid].tick <= tick) lo = mid; else hi = mid;
  }
  const t = (tick - samples[lo].tick) / (samples[hi].tick - samples[lo].tick);
  return {
    x: samples[lo].x + t * (samples[hi].x - samples[lo].x),
    y: samples[lo].y + t * (samples[hi].y - samples[lo].y),
  };
}

// Build the user SituationPane live from round data at a given tick.
function buildLivePane(
  rd: RoundData,
  currentTick: number,
  matchDisplay: string,
): SituationPane {
  const focal = rd.players.find(p => p.is_focal) ?? rd.players[0];
  const others = rd.players.filter(p => p !== focal);

  const focalPos = focal ? lerpPos(focal.samples, currentTick) : { x: 0.5, y: 0.5 };
  const side: Side = (focal?.side ?? "ct") === "ct" ? "CT" : "T";

  const trail: [number, number][] = focal
    ? focal.samples
        .filter(s => s.tick <= currentTick)
        .slice(-12)                         // last 12 samples for the fading trail
        .map(s => [s.x, s.y])
    : [];

  const players = others.map(p => {
    const pos = lerpPos(p.samples, currentTick);
    const lastTick = p.samples[p.samples.length - 1]?.tick ?? 0;
    // Dead if no sample within last ~10 game-seconds (640 ticks @ 64Hz)
    const alive = lastTick >= currentTick - 640 && p.samples[0]?.tick <= currentTick;
    return {
      id: p.steamid,
      name: p.name,
      side: (p.side.toUpperCase() as Side),
      x: pos.x,
      y: pos.y,
      alive,
    };
  });

  return {
    label: "YOUR ROUND",
    sub: `Round ${rd.round_num}`,
    matchMeta: `${matchDisplay} · ${side === "CT" ? "CT-side" : "T-side"}`,
    focal: {
      id: "u-focal",
      name: focal?.name ?? "you",
      side,
      steamid: focal?.steamid ?? "",
      x: focalPos.x,
      y: focalPos.y,
    },
    players,
    smokes: [], molotovs: [], kills: [], trail,
  };
}

// Situation snapshot used for the feature strip and metadata.
function buildSituation(sit: ApiSituation, match: ApiMatch): Situation {
  const side: Side = (sit.player_side ?? "ct").toLowerCase() === "ct" ? "CT" : "T";
  return {
    round: sit.round_num,
    score: { ct: match.score_ct ?? 0, t: match.score_t ?? 0 },
    tick: { start: sit.clip_start_tick, current: sit.tick, end: sit.clip_end_tick },
    tickRate: 64,
    features: {
      area:          sit.player_place || "—",
      side,
      playerCount:   `${sit.alive_ct}v${sit.alive_t}`,
      economy:       ECON_LABELS[sit.economy_bucket] ?? sit.economy_bucket,
      phase:         PHASE_LABELS[sit.phase] ?? sit.phase,
      timeRemaining: Math.round(sit.time_remaining_s ?? 0),
      utility: { smokes: sit.smokes_active, molotovs: sit.mollies_active, flashes: 0 },
    },
    match: { score: 0, breakdown: [], note: "" },
    user: {
      label: "YOUR ROUND",
      sub: `Round ${sit.round_num} · ${Math.round(sit.time_remaining_s ?? 0)}s remaining`,
      matchMeta: `${match.map?.display ?? "?"} · ${side === "CT" ? "CT-side" : "T-side"}`,
      focal: {
        id: "u-focal", name: sit.player_name ?? "you", side, steamid: "",
        x: sit.player_x_norm, y: sit.player_y_norm,
      },
      players: [], smokes: [], molotovs: [], kills: [], trail: [],
    },
    pro: SITUATION_DATA.pro,
  };
}

// ── Tweak defaults ─────────────────────────────────────────────────────────────

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

// ── Playback ───────────────────────────────────────────────────────────────────
// durationSecs = how long progress 0→1 takes at speed=1 (real seconds).
// Speed is a game-time multiplier: speed=4 → 4× faster than real-time.

function usePlayback(durationSecs: number) {
  const [progress, setProgress] = useState(0);
  const [playing, setPlaying] = useState(true);
  const [speed, setSpeed] = useState(4);
  const rafRef = useRef<number | null>(null);
  const lastRef = useRef<number>(performance.now());

  useEffect(() => {
    setProgress(0);
  }, [durationSecs]);

  useEffect(() => {
    if (!playing) return;
    const tick = (now: number) => {
      const dt = (now - lastRef.current) / 1000;
      lastRef.current = now;
      setProgress((p) => {
        const next = p + (dt * speed) / durationSecs;
        if (next >= 1) { setPlaying(false); return 1; }
        return next;
      });
      rafRef.current = requestAnimationFrame(tick);
    };
    lastRef.current = performance.now();
    rafRef.current = requestAnimationFrame(tick);
    return () => { if (rafRef.current != null) cancelAnimationFrame(rafRef.current); };
  }, [playing, speed, durationSecs]);

  return { progress, setProgress, playing, setPlaying, speed, setSpeed };
}

// ── Sub-components ─────────────────────────────────────────────────────────────

type PaneHeaderProps = {
  kind: "user" | "pro"; theme: Theme;
  label: string; sub: string; meta: string; score?: number;
};
function PaneHeader({ kind, theme, label, sub, meta, score }: PaneHeaderProps) {
  const tone = kind === "pro" ? theme.accent : theme.ink;
  return (
    <div className="pane-head" style={{ borderColor: theme.border }}>
      <div className="pane-head-left">
        <div className="pane-tag" style={{ color: theme.dim }}>
          {kind === "user" ? "SOURCE · DEMO" : "MATCH · HLTV CORPUS"}
        </div>
        <div className="pane-label" style={{ color: tone, fontFamily: theme.fontHead }}>{label}</div>
        <div className="pane-sub" style={{ color: theme.dim }}>{sub}</div>
      </div>
      <div className="pane-head-right">
        {score != null && score > 0 && (
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
  tickStart: number;
  tickEnd: number;
  events: RoundEvent[];
};
function Scrubber({ theme, progress, setProgress, playing, setPlaying, speed, setSpeed, tickStart, tickEnd, events }: ScrubberProps) {
  const trackRef = useRef<HTMLDivElement | null>(null);
  const onTrack = (e: React.MouseEvent<HTMLDivElement>) => {
    if (!trackRef.current) return;
    const r = trackRef.current.getBoundingClientRect();
    setProgress(Math.max(0, Math.min(1, (e.clientX - r.left) / r.width)));
  };

  const gameDuration = (tickEnd - tickStart) / 64;
  const elapsedSec = progress * gameDuration;
  const currentTick = Math.round(tickStart + progress * (tickEnd - tickStart));

  const marks = useMemo(() => {
    const span = tickEnd - tickStart;
    if (!span) return [];
    return events
      .filter(e => e.type === "kill")
      .map(e => ({
        pct: Math.max(0, Math.min(1, (e.tick - tickStart) / span)),
        side: e.side,
      }));
  }, [events, tickStart, tickEnd]);

  const fmt = (s: number) => `${Math.floor(s / 60)}:${String(Math.floor(s % 60)).padStart(2, "0")}`;

  return (
    <div className="scrubber-row" style={{ borderColor: theme.border, background: theme.panel }}>
      <div className="transport">
        <button className="btn-xs"
          onClick={() => { setProgress(Math.max(0, progress - 5 / gameDuration)); setPlaying(false); }}
          style={{ color: theme.ink, borderColor: theme.border }}>‹‹</button>
        <button className="btn-play"
          onClick={() => { if (progress >= 1) setProgress(0); setPlaying(!playing); }}
          style={{ color: theme.paper, background: theme.accent }}>
          {playing ? "❚❚" : "▶"}
        </button>
        <button className="btn-xs"
          onClick={() => { setProgress(Math.min(1, progress + 5 / gameDuration)); setPlaying(false); }}
          style={{ color: theme.ink, borderColor: theme.border }}>››</button>
      </div>

      <div className="timeline" ref={trackRef}
        onClick={onTrack}
        onMouseMove={(e) => { if (e.buttons === 1) onTrack(e); }}>
        <div className="timeline-bg" style={{ background: theme.panelSoft, borderColor: theme.border }} />
        {/* Grid lines every 10 game-seconds */}
        <div className="timeline-grid">
          {Array.from({ length: Math.ceil(gameDuration / 10) + 1 }).map((_, i) => (
            <div key={i} className="tl-grid"
              style={{ left: `${Math.min(1, i * 10 / gameDuration) * 100}%`, background: theme.border }} />
          ))}
        </div>
        {marks.map((m, i) => (
          <div key={i} className={`tl-mark ${m.side === "ct" ? "user" : "pro"}`}
            style={{
              left: `${m.pct * 100}%`,
              background: m.side === "ct" ? theme.ct : theme.tside,
            }} />
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
          <span style={{ color: theme.dim }}>R</span>
          <span style={{ color: theme.ink }}>{fmt(elapsedSec)}</span>
        </div>
        <div className="readout-row">
          <span style={{ color: theme.dim }}>TICK</span>
          <span style={{ color: theme.ink }}>{currentTick}</span>
        </div>
      </div>

      <div className="speed-group">
        {[1, 2, 4].map((s) => (
          <button key={s}
            onClick={() => setSpeed(s)}
            className="speed-btn"
            style={{
              color: speed === s ? theme.paper : theme.dim,
              background: speed === s ? theme.accent : "transparent",
              borderColor: theme.border,
              fontFamily: theme.fontMono,
            }}>
            {s}×
          </button>
        ))}
      </div>
    </div>
  );
}

function NoMatchPanel({ theme, onNext, hasNext }: { theme: Theme; onNext: () => void; hasNext: boolean }) {
  return (
    <aside className="why" style={{ background: theme.panel, borderColor: theme.border }}>
      <div className="why-head">
        <div className="why-tag" style={{ color: theme.dim }}>/// PRO MATCH</div>
      </div>
      <div style={{ padding: "28px 20px 0" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 20 }}>
          <svg viewBox="0 0 80 80" width="80" height="80" style={{ flexShrink: 0 }}>
            <circle cx="40" cy="40" r="34" fill="none" stroke={theme.border} strokeWidth="4" />
            <text x="40" y="44" textAnchor="middle"
              fill={theme.dimmer} fontFamily={theme.fontMono} fontSize="11" fontWeight="600">—</text>
          </svg>
          <div>
            <div style={{ color: theme.ink, fontFamily: theme.fontHead, fontSize: 15, marginBottom: 4 }}>
              No match yet
            </div>
            <div style={{ color: theme.dim, fontFamily: theme.fontMono, fontSize: 11, lineHeight: 1.5 }}>
              Pro situation index is being built.<br />Matches will appear here automatically.
            </div>
          </div>
        </div>
      </div>
      <div className="why-note-box" style={{ borderColor: theme.border, background: theme.panelSoft, margin: "0 20px 20px" }}>
        <div className="why-note-tag" style={{ color: theme.dim, fontFamily: theme.fontMono }}>/// STATUS</div>
        <div className="why-note-body" style={{ color: theme.dim, fontFamily: theme.fontMono, fontSize: 11 }}>
          HLTV corpus indexing in progress. Once complete, each situation will be matched against 200+ pro demos.
        </div>
      </div>
      <div className="why-actions">
        {hasNext && (
          <button className="act primary" onClick={onNext}
            style={{ background: theme.accent, color: theme.paper, fontFamily: theme.fontMono }}>
            NEXT SITUATION →
          </button>
        )}
      </div>
    </aside>
  );
}

function WhyMatched({ theme, match, onNext, hasNext }: {
  theme: Theme; match: Situation["match"]; onNext: () => void; hasNext: boolean;
}) {
  const score = match.score;
  return (
    <aside className="why" style={{ background: theme.panel, borderColor: theme.border }}>
      <div className="why-head">
        <div className="why-tag" style={{ color: theme.dim }}>/// WHY IT MATCHED</div>
        <div className="why-score">
          <svg viewBox="0 0 80 80" width="80" height="80">
            <circle cx="40" cy="40" r="34" fill="none" stroke={theme.border} strokeWidth="4" />
            <circle cx="40" cy="40" r="34" fill="none" stroke={theme.accent} strokeWidth="4"
              strokeLinecap="round"
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
        <div className="why-note-body" style={{ color: theme.ink, fontFamily: theme.fontHead }}>{match.note}</div>
      </div>
      <div className="why-actions">
        <button className="act primary" onClick={onNext} disabled={!hasNext}
          style={{ background: theme.accent, color: theme.paper, fontFamily: theme.fontMono }}>
          NEXT SITUATION →
        </button>
        <button className="act" style={{ borderColor: theme.border, color: theme.ink, fontFamily: theme.fontMono }}>PIN</button>
        <button className="act" style={{ borderColor: theme.border, color: theme.ink, fontFamily: theme.fontMono }}>DISMISS</button>
      </div>
    </aside>
  );
}

type TopBarProps = {
  theme: Theme; data: Situation; steamId: string; matchId: string;
  situationIdx: number; situationCount: number;
  onSignOut: () => void; onBack?: () => void;
};
function TopBar({ theme, data, steamId, matchId, situationIdx, situationCount, onSignOut, onBack }: TopBarProps) {
  const shortId = steamId.slice(-8);
  const shortMatch = matchId.length > 24 ? matchId.slice(0, 24) + "…" : matchId;
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
        {onBack ? (
          <button onClick={onBack} style={{
            color: theme.ink, fontFamily: theme.fontMono, border: 0,
            background: "transparent", padding: 0, cursor: "pointer",
            letterSpacing: "inherit", fontSize: "inherit",
          }}>← matches</button>
        ) : <span>matches</span>}
        <span style={{ color: theme.dimmer }}>/</span>
        <span title={matchId}>{shortMatch}</span>
        <span style={{ color: theme.dimmer }}>/</span>
        <span style={{ color: theme.ink }}>round {data.round}</span>
        <span style={{ color: theme.dimmer }}>/</span>
        <span style={{ color: theme.accent }}>situation {situationIdx + 1} of {situationCount}</span>
      </nav>
      <div className="tb-right">
        <div className="scoreboard" style={{ fontFamily: theme.fontMono, color: theme.dim }}>
          <span style={{ color: theme.ct }}>CT {data.score.ct}</span>
          <span style={{ color: theme.dimmer }}>—</span>
          <span style={{ color: theme.tside }}>T {data.score.t}</span>
        </div>
        <div className="user-chip" style={{ borderColor: theme.border, color: theme.ink, fontFamily: theme.fontMono }}>
          <span className="dot ok" style={{ background: theme.accent }} />
          STEAM · {shortId}
        </div>
        <button className="signout-btn" onClick={onSignOut}
          style={{ color: theme.dim, borderColor: theme.border, fontFamily: theme.fontMono }}>
          SIGN OUT
        </button>
      </div>
    </header>
  );
}

type RoundRailProps = {
  theme: Theme; roundCount: number; currentRound: number;
  situationsByRound: Map<number, number>; totalSituations: number;
  onSelectRound: (round: number) => void;
};
function RoundRail({ theme, roundCount, currentRound, situationsByRound, totalSituations, onSelectRound }: RoundRailProps) {
  const rounds = Array.from({ length: roundCount }, (_, i) => {
    const n = i + 1;
    return { n, found: situationsByRound.get(n) ?? 0, current: n === currentRound };
  });
  return (
    <div className="round-rail" style={{ borderColor: theme.border, background: theme.panel }}>
      <div className="rail-tag" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
        ROUNDS <span style={{ color: theme.ink }}>{roundCount}</span>
        <span style={{ color: theme.dimmer }}> · </span>
        <span style={{ color: theme.accent }}>{totalSituations} situations</span>
      </div>
      <div className="rail-track">
        {rounds.map((r) => (
          <button key={r.n}
            className={`rail-dot ${r.current ? "current" : ""} ${r.found === 0 ? "empty" : ""}`}
            onClick={() => r.found > 0 && onSelectRound(r.n)}
            style={{
              borderColor: r.current ? theme.accent : theme.border,
              background: r.current ? theme.accent + "22" : r.found ? theme.panelSoft : "transparent",
              color: r.current ? theme.accent : r.found ? theme.ink : theme.dimmer,
              fontFamily: theme.fontMono,
              cursor: r.found > 0 ? "pointer" : "default",
            }}
            title={r.found ? `${r.found} situation${r.found > 1 ? "s" : ""}` : "no data"}>
            <span className="rn">{r.n}</span>
            {r.found > 0 && <span className="cnt" style={{ color: r.current ? theme.accent : theme.dim }}>{r.found}</span>}
          </button>
        ))}
      </div>
    </div>
  );
}

type TweaksPanelProps = {
  theme: Theme; state: TweakState; set: (patch: Partial<TweakState>) => void;
  open: boolean; onClose: () => void;
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
          <button key={o.v} onClick={() => onChange(o.v)} style={{
            background: value === o.v ? theme.accent : "transparent",
            color: value === o.v ? theme.paper : theme.dim,
            fontFamily: theme.fontMono, borderColor: theme.border,
          }}>{o.l}</button>
        ))}
      </div>
    );
  }
  const Toggle = ({ v, onChange }: { v: boolean; onChange: (b: boolean) => void }) => (
    <button className="tw-toggle" onClick={() => onChange(!v)} style={{
      background: v ? theme.accent : "transparent",
      borderColor: v ? theme.accent : theme.border,
    }}>
      <span style={{ background: v ? theme.paper : theme.dim, transform: v ? "translateX(18px)" : "translateX(2px)" }} />
    </button>
  );
  return (
    <div className="tweaks" style={{ background: theme.panel, borderColor: theme.borderHi, color: theme.ink }}>
      <div className="tw-head" style={{ borderColor: theme.border }}>
        <span style={{ fontFamily: theme.fontMono, color: theme.dim }}>/// TWEAKS</span>
        <button className="tw-close" onClick={onClose} style={{ color: theme.dim }}>×</button>
      </div>
      <Row label="THEME">
        <Seg options={[{ v: "tactical", l: "Tactical" }, { v: "editorial", l: "Editorial" }, { v: "broadcast", l: "Broadcast" }]}
          value={state.theme} onChange={(v) => set({ theme: v })} />
      </Row>
      <Row label="LAYOUT">
        <Seg options={[{ v: "side-by-side", l: "Side" }, { v: "stacked", l: "Stack" }, { v: "overlay", l: "Overlay" }]}
          value={state.layout} onChange={(v) => set({ layout: v })} />
      </Row>
      <Row label="MAP">
        <Seg options={[{ v: "full", l: "Full" }, { v: "dim", l: "Dim" }, { v: "wire", l: "Wire" }]}
          value={state.radarIntensity} onChange={(v) => set({ radarIntensity: v })} />
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

function PaneOverlay({ theme, data }: { theme: Theme; side: "user" | "pro"; data: SituationPane }) {
  const alive = {
    ct: data.players.filter(p => p.side === "CT" && p.alive).length + (data.focal.side === "CT" ? 1 : 0),
    t:  data.players.filter(p => p.side === "T"  && p.alive).length + (data.focal.side === "T"  ? 1 : 0),
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

// ── Main Viewer ────────────────────────────────────────────────────────────────

type ViewerProps = { matchId: string; steamId: string; onSignOut: () => void; onBack?: () => void };
export function Viewer({ matchId, steamId, onSignOut, onBack }: ViewerProps) {
  const [tweakState, setTweakState] = useState<TweakState>(TWEAK_DEFAULTS);
  const [tweaksOpen, setTweaksOpen] = useState(true);
  const theme = THEMES[tweakState.theme];

  // Match + situations list
  const [situations, setSituations] = useState<ApiSituation[]>([]);
  const [apiMatch, setApiMatch] = useState<ApiMatch | null>(null);
  const [loading, setLoading] = useState(true);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [selectedIdx, setSelectedIdx] = useState(0);

  // Round playback data
  const [roundData, setRoundData] = useState<RoundData | null>(null);
  const [roundLoading, setRoundLoading] = useState(false);

  useEffect(() => {
    setLoading(true);
    setFetchError(null);
    fetch(`/api/situations/${encodeURIComponent(matchId)}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(data => { setApiMatch(data.match); setSituations(data.situations ?? []); setSelectedIdx(0); })
      .catch(err => setFetchError(String(err)))
      .finally(() => setLoading(false));
  }, [matchId]);

  // Fetch full round data whenever the selected round changes
  const currentSit = situations[selectedIdx] ?? null;
  const currentRoundNum = currentSit?.round_num ?? null;
  useEffect(() => {
    if (!currentRoundNum) return;
    setRoundLoading(true);
    setRoundData(null);
    fetch(`/api/round/${encodeURIComponent(matchId)}/${currentRoundNum}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => setRoundData(data))
      .catch(() => setRoundData(null))
      .finally(() => setRoundLoading(false));
  }, [matchId, currentRoundNum]);

  // Playback: duration = game-seconds for this round at 64 tick
  const gameDurationSecs = roundData
    ? (roundData.tick_end - roundData.tick_start) / 64
    : 120;
  const { progress, setProgress, playing, setPlaying, speed, setSpeed } = usePlayback(gameDurationSecs);

  // Map background image
  const [bg, setBg] = useState<HTMLImageElement | null>(null);
  useEffect(() => {
    if (!apiMatch) return;
    setBg(null);
    const img = new Image();
    img.src = `/maps/${apiMatch.map.name}.png`;
    img.onload = () => setBg(img);
    img.onerror = () => setBg(null);
  }, [apiMatch?.map.name]);

  const update = (patch: Partial<TweakState>) => setTweakState(s => ({ ...s, ...patch }));

  const density: Density = {
    trails: tweakState.showTrails, smokes: tweakState.showSmokes,
    molotovs: tweakState.showMolotovs, kills: tweakState.showKills,
    labels: tweakState.showLabels, callouts: tweakState.showCallouts,
  };

  useEffect(() => {
    const r = document.documentElement.style;
    r.setProperty("--bg", theme.bg); r.setProperty("--panel", theme.panel);
    r.setProperty("--panelSoft", theme.panelSoft); r.setProperty("--border", theme.border);
    r.setProperty("--ink", theme.ink); r.setProperty("--dim", theme.dim);
    r.setProperty("--accent", theme.accent); r.setProperty("--fontHead", theme.fontHead);
    r.setProperty("--fontMono", theme.fontMono);
  }, [theme]);

  // Situation snapshot (features strip, score, metadata)
  const sitSnap: Situation = useMemo(
    () => (currentSit && apiMatch ? buildSituation(currentSit, apiMatch) : SITUATION_DATA),
    [currentSit, apiMatch],
  );

  // Live user pane — recomputed on every progress change via currentTick
  const currentTick = roundData
    ? Math.round(roundData.tick_start + progress * (roundData.tick_end - roundData.tick_start))
    : sitSnap.tick.current;

  const liveUserPane: SituationPane = roundData
    ? buildLivePane(roundData, currentTick, apiMatch?.map.display ?? "")
    : sitSnap.user;

  const situationsByRound = useMemo(() => {
    const m = new Map<number, number>();
    for (const s of situations) m.set(s.round_num, (m.get(s.round_num) ?? 0) + 1);
    return m;
  }, [situations]);

  const roundCount = apiMatch?.round_count
    ?? (situationsByRound.size > 0 ? Math.max(...situationsByRound.keys()) : 24);

  const goToRound = (round: number) => {
    const idx = situations.findIndex(s => s.round_num === round);
    if (idx >= 0) setSelectedIdx(idx);
  };
  const goNext = () => {
    if (selectedIdx < situations.length - 1) setSelectedIdx(selectedIdx + 1);
  };

  const radarUser = { side: "user" as const, data: liveUserPane, progress, theme, density, bg, intensity: tweakState.radarIntensity };
  const radarPro  = { side: "pro"  as const, data: sitSnap.pro,   progress, theme, density, bg, intensity: tweakState.radarIntensity };

  // ── Loading / error / empty ──────────────────────────────────────────────────

  if (loading) {
    return (
      <div className="app" style={{ background: theme.bg, color: theme.ink, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ fontFamily: theme.fontMono, fontSize: 12, color: theme.dim, letterSpacing: "0.08em" }}>
          LOADING SITUATIONS…
        </div>
      </div>
    );
  }

  if (fetchError) {
    return (
      <div className="app" style={{ background: theme.bg, color: theme.ink, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 16 }}>
        <div style={{ fontFamily: theme.fontMono, fontSize: 12, color: theme.dim }}>FAILED TO LOAD</div>
        <div style={{ fontFamily: theme.fontMono, fontSize: 11, color: theme.dimmer }}>{fetchError}</div>
        <button onClick={onBack} style={{ fontFamily: theme.fontMono, fontSize: 11, color: theme.ink, background: "transparent", border: `1px solid ${theme.border}`, padding: "6px 14px", cursor: "pointer" }}>← BACK</button>
      </div>
    );
  }

  if (situations.length === 0) {
    return (
      <div className="app" style={{ background: theme.bg, color: theme.ink }}>
        <header className="topbar" style={{ borderColor: theme.border, background: theme.panel }}>
          <div className="tb-left">
            <div className="brand" style={{ fontFamily: theme.fontHead, color: theme.ink }}>
              <span className="brand-mark" style={{ background: theme.accent, color: theme.paper }}>SP</span>
              <span>ShadowPro</span>
            </div>
          </div>
          <nav className="tb-crumbs" style={{ fontFamily: theme.fontMono, color: theme.dim }}>
            <button onClick={onBack} style={{ color: theme.ink, fontFamily: theme.fontMono, border: 0, background: "transparent", padding: 0, cursor: "pointer" }}>← matches</button>
          </nav>
          <div className="tb-right">
            <button className="signout-btn" onClick={onSignOut} style={{ color: theme.dim, borderColor: theme.border, fontFamily: theme.fontMono }}>SIGN OUT</button>
          </div>
        </header>
        <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: "calc(100vh - 48px)", gap: 12 }}>
          <div style={{ fontFamily: theme.fontMono, fontSize: 13, color: theme.dim }}>NO SITUATIONS FOUND</div>
          <div style={{ fontFamily: theme.fontMono, fontSize: 11, color: theme.dimmer }}>This demo may still be processing.</div>
        </div>
      </div>
    );
  }

  // ── Main view ────────────────────────────────────────────────────────────────

  return (
    <div className="app" style={{ background: theme.bg, color: theme.ink, fontFamily: theme.fontHead }}>
      <TopBar
        theme={theme} data={sitSnap} steamId={steamId} matchId={matchId}
        situationIdx={selectedIdx} situationCount={situations.length}
        onSignOut={onSignOut} onBack={onBack}
      />
      <RoundRail
        theme={theme} roundCount={roundCount} currentRound={sitSnap.round}
        situationsByRound={situationsByRound} totalSituations={situations.length}
        onSelectRound={goToRound}
      />

      <main className={`stage layout-${tweakState.layout}`}>
        <section className="radars">
          <div className="pane user">
            <PaneHeader kind="user" theme={theme}
              label={liveUserPane.label} sub={liveUserPane.sub} meta={liveUserPane.matchMeta} />
            <div className="radar-hold" style={{ background: theme.paper, borderColor: theme.border }}>
              {roundLoading && (
                <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", background: theme.bg + "aa", zIndex: 2 }}>
                  <span style={{ fontFamily: theme.fontMono, fontSize: 10, color: theme.dim }}>LOADING…</span>
                </div>
              )}
              <Radar {...radarUser} />
              <PaneOverlay theme={theme} side="user" data={liveUserPane} />
            </div>
          </div>
          <div className="pane pro">
            <PaneHeader kind="pro" theme={theme}
              label={sitSnap.pro.label} sub={sitSnap.pro.sub} meta={sitSnap.pro.matchMeta}
              score={sitSnap.match.score > 0 ? sitSnap.match.score : undefined} />
            <div className="radar-hold" style={{ background: theme.paper, borderColor: theme.border }}>
              <Radar {...radarPro} />
              <PaneOverlay theme={theme} side="pro" data={sitSnap.pro} />
            </div>
          </div>
        </section>

        {sitSnap.match.score > 0 ? (
          <WhyMatched theme={theme} match={sitSnap.match} onNext={goNext} hasNext={selectedIdx < situations.length - 1} />
        ) : (
          <NoMatchPanel theme={theme} onNext={goNext} hasNext={selectedIdx < situations.length - 1} />
        )}
      </main>

      <FeatureStrip theme={theme} features={sitSnap.features} />
      <Scrubber
        theme={theme}
        progress={progress} setProgress={setProgress}
        playing={playing} setPlaying={setPlaying}
        speed={speed} setSpeed={setSpeed}
        tickStart={roundData?.tick_start ?? sitSnap.tick.start}
        tickEnd={roundData?.tick_end ?? sitSnap.tick.end}
        events={roundData?.events ?? []}
      />

      <TweaksPanel
        theme={theme} state={tweakState} set={update}
        open={tweaksOpen} onClose={() => setTweaksOpen(false)} />

      {!tweaksOpen && (
        <button className="tweaks-fab" onClick={() => setTweaksOpen(true)}
          style={{ background: theme.panel, borderColor: theme.borderHi, color: theme.dim, fontFamily: theme.fontMono }}>
          /// TWEAKS
        </button>
      )}
    </div>
  );
}
