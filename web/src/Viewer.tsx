import { useEffect, useMemo, useRef, useState } from "react";
import { THEMES } from "./themes";
import type { ThemeKey } from "./types";
import { RadarCanvas } from "./replay/RadarCanvas";
import { useRoundPlayback, buildWeaponMap } from "./replay/useRoundPlayback";
import type {
  MapConfig,
  RoundAnalysisResponse,
  RoundMeta,
  RoundReplayData,
} from "./replay/types";
import { TICKRATE } from "./replay/constants";

// ── Props ──────────────────────────────────────────────────────────────────────

type ViewerProps = {
  matchId: string;
  steamId: string;
  initialRound?: number;
  roundCount?: number | null;
  mapDisplay?: string | null;
  onSignOut: () => void;
  onBack?: () => void;
};

// ── Helpers ────────────────────────────────────────────────────────────────────

function useImage(url: string | null): HTMLImageElement | null {
  const [img, setImg] = useState<HTMLImageElement | null>(null);
  useEffect(() => {
    if (!url) { setImg(null); return; }
    const el = new Image();
    el.onload = () => setImg(el);
    el.src = url;
  }, [url]);
  return img;
}

function fmtElapsed(curTick: number, freezeEnd: number): string {
  const s = Math.max(0, (curTick - freezeEnd) / TICKRATE);
  const m = Math.floor(s / 60);
  const sec = Math.floor(s % 60);
  return `${m}:${String(sec).padStart(2, "0")}`;
}

function findNearestTickIndex(tickList: number[], targetTick: number): number {
  if (!tickList.length) return 0;
  let bestIdx = 0;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (let idx = 0; idx < tickList.length; idx += 1) {
    const distance = Math.abs(tickList[idx] - targetTick);
    if (distance < bestDistance) {
      bestDistance = distance;
      bestIdx = idx;
    }
  }
  return bestIdx;
}

function userSideThisRound(data: RoundReplayData | null, steamId: string): "ct" | "t" | null {
  const first = data?.ticks[0];
  if (!first) return null;
  return first.players.find(p => p.steamid === steamId)?.side ?? null;
}

function fmtReason(reason: string | null): string {
  if (!reason) return "—";
  return reason.replaceAll("_", " ").toLowerCase();
}

// ── Main ───────────────────────────────────────────────────────────────────────

export function Viewer({
  matchId, steamId, initialRound = 1, roundCount, mapDisplay,
  onSignOut, onBack,
}: ViewerProps) {
  const [themeKey, setThemeKey] = useState<ThemeKey>("tactical");
  const [tweaksOpen, setTweaksOpen] = useState(false);
  const theme = THEMES[themeKey];

  const [roundNum, setRoundNum] = useState(initialRound);
  const [data, setData]         = useState<RoundReplayData | null>(null);
  const [maps, setMaps]         = useState<MapConfig[]>([]);
  const [loading, setLoading]   = useState(true);
  const [error, setError]       = useState<string | null>(null);
  const [showLower, setShowLower] = useState(false);
  const [analysis, setAnalysis] = useState<RoundAnalysisResponse | null>(null);
  const [proData, setProData] = useState<RoundReplayData | null>(null);
  const [proError, setProError] = useState<string | null>(null);

  const containerRef = useRef<HTMLDivElement>(null);
  const proContainerRef = useRef<HTMLDivElement>(null);
  const [canvasSize, setCanvasSize] = useState({ w: 600, h: 600 });
  const [proCanvasSize, setProCanvasSize] = useState({ w: 600, h: 600 });

  // Apply theme css vars.
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

  // Map configs.
  useEffect(() => {
    fetch("/api/maps").then(r => r.json()).then(setMaps).catch(() => {});
  }, []);

  // User round + analysis fetched in parallel — analysis is precomputed at ingest
  // so we can request it immediately without waiting for the replay payload.
  useEffect(() => {
    setLoading(true);
    setError(null);
    setData(null);
    setAnalysis(null);

    let active = true;

    fetch(`/api/round-replay/${encodeURIComponent(matchId)}/${roundNum}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((d: RoundReplayData) => { if (!active) return; setData(d); setLoading(false); })
      .catch(e => { if (!active) return; setError(String(e)); setLoading(false); });

    fetch(`/api/round-analysis/${encodeURIComponent(matchId)}/${roundNum}?logic=both`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((payload: RoundAnalysisResponse) => { if (active) setAnalysis(payload); })
      .catch(() => { if (active) setAnalysis(null); });

    return () => { active = false; };
  }, [matchId, roundNum]);

  // Responsive canvas sizing.
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ob = new ResizeObserver(() => {
      const rect = el.getBoundingClientRect();
      const sz = Math.floor(Math.min(rect.width, rect.height));
      setCanvasSize({ w: sz, h: sz });
    });
    ob.observe(el);
    return () => ob.disconnect();
  }, []);

  useEffect(() => {
    const el = proContainerRef.current;
    if (!el) return;
    const ob = new ResizeObserver(() => {
      const rect = el.getBoundingClientRect();
      const sz = Math.floor(Math.min(rect.width, rect.height));
      setProCanvasSize({ w: sz, h: sz });
    });
    ob.observe(el);
    return () => ob.disconnect();
  }, []);

  const mapConfig = maps.find(m => m.name === data?.map) ?? null;
  const weaponMap = useMemo(
    () => (data ? buildWeaponMap(data.shots, data.tick_list) : {}),
    [data],
  );
  const tickCount = data?.tick_list.length ?? 0;
  const { tickIdx, setTickIdx, playing, setPlaying, speed, setSpeed } =
    useRoundPlayback(tickCount);

  const radarUrl      = mapConfig ? `/api/radar/${mapConfig.name}` : null;
  const lowerRadarUrl = mapConfig?.has_lower_level ? `/api/radar/${mapConfig.name}_lower` : null;
  const radarImg      = useImage(radarUrl);
  const lowerImg      = useImage(lowerRadarUrl);

  // Keyboard controls.
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (!data) return;
      const step = e.shiftKey ? 5 * TICKRATE : TICKRATE;
      switch (e.key) {
        case " ":           e.preventDefault(); setPlaying(!playing); break;
        case "ArrowRight":  e.preventDefault(); setTickIdx(Math.min(tickCount - 1, tickIdx + step)); break;
        case "ArrowLeft":   e.preventDefault(); setTickIdx(Math.max(0, tickIdx - step)); break;
        case "[":           setRoundNum(n => Math.max(1, n - 1)); break;
        case "]":           setRoundNum(n => (roundCount ? Math.min(roundCount, n + 1) : n + 1)); break;
        case "Home":        e.preventDefault(); setTickIdx(0); break;
        case "l": case "L": if (mapConfig?.has_lower_level) setShowLower(v => !v); break;
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [data, playing, tickIdx, tickCount, mapConfig, roundCount, setPlaying, setTickIdx]);

  const curTick   = data?.tick_list[tickIdx] ?? 0;
  const freezeEnd = data?.freeze_end_tick ?? 0;
  const mapName   = mapDisplay ?? mapConfig?.display_name ?? data?.map ?? "—";
  const shortId   = steamId.slice(-8);
  const shortMatch = matchId.length > 24 ? matchId.slice(0, 24) + "…" : matchId;

  const analysisResult = analysis?.result ?? null;
  const bestMatch = analysisResult?.best_match ?? null;

  // Pro replay loads as soon as we know which pro round to fetch.
  useEffect(() => {
    if (!bestMatch) {
      setProData(null);
      setProError(null);
      return;
    }

    let active = true;
    setProError(null);
    fetch(`/api/round-replay/${encodeURIComponent(bestMatch.source_match_id)}/${bestMatch.round_num}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((payload: RoundReplayData) => { if (active) setProData(payload); })
      .catch(err => { if (!active) return; setProError(String(err)); setProData(null); });

    return () => { active = false; };
  }, [bestMatch]);

  const proWeaponMap = useMemo(
    () => (proData ? buildWeaponMap(proData.shots, proData.tick_list) : {}),
    [proData],
  );
  const proMapConfig = maps.find(m => m.name === proData?.map) ?? null;
  const proRadarUrl = proMapConfig ? `/api/radar/${proMapConfig.name}` : null;
  const proLowerRadarUrl = proMapConfig?.has_lower_level ? `/api/radar/${proMapConfig.name}_lower` : null;
  const proRadarImg = useImage(proRadarUrl);
  const proLowerImg = useImage(proLowerRadarUrl);

  const commonElapsedS = data ? Math.max(0, (curTick - data.freeze_end_tick) / TICKRATE) : 0;
  const proTickIdx = useMemo(() => {
    if (!proData || !data) return 0;
    const targetTick = proData.freeze_end_tick + Math.round(commonElapsedS * TICKRATE);
    return findNearestTickIndex(proData.tick_list, targetTick);
  }, [commonElapsedS, data, proData]);
  const proCurTick = proData?.tick_list[proTickIdx] ?? 0;

  const userSide = userSideThisRound(data, steamId);
  const roundMeta: RoundMeta | undefined = data?.round_meta;
  const scoreBefore = roundMeta?.score_before;
  const outcome = roundMeta?.outcome;

  const userScoreGoingIn = userSide && scoreBefore
    ? (userSide === "ct" ? scoreBefore.ct : scoreBefore.t)
    : null;
  const enemyScoreGoingIn = userSide && scoreBefore
    ? (userSide === "ct" ? scoreBefore.t : scoreBefore.ct)
    : null;

  const userWonRound =
    userSide && outcome?.winner_side
      ? outcome.winner_side === userSide
      : null;

  // Per-player mapping is not produced by the placeholder mapper; highlight only
  // the user's own steam id on their side, and nothing on the pro side.
  const userHighlightedSteamIds = bestMatch ? [steamId] : [];
  const proHighlightedSteamIds: string[]  = [];

  return (
    <div className="app" style={{ background: theme.bg, color: theme.ink, fontFamily: theme.fontHead }}>
      {/* ── TopBar ─────────────────────────────────────────────────────────── */}
      <header className="topbar" style={{ borderColor: theme.border, background: theme.panel }}>
        <div className="tb-left">
          <div className="brand" style={{ fontFamily: theme.fontHead, color: theme.ink }}>
            <span className="brand-mark" style={{ background: theme.accent, color: theme.paper }}>SP</span>
            <span>ShadowPro</span>
            <span className="brand-slash" style={{ color: theme.dimmer }}>/</span>
            <span style={{ color: theme.dim, fontFamily: theme.fontMono, fontSize: 12 }}>round replay</span>
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
          <span style={{ color: theme.ink }}>round {roundNum}</span>
          {mapConfig?.has_lower_level && (
            <>
              <span style={{ color: theme.dimmer }}>/</span>
              <span style={{ color: theme.accent }}>{showLower ? "LOWER" : "UPPER"}</span>
            </>
          )}
        </nav>
        <div className="tb-right">
          <div className="scoreboard" style={{ fontFamily: theme.fontMono, color: theme.dim }}>
            <span style={{ color: theme.dim }}>{mapName}</span>
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

      {/* ── RoundRail ──────────────────────────────────────────────────────── */}
      <div className="round-rail" style={{ borderColor: theme.border, background: theme.panel }}>
        <div className="rail-tag" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
          ROUNDS{roundCount ? <> <span style={{ color: theme.ink }}>{roundCount}</span></> : null}
          <span style={{ color: theme.dimmer }}> · </span>
          <span style={{ color: theme.accent }}>round {roundNum}</span>
        </div>
        <div className="rail-track">
          {Array.from({ length: Math.max(roundCount ?? roundNum, 1) }, (_, i) => {
            const n = i + 1;
            const current = n === roundNum;
            return (
              <button
                key={n}
                className={`rail-dot ${current ? "current" : ""}`}
                onClick={() => setRoundNum(n)}
                style={{
                  borderColor: current ? theme.accent : theme.border,
                  background: current ? theme.accent + "22" : theme.panelSoft,
                  color: current ? theme.accent : theme.ink,
                  fontFamily: theme.fontMono,
                  cursor: "pointer",
                }}>
                <span className="rn">{n}</span>
              </button>
            );
          })}
        </div>
      </div>

      {/* ── Stage ──────────────────────────────────────────────────────────── */}
      <main className="stage layout-side-by-side">
        <section className="radars">
          {/* USER pane */}
          <div className="pane user">
            <div className="pane-head" style={{ borderColor: theme.border }}>
              <div className="pane-head-left">
                <div className="pane-tag" style={{ color: theme.dim }}>SOURCE · DEMO</div>
                <div className="pane-label" style={{ color: theme.ink, fontFamily: theme.fontHead }}>YOUR ROUND</div>
                <div className="pane-sub" style={{ color: theme.dim }}>Round {roundNum}</div>
              </div>
              <div className="pane-head-right">
                <div className="pane-meta" style={{ color: theme.dim }}>
                  {mapName}
                </div>
              </div>
            </div>
            <div ref={containerRef} className="radar-hold"
              style={{ background: theme.paper, borderColor: theme.border }}>
              {loading && (
                <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center",
                  justifyContent: "center", color: theme.dim, fontFamily: theme.fontMono, fontSize: 11 }}>
                  LOADING…
                </div>
              )}
              {error && (
                <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center",
                  justifyContent: "center", color: theme.dim, fontFamily: theme.fontMono, fontSize: 11 }}>
                  ERROR: {error}
                </div>
              )}
              {data && radarImg && mapConfig && (
                <RadarCanvas
                  mapConfig={mapConfig}
                  radarImage={radarImg}
                  lowerRadarImage={lowerImg ?? undefined}
                  data={data}
                  tickIdx={tickIdx}
                  weaponMap={weaponMap}
                  highlightedSteamIds={userHighlightedSteamIds}
                  showLower={showLower}
                  width={canvasSize.w}
                  height={canvasSize.h}
                />
              )}
            </div>
          </div>

          {/* PRO pane */}
          <div className="pane pro">
            <div className="pane-head" style={{ borderColor: theme.border }}>
              <div className="pane-head-left">
                <div className="pane-tag" style={{ color: theme.dim }}>MATCH · HLTV CORPUS</div>
                <div className="pane-label" style={{ color: theme.accent, fontFamily: theme.fontHead }}>PRO MATCH</div>
                <div className="pane-sub" style={{ color: theme.dim }}>
                  {bestMatch
                    ? `${bestMatch.map.display} · round ${bestMatch.round_num}`
                    : "no pro round mapped"}
                </div>
              </div>
              {bestMatch && (
                <div className="pane-head-right">
                  <div className="pane-meta" style={{ color: theme.dim }}>
                    {Math.round(bestMatch.score * 100)}% match
                  </div>
                </div>
              )}
            </div>
            <div ref={proContainerRef} className="radar-hold"
              style={{ background: theme.paper, borderColor: theme.border, display: "flex",
                       alignItems: "center", justifyContent: "center" }}>
              {!bestMatch && !proError && (
                <span style={{ color: theme.dimmer, fontFamily: theme.fontMono, fontSize: 11,
                               letterSpacing: "0.12em" }}>
                  NO PRO ROUND MAPPED
                </span>
              )}
              {proError && (
                <span style={{ color: theme.dimmer, fontFamily: theme.fontMono, fontSize: 11,
                               letterSpacing: "0.08em", textAlign: "center", maxWidth: "70%" }}>
                  PRO REPLAY ERROR: {proError}
                </span>
              )}
              {proData && proRadarImg && proMapConfig && bestMatch && (
                <RadarCanvas
                  mapConfig={proMapConfig}
                  radarImage={proRadarImg}
                  lowerRadarImage={proLowerImg ?? undefined}
                  data={proData}
                  tickIdx={proTickIdx}
                  weaponMap={proWeaponMap}
                  highlightedSteamIds={proHighlightedSteamIds}
                  showLower={false}
                  width={proCanvasSize.w}
                  height={proCanvasSize.h}
                />
              )}
            </div>
          </div>
        </section>

        {/* ── Side panel ──────────────────────────────────────────────────── */}
        <aside className="why" style={{ borderColor: theme.border, background: theme.panel, color: theme.ink }}>
          <div className="why-head">
            <div className="why-tag" style={{ color: theme.dim }}>/// ROUND DETAIL</div>
            <div className="why-score">
              <span className="why-score-label" style={{ color: theme.ink }}>Round {roundNum}</span>
              <span style={{ color: theme.dim, fontFamily: theme.fontMono, fontSize: 11 }}>
                {mapName}
              </span>
            </div>
          </div>

          {/* Scoreline going in */}
          {scoreBefore && (
            <div className="why-note-box" style={{ borderColor: theme.border, marginTop: 14 }}>
              <div className="why-note-tag" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
                SCOREBOARD · GOING IN
              </div>
              <div style={{ display: "flex", alignItems: "baseline", gap: 12, marginTop: 4 }}>
                <span style={{
                  fontSize: 34, fontWeight: 700, letterSpacing: "-0.02em",
                  fontFamily: theme.fontMono, color: theme.ink,
                }}>
                  {userScoreGoingIn ?? scoreBefore.ct} <span style={{ color: theme.dimmer }}>:</span> {enemyScoreGoingIn ?? scoreBefore.t}
                </span>
                <span style={{ color: theme.dim, fontFamily: theme.fontMono, fontSize: 11, letterSpacing: "0.08em" }}>
                  {userSide
                    ? `YOU (${userSide.toUpperCase()}) vs ${userSide === "ct" ? "T" : "CT"}`
                    : `CT vs T`}
                </span>
              </div>
              <div style={{ color: theme.dim, fontFamily: theme.fontMono, fontSize: 11, marginTop: 8, letterSpacing: "0.04em" }}>
                Round {roundNum} of {roundCount ?? "?"} · half {roundNum <= 12 ? 1 : 2}
              </div>
            </div>
          )}

          {/* Outcome */}
          {outcome && outcome.winner_side && (
            <div className="why-note-box" style={{ borderColor: theme.border, marginTop: 12 }}>
              <div className="why-note-tag" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
                OUTCOME
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 2 }}>
                <span style={{
                  padding: "3px 8px", borderRadius: 3,
                  background: userWonRound === true ? theme.accent : userWonRound === false ? "#cd3737" : theme.panelSoft,
                  color: userWonRound !== null ? theme.paper : theme.ink,
                  fontFamily: theme.fontMono, fontSize: 11, letterSpacing: "0.08em",
                }}>
                  {userWonRound === true ? "WON" : userWonRound === false ? "LOST" : outcome.winner_side.toUpperCase() + " WIN"}
                </span>
                <span style={{ color: theme.ink, fontFamily: theme.fontMono, fontSize: 12 }}>
                  {fmtReason(outcome.reason)}
                </span>
                {outcome.bomb_site && (
                  <span style={{ color: theme.dim, fontFamily: theme.fontMono, fontSize: 11 }}>
                    @ {outcome.bomb_site.toUpperCase()}
                  </span>
                )}
              </div>
              {outcome.bomb_plant_offset_s != null && (
                <div style={{ color: theme.dim, fontFamily: theme.fontMono, fontSize: 11, marginTop: 8, letterSpacing: "0.04em" }}>
                  bomb planted at {outcome.bomb_plant_offset_s.toFixed(1)}s
                  {outcome.bomb_site ? ` · site ${outcome.bomb_site.toUpperCase()}` : ""}
                </div>
              )}
            </div>
          )}

          {/* Pro match summary (compact) */}
          {bestMatch && (
            <div className="why-note-box" style={{ borderColor: theme.border, marginTop: 12 }}>
              <div className="why-note-tag" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
                PRO MATCH · {Math.round(bestMatch.score * 100)}%
              </div>
              <div style={{ display: "grid", gap: 4, fontFamily: theme.fontMono, fontSize: 11 }}>
                <div style={{ color: theme.ink }}>
                  {bestMatch.team1_name ?? bestMatch.team_ct ?? "Team 1"} vs {bestMatch.team2_name ?? bestMatch.team_t ?? "Team 2"}
                </div>
                <div style={{ color: theme.dim }}>
                  {bestMatch.event_name ?? "HLTV corpus"} · round {bestMatch.round_num}
                </div>
              </div>
            </div>
          )}

          {/* Roster */}
          <div className="why-note-box" style={{ borderColor: theme.border, marginTop: 12 }}>
            <div className="why-note-tag" style={{ color: theme.dim, fontFamily: theme.fontMono }}>ROSTER</div>
            <div style={{ display: "grid", gap: 4, fontFamily: theme.fontMono, fontSize: 11 }}>
              {(data?.ticks[tickIdx]?.players ?? []).map((p) => (
                <div key={p.steamid} style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                  <span style={{ color: p.side === "ct" ? theme.ct : theme.tside }}>
                    {p.side.toUpperCase()} · {p.name}
                  </span>
                  <span style={{ color: p.health > 0 ? theme.dim : theme.dimmer }}>
                    {p.health > 0 ? `${p.health} HP` : "DEAD"}
                  </span>
                </div>
              ))}
              {!data && (
                <span style={{ color: theme.dimmer }}>—</span>
              )}
            </div>
          </div>

          <div className="why-cta" style={{ color: theme.dimmer, fontFamily: theme.fontMono, marginTop: 10 }}>
            space · play/pause  ·  ←/→ step  ·  [/] round  ·  L level
          </div>
        </aside>
      </main>

      {/* ── Scrubber ──────────────────────────────────────────────────────── */}
      <div className="scrubber-row" style={{ borderColor: theme.border, background: theme.panel }}>
        <div className="transport">
          <button className="btn-xs"
            onClick={() => setTickIdx(Math.max(0, tickIdx - TICKRATE))}
            style={{ color: theme.ink, borderColor: theme.border }}>‹‹</button>
          <button className="btn-play"
            onClick={() => setPlaying(!playing)}
            style={{ color: theme.paper, background: theme.accent }}>
            {playing ? "❚❚" : "▶"}
          </button>
          <button className="btn-xs"
            onClick={() => setTickIdx(Math.min(tickCount - 1, tickIdx + TICKRATE))}
            style={{ color: theme.ink, borderColor: theme.border }}>››</button>
        </div>

        <div className="timeline" style={{ position: "relative" }}>
          <input
            type="range"
            min={0}
            max={Math.max(0, tickCount - 1)}
            value={tickIdx}
            onChange={(e) => { setPlaying(false); setTickIdx(Number(e.target.value)); }}
            style={{ width: "100%", accentColor: theme.accent, cursor: "pointer" }}
          />
        </div>

        <div className="readouts" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
          <div className="readout-row">
            <span style={{ color: theme.dim }}>R</span>
            <span style={{ color: theme.ink }}>{fmtElapsed(curTick, freezeEnd)}</span>
          </div>
          <div className="readout-row">
            <span style={{ color: theme.dim }}>TICK</span>
            <span style={{ color: theme.ink }}>{curTick}</span>
          </div>
          {bestMatch && (
            <div className="readout-row">
              <span style={{ color: theme.dim }}>PRO</span>
              <span style={{ color: theme.ink }}>
                {proData ? fmtElapsed(proCurTick, proData.freeze_end_tick) : "—"}
              </span>
            </div>
          )}
        </div>

        <div className="speed-group">
          {([1, 2, 4] as const).map((s) => (
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

      {/* ── Tweaks ────────────────────────────────────────────────────────── */}
      {tweaksOpen && (
        <div className="tweaks" style={{ background: theme.panel, borderColor: theme.borderHi, color: theme.ink }}>
          <div className="tw-head" style={{ borderColor: theme.border }}>
            <span style={{ fontFamily: theme.fontMono, color: theme.dim }}>/// TWEAKS</span>
            <button className="tw-close" onClick={() => setTweaksOpen(false)} style={{ color: theme.dim }}>×</button>
          </div>
          <div className="tw-row">
            <div className="tw-label" style={{ color: theme.dim, fontFamily: theme.fontMono }}>THEME</div>
            <div className="tw-ctrl">
              <div className="tw-seg" style={{ borderColor: theme.border }}>
                {(["tactical", "editorial", "broadcast"] as const).map(k => (
                  <button key={k} onClick={() => setThemeKey(k)} style={{
                    background: themeKey === k ? theme.accent : "transparent",
                    color: themeKey === k ? theme.paper : theme.dim,
                    fontFamily: theme.fontMono, borderColor: theme.border,
                  }}>
                    {k[0].toUpperCase() + k.slice(1)}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}
      {!tweaksOpen && (
        <button className="tweaks-fab" onClick={() => setTweaksOpen(true)}
          style={{ background: theme.panel, borderColor: theme.borderHi, color: theme.dim, fontFamily: theme.fontMono }}>
          /// TWEAKS
        </button>
      )}
    </div>
  );
}
