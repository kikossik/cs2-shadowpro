import { useEffect, useMemo, useRef, useState } from "react";
import { THEMES } from "./themes";
import type { ThemeKey } from "./types";
import { RadarCanvas } from "./replay/RadarCanvas";
import { useRoundPlayback, buildWeaponMap } from "./replay/useRoundPlayback";
import type { MapConfig, RoundReplayData, SimilarityMapResponse } from "./replay/types";
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

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function highlightPercentages(
  tickList: number[],
  startTick: number | null | undefined,
  endTick: number | null | undefined,
): { left: string; width: string } | null {
  if (!tickList.length || startTick == null || endTick == null) return null;
  const first = tickList[0];
  const last = tickList[tickList.length - 1];
  const span = Math.max(last - first, 1);
  const start = clamp((startTick - first) / span, 0, 1);
  const end = clamp((endTick - first) / span, start, 1);
  return {
    left: `${start * 100}%`,
    width: `${Math.max((end - start) * 100, 1)}%`,
  };
}

function phaseLabel(phase: string | null | undefined): string {
  if (!phase) return "mapped event";
  return phase.replaceAll("_", " ");
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
  const [proMappingEnabled, setProMappingEnabled] = useState(false);
  const [mapping, setMapping] = useState<SimilarityMapResponse | null>(null);
  const [mappingLoading, setMappingLoading] = useState(false);
  const [mappingError, setMappingError] = useState<string | null>(null);
  const [proData, setProData] = useState<RoundReplayData | null>(null);
  const [proLoading, setProLoading] = useState(false);
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

  // Round data.
  useEffect(() => {
    setLoading(true);
    setError(null);
    setData(null);
    fetch(`/api/round-replay/${encodeURIComponent(matchId)}/${roundNum}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((d: RoundReplayData) => { setData(d); setLoading(false); })
      .catch(e => { setError(String(e)); setLoading(false); });
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

  // Fetch the similarity mapping once per (round, match, toggle). Re-querying
  // on every scrub thrashes the pro pane and resets the highlight; the anchor
  // is pinned to freeze_end so the mapping stays stable while playing.
  useEffect(() => {
    if (!proMappingEnabled || !data) return undefined;

    const anchorTick = data.freeze_end_tick || data.tick_list[0] || 0;
    let active = true;
    setMappingLoading(true);
    setMappingError(null);
    fetch(`/api/similarity-map/${encodeURIComponent(matchId)}/${roundNum}/${anchorTick}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((payload: SimilarityMapResponse) => {
        if (!active) return;
        setMapping(payload);
        setMappingLoading(false);
      })
      .catch(err => {
        if (!active) return;
        setMappingError(String(err));
        setMappingLoading(false);
        setMapping(null);
      });

    return () => { active = false; };
  }, [data, matchId, proMappingEnabled, roundNum]);

  useEffect(() => {
    if (!proMappingEnabled) {
      setMapping(null);
      setMappingError(null);
      setMappingLoading(false);
      setProData(null);
      setProError(null);
      setProLoading(false);
      return;
    }
    if (!mapping?.best_match) {
      setProData(null);
      return;
    }

    const best = mapping.best_match;
    setProLoading(true);
    setProError(null);
    let active = true;
    fetch(`/api/round-replay/${encodeURIComponent(best.source_match_id)}/${best.round_num}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((payload: RoundReplayData) => {
        if (!active) return;
        setProData(payload);
        setProLoading(false);
      })
      .catch(err => {
        if (!active) return;
        setProError(String(err));
        setProData(null);
        setProLoading(false);
      });

    return () => {
      active = false;
    };
  }, [mapping, proMappingEnabled]);

  const proWeaponMap = useMemo(
    () => (proData ? buildWeaponMap(proData.shots, proData.tick_list) : {}),
    [proData],
  );
  const proMapConfig = maps.find(m => m.name === proData?.map) ?? null;
  const proRadarUrl = proMapConfig ? `/api/radar/${proMapConfig.name}` : null;
  const proLowerRadarUrl = proMapConfig?.has_lower_level ? `/api/radar/${proMapConfig.name}_lower` : null;
  const proRadarImg = useImage(proRadarUrl);
  const proLowerImg = useImage(proLowerRadarUrl);
  // Both timelines are trimmed to start at freeze_end, so user tickIdx and
  // pro tickIdx are index-aligned at 64Hz. Clamp to the pro round length.
  const proTickIdx = useMemo(() => {
    if (!proData) return 0;
    return Math.min(tickIdx, Math.max(0, proData.tick_list.length - 1));
  }, [tickIdx, proData]);
  const proCurTick = proData?.tick_list[proTickIdx] ?? 0;
  const bestMatch = mapping?.best_match ?? null;
  const userHighlight = highlightPercentages(data?.tick_list ?? [], mapping?.query.start_tick, mapping?.query.end_tick);
  const proHighlight = highlightPercentages(proData?.tick_list ?? [], bestMatch?.start_tick, bestMatch?.end_tick);
  const mappingStatus = !proMappingEnabled
    ? "USER ONLY"
    : mappingLoading
      ? "MAPPING..."
      : bestMatch
        ? `TOP MATCH · ${Math.round(bestMatch.score * 100)}%`
        : "NO MATCH";

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
          <button
            onClick={() => setProMappingEnabled(v => !v)}
            style={{
              color: proMappingEnabled ? theme.paper : theme.dim,
              background: proMappingEnabled ? theme.accent : "transparent",
              border: `1px solid ${theme.border}`,
              borderRadius: 999,
              padding: "6px 10px",
              fontFamily: theme.fontMono,
              fontSize: 11,
              letterSpacing: "0.06em",
              cursor: "pointer",
            }}
          >
            {proMappingEnabled ? "PRO MAPPING ON" : "USER ONLY"}
          </button>
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
                  {proMappingEnabled
                    ? bestMatch
                      ? `${bestMatch.map.display} · round ${bestMatch.round_num}`
                      : mappingStatus
                    : "mapping disabled"}
                </div>
              </div>
              {proMappingEnabled && (
                <div className="pane-head-right">
                  <div className="pane-meta" style={{ color: theme.dim }}>
                    {mappingStatus}
                  </div>
                </div>
              )}
            </div>
            <div ref={proContainerRef} className="radar-hold"
              style={{ background: theme.paper, borderColor: theme.border, display: "flex",
                       alignItems: "center", justifyContent: "center" }}>
              {!proMappingEnabled && (
                <span style={{ color: theme.dimmer, fontFamily: theme.fontMono, fontSize: 11,
                               letterSpacing: "0.12em" }}>
                  USER-ONLY VIEW
                </span>
              )}
              {proMappingEnabled && mappingLoading && (
                <span style={{ color: theme.dimmer, fontFamily: theme.fontMono, fontSize: 11,
                               letterSpacing: "0.12em" }}>
                  MAPPING CURRENT MOMENT…
                </span>
              )}
              {proMappingEnabled && mappingError && (
                <span style={{ color: theme.dimmer, fontFamily: theme.fontMono, fontSize: 11,
                               letterSpacing: "0.08em", textAlign: "center", maxWidth: "70%" }}>
                  MAPPING ERROR: {mappingError}
                </span>
              )}
              {proMappingEnabled && !mappingLoading && !mappingError && !bestMatch && (
                <span style={{ color: theme.dimmer, fontFamily: theme.fontMono, fontSize: 11,
                               letterSpacing: "0.12em" }}>
                  NO PRO MATCH FOUND
                </span>
              )}
              {proMappingEnabled && proLoading && (
                <span style={{ color: theme.dimmer, fontFamily: theme.fontMono, fontSize: 11,
                               letterSpacing: "0.12em" }}>
                  LOADING PRO REPLAY…
                </span>
              )}
              {proMappingEnabled && proError && (
                <span style={{ color: theme.dimmer, fontFamily: theme.fontMono, fontSize: 11,
                               letterSpacing: "0.08em", textAlign: "center", maxWidth: "70%" }}>
                  PRO REPLAY ERROR: {proError}
                </span>
              )}
              {proMappingEnabled && proData && proRadarImg && proMapConfig && bestMatch && (
                <RadarCanvas
                  mapConfig={proMapConfig}
                  radarImage={proRadarImg}
                  lowerRadarImage={proLowerImg ?? undefined}
                  data={proData}
                  tickIdx={proTickIdx}
                  weaponMap={proWeaponMap}
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

          <div className="why-break" style={{ marginTop: 14 }}>
            <div className="why-row" style={{ borderColor: theme.border }}>
              <div className="why-row-top" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
                <span>ELAPSED</span><span>{fmtElapsed(curTick, freezeEnd)}</span>
              </div>
              <div className="why-row-val" style={{ color: theme.ink, fontFamily: theme.fontMono }}>
                tick {curTick}
              </div>
            </div>
            <div className="why-row" style={{ borderColor: theme.border }}>
              <div className="why-row-top" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
                <span>TICKS</span><span>{tickIdx + 1} / {tickCount}</span>
              </div>
              <div className="why-row-val" style={{ color: theme.ink, fontFamily: theme.fontMono }}>
                freeze-end {freezeEnd}
              </div>
            </div>
            <div className="why-row" style={{ borderColor: theme.border }}>
              <div className="why-row-top" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
                <span>PRO MAP</span><span>{mappingStatus}</span>
              </div>
              <div className="why-row-val" style={{ color: theme.ink, fontFamily: theme.fontMono }}>
                {bestMatch
                  ? `${phaseLabel(bestMatch.phase)}${bestMatch.site ? ` · ${bestMatch.site.toUpperCase()} site` : ""}`
                  : proMappingEnabled
                    ? "waiting for best match"
                    : "toggle to compare"}
              </div>
            </div>
          </div>

          {proMappingEnabled && bestMatch && (
            <div className="why-note-box" style={{ borderColor: theme.border, marginTop: 12 }}>
              <div className="why-note-tag" style={{ color: theme.dim, fontFamily: theme.fontMono }}>PRO MATCH</div>
              <div style={{ display: "grid", gap: 6, fontFamily: theme.fontMono, fontSize: 11 }}>
                <div style={{ color: theme.ink }}>
                  {bestMatch.team_ct ?? "CT"} vs {bestMatch.team_t ?? "T"}
                </div>
                <div style={{ color: theme.dim }}>
                  {bestMatch.event_name ?? "HLTV corpus"} · round {bestMatch.round_num}
                </div>
                <div style={{ color: theme.dim }}>
                  {bestMatch.reason}
                </div>
              </div>
            </div>
          )}

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
          {userHighlight && (
            <div
              style={{
                position: "absolute",
                left: userHighlight.left,
                width: userHighlight.width,
                top: "50%",
                transform: "translateY(-50%)",
                height: 10,
                borderRadius: 999,
                background: theme.accent,
                opacity: 0.3,
                pointerEvents: "none",
              }}
            />
          )}
          <input
            type="range"
            min={0}
            max={Math.max(0, tickCount - 1)}
            value={tickIdx}
            onChange={(e) => { setPlaying(false); setTickIdx(Number(e.target.value)); }}
            style={{ width: "100%", accentColor: theme.accent, cursor: "pointer" }}
          />
        </div>

        {proMappingEnabled && (
          <div className="timeline" style={{ position: "relative" }}>
            {proHighlight && (
              <div
                style={{
                  position: "absolute",
                  left: proHighlight.left,
                  width: proHighlight.width,
                  top: "50%",
                  transform: "translateY(-50%)",
                  height: 10,
                  borderRadius: 999,
                  background: theme.tside,
                  opacity: 0.3,
                  pointerEvents: "none",
                }}
              />
            )}
            <input
              type="range"
              min={0}
              max={Math.max(0, (proData?.tick_list.length ?? 0) - 1)}
              value={Math.min(proTickIdx, Math.max(0, (proData?.tick_list.length ?? 0) - 1))}
              disabled
              style={{ width: "100%", accentColor: theme.tside, cursor: "default", opacity: 0.9 }}
            />
          </div>
        )}

        <div className="readouts" style={{ color: theme.dim, fontFamily: theme.fontMono }}>
          <div className="readout-row">
            <span style={{ color: theme.dim }}>R</span>
            <span style={{ color: theme.ink }}>{fmtElapsed(curTick, freezeEnd)}</span>
          </div>
          <div className="readout-row">
            <span style={{ color: theme.dim }}>TICK</span>
            <span style={{ color: theme.ink }}>{curTick}</span>
          </div>
          {proMappingEnabled && (
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
