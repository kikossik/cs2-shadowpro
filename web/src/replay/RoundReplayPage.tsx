import { useEffect, useMemo, useRef, useState } from "react";
import { RadarCanvas } from "./RadarCanvas";
import { useRoundPlayback, buildWeaponMap } from "./useRoundPlayback";
import type {
  MapConfig,
  RoundAnalysisResponse,
  RoundReplayData,
} from "./types";
import { TICKRATE } from "./constants";

function formatTime(curTick: number, freezeEndTick: number): string {
  const elapsed = Math.max(0, (curTick - freezeEndTick) / TICKRATE);
  const mins = Math.floor(elapsed / 60);
  const secs = elapsed % 60;
  return `${mins}:${secs.toFixed(2).padStart(5, "0")}`;
}

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

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function findNearestTickIndex(tickList: number[], targetTick: number): number {
  if (!tickList.length) return 0;
  let bestIdx = 0;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (let i = 0; i < tickList.length; i += 1) {
    const distance = Math.abs(tickList[i] - targetTick);
    if (distance < bestDistance) {
      bestDistance = distance;
      bestIdx = i;
    }
  }
  return bestIdx;
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
  if (!phase) return "round analysis";
  return phase.replaceAll("_", " ");
}

interface TimelineBarProps {
  label: string;
  tickList: number[];
  tickIdx: number;
  onChange?: (nextIdx: number) => void;
  highlightStart?: number | null;
  highlightEnd?: number | null;
  accentColor: string;
  disabled?: boolean;
  note?: string | null;
}

function TimelineBar({
  label,
  tickList,
  tickIdx,
  onChange,
  highlightStart,
  highlightEnd,
  accentColor,
  disabled = false,
  note = null,
}: TimelineBarProps) {
  const highlight = highlightPercentages(tickList, highlightStart, highlightEnd);

  return (
    <div style={styles.timelineRow}>
      <div style={styles.timelineLabelWrap}>
        <div style={styles.timelineLabel}>{label}</div>
        {note && <div style={styles.timelineNote}>{note}</div>}
      </div>
      <div style={styles.timelineTrackWrap}>
        {highlight && (
          <div
            style={{
              ...styles.timelineHighlight,
              left: highlight.left,
              width: highlight.width,
              background: accentColor,
            }}
          />
        )}
        <input
          type="range"
          min={0}
          max={Math.max(0, tickList.length - 1)}
          value={Math.min(tickIdx, Math.max(0, tickList.length - 1))}
          disabled={disabled || tickList.length === 0}
          onChange={(e) => onChange?.(Number(e.target.value))}
          style={{
            ...styles.scrubber,
            accentColor: accentColor,
            cursor: disabled ? "default" : "pointer",
            opacity: disabled ? 0.95 : 1,
          }}
        />
      </div>
    </div>
  );
}

interface Props {
  demoId: string;
  initialRound: number;
  onBack: () => void;
}

export function RoundReplayPage({ demoId, initialRound, onBack }: Props) {
  const [roundNum, setRoundNum] = useState(initialRound);
  const [data, setData] = useState<RoundReplayData | null>(null);
  const [maps, setMaps] = useState<MapConfig[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showLower, setShowLower] = useState(false);
  const [proMappingEnabled, setProMappingEnabled] = useState(false);
  const [analysis, setAnalysis] = useState<RoundAnalysisResponse | null>(null);
  const [analysisLoading, setAnalysisLoading] = useState(false);
  const [analysisError, setAnalysisError] = useState<string | null>(null);
  const [proData, setProData] = useState<RoundReplayData | null>(null);
  const [proLoading, setProLoading] = useState(false);
  const [proError, setProError] = useState<string | null>(null);

  const containerRef = useRef<HTMLDivElement>(null);
  const proContainerRef = useRef<HTMLDivElement>(null);
  const [canvasSize, setCanvasSize] = useState({ w: 600, h: 600 });
  const [proCanvasSize, setProCanvasSize] = useState({ w: 600, h: 600 });

  useEffect(() => {
    fetch("/api/maps")
      .then((r) => r.json())
      .then(setMaps)
      .catch(() => {});
  }, []);

  useEffect(() => {
    setLoading(true);
    setError(null);
    setData(null);
    fetch(`/api/round-replay/${encodeURIComponent(demoId)}/${roundNum}`)
      .then((r) => { if (!r.ok) throw new Error(`${r.status}`); return r.json(); })
      .then((d: RoundReplayData) => { setData(d); setLoading(false); })
      .catch((e) => { setError(String(e)); setLoading(false); });
  }, [demoId, roundNum]);

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

  const mapConfig = maps.find((m) => m.name === data?.map) ?? null;
  const weaponMap = useMemo(
    () => (data ? buildWeaponMap(data.shots, data.tick_list) : {}),
    [data],
  );
  const tickCount = data?.tick_list.length ?? 0;
  const { tickIdx, setTickIdx, playing, setPlaying, speed, setSpeed } =
    useRoundPlayback(tickCount);

  const radarUrl = mapConfig ? `/api/radar/${mapConfig.name}` : null;
  const lowerRadarUrl = mapConfig?.has_lower_level ? `/api/radar/${mapConfig.name}_lower` : null;
  const radarImg = useImage(radarUrl);
  const lowerImg = useImage(lowerRadarUrl);

  const curTick = data?.tick_list[tickIdx] ?? 0;
  const freezeEnd = data?.freeze_end_tick ?? 0;
  const timeStr = data ? formatTime(curTick, freezeEnd) : "—";
  const mapDisplay = mapConfig?.display_name ?? data?.map ?? "—";

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (!data) return;
      const step = e.shiftKey ? 5 * TICKRATE : TICKRATE;
      switch (e.key) {
        case " ":
          e.preventDefault();
          setPlaying(!playing);
          break;
        case "ArrowRight":
          e.preventDefault();
          setTickIdx(Math.min(tickCount - 1, tickIdx + step));
          break;
        case "ArrowLeft":
          e.preventDefault();
          setTickIdx(Math.max(0, tickIdx - step));
          break;
        case "[":
          setRoundNum((n) => Math.max(1, n - 1));
          break;
        case "]":
          setRoundNum((n) => n + 1);
          break;
        case "Home":
          e.preventDefault();
          setTickIdx(0);
          break;
        case "l":
        case "L":
          if (mapConfig?.has_lower_level) setShowLower((v) => !v);
          break;
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [data, mapConfig, playing, setPlaying, setTickIdx, tickCount, tickIdx]);

  useEffect(() => {
    if (!proMappingEnabled || !data) return undefined;

    let active = true;
    let retryTimer: number | null = null;
    setAnalysisLoading(true);
    setAnalysisError(null);
    setAnalysis(null);

    const fetchAnalysis = (reset = false) => {
      if (!active) return;
      if (reset) {
        setAnalysis(null);
      }

      fetch(`/api/round-analysis/${encodeURIComponent(demoId)}/${roundNum}?logic=both`)
        .then((r) => { if (!r.ok) throw new Error(`${r.status}`); return r.json(); })
        .then((payload: RoundAnalysisResponse) => {
          if (!active) return;
          setAnalysis(payload);
          setAnalysisError(payload.status === "error" ? (payload.error ?? "Round analysis failed") : null);
          setAnalysisLoading(false);

          const shouldPoll =
            payload.status === "pending"
            || payload.analysis.cache_status === "stale";
          if (shouldPoll) {
            retryTimer = window.setTimeout(() => fetchAnalysis(false), 1500);
          }
        })
        .catch((err) => {
          if (!active) return;
          setAnalysisError(String(err));
          setAnalysisLoading(false);
          setAnalysis(null);
        });
    };

    fetchAnalysis(true);

    return () => {
      active = false;
      if (retryTimer !== null) {
        window.clearTimeout(retryTimer);
      }
    };
  }, [data, demoId, proMappingEnabled, roundNum]);

  useEffect(() => {
    if (!proMappingEnabled) {
      setAnalysis(null);
      setAnalysisError(null);
      setAnalysisLoading(false);
      setProData(null);
      setProError(null);
      setProLoading(false);
      return;
    }

    if (analysis?.status !== "done" || !analysis.result?.best_match) {
      setProData(null);
      setProError(null);
      setProLoading(false);
      return;
    }

    const best = analysis.result.best_match;
    setProLoading(true);
    setProError(null);
    let active = true;
    fetch(`/api/round-replay/${encodeURIComponent(best.source_match_id)}/${best.round_num}`)
      .then((r) => { if (!r.ok) throw new Error(`${r.status}`); return r.json(); })
      .then((payload: RoundReplayData) => {
        if (!active) return;
        setProData(payload);
        setProLoading(false);
      })
      .catch((err) => {
        if (!active) return;
        setProError(String(err));
        setProData(null);
        setProLoading(false);
      });
    return () => {
      active = false;
    };
  }, [analysis, proMappingEnabled]);

  const proWeaponMap = useMemo(
    () => (proData ? buildWeaponMap(proData.shots, proData.tick_list) : {}),
    [proData],
  );
  const proMapConfig = maps.find((m) => m.name === proData?.map) ?? null;
  const proRadarUrl = proMapConfig ? `/api/radar/${proMapConfig.name}` : null;
  const proLowerRadarUrl = proMapConfig?.has_lower_level ? `/api/radar/${proMapConfig.name}_lower` : null;
  const proRadarImg = useImage(proRadarUrl);
  const proLowerImg = useImage(proLowerRadarUrl);
  const analysisResult = analysis?.result ?? null;
  const bestMatch = analysisResult?.best_match ?? null;
  const proTimeOffsetS =
    bestMatch?.timeline_sync?.pro_time_offset_s
    ?? bestMatch?.pro_time_offset_s
    ?? (
      proData && bestMatch && analysisResult?.query
        ? ((bestMatch.anchor_tick - proData.freeze_end_tick) - (analysisResult.query.anchor_tick - freezeEnd)) / TICKRATE
        : 0
    );

  const proTickIdx = useMemo(() => {
    if (!proData || !data || !bestMatch) return 0;
    const userElapsedS = Math.max(0, (curTick - data.freeze_end_tick) / TICKRATE);
    const targetTick = proData.freeze_end_tick + Math.round((userElapsedS + proTimeOffsetS) * TICKRATE);
    return findNearestTickIndex(proData.tick_list, targetTick);
  }, [bestMatch, curTick, data, proData, proTimeOffsetS]);

  const proCurTick = proData?.tick_list[proTickIdx] ?? 0;
  const proTimeStr = proData ? formatTime(proCurTick, proData.freeze_end_tick) : "—";
  const mappingStatus = !proMappingEnabled
    ? "User-only view"
    : analysisLoading
      ? "Analyzing round..."
      : bestMatch
        ? `Top match · ${Math.round(bestMatch.score * 100)}%`
        : analysisError
          ? "Round analysis error"
          : "No pro round available";

  const showProPanel = proMappingEnabled;
  const userTimelineNote = analysisResult?.query
    ? `${phaseLabel(analysisResult.query.phase)}${analysisResult.query.site ? ` · site ${analysisResult.query.site.toUpperCase()}` : ""}`
    : null;
  const proTimelineNote = bestMatch
    ? `${phaseLabel(bestMatch.phase)}${bestMatch.site ? ` · site ${bestMatch.site.toUpperCase()}` : ""}`
    : null;

  return (
    <div style={styles.root}>
      <div style={styles.topBar}>
        <button style={styles.backBtn} onClick={onBack}>← Matches</button>
        <div style={styles.topCenter}>
          <span style={styles.roundLabel}>Round {roundNum}</span>
          <span style={styles.mapLabel}>· {mapDisplay}</span>
          {mapConfig?.has_lower_level && (
            <span style={styles.levelBadge}>{showLower ? "[LOWER]" : "[UPPER]"}</span>
          )}
          <span style={styles.mappingStatus}>{mappingStatus}</span>
        </div>
        <div style={styles.topControls}>
          <button
            style={{
              ...styles.toggleBtn,
              ...(proMappingEnabled ? styles.toggleBtnActive : {}),
            }}
            onClick={() => setProMappingEnabled((value) => !value)}
          >
            {proMappingEnabled ? "Pro Mapping On" : "User Only"}
          </button>
          <div style={styles.roundNav}>
            <button style={styles.navBtn} onClick={() => setRoundNum((n) => Math.max(1, n - 1))}>◄</button>
            <button style={styles.navBtn} onClick={() => setRoundNum((n) => n + 1)}>►</button>
          </div>
        </div>
      </div>

      <div style={styles.panels}>
        <div style={{ ...styles.panel, ...(showProPanel ? {} : styles.panelSolo) }}>
          <div style={styles.panelLabel}>YOUR REPLAY</div>
          <div ref={containerRef} style={styles.canvasWrap}>
            {loading && <div style={styles.overlay}>Loading…</div>}
            {error && <div style={styles.overlay}>Error: {error}</div>}
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

        {showProPanel && (
          <div style={styles.panel}>
            <div style={styles.panelLabel}>PRO MATCH</div>
            {bestMatch && (
              <div style={styles.proMeta}>
                <div style={styles.proMetaTitle}>
                  {bestMatch.team_ct ?? "CT"} vs {bestMatch.team_t ?? "T"}
                </div>
                <div style={styles.proMetaSub}>
                  {bestMatch.event_name ?? "HLTV corpus"} · round {bestMatch.round_num}
                </div>
                <div style={styles.proMetaSub}>{bestMatch.reason}</div>
              </div>
            )}
            <div ref={proContainerRef} style={styles.canvasWrap}>
              {analysisLoading && <div style={styles.overlay}>Analyzing round…</div>}
              {analysisError && <div style={styles.overlay}>Analysis error: {analysisError}</div>}
              {!analysisLoading && !analysisError && !bestMatch && (
                <div style={styles.overlay}>No matched pro round for this round yet.</div>
              )}
              {proLoading && <div style={styles.overlay}>Loading pro replay…</div>}
              {proError && <div style={styles.overlay}>Pro replay error: {proError}</div>}
              {proData && proRadarImg && proMapConfig && bestMatch && (
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
        )}
      </div>

      <div style={styles.hud}>
        <div style={styles.transport}>
          <button
            style={styles.transportBtn}
            onClick={() => setTickIdx(Math.max(0, tickIdx - TICKRATE))}
          >
            ◄◄
          </button>
          <button
            style={{ ...styles.transportBtn, ...styles.playBtn }}
            onClick={() => setPlaying(!playing)}
          >
            {playing ? "⏸" : "▶"}
          </button>
          <button
            style={styles.transportBtn}
            onClick={() => setTickIdx(Math.min(tickCount - 1, tickIdx + TICKRATE))}
          >
            ▶▶
          </button>
        </div>

        <div style={styles.timelineStack}>
          <TimelineBar
            label={`USER · ${timeStr}`}
            tickList={data?.tick_list ?? []}
            tickIdx={tickIdx}
            onChange={(nextIdx) => { setPlaying(false); setTickIdx(nextIdx); }}
            highlightStart={analysisResult?.query.start_tick}
            highlightEnd={analysisResult?.query.end_tick}
            accentColor="#6882d2"
            note={userTimelineNote}
          />
          {showProPanel && (
            <TimelineBar
              label={`PRO · ${proTimeStr}`}
              tickList={proData?.tick_list ?? []}
              tickIdx={proTickIdx}
              highlightStart={bestMatch?.start_tick}
              highlightEnd={bestMatch?.end_tick}
              accentColor="#ff9f5a"
              disabled
              note={proTimelineNote}
            />
          )}
        </div>

        <div style={styles.speedGroup}>
          {[1, 2, 4].map((s) => (
            <button
              key={s}
              style={{ ...styles.speedBtn, ...(speed === s ? styles.speedActive : {}) }}
              onClick={() => setSpeed(s as 1 | 2 | 4)}
            >
              {s}×
            </button>
          ))}
        </div>

        <div style={styles.hint}>
          Space=pause · ←/→=±1s · Shift=±5s · [/]=round · Home=restart
          {mapConfig?.has_lower_level ? " · L=level" : ""}
          {showProPanel ? " · pro panel follows the matched round alignment" : ""}
        </div>
      </div>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    display: "flex",
    flexDirection: "column",
    height: "100vh",
    background: "#12121a",
    color: "#d2d2e1",
    fontFamily: "monospace",
    overflow: "hidden",
  },
  topBar: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "8px 16px",
    borderBottom: "1px solid #32323f",
    background: "#0c0c14",
    flexShrink: 0,
    gap: 12,
  },
  backBtn: {
    background: "none",
    border: "1px solid #3a3a4f",
    color: "#9090a8",
    padding: "4px 10px",
    cursor: "pointer",
    fontFamily: "monospace",
    fontSize: 12,
    borderRadius: 3,
  },
  topCenter: {
    display: "flex",
    gap: 8,
    alignItems: "center",
    flexWrap: "wrap",
    justifyContent: "center",
  },
  topControls: {
    display: "flex",
    gap: 10,
    alignItems: "center",
  },
  roundLabel: {
    fontSize: 16,
    fontWeight: "bold",
    color: "#e0e0f0",
  },
  mapLabel: {
    fontSize: 13,
    color: "#8080a0",
  },
  mappingStatus: {
    fontSize: 11,
    color: "#a8b4d8",
    background: "#1a2132",
    padding: "3px 8px",
    borderRadius: 999,
    letterSpacing: "0.04em",
  },
  levelBadge: {
    fontSize: 11,
    color: "#a0a0ff",
    background: "#1e1e38",
    padding: "2px 6px",
    borderRadius: 3,
  },
  roundNav: {
    display: "flex",
    gap: 4,
  },
  navBtn: {
    background: "none",
    border: "1px solid #3a3a4f",
    color: "#9090a8",
    padding: "4px 10px",
    cursor: "pointer",
    fontFamily: "monospace",
    fontSize: 13,
    borderRadius: 3,
  },
  toggleBtn: {
    background: "#171b28",
    border: "1px solid #31405f",
    color: "#9fb5e2",
    padding: "5px 10px",
    cursor: "pointer",
    fontFamily: "monospace",
    fontSize: 11,
    borderRadius: 999,
    letterSpacing: "0.04em",
  },
  toggleBtnActive: {
    background: "#243250",
    border: "1px solid #5e84c7",
    color: "#dbe8ff",
  },
  panels: {
    display: "flex",
    flex: 1,
    overflow: "hidden",
    gap: 1,
    background: "#2a2a3a",
  },
  panel: {
    flex: 1,
    display: "flex",
    flexDirection: "column",
    background: "#12121a",
    overflow: "hidden",
    minWidth: 0,
  },
  panelSolo: {
    flex: 1,
  },
  panelLabel: {
    padding: "4px 12px",
    fontSize: 10,
    letterSpacing: "0.1em",
    color: "#5a5a72",
    borderBottom: "1px solid #232330",
    flexShrink: 0,
  },
  proMeta: {
    borderBottom: "1px solid #232330",
    padding: "10px 12px",
    display: "flex",
    flexDirection: "column",
    gap: 4,
    background: "#0f1018",
  },
  proMetaTitle: {
    color: "#f3f5ff",
    fontSize: 13,
    fontWeight: 700,
  },
  proMetaSub: {
    color: "#8b93ae",
    fontSize: 11,
    lineHeight: 1.4,
  },
  canvasWrap: {
    flex: 1,
    position: "relative",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    overflow: "hidden",
  },
  overlay: {
    position: "absolute",
    color: "#5a5a72",
    fontSize: 13,
    textAlign: "center",
    maxWidth: "70%",
    lineHeight: 1.5,
  },
  hud: {
    display: "flex",
    alignItems: "center",
    gap: 12,
    padding: "10px 16px",
    borderTop: "1px solid #32323f",
    background: "#0c0c14",
    flexShrink: 0,
    flexWrap: "wrap",
  },
  transport: {
    display: "flex",
    gap: 4,
  },
  transportBtn: {
    background: "#1e1e2e",
    border: "1px solid #3a3a4f",
    color: "#c8c8e0",
    padding: "4px 10px",
    cursor: "pointer",
    fontFamily: "monospace",
    fontSize: 13,
    borderRadius: 3,
  },
  playBtn: {
    background: "#1e2e4e",
    border: "1px solid #3a5080",
    color: "#80b0ff",
    padding: "4px 14px",
    fontSize: 15,
  },
  timelineStack: {
    flex: 1,
    minWidth: 220,
    display: "flex",
    flexDirection: "column",
    gap: 8,
  },
  timelineRow: {
    display: "flex",
    alignItems: "center",
    gap: 10,
  },
  timelineLabelWrap: {
    width: 120,
    flexShrink: 0,
  },
  timelineLabel: {
    fontSize: 12,
    color: "#d0d0e8",
    fontVariantNumeric: "tabular-nums",
  },
  timelineNote: {
    fontSize: 10,
    color: "#78809d",
    marginTop: 2,
    textTransform: "uppercase",
    letterSpacing: "0.06em",
  },
  timelineTrackWrap: {
    position: "relative",
    flex: 1,
    minWidth: 100,
  },
  timelineHighlight: {
    position: "absolute",
    top: "50%",
    height: 10,
    transform: "translateY(-50%)",
    borderRadius: 999,
    opacity: 0.35,
    pointerEvents: "none",
  },
  scrubber: {
    width: "100%",
    background: "transparent",
  },
  speedGroup: {
    display: "flex",
    gap: 3,
  },
  speedBtn: {
    background: "none",
    border: "1px solid #3a3a4f",
    color: "#7070a0",
    padding: "3px 7px",
    cursor: "pointer",
    fontFamily: "monospace",
    fontSize: 11,
    borderRadius: 3,
  },
  speedActive: {
    background: "#1e2a4e",
    border: "1px solid #4a5a8a",
    color: "#a0c0ff",
  },
  hint: {
    width: "100%",
    fontSize: 10,
    color: "#404058",
    letterSpacing: "0.04em",
    textAlign: "center",
  },
};
