import type { Match, MatchResult, SteamProfile } from "./types";
import { MAP_OPTIONS, RESULT_OPTIONS } from "./utils";

type TopBarProps = {
  steamId: string;
  profile: SteamProfile | null;
  onSignOut: () => void;
  onImport: () => void;
};
export function TopBar({ steamId, profile, onSignOut, onImport }: TopBarProps) {
  const displayName = profile?.personaname ?? `…${steamId.slice(-6)}`;
  return (
    <div className="topbar" role="banner">
      <div className="brand">
        <span className="brand-mark">SP</span>
        <span>ShadowPro</span>
      </div>
      <div className="tb-crumbs">
        <span className="cur">MATCHES</span>
        <span className="slash">/</span>
        <span>select a match to review</span>
      </div>
      <div className="tb-right">
        <button className="import-fab" onClick={onImport}>+ IMPORT DEMO</button>
        <div className="user-chip" title={`Steam ID: ${steamId}`}>
          {profile?.avatar
            ? <img className="avatar" src={profile.avatar} alt="" />
            : <span className="avatar" />}
          <span style={{ color: "var(--ink)" }}>{displayName}</span>
        </div>
        <button className="signout-btn" onClick={onSignOut}>SIGN OUT</button>
      </div>
    </div>
  );
}

type ImportBannerProps = {
  state: "idle" | "processing" | "done" | "error";
  progress: number;
  situationsFound?: number;
  errorMessage?: string;
  onDismiss: () => void;
};
export function ImportBanner({ state, progress, situationsFound, errorMessage, onDismiss }: ImportBannerProps) {
  if (state === "idle") return null;
  if (state === "done") {
    return (
      <div className="import-banner done" role="status">
        <span className="import-title" style={{ color: "var(--accent)" }}>
          <span className="import-check">✓</span>
          IMPORT COMPLETE
        </span>
        <div />
        {situationsFound !== undefined && (
          <span className="import-count">{situationsFound} SITUATIONS INDEXED</span>
        )}
        <button className="import-dismiss" onClick={onDismiss}>DISMISS</button>
      </div>
    );
  }
  if (state === "error") {
    return (
      <div className="import-banner" style={{ borderColor: "var(--loss)" }} role="alert">
        <span className="import-title" style={{ color: "var(--loss)" }}>IMPORT FAILED</span>
        <div />
        <span className="import-count" style={{ color: "var(--dim)" }}>{errorMessage}</span>
        <button className="import-dismiss" onClick={onDismiss}>DISMISS</button>
      </div>
    );
  }
  return (
    <div className="import-banner" role="status">
      <span className="import-title">
        <span className="import-spinner" />
        PROCESSING DEMO…
      </span>
      <div className="import-progress-bar">
        <div className="import-progress-fill" style={{ width: `${progress * 100}%` }} />
      </div>
      <span className="import-count">parsing + indexing</span>
      <button className="import-dismiss" onClick={onDismiss}>DISMISS</button>
    </div>
  );
}

type FilterBarProps = {
  mapFilter: string;
  setMapFilter: (v: string) => void;
  resultFilter: "all" | MatchResult;
  setResultFilter: (v: "all" | MatchResult) => void;
  count: number;
};
export function FilterBar({
  mapFilter,
  setMapFilter,
  resultFilter,
  setResultFilter,
  count,
}: FilterBarProps) {
  return (
    <div className="page-head">
      <div className="ph-title">
        <div className="ph-tag">POSTGAME · STEAM RANKED</div>
        <h1 className="ph-h1">
          Your matches
          <span className="count">{count} shown</span>
        </h1>
      </div>
      <div className="filters">
        <div className="filter-group">
          <label className="filter-label">MAP</label>
          <div className="select-wrap">
            <select value={mapFilter} onChange={(e) => setMapFilter(e.target.value)}>
              {MAP_OPTIONS.map((m) => (
                <option key={m} value={m}>{m.toUpperCase()}</option>
              ))}
            </select>
          </div>
        </div>
        <div className="filter-group">
          <label className="filter-label">RESULT</label>
          <div className="seg" role="tablist" aria-label="Result filter">
            {RESULT_OPTIONS.map((o) => (
              <button
                key={o.key}
                className={resultFilter === o.key ? "on" : ""}
                onClick={() => setResultFilter(o.key)}
              >
                {o.key !== "all" && (
                  <span
                    className="seg-dot"
                    style={{
                      background:
                        o.key === "win" ? "var(--win)" :
                        o.key === "loss" ? "var(--loss)" :
                        "var(--draw)",
                    }}
                  />
                )}
                {o.label.toUpperCase()}
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

type RoundStripProps = { rounds: Match["rounds"]; compact?: boolean };
export function RoundStrip({ rounds, compact }: RoundStripProps) {
  const rs = rounds ?? [];
  const padLength = Math.max(0, 24 - rs.length);
  return (
    <div className={compact ? "tl-rail" : "round-strip"}>
      {rs.map((r, i) => (
        <div
          key={i}
          className={`round-dot ${r.side} ${r.won ? "won" : "lost"} ${r.ot ? "ot" : ""}`}
          title={`Round ${i + 1}: ${r.side.toUpperCase()} · ${r.won ? "won" : "lost"}`}
        />
      ))}
      {Array.from({ length: padLength }).map((_, i) => (
        <div key={`pad-${i}`} className="round-dot" />
      ))}
    </div>
  );
}

type MapThumbProps = { mapKey: string; size?: "row" | "card" | "small" };
export function MapThumb({ mapKey, size = "row" }: MapThumbProps) {
  const cls =
    size === "card" ? "card-map-thumb map-thumb"
    : size === "small" ? "tl-thumb map-thumb"
    : "map-thumb";
  return (
    <div className={`${cls} ${mapKey}`}>
      {size === "row" && <span>DE</span>}
    </div>
  );
}
