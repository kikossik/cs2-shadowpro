import type { Match } from "./types";
import { MapThumb, RoundStrip } from "./Shell";
import { fmtDay, fmtFullDate, fmtTime, groupByDay } from "./utils";

const EMPTY_ASCII = `
  ┌─────────────┐
  │  NO MATCHES │
  └─────────────┘`;

type TimelineLayoutProps = {
  matches: Match[];
  showRoundStrip: boolean;
  onOpen: (m: Match) => void;
};
export function TimelineLayout({ matches, showRoundStrip, onOpen }: TimelineLayoutProps) {
  const groups = groupByDay(matches);
  return (
    <div>
      {groups.map((g, i) => (
        <div className="timeline-group" key={i}>
          <div className="timeline-day">
            <div className="timeline-day-tag">
              <div className="tl-day-label">{fmtDay(g.date)}</div>
              <div className="tl-day-date">{fmtFullDate(g.matches[0].date)}</div>
              <div className="tl-day-sub">
                {g.matches.length} MATCH{g.matches.length === 1 ? "" : "ES"}
              </div>
            </div>
            <div className="tl-matches">
              {g.matches.map((m) => (
                <TimelineMatch
                  key={m.id}
                  m={m}
                  onOpen={onOpen}
                  showRoundStrip={showRoundStrip}
                />
              ))}
            </div>
          </div>
        </div>
      ))}
      {matches.length === 0 && (
        <div className="empty-state">
          <pre className="empty-ascii">{EMPTY_ASCII}</pre>
          <div className="empty-title">No matches in this filter</div>
        </div>
      )}
    </div>
  );
}

type TimelineMatchProps = {
  m: Match;
  onOpen: (m: Match) => void;
  showRoundStrip: boolean;
};
function TimelineMatch({ m, onOpen, showRoundStrip }: TimelineMatchProps) {
  const stripeColor =
    m.result === "win" ? "var(--win)" :
    m.result === "loss" ? "var(--loss)" :
    "var(--draw)";
  return (
    <div
      className="tl-match"
      role="button"
      tabIndex={0}
      onClick={() => onOpen(m)}
      onKeyDown={(e) => { if (e.key === "Enter") onOpen(m); }}
    >
      <div className="tl-match-time">
        <span className="res-mark" style={{ background: stripeColor }} />
        <span>{fmtTime(m.date)}</span>
      </div>
      <div className="tl-match-body">
        <MapThumb mapKey={m.map.key} size="small" />
        <div className="tl-map-block">
          <div className="tl-map-name">{m.map.display}</div>
          <div className="tl-score">
            {m.score
              ? <>
                  <span style={{ color: stripeColor }}>{m.score.ct}</span>
                  <span style={{ color: "var(--dimmer)", margin: "0 3px" }}>:</span>
                  <span>{m.score.t}</span>
                </>
              : <span style={{ color: "var(--dim)" }}>—</span>
            }
          </div>
        </div>
        {showRoundStrip ? <RoundStrip rounds={m.rounds} compact /> : <div />}
        <div className="tl-stats">
          <div className="stat"><span className="k">K/D</span><span className="v">{m.stats?.kd ?? "—"}</span></div>
          <div className="stat"><span className="k">HS</span><span className="v">{m.stats ? `${m.stats.hs_pct}%` : "—"}</span></div>
          <div className="stat"><span className="k">SIT</span><span className="v accent">{m.situations}</span></div>
        </div>
      </div>
      <div className="chevron">›</div>
    </div>
  );
}
