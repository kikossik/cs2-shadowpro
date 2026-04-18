import type { Match } from "./types";
import { MapThumb, RoundStrip } from "./Shell";
import { fmtFullDate, fmtTime } from "./utils";

const EMPTY_ASCII = `
  ┌─────────────┐
  │  NO MATCHES │
  │   MATCHED   │
  └─────────────┘`;

type LedgerLayoutProps = {
  matches: Match[];
  compact: boolean;
  showRoundStrip: boolean;
  onOpen: (m: Match) => void;
};
export function LedgerLayout({ matches, compact, showRoundStrip, onOpen }: LedgerLayoutProps) {
  return (
    <div className={`ledger ${compact ? "compact" : ""}`}>
      <div className="ledger-head">
        <div>MAP</div>
        <div>MATCH</div>
        <div>SCORE</div>
        <div>K / D / A</div>
        <div>HS%</div>
        <div>{showRoundStrip ? "ROUNDS" : ""}</div>
        <div>SITUATIONS</div>
        <div />
      </div>
      {matches.map((m) => (
        <LedgerRow key={m.id} m={m} showRoundStrip={showRoundStrip} onOpen={onOpen} />
      ))}
      {matches.length === 0 && (
        <div className="empty-state">
          <pre className="empty-ascii">{EMPTY_ASCII}</pre>
          <div className="empty-title">No matches in this filter</div>
          <div className="empty-sub">Try another map or a different result.</div>
        </div>
      )}
    </div>
  );
}

type LedgerRowProps = {
  m: Match;
  showRoundStrip: boolean;
  onOpen: (m: Match) => void;
};
function LedgerRow({ m, showRoundStrip, onOpen }: LedgerRowProps) {
  const stripeColor =
    m.result === "win" ? "var(--win)" :
    m.result === "loss" ? "var(--loss)" :
    "var(--draw)";
  const k = m.stats?.k ?? 0;
  const d = m.stats?.d ?? 0;
  const kdDelta = k - d;
  return (
    <div
      className="ledger-row"
      role="button"
      tabIndex={0}
      onClick={() => onOpen(m)}
      onKeyDown={(e) => { if (e.key === "Enter") onOpen(m); }}
    >
      <div className="result-stripe" style={{ background: stripeColor }} />
      <MapThumb mapKey={m.map.key} />
      <div className="map-info">
        <div className="map-name">{m.map.display}</div>
        <div className="map-sub">
          COMPETITIVE · {fmtFullDate(m.date)} {fmtTime(m.date)}
        </div>
      </div>
      <div className="score-cell">
        {m.score
          ? <>
              <span className={`mine ${m.result ?? ""}`}>{m.score.ct}</span>
              <span className="sep">:</span>
              <span className="theirs">{m.score.t}</span>
              {m.result && (
                <span className={`result-tag rtag-${m.result}`}>
                  {m.result === "win" ? "W" : m.result === "loss" ? "L" : "D"}
                </span>
              )}
            </>
          : <span style={{ color: "var(--dim)" }}>—</span>
        }
      </div>
      <div className="kd-cell">
        {m.stats
          ? <>
              <span>{m.stats.k}-{m.stats.d}-{m.stats.a}</span>
              <span className={`delta ${kdDelta > 0 ? "pos" : kdDelta < 0 ? "neg" : ""}`}>
                {kdDelta > 0 ? "+" : ""}{kdDelta}
              </span>
            </>
          : <span style={{ color: "var(--dim)" }}>—</span>
        }
      </div>
      <div className="num-cell dim">{m.stats ? `${m.stats.hs_pct}%` : "—"}</div>
      <div>
        {showRoundStrip ? <RoundStrip rounds={m.rounds} /> : (
          m.user_side_first
            ? <span style={{ fontFamily: "var(--fontMono)", fontSize: 10, color: "var(--dim)", letterSpacing: "0.06em" }}>
                {m.user_side_first.toUpperCase()} START
              </span>
            : null
        )}
      </div>
      <div className="situations-cell">
        <span className="sit-count">
          <span className="n">{m.situations}</span>
          <span className="lbl">SIT</span>
        </span>
      </div>
      <div className="chevron">›</div>
    </div>
  );
}
