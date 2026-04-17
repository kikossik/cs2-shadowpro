import type { Match } from "./types";
import { MapThumb, RoundStrip } from "./Shell";
import { fmtFullDate, fmtTime } from "./mockMatches";

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
        <div>ADR</div>
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
  const kdDelta = m.stats.k - m.stats.d;
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
          {m.mode.toUpperCase()} · {fmtFullDate(m.date)} {fmtTime(m.date)} · {m.durationMin}M
        </div>
      </div>
      <div className="score-cell">
        <span className={`mine ${m.result}`}>{m.score.mine}</span>
        <span className="sep">:</span>
        <span className="theirs">{m.score.theirs}</span>
        <span className={`result-tag rtag-${m.result}`}>
          {m.result === "win" ? "W" : m.result === "loss" ? "L" : "D"}
        </span>
      </div>
      <div className="kd-cell">
        <span>{m.stats.k}-{m.stats.d}-{m.stats.a}</span>
        <span className={`delta ${kdDelta > 0 ? "pos" : kdDelta < 0 ? "neg" : ""}`}>
          {kdDelta > 0 ? "+" : ""}{kdDelta}
        </span>
      </div>
      <div className="num-cell">{m.stats.adr}</div>
      <div className="num-cell dim">{m.stats.hs}%</div>
      <div>
        {showRoundStrip ? <RoundStrip rounds={m.rounds} /> : (
          <span style={{
            fontFamily: "var(--fontMono)", fontSize: 10,
            color: "var(--dim)", letterSpacing: "0.06em",
          }}>
            {m.userSide.toUpperCase()} START
          </span>
        )}
      </div>
      <div className="situations-cell">
        <span className="sit-count">
          <span className="n">{m.situations}</span>
          <span className="lbl">SIT</span>
        </span>
        <span className="top-match">
          {m.topMatch.pro}<br />
          <span className="pct">{m.topMatch.pct}%</span>
        </span>
      </div>
      <div className="chevron">›</div>
    </div>
  );
}
