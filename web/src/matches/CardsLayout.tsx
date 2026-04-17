import type { Match } from "./types";
import { MapThumb, RoundStrip } from "./Shell";
import { fmtFullDate, fmtTime } from "./mockMatches";

const EMPTY_ASCII = `
  ┌─────────────┐
  │  NO MATCHES │
  └─────────────┘`;

type CardsLayoutProps = {
  matches: Match[];
  showRoundStrip: boolean;
  onOpen: (m: Match) => void;
};
export function CardsLayout({ matches, showRoundStrip, onOpen }: CardsLayoutProps) {
  return (
    <div className="cards">
      {matches.map((m) => (
        <MatchCard key={m.id} m={m} onOpen={onOpen} showRoundStrip={showRoundStrip} />
      ))}
      {matches.length === 0 && (
        <div className="empty-state" style={{ gridColumn: "1 / -1" }}>
          <pre className="empty-ascii">{EMPTY_ASCII}</pre>
          <div className="empty-title">No matches in this filter</div>
        </div>
      )}
    </div>
  );
}

type MatchCardProps = {
  m: Match;
  onOpen: (m: Match) => void;
  showRoundStrip: boolean;
};
function MatchCard({ m, onOpen, showRoundStrip }: MatchCardProps) {
  const stripeColor =
    m.result === "win" ? "var(--win)" :
    m.result === "loss" ? "var(--loss)" :
    "var(--draw)";
  return (
    <div
      className="match-card"
      role="button"
      tabIndex={0}
      onClick={() => onOpen(m)}
      onKeyDown={(e) => { if (e.key === "Enter") onOpen(m); }}
    >
      <div className="card-stripe" style={{ background: stripeColor }} />
      <div className="card-top">
        <MapThumb mapKey={m.map.key} size="card" />
        <div className="card-top-info">
          <div className="card-map-name">{m.map.display}</div>
          <div className="card-sub">
            {m.mode.toUpperCase()} · {fmtFullDate(m.date)} · {fmtTime(m.date)} · {m.durationMin}M
          </div>
          <div className="card-sub" style={{ color: "var(--dimmer)" }}>
            VS {m.opponent.toUpperCase()} · YOUR START: {m.userSide.toUpperCase()}
          </div>
        </div>
        <div className="card-score">
          <div className="label">
            <span
              className="result-tag"
              style={{
                color: stripeColor,
                borderColor: stripeColor,
                padding: "1px 5px",
                border: "1px solid",
              }}
            >
              {m.result.toUpperCase()}
            </span>
          </div>
          <div style={{ fontVariantNumeric: "tabular-nums" }}>
            <span style={{ color: stripeColor }}>{m.score.mine}</span>
            <span style={{ color: "var(--dimmer)", margin: "0 4px" }}>:</span>
            <span style={{ color: "var(--dim)" }}>{m.score.theirs}</span>
          </div>
        </div>
      </div>

      {showRoundStrip && (
        <div className="card-rail">
          <RoundStrip rounds={m.rounds} />
        </div>
      )}

      <div className="card-stats">
        <div className="card-stat">
          <div className="cstat-k">K / D / A</div>
          <div className="cstat-v">{m.stats.k}·{m.stats.d}·{m.stats.a}</div>
        </div>
        <div className="card-stat">
          <div className="cstat-k">ADR</div>
          <div className="cstat-v">{m.stats.adr}</div>
        </div>
        <div className="card-stat">
          <div className="cstat-k">HS%</div>
          <div className="cstat-v">{m.stats.hs}</div>
        </div>
        <div className="card-stat">
          <div className="cstat-k">KAST</div>
          <div className="cstat-v">{m.stats.kast}</div>
        </div>
        <div className="card-stat">
          <div className="cstat-k">SITUATIONS</div>
          <div className="cstat-v accent">{m.situations}</div>
        </div>
      </div>

      <div className="card-foot">
        <span>
          TOP MATCH: <span style={{ color: "var(--ink)" }}>{m.topMatch.pro}</span>
          <span style={{ color: "var(--accent)", marginLeft: 6 }}>{m.topMatch.pct}%</span>
        </span>
        <span className="cta">REVIEW →</span>
      </div>
    </div>
  );
}
