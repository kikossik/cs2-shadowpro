import type { Match, MapInfo, MatchResult, MatchRound, MatchSide } from "./types";

const RNG = (seed: number) => {
  let s = seed;
  return () => (s = (s * 9301 + 49297) % 233280) / 233280;
};

export const MAPS: MapInfo[] = [
  { key: "mirage", name: "de_mirage", display: "Mirage" },
  { key: "inferno", name: "de_inferno", display: "Inferno" },
  { key: "dust2", name: "de_dust2", display: "Dust II" },
  { key: "ancient", name: "de_ancient", display: "Ancient" },
  { key: "nuke", name: "de_nuke", display: "Nuke" },
  { key: "anubis", name: "de_anubis", display: "Anubis" },
];

function genRounds(
  rnd: () => number,
  totalRounds: number,
  userSide: MatchSide,
  mineWinning: boolean,
): MatchRound[] {
  const rounds: MatchRound[] = [];
  for (let i = 0; i < totalRounds; i++) {
    const firstHalf = i < 12;
    const mySide: MatchSide = firstHalf ? userSide : userSide === "ct" ? "t" : "ct";
    const ot = i >= 24;
    const baseProb = mineWinning ? 0.58 : 0.42;
    rounds.push({ side: mySide, won: rnd() < baseProb, ot });
  }
  return rounds;
}

export const MOCK_MATCHES: Match[] = (() => {
  const rnd = RNG(42);
  const arr: Match[] = [];
  const now = new Date("2026-04-16T20:30:00");
  for (let i = 0; i < 20; i++) {
    const map = MAPS[Math.floor(rnd() * MAPS.length)];
    const mineWins = Math.floor(10 + rnd() * 5);
    const theirWins = Math.floor(8 + rnd() * 6);
    const draw = mineWins === theirWins;
    const result: MatchResult = draw ? "draw" : mineWins > theirWins ? "win" : "loss";
    const total = mineWins + theirWins;
    const userSide: MatchSide = rnd() > 0.5 ? "ct" : "t";
    const rounds = genRounds(rnd, total, userSide, result === "win");

    // Force round wins to match the overall score.
    let wonSoFar = 0;
    let lostSoFar = 0;
    for (const r of rounds) {
      if (wonSoFar < mineWins && (lostSoFar >= theirWins || r.won)) {
        r.won = true;
        wonSoFar++;
      } else {
        r.won = false;
        lostSoFar++;
      }
    }

    const kills = Math.floor(12 + rnd() * 20);
    const deaths = Math.floor(10 + rnd() * 16);
    const assists = Math.floor(2 + rnd() * 8);
    const hs = Math.floor(30 + rnd() * 45);
    const situations = 4 + Math.floor(rnd() * 14);

    const minutesAgo = i * 47 + Math.floor(rnd() * 30);
    const date = new Date(now.getTime() - minutesAgo * 60 * 1000);

    arr.push({
      id: `m_${String(i).padStart(4, "0")}`,
      map,
      date: Math.floor(date.getTime() / 1000),
      match_type: i % 5 === 0 ? "premier" : "competitive",
      score: { ct: mineWins, t: theirWins },
      result,
      user_side_first: userSide,
      round_count: total,
      rounds,
      stats: {
        k: kills, d: deaths, a: assists,
        kd: (kills / Math.max(1, deaths)).toFixed(2),
        hs_pct: hs,
      },
      situations,
    });
  }
  return arr;
})();

export const MAP_OPTIONS = ["All maps", ...MAPS.map((m) => m.display)];

export const RESULT_OPTIONS: { key: "all" | MatchResult; label: string }[] = [
  { key: "all", label: "All" },
  { key: "win", label: "Wins" },
  { key: "loss", label: "Losses" },
  { key: "draw", label: "Draws" },
];

export type DayGroup = { date: Date; matches: Match[] };

export function groupByDay(matches: Match[]): DayGroup[] {
  const groups = new Map<string, DayGroup>();
  for (const m of matches) {
    const d = new Date(m.date * 1000);
    const key = `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`;
    if (!groups.has(key)) groups.set(key, { date: d, matches: [] });
    groups.get(key)!.matches.push(m);
  }
  return [...groups.values()];
}

export function fmtDay(d: Date): string {
  const now = new Date("2026-04-16T23:59:00");
  const diffDays = Math.floor((now.getTime() - d.getTime()) / (1000 * 60 * 60 * 24));
  if (diffDays === 0) return "TODAY";
  if (diffDays === 1) return "YESTERDAY";
  return d.toLocaleDateString("en-US", { weekday: "short" }).toUpperCase();
}

export function fmtFullDate(ts: number): string {
  return new Date(ts * 1000).toLocaleDateString("en-US", { month: "short", day: "numeric" }).toUpperCase();
}

export function fmtTime(ts: number): string {
  return new Date(ts * 1000).toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false });
}
