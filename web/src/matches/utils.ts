import type { Match, MatchResult, MatchType } from "./types";

export const MAP_OPTIONS = [
  "All maps", "Mirage", "Inferno", "Dust II", "Ancient", "Nuke", "Anubis",
  "Vertigo", "Overpass", "Cache",
];

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
  const now = new Date();
  const diffDays = Math.floor((now.getTime() - d.getTime()) / (1000 * 60 * 60 * 24));
  if (diffDays === 0) return "TODAY";
  if (diffDays === 1) return "YESTERDAY";
  return d.toLocaleDateString("en-US", { weekday: "short" }).toUpperCase();
}

export function fmtFullDate(ts: number): string {
  return new Date(ts * 1000).toLocaleDateString("en-US", {
    month: "short", day: "numeric",
  }).toUpperCase();
}

export function fmtTime(ts: number): string {
  return new Date(ts * 1000).toLocaleTimeString("en-US", {
    hour: "2-digit", minute: "2-digit", hour12: false,
  });
}

export function matchTypeLabel(matchType: MatchType | string | null | undefined): string {
  switch (matchType) {
    case "premier":
      return "PREMIER";
    case "competitive":
      return "COMPETITIVE";
    case "faceit":
      return "FACEIT";
    case "hltv":
      return "HLTV";
    default:
      return "MATCH";
  }
}
