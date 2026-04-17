export type MatchResult = "win" | "loss" | "draw";
export type MatchSide = "ct" | "t";

export type MatchRound = {
  side: MatchSide;
  won: boolean;
  ot: boolean;
};

export type MapInfo = {
  key: string;
  name: string;
  display: string;
};

export type MatchStats = {
  k: number;
  d: number;
  a: number;
  kd: string;
  adr: number;
  hs: number;
  kast: number;
  rating: string;
  mvps: number;
};

export type Match = {
  id: string;
  map: MapInfo;
  date: Date;
  mode: string;
  rank: string;
  opponent: string;
  myTeam: string;
  durationMin: number;
  score: { mine: number; theirs: number };
  result: MatchResult;
  userSide: MatchSide;
  rounds: MatchRound[];
  stats: MatchStats;
  situations: number;
  topMatch: { pro: string; pct: number };
};

export type ImportState = "idle" | "loading" | "done";
export type Layout = "ledger" | "cards" | "timeline";
export type Density = "comfortable" | "compact";

export type MatchesTweakState = {
  layout: Layout;
  importState: ImportState;
  density: Density;
  showRoundStrip: boolean;
  skeleton: boolean;
};
