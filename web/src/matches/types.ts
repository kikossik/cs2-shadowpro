export type MatchResult = "win" | "loss" | "draw";
export type MatchSide = "ct" | "t";
export type MatchType = "unknown" | "premier" | "competitive" | "faceit" | "hltv";

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
  hs_pct: number;
};

export type Match = {
  id: string;
  map: MapInfo;
  /** Unix timestamp (seconds) from the server. */
  date: number;
  match_type: MatchType;
  result: MatchResult | null;
  user_side_first: MatchSide | null;
  score: { ct: number; t: number } | null;
  round_count: number | null;
  rounds: MatchRound[] | null;
  stats: MatchStats | null;
  situations: number;
};

export type SteamProfile = {
  steam_id: string;
  personaname: string | null;
  avatar: string | null;
};

export type ImportState = "idle" | "loading" | "done" | "error";
export type Layout = "ledger" | "cards" | "timeline";
export type Density = "comfortable" | "compact";

export type MatchesTweakState = {
  layout: Layout;
  density: Density;
  showRoundStrip: boolean;
  skeleton: boolean;
};
