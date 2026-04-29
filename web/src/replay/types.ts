export interface MapConfig {
  name: string;
  display_name: string;
  pos_x: number;
  pos_y: number;
  scale: number;
  has_lower_level: boolean;
  lower_level_max_z: number;
}

export interface PlayerTick {
  steamid: string;
  name: string;
  side: "ct" | "t";
  x: number;
  y: number;
  z: number;
  yaw: number;
  health: number;
  inventory: string[];
  flash_duration: number;
}

export interface TickFrame {
  tick: number;
  players: PlayerTick[];
}

export interface ShotEvent {
  tick: number;
  player_steamid: string;
  weapon: string;
}

export interface SmokeEvent {
  start_tick: number;
  end_tick: number;
  x: number;
  y: number;
  thrower_name: string;
}

export interface InfernoEvent {
  start_tick: number;
  end_tick: number;
  x: number;
  y: number;
}

export interface FlashEvent {
  tick: number;
  x: number;
  y: number;
}

export interface GrenadePathPoint {
  tick: number;
  x: number;
  y: number;
}

export interface GrenadePathEvent {
  entity_id: number;
  grenade_type: string;
  path: GrenadePathPoint[];
}

export interface RoundOutcome {
  winner_side: "ct" | "t" | null;
  reason: string | null;
  bomb_site: string | null;
  bomb_plant_tick: number | null;
  bomb_plant_offset_s: number | null;
  official_end_tick: number | null;
}

export interface RoundMeta {
  score_before: { ct: number; t: number };
  outcome: RoundOutcome;
}

export interface RoundReplayData {
  map: string;
  round_num: number;
  freeze_end_tick: number;
  tick_list: number[];
  ticks: TickFrame[];
  shots: ShotEvent[];
  smokes: SmokeEvent[];
  infernos: InfernoEvent[];
  flashes: FlashEvent[];
  grenade_paths: GrenadePathEvent[];
  round_meta?: RoundMeta;
}

export interface SimilarityQuery {
  demo_id: string;
  round_num: number;
  anchor_tick: number;
  start_tick: number;
  end_tick: number;
  phase: string | null;
  site: string | null;
  side_to_query: "ct" | "t" | null;
  time_since_freeze_end_s?: number | null;
}

export interface BestMatch {
  source_match_id: string;
  round_num: number;
  score: number;
  best_window_score?: number;
  coverage?: number;
  supporting_window_hits?: number;
  matched_query_windows?: number;
  map_name: string | null;
  map: { key: string; name: string; display: string };
  event_name: string | null;
  team1_name: string | null;
  team2_name: string | null;
  team_ct: string | null;
  team_t: string | null;
  match_date: string | null;
}

export type RoundAnalysisStatus = "pending" | "done" | "error";

export interface RoundAnalysisResult {
  query: SimilarityQuery;
  best_match: BestMatch | null;
}

export interface RoundAnalysisResponse {
  status: RoundAnalysisStatus;
  result: RoundAnalysisResult | null;
  error: string | null;
}

/** Pre-computed per-player weapon at each tick index. */
export type WeaponMap = Record<string, Array<string | null>>;
