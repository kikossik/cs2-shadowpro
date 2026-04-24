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

export interface SimilarityBestMatch {
  window_id: string;
  source_match_id: string;
  map_name: string | null;
  map: { key: string; name: string; display: string };
  round_num: number;
  anchor_tick: number;
  start_tick: number;
  end_tick: number;
  phase: string | null;
  site: string | null;
  anchor_kind: string | null;
  score: number;
  reason: string;
  event_name: string | null;
  team_ct: string | null;
  team_t: string | null;
  match_date: string | null;
  feature_path: string;
  candidate_score?: number;
  coverage?: number;
  matched_query_windows?: number;
  supporting_window_hits?: number;
  shortlist_rank?: number;
  retrieval_score?: number;
  logic?: RoundAnalysisLogic;
  break_event?: RoundAnalysisBreakEvent;
  shared_prefix?: RoundAnalysisSharedPrefix;
  divergence?: RoundAnalysisDivergence;
  timeline_sync?: RoundAnalysisTimelineSync;
  pro_time_offset_s?: number;
  divergence_start_sec?: number;
  divergence_end_sec?: number;
  summary?: string;
}

export interface SimilarityMapResponse {
  query: SimilarityQuery;
  best_match: SimilarityBestMatch | null;
}

export type RoundAnalysisLogic = "nav" | "original" | "both";

export type RoundAnalysisStatus = "pending" | "done" | "error";

export type RoundAnalysisCacheStatus = "fresh" | "pending" | "stale" | "missing";

export interface RoundAnalysisMeta {
  demo_id: string;
  round_num: number;
  logic: RoundAnalysisLogic;
  cache_key: string;
  cache_status: RoundAnalysisCacheStatus;
  resolved_from_cache: boolean;
  matcher_version: string;
  pro_corpus_version: string;
}

export interface RoundAnalysisResult {
  query: SimilarityQuery;
  best_match: SimilarityBestMatch | null;
  shortlist?: RoundAnalysisCandidate[];
  retrieval?: RoundAnalysisRetrievalMeta;
  logic?: RoundAnalysisLogic;
  matches?: RoundAnalysisMatch[];
  selected_match?: RoundAnalysisMatch | null;
}

export interface RoundAnalysisCandidate {
  source_match_id: string;
  round_num: number;
  map_name: string | null;
  map: { key: string; name: string; display: string };
  event_name: string | null;
  team_ct: string | null;
  team_t: string | null;
  match_date: string | null;
  score: number;
  best_window_score: number;
  coverage: number;
  supporting_window_hits: number;
  matched_query_windows: number;
  query_anchor_kinds: string[];
  shortlist_rank: number;
  top_window: SimilarityBestMatch;
  window_hits: SimilarityBestMatch[];
}

export interface RoundAnalysisRetrievalMeta {
  query_window_count: number;
  window_hit_count: number;
  candidate_round_count: number;
  stage: string;
}

export interface RoundAnalysisBreakEvent {
  type: string;
  label: string;
  reason: string;
  user_time_s: number;
  pro_time_s: number;
  user_place: string | null;
  pro_place: string | null;
}

export interface RoundAnalysisSharedPrefix {
  duration_s: number;
  ratio: number;
  end_s?: number;
  user_place?: string | null;
  pro_place?: string | null;
  user_phase?: string | null;
  pro_phase?: string | null;
}

export interface RoundAnalysisDivergence {
  start_s: number;
  end_s?: number;
  user_place?: string | null;
  pro_place?: string | null;
  user_phase?: string | null;
  pro_phase?: string | null;
}

export interface RoundAnalysisTimelineSync {
  time_base: "freeze_end_relative_seconds";
  user_freeze_end_tick: number;
  pro_freeze_end_tick: number;
  user_tick_rate: number;
  pro_tick_rate: number;
  pro_time_offset_s: number;
  shared_prefix_end_sec: number;
  divergence_start_sec: number;
  divergence_end_sec: number;
}

export interface RoundAnalysisLogicScore {
  logic: "nav" | "original";
  score: number;
  summary: string;
  components: Record<string, number>;
  shared_prefix: RoundAnalysisSharedPrefix;
  divergence: RoundAnalysisDivergence;
  break_event: RoundAnalysisBreakEvent;
  survival_gap_s: number;
  query_side?: "ct" | "t";
  timeline_sync?: RoundAnalysisTimelineSync;
  pro_time_offset_s?: number;
  divergence_start_sec?: number;
  divergence_end_sec?: number;
}

export interface RoundAnalysisMatch extends RoundAnalysisCandidate {
  deep_score: number;
  logic: RoundAnalysisLogic;
  logic_scores: {
    nav: number;
    original: number;
    both: number;
  };
  nav: RoundAnalysisLogicScore;
  original: RoundAnalysisLogicScore;
  summary: string;
  break_event: RoundAnalysisBreakEvent;
  shared_prefix: RoundAnalysisSharedPrefix;
  divergence: RoundAnalysisDivergence;
  timeline_sync?: RoundAnalysisTimelineSync;
  pro_time_offset_s?: number;
  divergence_start_sec?: number;
  divergence_end_sec?: number;
}

export interface RoundAnalysisResponse {
  status: RoundAnalysisStatus;
  analysis: RoundAnalysisMeta;
  result: RoundAnalysisResult | null;
  error: string | null;
}

/** Pre-computed per-player weapon at each tick index. */
export type WeaponMap = Record<string, Array<string | null>>;
