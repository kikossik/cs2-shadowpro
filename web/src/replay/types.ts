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
}

export interface SimilarityMapResponse {
  query: SimilarityQuery;
  best_match: SimilarityBestMatch | null;
}

/** Pre-computed per-player weapon at each tick index. */
export type WeaponMap = Record<string, Array<string | null>>;
