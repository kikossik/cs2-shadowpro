// Shape of the data the Situation Viewer renders.
// Coordinates are normalized 0..1 across the radar square so the renderer
// stays independent of map dimensions.

export type Side = "CT" | "T";

export type Player = {
  id: string;
  name: string;
  side: Side;
  x: number;
  y: number;
  alive: boolean;
};

export type Focal = {
  id: string;
  name: string;
  side: Side;
  steamid: string;
  team?: string;
  x: number;
  y: number;
  // Filled in by Radar at render time; kept on the object so PaneOverlay etc.
  // can read current position without re-deriving.
  __x?: number;
  __y?: number;
};

export type Smoke    = { x: number; y: number; r: number; age: number };
export type Molotov  = { x: number; y: number; r: number; age: number };
export type Kill     = { x: number; y: number; t: number; killer: string; victim: string; weapon: string };
export type Bomb     = { x: number; y: number; planted: boolean; plantedAt: number };

export type SituationPane = {
  label: string;
  sub: string;
  matchMeta: string;
  focal: Focal;
  players: Player[];
  smokes: Smoke[];
  molotovs: Molotov[];
  kills: Kill[];
  bomb?: Bomb;
  trail: [number, number][];
};

export type MatchBreakdownRow = {
  label: string;
  value: string;
  weight: number;
  matched: boolean;
  note?: string;
};

export type Situation = {
  round: number;
  score: { ct: number; t: number };
  tick: { start: number; current: number; end: number };
  tickRate: number;
  features: {
    area: string;
    side: Side;
    playerCount: string;
    economy: string;
    phase: string;
    timeRemaining: number;
    utility: { smokes: number; molotovs: number; flashes: number };
  };
  match: {
    score: number;
    breakdown: MatchBreakdownRow[];
    note: string;
  };
  user: SituationPane;
  pro: SituationPane;
};

export type Theme = {
  name: string;
  bg: string;
  panel: string;
  panelSoft: string;
  border: string;
  borderHi: string;
  ink: string;
  paper: string;
  dim: string;
  dimmer: string;
  accent: string;
  accent2: string;
  ct: string;
  tside: string;
  grid: string;
  stripe: string;
  smoke: string;
  fire: string;
  fontHead: string;
  fontMono: string;
};

export type ThemeKey = "tactical" | "editorial" | "broadcast";

export type Density = {
  trails: boolean;
  smokes: boolean;
  molotovs: boolean;
  kills: boolean;
  labels: boolean;
  callouts: boolean;
};

export type RadarIntensity = "full" | "dim" | "wire";

export type LayoutKey = "side-by-side" | "stacked" | "overlay";

export type TweakState = {
  theme: ThemeKey;
  layout: LayoutKey;
  radarIntensity: RadarIntensity;
  showTrails: boolean;
  showSmokes: boolean;
  showMolotovs: boolean;
  showKills: boolean;
  showLabels: boolean;
  showCallouts: boolean;
};
