import type { Situation } from "./types";

// Mock until M6 wires this to GET /situation/:id.
// Coordinates are normalized 0..1 across the radar square.
export const SITUATION_DATA: Situation = {
  round: 14,
  score: { ct: 8, t: 6 },
  tick: { start: 18432, current: 19008, end: 20352 }, // ~15s @ 64 tick
  tickRate: 64,
  features: {
    area: "B Apps",
    side: "T",
    playerCount: "2v2",
    economy: "Full-buy",
    phase: "Post-plant",
    timeRemaining: 42,
    utility: { smokes: 1, molotovs: 1, flashes: 2 },
  },
  match: {
    score: 0.87,
    breakdown: [
      { label: "Map area",          value: "B Apps",     weight: 0.4, matched: true },
      { label: "Player count",      value: "2v2",        weight: 0.3, matched: true },
      { label: "Economy",           value: "Full-buy",   weight: 0.2, matched: true },
      { label: "Spatial proximity", value: "320 units",  weight: 0.1, matched: true,
        note: "NiKo was 320u away" },
    ],
    note: "Same map area, same 2v2, same full-buy. NiKo held a near-identical angle 320 units from your position.",
  },

  user: {
    label: "YOUR ROUND",
    sub: "Round 14 · 0:42 remaining",
    matchMeta: "FACEIT · Level 9 lobby · de_mirage",
    focal: {
      id: "u-focal", name: "you", side: "T",
      steamid: "STEAM_1:0:84210571",
      x: 0.62, y: 0.58,
    },
    players: [
      { id: "u-mate", name: "k1llj0y", side: "T",  x: 0.56, y: 0.64, alive: true },
      { id: "u-ct1",  name: "rhein",   side: "CT", x: 0.74, y: 0.52, alive: true },
      { id: "u-ct2",  name: "axiom",   side: "CT", x: 0.69, y: 0.43, alive: true },
    ],
    smokes:   [{ x: 0.68, y: 0.50, r: 0.055, age: 6 }],
    molotovs: [{ x: 0.71, y: 0.57, r: 0.045, age: 3 }],
    kills: [
      { x: 0.45, y: 0.71, t: -8, killer: "you",     victim: "skar",  weapon: "AK-47" },
      { x: 0.58, y: 0.66, t: -3, killer: "k1llj0y", victim: "lunis", weapon: "Glock" },
    ],
    bomb: { x: 0.60, y: 0.61, planted: true, plantedAt: -22 },
    trail: [
      [0.52, 0.70], [0.54, 0.68], [0.56, 0.66], [0.58, 0.64],
      [0.60, 0.62], [0.61, 0.60], [0.62, 0.58], [0.63, 0.57],
      [0.64, 0.56], [0.66, 0.55], [0.68, 0.54],
    ],
  },

  pro: {
    label: "PRO MIRROR",
    sub: "FaZe vs. G2 · IEM Katowice 2026 · Round 19",
    matchMeta: "NiKo · T-side · 0:39 remaining",
    focal: {
      id: "p-focal", name: "NiKo", team: "FaZe", side: "T",
      steamid: "STEAM_1:1:11560985",
      x: 0.64, y: 0.56,
    },
    players: [
      { id: "p-mate", name: "karrigan", side: "T",  x: 0.57, y: 0.62, alive: true },
      { id: "p-ct1",  name: "huNter-",  side: "CT", x: 0.76, y: 0.49, alive: true },
      { id: "p-ct2",  name: "m0NESY",   side: "CT", x: 0.71, y: 0.41, alive: true },
    ],
    smokes:   [{ x: 0.70, y: 0.48, r: 0.055, age: 5 }],
    molotovs: [{ x: 0.73, y: 0.55, r: 0.045, age: 2 }],
    kills: [
      { x: 0.46, y: 0.69, t: -9, killer: "NiKo",     victim: "jks",   weapon: "AK-47" },
      { x: 0.59, y: 0.65, t: -2, killer: "karrigan", victim: "stavn", weapon: "USP-S" },
    ],
    bomb: { x: 0.61, y: 0.60, planted: true, plantedAt: -21 },
    trail: [
      [0.54, 0.68], [0.56, 0.66], [0.58, 0.64], [0.60, 0.62],
      [0.62, 0.60], [0.63, 0.58], [0.64, 0.56], [0.66, 0.55],
      [0.67, 0.54], [0.69, 0.53], [0.71, 0.52],
    ],
  },
};
