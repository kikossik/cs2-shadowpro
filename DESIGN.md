# DESIGN — Situation Viewer (hero feature)

The product's hero screen. Side-by-side 2D radar comparing the user's situation
to the closest pro situation, with synced scrubbing, a feature strip, and a
"why-it-matched" sidebar.

This doc tracks what got built, the visual decisions baked into the design, and
the data contract the components consume. The prototype came from
[Claude Design](https://claude.ai/design); this is the implementation.

## Where the code lives

```
web/                         Vite + React + TypeScript app
  index.html
  public/
    mirage-placeholder.svg   ← swap for licensed Mirage radar PNG before launch
  src/
    main.tsx
    Viewer.tsx               main layout, themes, scrubber, why-matched panel
    Radar.tsx                single radar pane on a <canvas>
    themes.ts                three theme variants
    mockData.ts              static SITUATION_DATA (replace with API fetch)
    types.ts                 shape of a situation pair
    styles.css
```

Run: `cd web && npm install && npm run dev`.

## Visual decisions

- **Three themes**, switchable at runtime via a tweaks panel:
  - **Tactical Operator** (default) — warm tan/amber on near-black; Inter Tight
  - **Editorial Analyst** — cream + serif headings (Instrument Serif) on dark
  - **Broadcast Timeline** — cyan/magenta on cool dark; broadcast-graphic vibe
- **Side colors**: CT blue, T amber. CS convention, not branded.
- **Type stack**: `Inter Tight` headings · `JetBrains Mono` numerals/labels ·
  `Instrument Serif` (editorial only). All from Google Fonts.
- **Layout** (default `side-by-side`): two square radars left, why-matched
  sidebar right. Alternate layouts: `stacked` (radars top/bottom), `overlay`
  (pro radar superimposed at 60% opacity, screen blend mode).
- **Density toggles**: trails, smokes, molotovs, kill X markers, name labels —
  all individually toggleable from the tweaks panel.

## Component map

```
<Viewer>                       owns theme + tweak state, playback hook
 ├── <TopBar>                  brand · breadcrumbs · scoreboard · user chip
 ├── <RoundRail>               24 round dots, current highlighted, count badge
 ├── <main class="stage">
 │    ├── <section class="radars">
 │    │    ├── <pane class="user">  ← <PaneHeader> + <Radar> + <PaneOverlay>
 │    │    └── <pane class="pro">   ← <PaneHeader> + <Radar> + <PaneOverlay>
 │    └── <WhyMatched>          score ring · weighted breakdown · note · CTAs
 ├── <FeatureStrip>             Area · Side · Count · Econ · Phase · Time · Util
 ├── <Scrubber>                 transport · timeline w/ event marks · readouts · speed
 └── <TweaksPanel>              theme · layout · map intensity · density toggles
```

`<Radar>` draws everything to a single canvas: smokes (radial gradient disks),
molotovs (orange patches with sparks), C4 plant marker (pulsing diamond), kill X
markers (fade in over 20s after their event time), focal-player ghost trail
(last ~4 sample points), other players (small CT/T circles), focal player
(large filled circle + halo + crosshair ring + name tag).

Time model: `progress` is `0..1` over a 15-second playback window
(`-3s ... +12s` relative to the situation tick). The `usePlayback` hook drives
`progress` via `requestAnimationFrame`, scaled by speed (0.5×/1×/2×).

## Data contract

`<Viewer>` reads a single object — currently from `src/mockData.ts`, eventually
from `GET /situation/:id`. Coordinates are normalized `0..1` across the radar
square so the renderer is independent of map dimensions.

```ts
type Situation = {
  round: number;
  score: { ct: number; t: number };
  tick: { start: number; current: number; end: number };
  tickRate: number;
  features: {
    area: string;          // e.g. "B Apps"
    side: "CT" | "T";
    playerCount: string;   // e.g. "2v2"
    economy: string;       // "Full-buy" | "Semi" | "Eco"
    phase: string;         // "Pre-plant" | "Post-plant" | "Freeze"
    timeRemaining: number; // seconds
    utility: { smokes: number; molotovs: number; flashes: number };
  };
  match: {
    score: number;         // 0..1
    breakdown: { label: string; value: string; weight: number; matched: boolean; note?: string }[];
    note: string;          // human-readable summary
  };
  user: SituationPane;
  pro:  SituationPane;
};

type SituationPane = {
  label: string;
  sub: string;
  matchMeta: string;
  focal:    { id: string; name: string; side: "CT" | "T"; steamid: string; x: number; y: number };
  players:  { id: string; name: string; side: "CT" | "T"; x: number; y: number; alive: boolean }[];
  smokes:   { x: number; y: number; r: number; age: number }[];
  molotovs: { x: number; y: number; r: number; age: number }[];
  kills:    { x: number; y: number; t: number; killer: string; victim: string; weapon: string }[];
  bomb?:    { x: number; y: number; planted: boolean; plantedAt: number };
  trail:    [number, number][];   // 11 sample points across -3s..+12s
};
```

## Adapting from `situations.db` (next step)

The DB stores **one row per (player, sampled tick)**. To populate a
`Situation`, the backend needs to:

1. Resolve the **user situation row** (by `id`).
2. Run the matching query (already in `match_situation.py`) to get the **top
   pro candidate**.
3. For each side, gather the **other players' positions at the same tick**
   (already in `ticks.parquet` — needs to be queryable; could index a small
   tick-by-tick player slice into a separate table or just keep parquets per
   demo and look up on demand).
4. Gather **utility (smokes/infernos)**, **kills in the round so far**, and
   **bomb plant** state for both panes.
5. Build the focal-player **trail** by sampling the same player's position
   every ~1.5s across the 15-second clip window
   (`clip_start_tick … clip_end_tick`).
6. Normalize all `(x, y)` to `0..1` using Mirage's bounding box so the radar
   renderer doesn't need to know map units.
7. Compute the human-readable **`features`** strings from the row's
   categorical fields.
8. Build **`match.breakdown`** from the scoring components already returned by
   `match_situation.score_match()`.

The backend endpoint shape is intentionally one-shot: a single GET returns
everything the viewer needs to render. No follow-up calls during scrubbing —
the trail and event lists are bundled.

## What is *not* implemented yet

- Real backend (`server/` is a future milestone)
- Real Mirage radar PNG (using a placeholder SVG with callout names; swap when
  Valve asset licensing is reviewed)
- Login / FACEIT match list / report list pages — only the hero is built
- Persisting tweak preferences (currently component state; localStorage is the
  obvious next step)
- Mobile / narrow viewport layouts (the design assumes ≥1280px)

## Out of scope for this design

- Session stats / pattern detection (deferred per CLAUDE.md MVP scope)
- Video playback (v2/premium per PLAN.md)
- Claude-narrated findings (v2/premium)
