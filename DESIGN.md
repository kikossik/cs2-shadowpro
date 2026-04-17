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
    main.tsx                 routing: LoginPage → MatchesPage → Viewer
    LoginPage.tsx            Steam OpenID login
    Viewer.tsx               situation viewer: layout, themes, scrubber, why-matched panel
    Radar.tsx                single radar pane on a <canvas>
    themes.ts                three theme variants
    mockData.ts              static SITUATION_DATA (replace with API fetch at M6)
    types.ts                 shape of a situation pair
    styles.css
    matches/                 post-login matches landing page
      MatchesPage.tsx        container: tweak/filter state, import banner, layout switcher
      Shell.tsx              TopBar, ImportBanner, FilterBar, RoundStrip, MapThumb
      LedgerLayout.tsx       dense HLTV-style tabular list (default)
      CardsLayout.tsx        2-col match cards with stats
      TimelineLayout.tsx     chronological feed grouped by date
      TweaksPanel.tsx        layout / import / density / round-strip toggles
      mockMatches.ts         20 mock Steam-ranked matches (replace with API fetch at M6)
      types.ts               Match, MatchRound, MatchesTweakState types
      matches.css            scoped under .matches-root (no collision with Viewer styles)
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

- Real backend (`server/` is M6)
- Real Mirage radar PNG (placeholder SVG; swap when Valve asset licensing is reviewed)
- Steam match history → demo URLs (M5; Steam API access needs verification)
- Server-side Steam OpenID `check_authentication` verification (noted in `main.tsx`; required before launch)
- Match list and situation list populated from API (Matches page uses mock data until M6)
- Mobile / narrow viewport layouts (assumes ≥1280px)

## Matches landing page

After login the user lands on `MatchesPage` (`web/src/matches/`), which lists their Steam-ranked
matches and lets them click through to the Viewer for any match.

**3 layouts** switchable via the Tweaks panel:
- **Ledger** (default) — dense HLTV-stats table: map thumb, score, K/D/A, ADR, HS%, 24-round strip, situation count + top pro match.
- **Cards** — 2-col grid with bigger map art, full stats row, "REVIEW →" CTA.
- **Timeline** — grouped by day (TODAY / YESTERDAY / weekday), each match a horizontal strip.

**Shell:** sticky topbar (brand, breadcrumbs, Steam user chip, sign-out), auto-import progress banner (spinner + progress bar, animates when import state = loading), Map dropdown + Result segmented filter, ASCII empty state when filter yields nothing, loading skeleton.

**CSS** is scoped under `.matches-root` so the Viewer's `styles.css` classes don't interfere.

**Data contract for M6** — backend needs `GET /matches/{steam_id}` returning an array with the same shape as `Match` in `web/src/matches/types.ts`. Replace `MOCK_MATCHES` import in `mockMatches.ts` with the API fetch.

## Out of scope for this design

- Session stats / pattern detection (deferred per CLAUDE.md MVP scope)
- Video playback (v2/premium per PLAN.md)
- Claude-narrated findings (v2/premium)
