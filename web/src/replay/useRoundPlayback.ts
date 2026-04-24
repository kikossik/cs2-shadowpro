import { useEffect, useRef, useState } from "react";

export type Speed = 1 | 2 | 4;

export interface RoundPlayback {
  tickIdx: number;
  setTickIdx: (n: number) => void;
  playing: boolean;
  setPlaying: (b: boolean) => void;
  speed: Speed;
  setSpeed: (s: Speed) => void;
}

/**
 * RAF-driven playback hook. Advances tickIdx through [0, tickCount) at tickRate
 * ticks per second scaled by speed. Auto-pauses at the last tick.
 *
 * All playback state is mirrored in a mutable ref so the RAF callback never
 * closes over stale values.
 */
export function useRoundPlayback(tickCount: number, tickRate = 64): RoundPlayback {
  const [tickIdx, _setTickIdx] = useState(0);
  const [playing, _setPlaying] = useState(false);
  const [speed, setSpeed]      = useState<Speed>(1);

  // Mutable ref so the RAF callback always reads current values without
  // re-creating the animation loop on every state change.
  const r = useRef({
    tickIdx,
    playing,
    speed,
    tickCount,
    tickRate,
    accum: 0,
  });
  r.current.tickIdx   = tickIdx;
  r.current.playing   = playing;
  r.current.speed     = speed;
  r.current.tickCount = tickCount;
  r.current.tickRate  = tickRate;

  const setTickIdx = (n: number) => {
    r.current.tickIdx = n;
    _setTickIdx(n);
  };
  const setPlaying = (b: boolean) => {
    r.current.playing = b;
    _setPlaying(b);
  };

  const prevTs = useRef<number | null>(null);

  // Single long-lived RAF loop — reads everything from the ref.
  useEffect(() => {
    let raf: number;
    const step = (ts: number) => {
      const cur = r.current;
      if (cur.playing) {
        if (prevTs.current !== null) {
          const dt = (ts - prevTs.current) / 1000;
          cur.accum += dt * cur.tickRate * cur.speed;
          const steps = Math.floor(cur.accum);
          cur.accum -= steps;
          if (steps > 0) {
            const next = Math.min(cur.tickIdx + steps, cur.tickCount - 1);
            cur.tickIdx = next;
            _setTickIdx(next);
            if (next >= cur.tickCount - 1) {
              cur.playing = false;
              cur.accum   = 0;
              _setPlaying(false);
              prevTs.current = null;
            }
          }
        }
        if (cur.playing) prevTs.current = ts;
      } else {
        prevTs.current = null;
        cur.accum = 0;
      }
      raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, []); // Intentionally empty — loop is stable; state flows through ref.

  // Reset when a new round is loaded (tickCount changes).
  const prevTickCount = useRef(tickCount);
  useEffect(() => {
    if (prevTickCount.current !== tickCount) {
      prevTickCount.current = tickCount;
      r.current.accum   = 0;
      r.current.tickIdx = 0;
      r.current.playing = false;
      _setTickIdx(0);
      _setPlaying(false);
    }
  }, [tickCount]);

  return { tickIdx, setTickIdx, playing, setPlaying, speed, setSpeed };
}

/** Build weapon-at-tickIdx map from shot events (port of build_round_weapon_map). */
export function buildWeaponMap(
  shots: Array<{ tick: number; player_steamid: string; weapon: string }>,
  tickList: number[],
): Record<string, Array<string | null>> {
  const byPlayer: Record<string, Array<[number, string]>> = {};
  for (const s of shots) {
    const sid = s.player_steamid;
    (byPlayer[sid] ??= []).push([s.tick, s.weapon]);
  }

  const result: Record<string, Array<string | null>> = {};
  for (const [sid, events] of Object.entries(byPlayer)) {
    events.sort((a, b) => a[0] - b[0]);
    const arr: Array<string | null> = [];
    let ei = 0;
    for (const t of tickList) {
      while (ei + 1 < events.length && events[ei + 1][0] <= t) ei++;
      arr.push(events[ei][0] <= t ? events[ei][1] : null);
    }
    result[sid] = arr;
  }
  return result;
}
