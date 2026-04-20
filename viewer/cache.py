from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl


_REQUIRED_COLS = frozenset({
    'round_num', 'tick', 'name', 'side', 'X', 'Y', 'health',
    'yaw', 'inventory', 'flash_duration',
})
_FILES = ('ticks', 'rounds', 'shots', 'smokes', 'infernos', 'flashes', 'grenade_paths')


@dataclass
class DemoData:
    ticks: pl.DataFrame
    rounds: pl.DataFrame
    shots: pl.DataFrame
    smokes: pl.DataFrame
    infernos: pl.DataFrame
    flashes: pl.DataFrame
    grenade_paths: pl.DataFrame


class DemoCache:
    """Parse a demo once, cache as Parquet, reload on subsequent runs.

    Cache files are named {demo_stem}_{field}.parquet so switching demos
    never accidentally reads another demo's stale cache.
    """

    def __init__(self, demo_path: Path, cache_dir: Path | None = None) -> None:
        self._demo_path = demo_path
        cache_dir = cache_dir or demo_path.parent
        stem = demo_path.stem
        self._paths: dict[str, Path] = {
            k: cache_dir / f"{stem}_{k}.parquet" for k in _FILES
        }

    def get(self) -> DemoData:
        if self.needs_parse():
            data = self._parse()
            self._save(data)
            return data
        return self._load()

    def needs_parse(self) -> bool:
        if not all(p.exists() for p in self._paths.values()):
            return True
        try:
            existing = set(pl.read_parquet(self._paths['ticks'], n_rows=0).columns)
            return not _REQUIRED_COLS.issubset(existing)
        except Exception:
            return True

    def _save(self, data: DemoData) -> None:
        for field in _FILES:
            getattr(data, field).write_parquet(self._paths[field])

    def _load(self) -> DemoData:
        print("Loading from cache …", flush=True)
        data = DemoData(**{k: pl.read_parquet(p) for k, p in self._paths.items()})
        print(
            f"  {data.ticks.height:,} tick rows, {data.rounds.height} rounds, "
            f"{data.smokes.height} smokes, {data.infernos.height} fires, "
            f"{data.flashes.height} flashes, {data.grenade_paths.height} grenade-path rows.",
            flush=True,
        )
        return data

    def _parse(self) -> DemoData:
        from awpy import Demo

        print(f"Parsing {self._demo_path.name} (first run, ~15 s) …", flush=True)
        dem = Demo(path=str(self._demo_path))
        dem.parse(player_props=[
            'balance', 'armor_value', 'has_defuser', 'flash_duration',
            'inventory', 'yaw', 'pitch', 'zoom_lvl',
        ])

        keep = [c for c in [
            'round_num', 'tick', 'steamid', 'name', 'side',
            'X', 'Y', 'Z', 'health', 'place',
            'yaw', 'pitch', 'inventory', 'flash_duration',
            'armor_value', 'has_defuser', 'balance', 'zoom_lvl',
        ] if c in dem.ticks.columns]
        ticks = dem.ticks.select(keep)
        rounds = dem.rounds

        shots_keep = [c for c in ['round_num', 'tick', 'player_steamid', 'weapon']
                      if c in dem.shots.columns]
        shots = dem.shots.select(shots_keep)

        smokes_keep = [c for c in ['round_num', 'start_tick', 'end_tick', 'X', 'Y', 'thrower_name']
                       if c in dem.smokes.columns]
        smokes = dem.smokes.select(smokes_keep)

        infernos_keep = [c for c in ['round_num', 'start_tick', 'end_tick', 'X', 'Y']
                         if c in dem.infernos.columns]
        infernos = dem.infernos.select(infernos_keep)

        gren_keep = [c for c in ['round_num', 'tick', 'entity_id', 'grenade_type', 'X', 'Y']
                     if c in dem.grenades.columns]
        grenade_paths = dem.grenades.filter(
            pl.col('grenade_type') != 'CDecoyProjectile'
        ).select(gren_keep)

        flash_raw = dem.grenades.filter(pl.col('grenade_type') == 'CFlashbangProjectile')
        if flash_raw.height > 0:
            flashes = (
                flash_raw
                .sort('tick')
                .group_by('entity_id')
                .last()
                .select([c for c in ['round_num', 'tick', 'X', 'Y'] if c in flash_raw.columns])
            )
        else:
            flashes = pl.DataFrame(schema={
                'round_num': pl.UInt32, 'tick': pl.Int32,
                'X': pl.Float32, 'Y': pl.Float32,
            })

        print(
            f"  Done — {ticks.height:,} tick rows, {rounds.height} rounds, "
            f"{shots.height} shots, {smokes.height} smokes, "
            f"{infernos.height} fires, {flashes.height} flashes, "
            f"{grenade_paths.height} grenade-path rows. Cached.",
            flush=True,
        )
        return DemoData(
            ticks=ticks, rounds=rounds, shots=shots, smokes=smokes,
            infernos=infernos, flashes=flashes, grenade_paths=grenade_paths,
        )
