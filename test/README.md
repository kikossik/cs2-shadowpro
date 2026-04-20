# Local Mapping Test Harness

This folder is a standalone pygame sandbox for testing the new user-to-pro event mapping without the API or database.

It does three things:

1. Picks your latest local user match from `parquet_user/` by file mtime.
2. Loads the `v2` pro feature corpus directly from `parquet_pro/*/window_features/*_v2.json`.
3. Opens a side-by-side pygame viewer:
   - left = your local user round
   - right = the best mapped pro round for the current user tick

## Requirements

- `pygame`
- `polars`
- existing user parquet files in `parquet_user/`
- existing `v2` pro feature blobs in `parquet_pro/*/window_features/`

If you have not built the pro corpus yet, do that first:

```bash
docker compose exec web python -m pipeline.jobs.build_pro_window_corpus
```

## Run

From the repo root:

```bash
python test/run_last_game_mapping.py
```

Or:

```bash
./test/run_last_game.sh
```

Optional:

```bash
python test/run_last_game_mapping.py --round 12
python test/run_last_game_mapping.py --demo-id user_76561198857367828_xxx.dem
```

## Controls

- `Space`: play / pause
- `Left` / `Right`: scrub by 1 second
- `Shift+Left` / `Shift+Right`: scrub by 5 seconds
- `[` / `]`: previous / next round
- `Home`: jump to round start
- `M` or `Enter`: remap the current user tick to the best pro situation
- `L`: toggle lower level on maps that support it
- `Q` / `Esc`: quit

## Notes

- This harness does not talk to Postgres.
- It uses the same `v2` situation features and scoring ideas as the main mapping rewrite, but loads them directly from disk for faster iteration.
- If a round is still inside the `20s after freeze_end` exclusion zone, no mapping will be shown until you scrub later or move to another round.
