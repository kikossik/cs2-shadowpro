# CS2 ShadowPro Architecture

This repo runs as a small Docker stack: a database, an API, a background worker,
a frontend, a Steam share-code resolver, and an optional pro-demo import tool.

## Docker Apps

### `db`
PostgreSQL with pgvector. Stores users, demo jobs, parsed games, pro match data,
and round-analysis cache data. Data persists in the `db_data` Docker volume.

### `web`
FastAPI backend on port `8000`. Handles API requests, user setup, demo uploads,
match lists, replay payloads, and round-analysis endpoints.

### `worker`
Background processor. Polls `demo_jobs`, parses uploaded user demos, writes
Parquet files, updates DB rows, precomputes round analysis, and periodically
syncs Steam share-code users.

### `resolver`
Node service used by backend sync. Talks to Steam Game Coordinator APIs and
turns CS2 share codes into downloadable demo URLs.

### `frontend`
Built React/Vite app served by nginx on port `4173`. This is the browser UI for
login/setup, matches, replay, and round comparison views.

### `pro-import`
Manual tool service under the `tools` profile. Scrapes HLTV, downloads pro demo
archives, extracts map demos, parses them, and inserts them into the pro corpus.

## Backend

Main folder: `backend/`

- `main.py`: FastAPI routes.
- `db.py`: database connection and query helpers.
- `config.py`: environment variables and managed paths.
- `processing.py`: parses user demos and writes user Parquet output.
- `worker.py`: background job loop.
- `sync.py`: Steam share-code sync and download flow.
- `round_mapper.py`: user-to-pro round matching logic.
- `round_analysis_service.py`: caches and serves round analysis results.

## Pipeline

Main folder: `pipeline/`

The pipeline is mainly for pro/HLTV demo ingestion.

- `jobs/refresh_pro_corpus.py`: scrape recent HLTV results and ingest demos.
- `jobs/seed_corpus.py`: fill the pro corpus across maps.
- `jobs/ingest_local_demos.py`: ingest local `.dem` files.
- `steps/scrape.py`: collect HLTV match and demo URLs.
- `steps/download.py`: download HLTV demo archives.
- `steps/decompress.py`: extract `.dem` files from archives.
- `steps/ingest.py`: parse pro demos and write DB/Parquet output.

## Services

Main folder: `services/`

Currently this contains `sharecode-resolver`, a small Node service that logs into
Steam and resolves CS2 share codes to demo download URLs.

## Web

Main folder: `web/`

React frontend. Important areas:

- `src/LoginPage.tsx` and `src/SetupPage.tsx`: user entry/setup.
- `src/matches/`: match list layouts and controls.
- `src/Viewer.tsx`: replay and round analysis view.
- `src/replay/`: radar canvas and playback helpers.
- `public/maps/`: radar images used by the viewer.

## Data Flow

User demo import:

`frontend -> web /api/import -> demo_jobs -> worker -> processing.py -> parquet_user + games`

Steam sync:

`worker/web -> Steam API -> resolver -> demo download -> processing.py -> parquet_user + games`

Pro corpus import:

`pro-import -> HLTV -> demos_pro -> parquet_pro + games`

## Storage

- `demos_user/`: uploaded or synced user `.dem` files.
- `demos_pro/`: downloaded HLTV demo archives and extracted pro demos.
- `parquet_user/`: parsed user demo artifacts.
- `parquet_pro/`: parsed pro demo artifacts.
- `db_data`: PostgreSQL Docker volume.
