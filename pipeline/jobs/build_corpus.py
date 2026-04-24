"""Single corpus build job: for each ingested pro match, build artifact + event windows.

Replaces the three separate jobs from the refactor:
  build_pro_window_corpus, build_pro_round_artifacts, refresh_pro_corpus
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from backend import db
from backend.log import get_logger
from pipeline.features.extract_windows import extract_match_event_windows

log = get_logger("CORPUS")
from pipeline.features.featurize_windows import FEATURE_VERSION
from pipeline.steps.build_artifact import ARTIFACT_VERSION, build_match_artifact

JOB_NAME = "build_corpus"


def _artifact_is_current(path: str) -> bool:
    try:
        with Path(path).open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return False
    return payload.get("artifact_version") == ARTIFACT_VERSION


async def build_corpus(limit: int | None = None, force: bool = False) -> dict:
    run_id = await db.start_job_run(JOB_NAME)
    matches = await db.get_pro_matches(limit=limit)

    processed = 0
    skipped = 0
    errors: list[dict] = []
    status = "done"
    error_message = None

    try:
        for match in matches:
            parquet_dir = match.get("parquet_dir")
            if not parquet_dir:
                skipped += 1
                continue

            existing_artifact = match.get("artifact_path")
            artifact_current = (
                existing_artifact
                and Path(existing_artifact).exists()
                and _artifact_is_current(existing_artifact)
            )
            indexed_windows = await db.count_event_windows(
                match["match_id"],
                feature_version=FEATURE_VERSION,
            )

            if artifact_current and indexed_windows > 0 and not force:
                skipped += 1
                continue

            match_id = match["match_id"]
            try:
                if artifact_current and existing_artifact and not force:
                    artifact_path = existing_artifact
                else:
                    artifact_path = build_match_artifact(
                        source_type="pro",
                        source_match_id=match_id,
                        parquet_dir=Path(parquet_dir),
                        stem=match_id,
                        map_name=match["map_name"],
                    )
                    await db.set_match_artifact_path("pro", match_id, artifact_path)

                windows = extract_match_event_windows(
                    source_type="pro",
                    source_match_id=match_id,
                    parquet_dir=Path(parquet_dir),
                    stem=match_id,
                    map_name=match["map_name"],
                )
                for window in windows:
                    window_id = window.pop("window_id")
                    await db.upsert_event_window(window_id, **window)

                processed += 1
                log.info("%s -> %s, %d windows", match_id, ARTIFACT_VERSION, len(windows))
            except Exception as exc:
                errors.append({"match_id": match_id, "error": str(exc)})
                log.error("%s: %s", match_id, exc)

    except Exception as exc:
        status = "error"
        error_message = f"{type(exc).__name__}: {exc}"

    await db.finish_job_run(
        run_id,
        status=status,
        items_processed=processed,
        error_message=error_message,
        stats={
            "processed": processed,
            "skipped": skipped,
            "errors": len(errors),
            "artifact_version": ARTIFACT_VERSION,
        },
    )

    return {
        "processed": processed,
        "skipped": skipped,
        "artifact_version": ARTIFACT_VERSION,
        "errors": errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true", help="Rebuild even if artifact already exists")
    args = parser.parse_args()
    summary = asyncio.run(build_corpus(limit=args.limit, force=args.force))
    log.info("done: processed=%d skipped=%d errors=%d", summary["processed"], summary["skipped"], len(summary["errors"]))


if __name__ == "__main__":
    main()
