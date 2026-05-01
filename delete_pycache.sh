#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Deleting __pycache__ directories under: $ROOT_DIR"

find "$ROOT_DIR" -type d -name "__pycache__" -prune -print -exec rm -rf {} +

echo "Done."
