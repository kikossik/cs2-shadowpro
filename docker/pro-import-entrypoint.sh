#!/bin/sh
set -eu

XVFB_PID=""

cleanup() {
    if [ -n "$XVFB_PID" ]; then
        kill "$XVFB_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

if [ "${PLAYWRIGHT_HEADLESS:-1}" = "0" ] && [ -z "${DISPLAY:-}" ]; then
    export DISPLAY="${XVFB_DISPLAY:-:99}"
    Xvfb "$DISPLAY" -screen 0 "${XVFB_SCREEN:-1920x1080x24}" -nolisten tcp >/tmp/xvfb.log 2>&1 &
    XVFB_PID="$!"
    sleep 1
fi

exec "$@"
