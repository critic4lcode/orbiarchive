#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="$SCRIPT_DIR/downloads"
DEST="/mnt/storagebox/downloads"
SYNC_LOG="$SCRIPT_DIR/synced.log"

mkdir -p "$DEST"

shopt -s nullglob
for f in "$SOURCE"/*.mp4; do
    filename="$(basename "$f")"
    filesize="$(stat -c%s "$f")"

    if cp --no-clobber "$f" "$DEST/$filename"; then
        printf '%s,%s,%s\n' "$(date -Iseconds)" "$filename" "$filesize" >> "$SYNC_LOG"
        rm -f "$f"
    else
        echo "[$(date -Iseconds)] WARN: failed to copy $filename" >&2
    fi
done
