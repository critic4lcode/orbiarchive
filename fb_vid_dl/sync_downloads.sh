#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="$SCRIPT_DIR/downloads"
DEST="/mnt/storagebox/downloads"
SYNC_LOG="$SCRIPT_DIR/synced.log"

mkdir -p "$DEST"

[[ -f "$SYNC_LOG" ]] || echo "timestamp,filename,filesize" > "$SYNC_LOG"

shopt -s nullglob
for f in "$SOURCE"/*.mp4; do
    # skip files modified within the last 300 seconds (may still be written)
    [[ $(( $(date +%s) - $(stat -c%Y "$f") )) -lt 300 ]] && continue

    filename="$(basename "$f")"
    filesize="$(stat -c%s "$f")"
    dest="$DEST/$filename"

    # skip if already at destination
    if [[ -f "$dest" ]]; then
        rm -f "$f"
        continue
    fi

    if cp "$f" "$dest"; then
        dest_size="$(stat -c%s "$dest" 2>/dev/null || echo 0)"
        if [[ "$dest_size" -eq "$filesize" ]]; then
            printf '%s,%s,%s\n' "$(date -Iseconds)" "$filename" "$filesize" >> "$SYNC_LOG"
            rm -f "$f"
        else
            echo "[$(date -Iseconds)] WARN: size mismatch for $filename (src=$filesize dst=$dest_size) — keeping source" >&2
        fi
    else
        echo "[$(date -Iseconds)] WARN: failed to copy $filename" >&2
    fi
done
