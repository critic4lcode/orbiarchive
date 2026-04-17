#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="$SCRIPT_DIR/downloads"
DEST="/mnt/storagebox/downloads"
SYNC_LOG="$SCRIPT_DIR/copied_to_storage.csv"

log()  { echo "[$(date -Iseconds)] $*"; }
warn() { echo "[$(date -Iseconds)] WARN: $*" >&2; }

mkdir -p "$DEST"

[[ -f "$SYNC_LOG" ]] || echo "timestamp,filename,filesize" > "$SYNC_LOG"

shopt -s nullglob
files=("$SOURCE"/*.mp4)
log "Found ${#files[@]} file(s) in $SOURCE"

for f in "${files[@]}"; do
    filename="$(basename "$f")"
    filesize="$(stat -c%s "$f")"
    age=$(( $(date +%s) - $(stat -c%Y "$f") ))
    dest="$DEST/$filename"

    if [[ $age -lt 300 ]]; then
        log "SKIP   $filename — too recent (${age}s old)"
        continue
    fi

    if [[ -f "$dest" ]]; then
        log "CLEAN  $filename — already at destination, removing local copy"
        rm -f "$f"
        continue
    fi

    log "COPY   $filename (${filesize} bytes) → $DEST"
    if cp "$f" "$dest"; then
        dest_size="$(stat -c%s "$dest" 2>/dev/null || echo 0)"
        if [[ "$dest_size" -eq "$filesize" ]]; then
            printf '%s,%s,%s\n' "$(date -Iseconds)" "$filename" "$filesize" >> "$SYNC_LOG"
            rm -f "$f"
            log "OK     $filename — copied and verified"
        else
            warn "$filename — size mismatch (src=$filesize dst=$dest_size), keeping source for retry"
        fi
    else
        warn "$filename — cp failed, keeping source for retry"
    fi
done

log "Done."
