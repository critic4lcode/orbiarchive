#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/sync_media.log"
DEST_BASE="/mnt/storagebox/data"

# Allow --data-dir= override, same convention as scrape.js
DATA_DIR="${DATA_DIR:-${SCRIPT_DIR}/data}"
for arg in "$@"; do
  if [[ "$arg" == --data-dir=* ]]; then
    DATA_DIR="${arg#--data-dir=}"
  fi
done

ts() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log() {
  echo "[$(ts)] $*" | tee -a "$LOG_FILE"
}

log "=== sync_media started. DATA_DIR=${DATA_DIR} DEST_BASE=${DEST_BASE} ==="

if [[ ! -d "$DATA_DIR" ]]; then
  log "ERROR: DATA_DIR does not exist: ${DATA_DIR}"
  exit 1
fi

if [[ ! -d "$DEST_BASE" ]]; then
  log "ERROR: Destination base does not exist or is not mounted: ${DEST_BASE}"
  exit 1
fi

synced=0
skipped=0
errors=0

for media_dir in "${DATA_DIR}"/*/media; do
  [[ -d "$media_dir" ]] || continue

  page_dir="$(dirname "$media_dir")"
  page_name="$(basename "$page_dir")"
  dest_dir="${DEST_BASE}/${page_name}/media"

  mkdir -p "$dest_dir"

  log "  Syncing ${page_name}/media -> ${dest_dir}"

  file_count=0
  error_count=0
  while IFS= read -r -d '' file; do
    dest_file="${dest_dir}/$(basename "$file")"
    if [[ -e "$dest_file" ]]; then
      rm -f "$file"
    elif mv "$file" "$dest_file" 2>> "$LOG_FILE"; then
      ((file_count++)) || true
    else
      log "  ERROR: failed to move $(basename "$file")"
      ((error_count++)) || true
    fi
  done < <(find "$media_dir" -maxdepth 1 -type f -print0)

  if [[ $error_count -eq 0 ]]; then
    log "  OK: ${page_name}/media — moved ${file_count} files to ${dest_dir}"
    ((synced++)) || true
  else
    log "  WARN: ${page_name}/media — moved ${file_count} files, ${error_count} errors"
    ((errors++)) || true
  fi
done

log "=== sync_media done. synced=${synced} errors=${errors} ==="
