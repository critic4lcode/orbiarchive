#!/usr/bin/env bash

# Installs an hourly cron job for sync_media.sh.
# Run once: bash install_cron.sh [--data-dir=/path/to/data]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SYNC_SCRIPT="${SCRIPT_DIR}/sync_media.sh"

chmod +x "$SYNC_SCRIPT"

EXTRA_ARGS=""
for arg in "$@"; do
  if [[ "$arg" == --data-dir=* ]]; then
    EXTRA_ARGS="$arg"
  fi
done

CRON_CMD="0 * * * * bash ${SYNC_SCRIPT} ${EXTRA_ARGS} >> ${SCRIPT_DIR}/sync_media.log 2>&1"

# Remove any existing entry for sync_media.sh, then add fresh
( crontab -l 2>/dev/null | grep -v "sync_media.sh"; echo "$CRON_CMD" ) | crontab -

echo "Cron job installed:"
crontab -l | grep sync_media.sh
