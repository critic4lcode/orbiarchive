#!/usr/bin/env python3
"""
Facebook Video Downloader — Hetzner floating-IP variant.
2 worker threads; each rotates through its own group of source IPs via
yt-dlp --source-address (all IPs must be bound on the host interface).
"""

import os
import csv
import sys
import signal
import logging
import subprocess
import argparse
import shutil
import queue
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional

# ── IP groups ──────────────────────────────────────────────────────────────────
# Add your primary IP to GROUP_1 to get a true 4-4 split.
GROUP_0: list[str] = [

]
GROUP_1: list[str] = [

]
THREAD_IP_GROUPS: list[list[str]] = [GROUP_0, GROUP_1]
NUM_WORKERS = 2

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [W%(thread_id)s] [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("downloader.log", encoding="utf-8"),
    ],
)

class _WorkerFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.thread_id = getattr(record, "worker_id", "-")
        return True

for h in logging.root.handlers:
    h.addFilter(_WorkerFilter())

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
FAILED_CSV     = "failed_downloads.csv"
DOWNLOADED_LOG = "downloaded.csv"
OUTPUT_DIR     = "downloads"

_shutdown    = False
_state_lock  = threading.Lock()
_downloaded: set[str] = set()


def _handle_sigint(sig, frame) -> None:
    global _shutdown
    if not _shutdown:
        log.info("Interrupt received — finishing current downloads then stopping…",
                 extra={"worker_id": "M"})
        _shutdown = True


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def extract_post_id(url: str) -> Optional[str]:
    import re
    for pattern in [
        r'/(?:reels?|videos?)/(\d+)',
        r'[?&]v=(\d+)',
        r'story_fbid=(\d+)',
        r'/posts/(\d+)',
    ]:
        m = re.search(pattern, url)
        if m:
            return m.group(1)
    candidates = re.findall(r'/(\d{8,})', url)
    return candidates[-1] if candidates else None


def build_output_path(page_name: str, post_id: str) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return os.path.join(OUTPUT_DIR, f"{page_name}_{post_id}.mp4")


def record_failure(page_name: str, url: str, reason: str) -> None:
    with _state_lock:
        file_exists = os.path.isfile(FAILED_CSV)
        with open(FAILED_CSV, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            if not file_exists:
                writer.writerow(["timestamp", "page_name", "url", "reason"])
            writer.writerow([datetime.now().isoformat(), page_name, url, reason])


def record_downloaded(filename: str) -> None:
    with _state_lock:
        size = os.path.getsize(os.path.join(OUTPUT_DIR, filename))
        file_exists = os.path.isfile(DOWNLOADED_LOG)
        with open(DOWNLOADED_LOG, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            if not file_exists:
                writer.writerow(["timestamp", "filename", "filesize"])
            writer.writerow([datetime.now().isoformat(), filename, size])


def load_downloaded() -> set[str]:
    known: set[str] = set()

    # Source 1: downloaded.csv (may have been written by either script)
    if os.path.isfile(DOWNLOADED_LOG):
        with open(DOWNLOADED_LOG, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if "filename" in (reader.fieldnames or []):
                known |= {row["filename"].strip() for row in reader if row.get("filename")}
        log.info("Loaded %d filename(s) from %s.", len(known), os.path.abspath(DOWNLOADED_LOG),
                 extra={"worker_id": "M"})

    # Source 2: actual .mp4 files on disk (ground truth — catches downloads logged elsewhere)
    disk: set[str] = set()
    if os.path.isdir(OUTPUT_DIR):
        disk = {f.name for f in Path(OUTPUT_DIR).glob("*.mp4")}
    new_on_disk = disk - known
    if new_on_disk:
        log.info("Found %d extra file(s) on disk not in CSV; adding to skip list.", len(new_on_disk),
                 extra={"worker_id": "M"})
        known |= new_on_disk

    return known


def _backfill_downloaded_log() -> None:
    pass  # replaced by merged load_downloaded above


def load_failed_urls() -> set[str]:
    if not os.path.isfile(FAILED_CSV):
        return set()
    with open(FAILED_CSV, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return {row["url"].strip() for row in reader if row.get("url")}


# ══════════════════════════════════════════════════════════════════════════════
# yt-dlp
# ══════════════════════════════════════════════════════════════════════════════

def ensure_ytdlp() -> None:
    if shutil.which("yt-dlp"):
        return
    install_dir  = os.path.expanduser("~/.local/bin")
    install_path = os.path.join(install_dir, "yt-dlp")
    log.info("yt-dlp not found — installing to %s …", install_path, extra={"worker_id": "M"})
    os.makedirs(install_dir, exist_ok=True)
    result = subprocess.run(
        ["curl", "-L",
         "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp",
         "-o", install_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        log.critical("Failed to download yt-dlp:\n%s", result.stderr, extra={"worker_id": "M"})
        sys.exit(1)
    os.chmod(install_path, 0o755)
    os.environ["PATH"] = install_dir + os.pathsep + os.environ.get("PATH", "")
    log.info("yt-dlp installed.", extra={"worker_id": "M"})


def _build_ytdlp_cmd(url: str, dest_path: str, source_ip: str) -> list[str]:
    return [
        "yt-dlp",
        "--source-address", source_ip,
        "-o", dest_path,
        url,
        "--quiet",
        "--no-warnings",
        "--no-playlist",
        "--merge-output-format", "mp4",
    ]


def run_ytdlp(url: str, dest_path: str, source_ip: str) -> tuple[bool, str]:
    cmd = _build_ytdlp_cmd(url, dest_path, source_ip)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        combined = result.stdout + result.stderr
        return result.returncode == 0, combined
    except subprocess.TimeoutExpired:
        return False, "TimeoutExpired"
    except FileNotFoundError:
        log.critical("yt-dlp not found.", extra={"worker_id": "?"})
        sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# IP rotator
# ══════════════════════════════════════════════════════════════════════════════

class IPRotator:
    """Round-robin over a list of source IPs. Not thread-safe — one per worker."""

    def __init__(self, ips: list[str]) -> None:
        if not ips:
            raise ValueError("IP list must not be empty")
        self._ips   = ips
        self._index = 0

    def current(self) -> str:
        return self._ips[self._index]

    def advance(self) -> None:
        self._index = (self._index + 1) % len(self._ips)

    def all_from_current(self) -> list[str]:
        """All IPs starting from current index (for fallback iteration)."""
        n = len(self._ips)
        return [self._ips[(self._index + i) % n] for i in range(n)]


# ══════════════════════════════════════════════════════════════════════════════
# Worker
# ══════════════════════════════════════════════════════════════════════════════

def _download_one(
    worker_id: int,
    rotator: IPRotator,
    idx: int,
    total: int,
    page_name: str,
    url: str,
) -> bool:
    extra = {"worker_id": worker_id}

    if not url:
        log.warning("[%d/%d] Empty URL for '%s'; skipping.", idx, total, page_name, extra=extra)
        return True

    log.info("─" * 50, extra=extra)
    log.info("[%d/%d] %s  →  %s", idx, total, page_name, url, extra=extra)

    post_id = extract_post_id(url)
    if not post_id:
        log.error("  Could not extract post ID; skipping.", extra=extra)
        record_failure(page_name, url, "Could not extract post ID")
        return False

    filename  = f"{page_name}_{post_id}.mp4"
    dest_path = build_output_path(page_name, post_id)

    with _state_lock:
        already = filename in _downloaded
    if already or os.path.isfile(dest_path):
        log.info("  Already downloaded; skipping.", extra=extra)
        with _state_lock:
            _downloaded.add(filename)
        return True

    # Try up to 2 IPs starting from the current one.
    for ip in rotator.all_from_current()[:2]:
        log.info("  Trying source-address %s", ip, extra=extra)
        ok, output = run_ytdlp(url, dest_path, ip)
        rotator.advance()
        if ok:
            log.info("  ✓ OK via %s", ip, extra=extra)
            record_downloaded(filename)
            with _state_lock:
                _downloaded.add(filename)
            return True
        log.warning("  ✗ Failed via %s: %s", ip, output.strip()[:200], extra=extra)
        if _shutdown or "Interrupted by user" in output:
            log.info("  Interrupted — stopping.", extra=extra)
            break

    record_failure(page_name, url, "All IPs failed")
    return False


def _worker(
    worker_id: int,
    work_queue: "queue.Queue[Optional[tuple[int, int, str, str]]]",
    rotator: IPRotator,
    counters: dict,
) -> None:
    extra = {"worker_id": worker_id}
    log.info("Worker started, IPs: %s", rotator._ips, extra=extra)
    while True:
        item = work_queue.get()
        if item is None:            # sentinel
            work_queue.task_done()
            break
        idx, total, page_name, url = item
        if _shutdown:
            work_queue.task_done()
            continue
        ok = _download_one(worker_id, rotator, idx, total, page_name, url)
        with _state_lock:
            if ok:
                counters["succeeded"] += 1
            else:
                counters["failed"] += 1
        work_queue.task_done()
    log.info("Worker done.", extra=extra)


# ══════════════════════════════════════════════════════════════════════════════
# CSV processing
# ══════════════════════════════════════════════════════════════════════════════

def process_csv(csv_path: str, retry_failed: bool = False) -> None:
    extra = {"worker_id": "M"}

    if not os.path.isfile(csv_path):
        log.critical("Input CSV not found: %s", csv_path, extra=extra)
        sys.exit(1)

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        required = {"page_name", "url"}
        if not required.issubset(set(reader.fieldnames or [])):
            log.critical("CSV must contain columns: %s (found: %s)",
                         required, reader.fieldnames, extra=extra)
            sys.exit(1)
        rows = list(reader)

    global _downloaded
    _downloaded = load_downloaded()
    log.info("Total skip-list size: %d file(s).", len(_downloaded), extra=extra)

    failed_urls: set[str] = set()
    if not retry_failed:
        failed_urls = load_failed_urls()
        if failed_urls:
            log.info("Skipping %d previously-failed URL(s).", len(failed_urls), extra=extra)

    rows = [r for r in rows if r.get("url", "").strip() not in failed_urls]
    total = len(rows)
    log.info("Starting download of %d video(s) across %d workers.", total, NUM_WORKERS, extra=extra)

    signal.signal(signal.SIGINT, _handle_sigint)

    work_queue: "queue.Queue[Optional[tuple]]" = queue.Queue()
    counters = {"succeeded": 0, "failed": 0}

    for idx, row in enumerate(rows, start=1):
        work_queue.put((idx, total, row["page_name"].strip(), row["url"].strip()))

    workers: list[threading.Thread] = []
    for wid in range(NUM_WORKERS):
        ips = THREAD_IP_GROUPS[wid] if wid < len(THREAD_IP_GROUPS) else THREAD_IP_GROUPS[-1]
        rotator = IPRotator(ips)
        work_queue.put(None)        # one sentinel per worker
        t = threading.Thread(
            target=_worker,
            args=(wid, work_queue, rotator, counters),
            daemon=True,
            name=f"worker-{wid}",
        )
        t.start()
        workers.append(t)

    for t in workers:
        t.join()

    log.info("═" * 60, extra=extra)
    log.info(
        "Done.  Succeeded: %d  |  Failed: %d  |  Total: %d",
        counters["succeeded"], counters["failed"], total,
        extra=extra,
    )
    if counters["failed"]:
        log.info("Failed downloads recorded in: %s", FAILED_CSV, extra=extra)


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Facebook videos via yt-dlp, rotating source IPs "
            "across 2 parallel worker threads (Hetzner floating-IP variant)."
        )
    )
    parser.add_argument("csv", nargs="?", default="videos.csv",
                        help="Input CSV file (default: videos.csv)")
    parser.add_argument("--output-dir", default=OUTPUT_DIR,
                        help=f"Destination folder (default: {OUTPUT_DIR})")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Retry URLs that previously failed all attempts")
    parser.add_argument("--debug", action="store_true",
                        help="Enable verbose debug logging")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    ensure_ytdlp()

    global OUTPUT_DIR
    OUTPUT_DIR = args.output_dir

    process_csv(args.csv, retry_failed=args.retry_failed)


if __name__ == "__main__":
    main()
