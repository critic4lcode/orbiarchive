#!/usr/bin/env python3
"""
Facebook Video Downloader using yt-dlp
Supports multiple cookie files with rate-limit detection and exponential backoff
"""

import os
import csv
import sys
import time
import glob
import random
import logging
import subprocess
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
from typing import Optional

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("downloader.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
RATE_LIMIT_SIGNAL   = "Cannot parse data string"   # text that signals rate-limiting
BACKOFF_MINUTES     = [5, 25, 60, 150]             # exponential-ish backoff ladder
COOKIES_DIR         = "cookies"                    # folder that holds *.txt cookie files
FAILED_CSV          = "failed_downloads.csv"       # output for downloads that never succeeded
_csv_lock           = threading.Lock()             # guards concurrent writes to FAILED_CSV
OUTPUT_DIR          = "downloads"                  # where finished videos are saved


# ══════════════════════════════════════════════════════════════════════════════
# Helper utilities
# ══════════════════════════════════════════════════════════════════════════════

def load_cookie_files(cookies_dir: str) -> list[str]:
    """Return a shuffled list of absolute paths to every .txt file in cookies_dir."""
    pattern = os.path.join(cookies_dir, "*.txt")
    files   = glob.glob(pattern)
    if not files:
        log.warning("No cookie files found in '%s'. Downloads may fail.", cookies_dir)
    else:
        log.info("Found %d cookie file(s): %s", len(files), [os.path.basename(f) for f in files])
    random.shuffle(files)
    return files


def extract_post_id(url: str) -> Optional[str]:
    """
    Extract the numeric post/reel ID from a Facebook URL.

    Handles:
      https://www.facebook.com/kormanyzat/videos/1264399547438987/
      https://www.facebook.com/reel/2001217130827376/
      https://www.facebook.com/video/1234567890/
      https://www.facebook.com/watch/?v=1234567890
      https://www.facebook.com/permalink.php?story_fbid=1234567890&id=987654321
    """
    import re

    # Pattern 1: /reel/<id>/  or  /video/<id>/  or  /videos/<id>/
    #            handles both singular and plural form of "video"
    m = re.search(r'/(?:reels?|videos?)/(\d+)', url)
    if m:
        return m.group(1)

    # Pattern 2: ?v=<id>  (watch links)
    m = re.search(r'[?&]v=(\d+)', url)
    if m:
        return m.group(1)

    # Pattern 3: story_fbid=<id>  (permalink links)
    m = re.search(r'story_fbid=(\d+)', url)
    if m:
        return m.group(1)

    # Pattern 4: /posts/<id>/
    m = re.search(r'/posts/(\d+)', url)
    if m:
        return m.group(1)

    # Fallback: last long numeric segment anywhere in the URL
    candidates = re.findall(r'/(\d{8,})', url)
    if candidates:
        return candidates[-1]

    return None



def build_output_path(page_name: str, post_id: str) -> str:
    """Construct the full output file path."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"{page_name}_{post_id}.mp4"
    return os.path.join(OUTPUT_DIR, filename)


def record_failure(page_name: str, url: str, reason: str) -> None:
    """Append a failed download entry to the failure CSV."""
    with _csv_lock:
        file_exists = os.path.isfile(FAILED_CSV)
        with open(FAILED_CSV, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            if not file_exists:
                writer.writerow(["timestamp", "page_name", "url", "reason"])
            writer.writerow([datetime.now().isoformat(), page_name, url, reason])
    log.warning("Recorded failure → %s", FAILED_CSV)


# ══════════════════════════════════════════════════════════════════════════════
# yt-dlp bootstrap
# ══════════════════════════════════════════════════════════════════════════════

def ensure_ytdlp() -> None:
    """Install yt-dlp to ~/.local/bin if it is not already on PATH."""
    import shutil
    if shutil.which("yt-dlp"):
        return

    install_dir = os.path.expanduser("~/.local/bin")
    install_path = os.path.join(install_dir, "yt-dlp")
    log.info("yt-dlp not found — installing to %s …", install_path)

    os.makedirs(install_dir, exist_ok=True)

    result = subprocess.run(
        ["curl", "-L", "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp",
         "-o", install_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        log.critical("Failed to download yt-dlp:\n%s", result.stderr)
        sys.exit(1)

    os.chmod(install_path, 0o755)

    # Make it available in the current process's PATH
    os.environ["PATH"] = install_dir + os.pathsep + os.environ.get("PATH", "")
    log.info("yt-dlp installed successfully.")


# ══════════════════════════════════════════════════════════════════════════════
# Core download logic
# ══════════════════════════════════════════════════════════════════════════════

def run_ytdlp(url: str, dest_path: str, cookie_file: Optional[str]) -> tuple[bool, str]:
    """
    Invoke yt-dlp as a subprocess.

    Returns:
        (success: bool, combined_output: str)
    """
    cmd = [
        "yt-dlp",
        "-o", dest_path,
        url,
        "--quiet",
        "--no-warnings",
        "--no-playlist",
        # Force mp4 container so the filename extension is always correct
        "--merge-output-format", "mp4",
    ]

    if cookie_file:
        cmd += ["--cookies", cookie_file]
        log.debug("Using cookie file: %s", os.path.basename(cookie_file))

    log.debug("Running: %s", " ".join(cmd))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,   # 5-minute hard timeout per attempt
        )
        combined = result.stdout + result.stderr
        success  = result.returncode == 0
        return success, combined

    except subprocess.TimeoutExpired:
        return False, "TimeoutExpired"
    except FileNotFoundError:
        log.critical("yt-dlp not found. Install it with:  pip install yt-dlp")
        sys.exit(1)


def is_rate_limited(output: str) -> bool:
    """Return True when the yt-dlp output contains the rate-limit signal."""
    return RATE_LIMIT_SIGNAL in output


def download_with_cookie_rotation(
    url: str,
    dest_path: str,
    cookie_files: list[str],
) -> tuple[bool, str]:
    """
    Try every available cookie file once (in order).

    Returns:
        (success: bool, last_output: str)
    """
    # Always attempt at least once, even without cookies
    candidates = cookie_files if cookie_files else [None]

    for cookie in candidates:
        cookie_label = os.path.basename(cookie) if cookie else "<no-cookie>"
        log.info("  Trying cookie: %s", cookie_label)

        success, output = run_ytdlp(url, dest_path, cookie)

        if success:
            log.info("  ✓ Download succeeded with cookie: %s", cookie_label)
            return True, output

        if is_rate_limited(output):
            log.warning(
                "  Rate-limited with cookie %s → rotating…", cookie_label
            )
            continue   # try next cookie

        # Non-rate-limit error – still try remaining cookies (maybe auth issue)
        log.warning(
            "  Non-rate-limit error with cookie %s:\n    %s",
            cookie_label,
            output.strip()[:300],
        )

    return False, output   # noqa: F821 – last output is always defined


def download_with_backoff(
    url: str,
    dest_path: str,
    cookie_files: list[str],
) -> bool:
    """
    Outer retry loop:  cookie rotation  →  exponential backoff.

    Returns True if the video was downloaded successfully.
    """
    # ── First attempt: try all cookies ───────────────────────────────────────
    success, last_output = download_with_cookie_rotation(url, dest_path, cookie_files)
    if success:
        return True

    if not is_rate_limited(last_output):
        # Persistent non-rate-limit error; no point backing off
        log.error("  Permanent error (not rate-limit); skipping backoff.\n  %s", last_output.strip()[:400])
        return False

    # ── Backoff ladder ────────────────────────────────────────────────────────
    for attempt, wait_minutes in enumerate(BACKOFF_MINUTES, start=1):
        wait_seconds = wait_minutes * 60
        log.warning(
            "  All cookies exhausted (attempt %d/%d). "
            "Backing off for %d minute(s)…",
            attempt,
            len(BACKOFF_MINUTES),
            wait_minutes,
        )
        time.sleep(wait_seconds)

        log.info("  Retrying after backoff (attempt %d)…", attempt)
        success, last_output = download_with_cookie_rotation(url, dest_path, cookie_files)

        if success:
            return True

        if not is_rate_limited(last_output):
            log.error("  Non-rate-limit error during backoff; stopping.\n  %s", last_output.strip()[:400])
            return False

        log.warning("  Still rate-limited after backoff attempt %d.", attempt)

    # ── Completely exhausted ──────────────────────────────────────────────────
    log.error("  Gave up on: %s", url)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# CSV processing
# ══════════════════════════════════════════════════════════════════════════════

def _download_one(idx: int, total: int, page_name: str, url: str, cookie_files: list[str]) -> bool:
    """Download a single video. Returns True on success (including skip)."""
    if not url:
        log.warning("[%d/%d] Empty URL for page '%s'; skipping.", idx, total, page_name)
        return True

    log.info("─" * 60)
    log.info("[%d/%d] %s  →  %s", idx, total, page_name, url)

    post_id = extract_post_id(url)
    if not post_id:
        log.error("  Could not extract post ID from URL; skipping.")
        record_failure(page_name, url, "Could not extract post ID")
        return False

    dest_path = build_output_path(page_name, post_id)
    log.info("  Output path: %s", dest_path)

    if os.path.isfile(dest_path) and os.path.getsize(dest_path) > 0:
        log.info("  Already exists; skipping.")
        return True

    ok = download_with_backoff(url, dest_path, list(cookie_files))
    if not ok:
        record_failure(page_name, url, "All retries exhausted")
    return ok


def load_failed_urls() -> set[str]:
    """Return the set of URLs that have previously failed all retries."""
    if not os.path.isfile(FAILED_CSV):
        return set()
    with open(FAILED_CSV, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return {row["url"].strip() for row in reader if row.get("url")}


def process_csv(csv_path: str, cookie_files: list[str], workers: int = 1, retry_failed: bool = False) -> None:
    """Read the input CSV and download every video entry."""
    if not os.path.isfile(csv_path):
        log.critical("Input CSV not found: %s", csv_path)
        sys.exit(1)

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)

        required = {"page_name", "url"}
        if not required.issubset(set(reader.fieldnames or [])):
            log.critical(
                "CSV must contain columns: %s  (found: %s)",
                required,
                reader.fieldnames,
            )
            sys.exit(1)

        rows = list(reader)

    failed_urls = set() if retry_failed else load_failed_urls()
    if failed_urls:
        log.info("Skipping %d previously-failed URL(s). Use --retry-failed to retry them.", len(failed_urls))

    rows = [r for r in rows if r.get("url", "").strip() not in failed_urls]
    total = len(rows)
    log.info("Starting download of %d video(s) with %d worker(s).", total, workers)

    succeeded = 0
    failed    = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _download_one,
                idx,
                total,
                row["page_name"].strip(),
                row["url"].strip(),
                cookie_files,
            ): idx
            for idx, row in enumerate(rows, start=1)
        }

        for future in as_completed(futures):
            if future.result():
                succeeded += 1
            else:
                failed += 1

    log.info("═" * 60)
    log.info("Done.  Succeeded: %d  |  Failed: %d  |  Total: %d", succeeded, failed, total)
    if failed:
        log.info("Failed downloads recorded in: %s", FAILED_CSV)


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Facebook videos via yt-dlp with cookie rotation and backoff."
    )
    parser.add_argument(
        "csv",
        nargs="?",
        default="videos.csv",
        help="Path to the input CSV file (default: videos.csv)",
    )
    parser.add_argument(
        "--cookies-dir",
        default=COOKIES_DIR,
        help=f"Folder containing .txt cookie files (default: {COOKIES_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        help=f"Destination folder for downloaded videos (default: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry URLs that previously exhausted all retries (default: skip them)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Number of parallel download workers (default: 3)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    ensure_ytdlp()

    # Allow CLI overrides of module-level constants
    global OUTPUT_DIR
    OUTPUT_DIR = args.output_dir

    cookie_files = load_cookie_files(args.cookies_dir)
    process_csv(args.csv, cookie_files, workers=args.workers, retry_failed=args.retry_failed)


if __name__ == "__main__":
    main()
