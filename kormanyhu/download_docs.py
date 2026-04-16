import json
import os
import time
import requests
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/zip, application/octet-stream, */*",
    "Accept-Language": "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7",
}

print_lock = Lock()


def log(msg: str):
    with print_lock:
        print(msg, flush=True)


def download_zip(
    session: requests.Session,
    slug: str,
    url: str,
    output_dir: str,
    retries: int = 3,
    delay: float = 2.0,
) -> bool:
    """Download a zip from `url` and save it as <output_dir>/<slug>.zip.
    Returns True on success, False on failure."""
    dest_path = os.path.join(output_dir, f"{slug}.zip")

    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        log(f"  [SKIP] {slug} (already exists)")
        return True

    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, headers=HEADERS, timeout=60, stream=True)
            response.raise_for_status()

            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

            size = os.path.getsize(dest_path)
            log(f"  [OK] {slug} ({size:,} bytes)")
            return True

        except requests.exceptions.HTTPError as e:
            log(f"  [HTTP Error] {slug}, attempt {attempt}/{retries}: {e}")
        except requests.exceptions.ConnectionError as e:
            log(f"  [Connection Error] {slug}, attempt {attempt}/{retries}: {e}")
        except requests.exceptions.Timeout:
            log(f"  [Timeout] {slug}, attempt {attempt}/{retries}")
        except Exception as e:
            log(f"  [Error] {slug}, attempt {attempt}/{retries}: {e}")

        # Remove partial file before retry
        if os.path.exists(dest_path):
            os.remove(dest_path)

        if attempt < retries:
            time.sleep(delay * attempt)

    log(f"  [FAILED] {slug} after {retries} attempts.")
    return False


def download_all(
    input_file: str = "all_document_groups.json",
    output_dir: str = "downloads",
    delay_between_requests: float = 0.3,
    workers: int = 4,
    retries: int = 3,
):
    os.makedirs(output_dir, exist_ok=True)

    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    items = data.get("data", [])
    total = len(items)
    log(f"Loaded {total} document groups from {input_file}")
    log(f"Output directory: {output_dir}")
    log(f"Workers: {workers}, delay: {delay_between_requests}s, retries: {retries}")
    log("-" * 60)

    failed = []
    completed = 0

    def task(item):
        slug = item.get("slug", "unknown")
        url = item.get("downloadUrl")
        if not url:
            log(f"  [SKIP] {slug} — no downloadUrl")
            return slug, True
        with requests.Session() as session:
            success = download_zip(session, slug, url, output_dir, retries=retries)
            time.sleep(delay_between_requests)
            return slug, success

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(task, item): item for item in items}
        for future in as_completed(futures):
            slug, success = future.result()
            completed += 1
            if not success:
                failed.append(slug)
            if completed % 50 == 0 or completed == total:
                log(f"  Progress: {completed}/{total} processed, {len(failed)} failed so far")

    log("-" * 60)
    log(f"Done. {total - len(failed)}/{total} succeeded.")
    if failed:
        log(f"Failed slugs ({len(failed)}):")
        for slug in failed:
            log(f"  - {slug}")
        failed_log = os.path.join(output_dir, "failed_downloads.txt")
        with open(failed_log, "w", encoding="utf-8") as f:
            f.write("\n".join(failed))
        log(f"Failed list saved to {failed_log}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download document group ZIPs from kormany.hu")
    parser.add_argument(
        "--input", default="all_document_groups.json",
        help="Path to the JSON file produced by dokutar.py (default: all_document_groups.json)"
    )
    parser.add_argument(
        "--output", default="downloads",
        help="Directory to save downloaded ZIP files (default: downloads)"
    )
    parser.add_argument(
        "--delay", type=float, default=0.3,
        help="Seconds to wait between requests per worker (default: 0.3)"
    )
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Number of parallel download workers (default: 4)"
    )
    parser.add_argument(
        "--retries", type=int, default=3,
        help="Number of retry attempts per download (default: 3)"
    )
    args = parser.parse_args()

    download_all(
        input_file=args.input,
        output_dir=args.output,
        delay_between_requests=args.delay,
        workers=args.workers,
        retries=args.retries,
    )
