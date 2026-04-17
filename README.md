# orbiarchive

A collection of tools for archiving Hungarian public content — Facebook pages, government documents, and post aggregators.

## Tools

### [`scrape.js`](scrape.js) — posztok.hu scraper

Scrapes posts and media from [posztok.hu](https://posztok.hu) Facebook page aggregations.

```bash
bun install
bun run scrape.js 0                          # all sources
bun run scrape.js 1                          # first half (parallelisable with group 2)
bun run scrape.js 2                          # second half
bun run scrape.js 0 --data-dir=/mnt/data --concurrency=5
```

See full documentation in the root README below, or the options table:

| Argument | Default | Description |
|----------|---------|-------------|
| `--data-dir` | `./data` | Scraped data and media output |
| `--links` | `posztok_links.txt` | File of `https://posztok.hu/s/<slug>` URLs to filter |
| `--concurrency` | `3` | Parallel source scrapers |

### [`retry_errors.js`](retry_errors.js) — Retry failed video downloads

Re-attempts videos that failed with "registered users only" by passing a cookie file to `yt-dlp`.

```bash
bun run retry_errors.js
bun run retry_errors.js --data-dir=/mnt/data --cookies=/path/to/cookies.txt --concurrency=5
```

### [`verify_media.py`](verify_media.py) — Verify media files

Checks that every `local_path` entry in the scraped JSON files exists on disk. Outputs a CSV of missing files.

```bash
python verify_media.py [--data-dir=./data] [--media-dir=/mnt/media]
```

### [`sync_media.sh`](sync_media.sh) — Sync media to storage

Moves media files from local storage to `/mnt/storagebox/data/<slug>/media/` using `mv`. Install as an hourly cron job with:

```bash
bash install_cron.sh [--data-dir=/mnt/data]
```

---

## Subdirectories

### [`fb_scrape/`](fb_scrape/README.md) — Facebook page scraper

Async Playwright scraper that archives posts and media directly from Facebook pages into a local SQLite database. Includes a login helper (`fblogin.py`) to save browser sessions.

### [`fb_vid_dl/`](fb_vid_dl/README.md) — Facebook video downloader

Downloads Facebook videos from a CSV list using `yt-dlp`. Features cookie rotation, rate-limit backoff, parallel workers, and resume support. `yt-dlp` is auto-installed if not found.

```bash
cd fb_vid_dl
python fb_vid_dl.py videos.csv --workers 3
python fb_vid_dl.py videos.csv --retry-failed   # re-attempt previously failed URLs
```

### [`kormanyhu/`](kormanyhu/README.md) — Hungarian government document scraper

Fetches the full document-group index from `kormany.hu` and downloads the associated ZIP archives.

```bash
cd kormanyhu
python dokutar.py           # build index → all_document_groups.json
python download_docs.py     # download ZIPs → downloads/
```

### [`posztok/`](posztok/README.md) — Post-processing tools

Flattens scraped posztok.hu JSON archives into a relational SQLite/DuckDB database and reconciles file manifests.

```bash
cd posztok
python json2db.py --data-dir=../data --db=posts.db
python import_file_list.py filelist.txt
```

---

## Requirements summary

| Tool | Runtime |
|------|---------|
| `scrape.js`, `retry_errors.js` | [Bun](https://bun.sh) or Node.js |
| `fb_scrape/` | Python 3.10+, Playwright, aiohttp |
| `fb_vid_dl/` | Python 3.10+, curl (yt-dlp auto-installed) |
| `kormanyhu/` | Python 3.10+, requests |
| `posztok/` | Python 3.10+, optional: duckdb, tqdm |
| `verify_media.py`, `sync_media.sh` | Python 3.10+ / bash |
