# posztok-scraper

A scraper for [posztok.hu](https://posztok.hu) that archives posts and media from Facebook page aggregations.

## Requirements

- [Bun](https://bun.sh) (or Node.js)
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) — for video downloads (`brew install yt-dlp`)
- `rsync` — for media sync to storage

## Installation

```bash
bun install
```

## Usage

### Scraping

```bash
# Process all sources
bun run scrape.js 0

# Process first half of sources
bun run scrape.js 1

# Process second half of sources (run in parallel with group 1)
bun run scrape.js 2
```

### Options

| Argument | Default | Description |
|---|---|---|
| `--data-dir=<path>` | `./data` | Where to store scraped data and media |
| `--links=<file>` | `posztok_links.txt` | File containing `https://posztok.hu/s/<slug>` URLs to filter sources |
| `--concurrency=<n>` | `3` | Number of sources to scrape in parallel |

### Example

```bash
bun run scrape.js 1 --data-dir=/mnt/data --links=my_links.txt --concurrency=5
```

### Links file format

One URL per line:

```
https://posztok.hu/s/somepage
https://posztok.hu/s/anotherpage
```

## Output

Each source is saved to `<data-dir>/<slug>/<slug>.json` with the following structure:

```json
{
  "metadata": { "slug": "...", "name": "...", "orig_url": "...", "handler": "facebook" },
  "scraped_at": "2024-01-01T00:00:00.000Z",
  "posts": [
    {
      "id": "123",
      "date": "Ma 10:30",
      "date_iso": "2024-01-01T10:30:00.000Z",
      "content": "Post text...",
      "media": [
        { "type": "image", "url": "https://...", "local_path": "media/123_abc.jpg" }
      ],
      "links": [],
      "download_errors": []
    }
  ]
}
```

Media files are saved to `<data-dir>/<slug>/media/`.

## Logs

| File | Description |
|---|---|
| `scrape.log` | Timestamped scrape activity log |
| `missing_files.log` | All failed media downloads (append-only, safe for parallel processes) |
| `sync_media.log` | Media sync activity log |

## Media Sync

Moves media files from local storage to `/mnt/storagebox/data/<slug>/media/` hourly.

### Install cron job

```bash
bash install_cron.sh
# or with a custom data dir:
bash install_cron.sh --data-dir=/mnt/data
```

### Run manually

```bash
bash sync_media.sh
# or:
bash sync_media.sh --data-dir=/mnt/data
```

Files are moved (not copied) using `mv` directly to the remote mount, freeing up local disk space immediately. If a file already exists at the destination it is removed from local storage without overwriting.
