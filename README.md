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

### Cookies (optional)

If a `cookies.txt` file is present in the current working directory, it will automatically be passed to `yt-dlp` via `--cookies cookies.txt`. This is useful for downloading age-restricted or login-required videos (e.g. Facebook Reels).

Export cookies from your browser by running `yt-dlp --cookies-from-browser chrome --cookies cookies.txt` and place the file as `cookies.txt` in the project root.

## Retrying Failed Downloads

Some videos fail with *"This video is only available for registered users"*. After placing a valid `cookies.txt` in the project root, run:

```bash
bun run retry_errors.js
# or with a custom data dir / cookies file:
bun run retry_errors.js --data-dir=/mnt/data --cookies=/path/to/cookies.txt
```

The script will:
1. Recursively scan all `*.json` files under the data directory.
2. Find posts whose `download_errors` contain the registered-users message.
3. Retry `yt-dlp` with `--cookies` for each such video.
4. Remove the error entry from the JSON and update `local_path` on success.

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
