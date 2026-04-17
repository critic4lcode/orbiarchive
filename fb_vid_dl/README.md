# fb_vid_dl

Downloads Facebook videos from a CSV list using `yt-dlp`, with cookie rotation, rate-limit backoff, parallel workers, and resume support.

## Requirements

- Python 3.10+
- `curl` (for auto-installing `yt-dlp`)
- `yt-dlp` — installed automatically to `~/.local/bin/` if not found on PATH

## Setup

Place one or more Netscape-format cookie files (exported from your browser) into the `cookies/` folder:

```
cookies/
  account1.txt
  account2.txt
```

Prepare an input CSV with at least these two columns:

```csv
page_name,url
kormanyzat,https://www.facebook.com/kormanyzat/videos/1264399547438987/
```

## Usage

```bash
python fb_vid_dl.py [videos.csv] [options]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `csv` (positional) | `videos.csv` | Input CSV file |
| `--cookies-dir` | `cookies/` | Folder containing cookie `.txt` files |
| `--output-dir` | `downloads/` | Destination folder for downloaded videos |
| `--workers` | `3` | Number of parallel download workers |
| `--retry-failed` | off | Retry URLs that previously exhausted all retries |
| `--debug` | off | Verbose logging |

## Behaviour

**Resume support** — if you stop the script, already-downloaded files (non-zero size in `downloads/`) are skipped on restart.

**Cookie rotation** — each download cycles through all available cookie files. Useful when one account gets rate-limited.

**Rate-limit backoff** — if all cookies are exhausted, the script backs off and retries up to 4 times with increasing waits (5 → 25 → 60 → 150 minutes).

**Failure tracking** — URLs that exhaust all retries are written to `failed_downloads.csv` and skipped on subsequent runs by default. Use `--retry-failed` to attempt them again.

## Output files

| File | Description |
|------|-------------|
| `downloads/<page_name>_<post_id>.mp4` | Downloaded videos |
| `failed_downloads.csv` | URLs that failed all retries |
| `downloader.log` | Full run log |
