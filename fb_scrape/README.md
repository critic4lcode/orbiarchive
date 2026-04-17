# fb_scrape

Async Playwright-based scraper that archives posts and media from Facebook pages directly, storing everything in a local SQLite database (`archive.db`).

## Requirements

- Python 3.10+
- [Playwright](https://playwright.dev/python/) — `pip install playwright && playwright install chromium`
- `aiohttp`, `aiosqlite`, `brotli` — `pip install aiohttp aiosqlite brotli`

## Setup

### 1. Save a Facebook session

Run `fblogin.py` once to log in interactively and persist the browser session:

```bash
python fblogin.py
```

A browser window opens. Log in manually, then press ENTER in the terminal. The session is saved and reused by `fb_scraper.py`.

### 2. Run the scraper

```bash
python fb_scraper.py <facebook_page_url> [<facebook_page_url> ...]
```

Example:

```bash
python fb_scraper.py https://www.facebook.com/kormanyzat
```

## Output

| Path | Description |
|------|-------------|
| `archive.db` | SQLite database with all posts and media metadata |
| `archive/<page>/media/` | Downloaded media files |

## Configuration (top of script)

| Constant | Default | Description |
|----------|---------|-------------|
| `SCROLL_LIMIT` | `2000` | Max scroll iterations per page |
| `MEDIA_WORKERS` | `4` | Parallel media download workers |
| `SCRAPER_THREADS` | `2` | Parallel page scrapers |
| `GLOBAL_API_LIMIT` | `2` | Max concurrent Facebook API calls |
