# posztok

Post-processing tools for the scraped posztok.hu data: import into a relational database and reconcile file lists.

## Requirements

- Python 3.10+
- `duckdb` or `sqlite3` (stdlib) — `pip install duckdb` (optional, for DuckDB support)
- `tqdm` — `pip install tqdm` (optional, for progress bars)

## Scripts

### `json2db.py` — Flatten JSON archives into a database

Reads all `*.json` files from the scraped data directory and inserts them into a SQLite or DuckDB database with a normalized schema.

```bash
python json2db.py [options]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | `./data` | Directory containing slug subfolders with JSON files |
| `--db` | `posts.db` | Output database file |
| `--engine` | `sqlite` | Database engine: `sqlite` or `duckdb` |

**Schema:**

| Table | Description |
|-------|-------------|
| `posts` | One row per post (id, slug, date, content, urls) |
| `posts_media` | Media attachments per post |
| `posts_links` | Links extracted from posts |
| `posts_download_errors` | Failed media downloads |

### `import_file_list.py` — Import a file manifest into SQLite

Reads a text file listing scraped files (one path per line, format: `slug/path`) and inserts them into a `files` table for reconciliation.

```bash
python import_file_list.py <filelist.txt> [--db posts.db]
```

## Output

| File | Description |
|------|-------------|
| `posts.db` | SQLite database with all post data |
| `missing.csv` | File paths referenced in JSON but missing from disk |