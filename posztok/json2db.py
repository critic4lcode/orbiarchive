#!/usr/bin/env python3
"""
JSON Flattener → DuckDB / SQLite
Flattens the posztok.hu Facebook post JSON files into a relational database.

Schema detected:
  root
  ├─ metadata   (slug, name, orig_url, handler)
  ├─ scraped_at
  └─ posts[]
       ├─ id, source_slug, source_name, date, date_iso
       ├─ original_url, local_url, content
       ├─ continuation_url  (optional)
       ├─ media[]           → posts_media table
       ├─ links[]           → posts_links table
       └─ download_errors[] → posts_download_errors table
"""

import json
import os
import sys
import argparse
import glob
from pathlib import Path
from datetime import datetime

# ── optional rich progress bar ─────────────────────────────────────────
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


# ═══════════════════════════════════════════════════════════════════════
#  DATABASE BACKENDS
# ═══════════════════════════════════════════════════════════════════════

class DBBackend:
    """Abstract base for DuckDB / SQLite backends."""

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        raise NotImplementedError

    def execute(self, sql: str, params: tuple = ()) -> None:
        raise NotImplementedError

    def commit(self) -> None:
        pass

    def close(self) -> None:
        pass


# ── DuckDB ─────────────────────────────────────────────────────────────
class DuckDBBackend(DBBackend):
    def __init__(self, path: str):
        try:
            import duckdb
        except ImportError:
            sys.exit("❌  duckdb not installed.  Run: pip install duckdb")
        self.con = duckdb.connect(path)

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.con.execute(sql, list(params))

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        if rows:
            self.con.executemany(sql, rows)

    def commit(self) -> None:
        pass          # DuckDB auto-commits

    def close(self) -> None:
        self.con.close()


# ── SQLite ──────────────────────────────────────────────────────────────
class SQLiteBackend(DBBackend):
    def __init__(self, path: str):
        import sqlite3
        self.con = sqlite3.connect(path)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=NORMAL")
        self.cur = self.con.cursor()

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.cur.execute(sql, params)

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        if rows:
            self.cur.executemany(sql, rows)

    def commit(self) -> None:
        self.con.commit()

    def close(self) -> None:
        self.con.commit()
        self.con.close()


# ═══════════════════════════════════════════════════════════════════════
#  SCHEMA CREATION
# ═══════════════════════════════════════════════════════════════════════

DDL = """
-- ── sources ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sources (
    slug        TEXT PRIMARY KEY,
    name        TEXT,
    orig_url    TEXT,
    handler     TEXT,
    scraped_at  TEXT,
    source_file TEXT
);

-- ── posts ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS posts (
    id                TEXT PRIMARY KEY,
    source_slug       TEXT,
    source_name       TEXT,
    date              TEXT,
    date_iso          TEXT,
    original_url      TEXT,
    local_url         TEXT,
    content           TEXT,
    continuation_url  TEXT,
    has_media         INTEGER DEFAULT 0,
    has_links         INTEGER DEFAULT 0,
    has_errors        INTEGER DEFAULT 0,
    FOREIGN KEY (source_slug) REFERENCES sources(slug)
);

-- ── posts_media ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS posts_media (
    row_id      INTEGER PRIMARY KEY {autoincrement},
    post_id     TEXT,
    source_slug TEXT,
    media_type  TEXT,
    url         TEXT,
    local_path  TEXT,
    FOREIGN KEY (post_id) REFERENCES posts(id)
);

-- ── posts_links ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS posts_links (
    row_id      INTEGER PRIMARY KEY {autoincrement},
    post_id     TEXT,
    source_slug TEXT,
    text        TEXT,
    url         TEXT,
    FOREIGN KEY (post_id) REFERENCES posts(id)
);

-- ── posts_download_errors ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS posts_download_errors (
    row_id      INTEGER PRIMARY KEY {autoincrement},
    post_id     TEXT,
    source_slug TEXT,
    url         TEXT,
    error       TEXT,
    FOREIGN KEY (post_id) REFERENCES posts(id)
);
"""


def create_schema(db: DBBackend, backend: str) -> None:
    """Create all tables (idempotent)."""
    # DuckDB uses SEQUENCE for autoincrement; SQLite uses AUTOINCREMENT
    if backend == "duckdb":
        ai = "DEFAULT nextval('global_seq')"
        db.execute("CREATE SEQUENCE IF NOT EXISTS global_seq START 1")
    else:
        ai = "AUTOINCREMENT"

    for statement in DDL.format(autoincrement=ai).split(";"):
        stmt = statement.strip()
        if stmt:
            db.execute(stmt)
    db.commit()


# ═══════════════════════════════════════════════════════════════════════
#  INDEXES
# ═══════════════════════════════════════════════════════════════════════

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_posts_source_slug  ON posts(source_slug)",
    "CREATE INDEX IF NOT EXISTS idx_posts_date_iso     ON posts(date_iso)",
    "CREATE INDEX IF NOT EXISTS idx_media_post_id      ON posts_media(post_id)",
    "CREATE INDEX IF NOT EXISTS idx_media_source       ON posts_media(source_slug)",
    "CREATE INDEX IF NOT EXISTS idx_links_post_id      ON posts_links(post_id)",
    "CREATE INDEX IF NOT EXISTS idx_errors_post_id     ON posts_download_errors(post_id)",
]


def create_indexes(db: DBBackend) -> None:
    for idx in INDEXES:
        db.execute(idx)
    db.commit()


# ═══════════════════════════════════════════════════════════════════════
#  FLATTENER
# ═══════════════════════════════════════════════════════════════════════

INSERT_SOURCE = """
INSERT OR REPLACE INTO sources
    (slug, name, orig_url, handler, scraped_at, source_file)
VALUES (?,?,?,?,?,?)
"""

INSERT_POST = """
INSERT OR REPLACE INTO posts
    (id, source_slug, source_name, date, date_iso,
     original_url, local_url, content, continuation_url,
     has_media, has_links, has_errors)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
"""

INSERT_MEDIA = """
INSERT INTO posts_media
    (post_id, source_slug, media_type, url, local_path)
VALUES (?,?,?,?,?)
"""

INSERT_LINK = """
INSERT INTO posts_links
    (post_id, source_slug, text, url)
VALUES (?,?,?,?)
"""

INSERT_ERROR = """
INSERT INTO posts_download_errors
    (post_id, source_slug, url, error)
VALUES (?,?,?,?)
"""

# DuckDB doesn't support "INSERT OR REPLACE" — use a different clause
INSERT_SOURCE_DUCK = INSERT_SOURCE.replace("INSERT OR REPLACE", "INSERT OR REPLACE")
INSERT_POST_DUCK   = INSERT_POST.replace("INSERT OR REPLACE", "INSERT OR REPLACE")


def _safe(val) -> str | None:
    """Return None for empty/None, else str."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def flatten_file(filepath: str, db: DBBackend, backend: str,
                 skip_existing: bool = False,
                 batch_size: int = 500) -> dict:
    """
    Parse one JSON file and insert all rows into the database.
    Returns a stats dict.
    """
    stats = {
        "file": filepath,
        "success": False,
        "posts": 0,
        "media": 0,
        "links": 0,
        "errors_dl": 0,
        "error": None,
    }

    # ── load ────────────────────────────────────────────────────────────
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as exc:
        stats["error"] = str(exc)
        return stats

    meta       = data.get("metadata", {})
    scraped_at = data.get("scraped_at")
    posts      = data.get("posts", [])
    slug       = meta.get("slug", Path(filepath).stem)

    # ── source row ──────────────────────────────────────────────────────
    insert_src = INSERT_SOURCE_DUCK if backend == "duckdb" else INSERT_SOURCE
    try:
        db.execute(insert_src, (
            _safe(slug),
            _safe(meta.get("name")),
            _safe(meta.get("orig_url")),
            _safe(meta.get("handler")),
            _safe(scraped_at),
            Path(filepath).name,
        ))
    except Exception as exc:
        stats["error"] = f"source insert failed: {exc}"
        return stats

    # ── posts ────────────────────────────────────────────────────────────
    insert_post = INSERT_POST_DUCK if backend == "duckdb" else INSERT_POST

    post_rows   = []
    media_rows  = []
    link_rows   = []
    error_rows  = []

    for post in posts:
        post_id = _safe(post.get("id"))
        if not post_id:
            continue

        media    = post.get("media", []) or []
        links    = post.get("links", []) or []
        dl_errs  = post.get("download_errors", []) or []

        post_rows.append((
            post_id,
            _safe(post.get("source_slug", slug)),
            _safe(post.get("source_name")),
            _safe(post.get("date")),
            _safe(post.get("date_iso")),
            _safe(post.get("original_url")),
            _safe(post.get("local_url")),
            _safe(post.get("content")),
            _safe(post.get("continuation_url")),
            1 if media    else 0,
            1 if links    else 0,
            1 if dl_errs  else 0,
        ))

        for m in media:
            media_rows.append((
                post_id,
                _safe(post.get("source_slug", slug)),
                _safe(m.get("type")),
                _safe(m.get("url")),
                _safe(m.get("local_path")),
            ))

        for lk in links:
            link_rows.append((
                post_id,
                _safe(post.get("source_slug", slug)),
                _safe(lk.get("text")),
                _safe(lk.get("url")),
            ))

        for er in dl_errs:
            error_rows.append((
                post_id,
                _safe(post.get("source_slug", slug)),
                _safe(er.get("url")),
                _safe(er.get("error")),
            ))

        # ── flush batch ─────────────────────────────────────────────────
        if len(post_rows) >= batch_size:
            db.executemany(insert_post, post_rows)
            db.executemany(INSERT_MEDIA,  media_rows)
            db.executemany(INSERT_LINK,   link_rows)
            db.executemany(INSERT_ERROR,  error_rows)
            stats["posts"]    += len(post_rows)
            stats["media"]    += len(media_rows)
            stats["links"]    += len(link_rows)
            stats["errors_dl"] += len(error_rows)
            post_rows  = []
            media_rows = []
            link_rows  = []
            error_rows = []

    # ── flush remainder ──────────────────────────────────────────────────
    db.executemany(insert_post, post_rows)
    db.executemany(INSERT_MEDIA,  media_rows)
    db.executemany(INSERT_LINK,   link_rows)
    db.executemany(INSERT_ERROR,  error_rows)
    stats["posts"]    += len(post_rows)
    stats["media"]    += len(media_rows)
    stats["links"]    += len(link_rows)
    stats["errors_dl"] += len(error_rows)

    db.commit()
    stats["success"] = True
    return stats


# ═══════════════════════════════════════════════════════════════════════
#  SUMMARY VIEWS  (useful SQL shortcuts)
# ═══════════════════════════════════════════════════════════════════════

VIEWS = """
-- posts per source with counts
CREATE OR REPLACE VIEW v_source_stats AS
SELECT
    s.slug,
    s.name,
    s.handler,
    s.scraped_at,
    COUNT(p.id)                          AS total_posts,
    SUM(p.has_media)                     AS posts_with_media,
    SUM(p.has_links)                     AS posts_with_links,
    SUM(p.has_errors)                    AS posts_with_errors,
    MIN(p.date_iso)                      AS earliest_post,
    MAX(p.date_iso)                      AS latest_post
FROM sources s
LEFT JOIN posts p ON p.source_slug = s.slug
GROUP BY s.slug, s.name, s.handler, s.scraped_at;

-- media type breakdown
CREATE OR REPLACE VIEW v_media_types AS
SELECT
    source_slug,
    media_type,
    COUNT(*) AS cnt
FROM posts_media
GROUP BY source_slug, media_type;

-- posts that have download errors
CREATE OR REPLACE VIEW v_failed_downloads AS
SELECT
    p.id          AS post_id,
    p.source_slug,
    p.date_iso,
    e.url         AS failed_url,
    e.error
FROM posts_download_errors e
JOIN posts p ON p.id = e.post_id;
"""


def create_views(db: DBBackend) -> None:
    for stmt in VIEWS.split(";"):
        s = stmt.strip()
        if s:
            try:
                db.execute(s)
            except Exception:
                pass   # view may already exist / backend quirk
    db.commit()


# ═══════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Flatten JSON post files → DuckDB or SQLite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # DuckDB (default)
  python json_to_db.py json/*.json --db posts.duckdb

  # SQLite
  python json_to_db.py json/*.json --db posts.sqlite --backend sqlite

  # Directory of JSON files
  python json_to_db.py json/ --db posts.duckdb

  # With glob + verbose output
  python json_to_db.py json/ --db posts.duckdb --verbose
        """,
    )
    p.add_argument(
        "inputs",
        nargs="+",
        help="JSON files or directories to process",
    )
    p.add_argument(
        "--db", "-d",
        default="posts.duckdb",
        help="Output database path (default: posts.duckdb)",
    )
    p.add_argument(
        "--backend", "-b",
        choices=["duckdb", "sqlite"],
        default="duckdb",
        help="Database backend (default: duckdb)",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Row batch size for inserts (default: 500)",
    )
    p.add_argument(
        "--no-views",
        action="store_true",
        help="Skip creating summary views",
    )
    p.add_argument(
        "--no-indexes",
        action="store_true",
        help="Skip creating indexes (faster import, slower queries)",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print per-file stats",
    )
    return p


def collect_files(inputs: list[str]) -> list[str]:
    files = []
    for inp in inputs:
        path = Path(inp)
        if path.is_dir():
            found = sorted(path.rglob("*.json"))
            print(f"  📁 {inp}: {len(found)} JSON file(s) found")
            files.extend(str(f) for f in found)
        elif "*" in inp or "?" in inp:
            found = sorted(glob.glob(inp))
            files.extend(found)
        elif path.is_file():
            files.append(str(path))
        else:
            print(f"  ⚠  Skipping '{inp}' – not found")
    return files


# ═══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    t0 = datetime.now()
    print("\n🚀 JSON → Database Flattener")
    print("=" * 50)
    print(f"  Backend  : {args.backend}")
    print(f"  Database : {args.db}")
    print(f"  Batch    : {args.batch_size} rows")

    # ── collect files ────────────────────────────────────────────────────
    json_files = collect_files(args.inputs)
    if not json_files:
        print("❌ No JSON files found.")
        sys.exit(1)
    print(f"\n📄 {len(json_files)} file(s) to process\n")

    # ── open database ────────────────────────────────────────────────────
    if args.backend == "duckdb":
        db = DuckDBBackend(args.db)
    else:
        db = SQLiteBackend(args.db)

    # ── schema ───────────────────────────────────────────────────────────
    print("🏗  Creating schema…")
    create_schema(db, args.backend)

    # ── process files ────────────────────────────────────────────────────
    total = {"files": 0, "posts": 0, "media": 0, "links": 0,
             "errors_dl": 0, "failed_files": 0}

    iterator = tqdm(json_files, unit="file") if HAS_TQDM else json_files

    for fp in iterator:
        stats = flatten_file(fp, db, args.backend,
                             batch_size=args.batch_size)
        total["files"] += 1

        if stats["success"]:
            total["posts"]    += stats["posts"]
            total["media"]    += stats["media"]
            total["links"]    += stats["links"]
            total["errors_dl"] += stats["errors_dl"]
            if args.verbose:
                print(
                    f"  ✓ {Path(fp).name:<45}"
                    f" posts={stats['posts']:>6}"
                    f" media={stats['media']:>5}"
                    f" links={stats['links']:>5}"
                )
        else:
            total["failed_files"] += 1
            print(f"  ✗ {Path(fp).name}: {stats['error']}")

    # ── indexes & views ──────────────────────────────────────────────────
    if not args.no_indexes:
        print("\n🔍 Building indexes…")
        create_indexes(db)

    if not args.no_views:
        print("📊 Creating summary views…")
        create_views(db)

    db.close()

    # ── summary ──────────────────────────────────────────────────────────
    elapsed = (datetime.now() - t0).total_seconds()
    print("\n" + "=" * 50)
    print("✅ Done!")
    print(f"   Files processed : {total['files']}  "
          f"(failed: {total['failed_files']})")
    print(f"   Posts inserted  : {total['posts']:,}")
    print(f"   Media rows      : {total['media']:,}")
    print(f"   Link rows       : {total['links']:,}")
    print(f"   Error rows      : {total['errors_dl']:,}")
    print(f"   Time elapsed    : {elapsed:.1f}s")
    print(f"   Database        : {os.path.abspath(args.db)}")
    print()


if __name__ == "__main__":
    main()
