#!/usr/bin/env python3
"""
JSON Consolidator
Merges two directories of Facebook post JSON files into a consolidated output.
Deduplicates posts by 'id', preferring the newer scraped version.
"""

import csv
import json
import os
import glob
from datetime import datetime
from pathlib import Path

# ============================================================
#  CONFIGURATION — edit these three paths
# ============================================================
DIR_OLD    = "remaining_json"          # older backup directory
DIR_NEW    = "all_json"          # newer backup directory
DIR_OUTPUT = "consolidated"  # output directory
# ============================================================


def load_json(path: str) -> dict | None:
    """Load a JSON file, return None on error."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"  ⚠️  Could not load {path}: {e}")
        return None


def save_json(path: str, data: dict) -> None:
    """Save a dict as a pretty-printed JSON file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_scraped_at(value: str) -> datetime:
    """Parse ISO timestamp string; return epoch on failure."""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return datetime.fromtimestamp(0)


def merge_posts(posts_old: list, posts_new: list) -> tuple[list, dict]:
    """
    Merge two post lists, deduplicating by 'id'.
    The version from the file with the NEWER 'scraped_at' wins on conflict.
    Returns (merged_list, stats).
    """
    seen: dict[str, dict] = {}

    for post in posts_old:
        pid = post.get("id")
        if pid:
            seen[pid] = post

    added_new   = 0
    updated     = 0
    only_in_old = 0

    old_ids = set(seen.keys())

    for post in posts_new:
        pid = post.get("id")
        if not pid:
            continue
        if pid in seen:
            updated += 1        # newer file's copy replaces older
            old_errors = seen[pid].get("download_errors") or []
            new_errors = post.get("download_errors") or []
            seen[pid] = post    # newer wins for all fields
            seen[pid]["download_errors"] = old_errors + new_errors
        else:
            added_new += 1      # present only in new dir — skipped

    new_ids = set(p.get("id") for p in posts_new if p.get("id"))
    old_only_ids = old_ids - new_ids
    old_only_posts = [p for p in posts_old if p.get("id") in old_only_ids]

    stats = {
        "total_old":   len(posts_old),
        "total_new":   len(posts_new),
        "duplicates_updated": updated,
        "only_in_old": len(old_only_ids),
        "only_in_new": added_new,
        "merged_total": len(seen),
    }

    # Sort by date_iso descending (newest first), fall back to list order
    merged = sorted(
        seen.values(),
        key=lambda p: p.get("date_iso", ""),
        reverse=True,
    )
    return merged, stats, old_only_posts


def consolidate_file(slug: str, path_old: str | None, path_new: str | None) -> dict:
    """
    Consolidate one slug. Handles cases where a file exists in only one dir.
    Returns a result-summary dict.
    """
    data_old = load_json(path_old) if path_old else None
    data_new = load_json(path_new) if path_new else None

    # ── Determine which file is actually newer by scraped_at ──────────────
    scraped_old = parse_scraped_at(data_old.get("scraped_at", "")) if data_old else None
    scraped_new = parse_scraped_at(data_new.get("scraped_at", "")) if data_new else None

    if data_old and data_new:
        # Both exist — swap if "old" dir is actually the newer scrape
        if scraped_old and scraped_new and scraped_old > scraped_new:
            data_old, data_new = data_new, data_old
            scraped_old, scraped_new = scraped_new, scraped_old
            print(f"    ℹ️  [{slug}] DIR_OLD was actually newer — swapped for merge.")

    # ── Merge ─────────────────────────────────────────────────────────────
    if data_old and data_new:
        posts_old = data_old.get("posts", [])
        posts_new = data_new.get("posts", [])
        merged_posts, stats, old_only_posts = merge_posts(posts_old, posts_new)

        # Use the newer file's metadata & scraped_at
        output = {
            "metadata":   data_new.get("metadata", data_old.get("metadata", {})),
            "scraped_at": data_new.get("scraped_at", data_old.get("scraped_at")),
            "consolidated_at": datetime.utcnow().isoformat() + "Z",
            "sources": {
                "old_scraped_at": scraped_old.isoformat() if scraped_old else None,
                "new_scraped_at": scraped_new.isoformat() if scraped_new else None,
            },
            "posts": merged_posts,
        }
        status = "merged"

    elif data_new:
        print(f"       skipped — only in NEW dir")
        return {"slug": slug, "status": "new_only", "stats": {}, "old_only_posts": []}

    elif data_old:
        print(f"       skipped — only in OLD dir")
        return {"slug": slug, "status": "old_only", "stats": {}, "old_only_posts": []}

    else:
        print(f"  ✖  [{slug}] Both files failed to load — skipping.")
        return {"slug": slug, "status": "error"}

    # ── Save ──────────────────────────────────────────────────────────────
    out_path = os.path.join(DIR_OUTPUT, f"{slug}.json")
    save_json(out_path, output)

    return {
        "slug": slug, "status": status, "stats": stats, "out_path": out_path,
        "old_only_posts": old_only_posts if status == "merged" else [],
    }


def main():
    print("=" * 60)
    print("  JSON Consolidator")
    print("=" * 60)
    print(f"  OLD dir : {DIR_OLD}")
    print(f"  NEW dir : {DIR_NEW}")
    print(f"  OUTPUT  : {DIR_OUTPUT}\n")

    # ── Collect all slugs from both directories ───────────────────────────
    def slugs_in(directory: str) -> dict[str, str]:
        """Return {slug: full_path} for every .json in directory."""
        result = {}
        if not os.path.isdir(directory):
            print(f"  ⚠️  Directory not found: {directory}")
            return result
        for fp in glob.glob(os.path.join(directory, "*.json")):
            slug = Path(fp).stem
            result[slug] = fp
        return result

    map_old = slugs_in(DIR_OLD)
    map_new = slugs_in(DIR_NEW)
    all_slugs = sorted(set(map_old) | set(map_new))

    print(f"  Found {len(map_old)} file(s) in OLD dir")
    print(f"  Found {len(map_new)} file(s) in NEW dir")
    print(f"  Total unique slugs : {len(all_slugs)}\n")

    # ── Process each slug ─────────────────────────────────────────────────
    results = []
    for slug in all_slugs:
        print(f"  → {slug}")
        result = consolidate_file(
            slug,
            path_old=map_old.get(slug),
            path_new=map_new.get(slug),
        )
        results.append(result)

        if result["status"] != "error":
            s = result.get("stats", {})
            if result["status"] == "merged":
                print(f"       old={s.get('total_old',0)}  "
                      f"new={s.get('total_new',0)}  "
                      f"updated={s.get('duplicates_updated',0)}  "
                      f"only_old={s.get('only_in_old',0)}  "
                      f"only_new={s.get('only_in_new',0)}  "
                      f"➜  merged={s.get('merged_total',0)}")
            else:
                print(f"       {s.get('note','')}, posts={s.get('merged_total',0)}")

    # ── Summary ───────────────────────────────────────────────────────────
    merged_count = sum(1 for r in results if r["status"] == "merged")
    new_only     = sum(1 for r in results if r["status"] == "new_only")
    old_only     = sum(1 for r in results if r["status"] == "old_only")
    errors       = sum(1 for r in results if r["status"] == "error")
    total_posts  = sum(
        r.get("stats", {}).get("merged_total", 0)
        for r in results if r["status"] != "error"
    )

    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)
    print(f"  Merged (both dirs)  : {merged_count}")
    print(f"  New dir only        : {new_only}")
    print(f"  Old dir only        : {old_only}")
    print(f"  Errors              : {errors}")
    print(f"  Total posts saved   : {total_posts:,}")
    print(f"  Output directory    : {DIR_OUTPUT}")
    # ── CSV report: only_in_old posts (20 sample per slug: 10 oldest + 10 newest) ──
    csv_path = os.path.join(DIR_OUTPUT, "consolidation_report.csv")
    fieldnames = [
        "file_slug",
        "id", "source_slug", "source_name",
        "date_iso", "date", "original_url",
        "content_preview", "media_count", "links_count",
    ]

    def make_row(slug, post):
        content = post.get("content") or ""
        return {
            "file_slug":       slug,
            "id":              post.get("id", ""),
            "source_slug":     post.get("source_slug", ""),
            "source_name":     post.get("source_name", ""),
            "date_iso":        post.get("date_iso", ""),
            "date":            post.get("date", ""),
            "original_url":    post.get("original_url", ""),
            "content_preview": content[:200].replace("\n", " "),
            "media_count":     len(post.get("media", []) or []),
            "links_count":     len(post.get("links", []) or []),
        }

    csv_rows = []
    for r in results:
        if r["status"] != "merged":
            continue
        posts = [p for p in r.get("old_only_posts", []) if p.get("date_iso")]
        posts_sorted = sorted(posts, key=lambda p: p["date_iso"])
        oldest = posts_sorted[:10]
        newest = posts_sorted[-10:]
        seen_ids: set = set()
        for p in oldest + newest:
            pid = p.get("id")
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            csv_rows.append(make_row(r["slug"], p))

    os.makedirs(DIR_OUTPUT, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)
    print(f"  CSV report saved    : {csv_path} ({len(csv_rows)} rows)")

    print("=" * 60)
    print("  ✅ Done!")


if __name__ == "__main__":
    main()
