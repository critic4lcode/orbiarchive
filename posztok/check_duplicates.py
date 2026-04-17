#!/usr/bin/env python3
"""Remove duplicate posts where source_slug, original_url, and date_iso all match.

Usage:
  python check_duplicates.py           # apply cleanup
  python check_duplicates.py --dry-run # preview only, no files written
"""

import json
import glob
import sys
from collections import defaultdict
from pathlib import Path

DIR = "all_json"
DRY_RUN = "--dry-run" in sys.argv

total_removed = 0

# Pass 1: collect url -> set of source_slugs for cross-post report (date may differ)
url_slugs = defaultdict(set)
for fp in sorted(glob.glob(f"{DIR}/*.json")):
    try:
        data = json.load(open(fp, encoding="utf-8"))
    except Exception:
        continue
    for post in data.get("posts", []):
        url = post.get("original_url", "").strip()
        slug = post.get("source_slug", "").strip()
        if url and slug:
            url_slugs[url].add(slug)

cross_posts = {url: slugs for url, slugs in url_slugs.items() if len(slugs) > 1}
print(f"Cross-posts (same URL, different slugs): {len(cross_posts)}")
for url, slugs in sorted(cross_posts.items()):
    print(f"  {url}")
    for s in sorted(slugs):
        print(f"    {s}")
print()

# Pass 2: deduplicate within same source_slug
for fp in sorted(glob.glob(f"{DIR}/*.json")):
    try:
        data = json.load(open(fp, encoding="utf-8"))
    except Exception as e:
        print(f"⚠️  {fp}: {e}")
        continue

    posts = data.get("posts", [])
    seen = {}
    deduped = []

    for post in posts:
        key = (
            post.get("source_slug", "").strip(),
            post.get("original_url", "").strip(),
            post.get("date_iso", "").strip(),
        )
        if key[1] and key not in seen:
            seen[key] = True
            deduped.append(post)
        elif key[1]:
            continue  # duplicate — skip
        else:
            deduped.append(post)  # no url, keep always

    removed = len(posts) - len(deduped)
    if removed:
        total_removed += removed
        action = "[dry-run] would remove" if DRY_RUN else "removed"
        print(f"  {Path(fp).name}: {action} {removed} duplicate(s), {len(deduped)} remaining")
        if not DRY_RUN:
            data["posts"] = deduped
            with open(fp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

if total_removed:
    verb = "Would remove" if DRY_RUN else "Removed"
    print(f"\n✅ Done. {verb} {total_removed} duplicate post(s) total.")
else:
    print("✅ No duplicates found (source_slug + original_url + date_iso).")
