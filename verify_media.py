#!/usr/bin/env python3
"""
Verify that all local_path entries in scraped JSON files actually exist on disk.

Usage:
    python verify_media.py [--data-dir=/path/to/data] [--media-dir=/path/to/media]

    --data-dir   Directory containing the scraped JSON files (slug subfolders).
                 Defaults to ./data or $DATA_DIR.
    --media-dir  Separate directory that mirrors the slug/media folder structure
                 but contains only media files (no JSONs). When omitted, media
                 is expected next to the JSON files (default scrape layout).

Env:
    DATA_DIR  — alternative to --data-dir
"""

import csv
import json
import os
import sys
from pathlib import Path

from tqdm import tqdm


def resolve_data_dir() -> Path:
    for arg in sys.argv[1:]:
        if arg.startswith('--data-dir='):
            return Path(arg.split('=', 1)[1]).resolve()
    env = os.environ.get('DATA_DIR')
    if env:
        return Path(env).resolve()
    return Path(__file__).parent / 'data'


def resolve_media_dir() -> Path | None:
    for arg in sys.argv[1:]:
        if arg.startswith('--media-dir='):
            return Path(arg.split('=', 1)[1]).resolve()
    return None


def find_json_files(data_dir: Path):
    for root, dirs, files in os.walk(data_dir):
        # Skip media directories — they contain no JSON we care about
        dirs[:] = [d for d in dirs if d != 'media']
        for fname in files:
            if fname.endswith('.json') and not fname.endswith('.tmp'):
                yield Path(root) / fname


def verify_file(json_path: Path, data_dir: Path, media_dir: Path | None):
    missing = []
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f'[SKIP] Could not parse {json_path}: {e}')
        return missing

    posts = data.get('posts')
    if not isinstance(posts, list):
        return missing

    slug = data.get('metadata', {}).get('slug') or json_path.parent.name
    # If a separate media-dir is given, resolve media relative to <media_dir>/<slug>/
    # Otherwise fall back to the JSON's own parent directory.
    base_dir = (media_dir / slug) if media_dir else json_path.parent

    for post in posts:
        for media in post.get('media', []):
            local_path = media.get('local_path')
            if not local_path:
                continue
            full_path = base_dir / local_path
            if not full_path.exists():
                missing.append({
                    'slug': slug,
                    'post_id': post.get('id', ''),
                    'url': media.get('url', ''),
                    'local_path': str(full_path),
                })

    return missing


def main():
    data_dir = resolve_data_dir()

    if not data_dir.exists():
        print(f'ERROR: Data dir does not exist: {data_dir}')
        sys.exit(1)

    media_dir = resolve_media_dir()

    if media_dir and not media_dir.exists():
        print(f'ERROR: Media dir does not exist: {media_dir}')
        sys.exit(1)

    print(f'Scanning JSONs : {data_dir}')
    if media_dir:
        print(f'Media root     : {media_dir}')

    report_path = Path(os.getcwd()) / 'verify_media_report.csv'
    total_checked = 0
    total_missing = 0
    files_with_missing = 0
    all_missing = []

    json_files = sorted(find_json_files(data_dir))
    for json_path in tqdm(json_files, unit='file', desc='Verifying'):
        missing = verify_file(json_path, data_dir, media_dir)
        total_checked += 1
        if missing:
            files_with_missing += 1
            total_missing += len(missing)
            all_missing.extend(missing)

    if all_missing:
        with open(report_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['slug', 'post_id', 'url', 'local_path'])
            writer.writeheader()
            writer.writerows(all_missing)

    print(f'\n--- Summary ---')
    print(f'JSON files checked : {total_checked}')
    print(f'Files with missing : {files_with_missing}')
    print(f'Total missing media: {total_missing}')
    if all_missing:
        print(f'Report written to  : {report_path}')
        sys.exit(1)


if __name__ == '__main__':
    main()
