import sqlite3
import os


def parse_line(line):
    """
    Parse a line like:
    adamkosamep/adamkosamep.json -> source_slug: adamkosamep, local_path: /adamkosamep.json
    adamkosamep/media/1000170_hxzpz.jpg -> source_slug: adamkosamep, local_path: /media/1000170_hxzpz.jpg
    """
    line = line.strip()
    if not line:
        return None

    # Split on first '/'
    parts = line.split('/', 1)

    if len(parts) < 2:
        return None

    source_slug = parts[0]  # e.g. "adamkosamep"
    local_path =  parts[1]  # e.g. "/adamkosamep.json" or "/media/1000170_hxzpz.jpg"

    return source_slug, local_path


def create_table(conn):
    conn.execute('''
        CREATE TABLE files (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_slug TEXT NOT NULL,
            local_path  TEXT NOT NULL
        )
    ''')
    conn.commit()


def import_txt_to_sqlite(txt_file, db_file):
    conn = sqlite3.connect(db_file)
    create_table(conn)

    inserted = 0
    skipped = 0

    with open(txt_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            result = parse_line(line)

            if result is None:
                print(f"  [SKIP] Line {line_num}: '{line.strip()}'")
                skipped += 1
                continue

            source_slug, local_path = result

            conn.execute(
                'INSERT INTO files (source_slug, local_path) VALUES (?, ?)',
                (source_slug, local_path)
            )
            inserted += 1

    conn.commit()
    conn.close()

    print(f"\n✅ Done! Inserted: {inserted}, Skipped: {skipped}")
    print(f"   Database: {db_file}")


# --- Run ---
if __name__ == '__main__':
    import_txt_to_sqlite('filelist_20260417_125521.txt', 'posts.db')
