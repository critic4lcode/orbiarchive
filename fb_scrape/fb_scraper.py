import asyncio, json, re, gzip, brotli, subprocess, hashlib, random, time
import aiohttp, aiosqlite
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from playwright.async_api import async_playwright

DB = "archive.db"
BASE = Path("archive")

SCROLL_LIMIT = 2000
MEDIA_WORKERS = 4
SCRAPER_THREADS = 2
GLOBAL_API_LIMIT = 2

API_SEM = asyncio.Semaphore(GLOBAL_API_LIMIT)

# ================= SAFE PATH =================

def get_page_name(url: str):
    parsed = urlparse(url)
    if "profile.php" in parsed.path:
        qs = parse_qs(parsed.query)
        if "id" in qs:
            return qs["id"][0]
    return parsed.path.strip("/")

def safe_name(name: str, url: str):
    raw = name.split("?")[0]
    raw = re.sub(r"[^a-zA-Z0-9._-]", "_", raw)
    raw = raw[:80]

    if "profile" in raw:
        m = re.search(r"\d+", raw)
        if m:
            raw = m.group(0)

    if not raw:
        raw = hashlib.sha1(url.encode()).hexdigest()[:16]

    return raw

# ================= THROTTLE =================

class Throttle:
    def __init__(self):
        self.hard = 0

    async def wait(self):
        if time.time() < self.hard:
            t = int(self.hard - time.time())
            print(f"[BACKOFF] {t}s")
            await asyncio.sleep(t)

    def hard_hit(self):
        print("[HARD RATE LIMIT] 30min sleep")
        self.hard = time.time() + 1800

THROTTLE = Throttle()

# ================= DB =================

async def init_db():
    db = await aiosqlite.connect(DB)

    await db.executescript("""
    CREATE TABLE IF NOT EXISTS posts(
        id TEXT PRIMARY KEY,
        page TEXT,
        text TEXT,
        ts INTEGER,
        short_id TEXT
    );

    CREATE TABLE IF NOT EXISTS media(
        id TEXT PRIMARY KEY,
        post_id TEXT,
        type TEXT,
        path TEXT
    );
    """)

    await db.commit()
    return db

async def load_existing(db, page):
    rows = await db.execute_fetchall(
        "SELECT id FROM posts WHERE page = ?", (page,)
    )
    return set(r[0] for r in rows)

# ================= UTILS =================

def short_id(x):
    return hashlib.sha1(x.encode()).hexdigest()[:16]

def decode(body, headers):
    enc = headers.get("content-encoding", "")
    try:
        if "br" in enc:
            return brotli.decompress(body).decode("utf-8", "ignore")
        if "gzip" in enc:
            return gzip.decompress(body).decode("utf-8", "ignore")
        return body.decode("utf-8", "ignore")
    except:
        return None

def split_chunks(text):
    if text.startswith("for (;;);"):
        text = text[9:]

    out = []
    for line in text.split("\n"):
        line = re.sub(r"^\d+:", "", line.strip())
        if line:
            out.append(line)
    return out

# ================= EXTRACT =================

def extract(data):
    posts, media = [], []

    edges = data.get("data", {}).get("node", {}) \
        .get("timeline_list_feed_units", {}).get("edges", [])

    for e in edges:
        n = e.get("node", {})
        if n.get("__typename") != "Story":
            continue

        story = n.get("comet_sections", {}).get("content", {}).get("story", {})
        if not isinstance(story, dict):
            continue

        msg = story.get("message")
        if not isinstance(msg, dict):
            continue

        text = msg.get("text")
        if not text:
            continue

        pid = story.get("id")
        ts = story.get("creation_time")

        posts.append((pid, text, ts))

        for att in story.get("attachments", []):
            m = att.get("styles", {}).get("attachment", {}).get("media", {})
            if not isinstance(m, dict):
                continue

            img = m.get("image")
            if isinstance(img, dict) and img.get("uri"):
                media.append(("img_"+str(m.get("id")), pid, "image", img["uri"]))

            if m.get("__typename") == "Video":
                media.append(("vid_"+str(m.get("id")), pid, "video", m.get("id")))

    return posts, media

# ================= MEDIA =================

async def media_worker(q, db):
    async with aiohttp.ClientSession() as s:
        while True:
            mid, pid, typ, val, pdir = await q.get()
            try:
                if typ == "image":
                    path = pdir / "images" / f"{mid}.jpg"
                    if not path.exists():
                        async with s.get(val) as r:
                            if r.status == 200:
                                path.write_bytes(await r.read())

                    await db.execute(
                        "INSERT OR IGNORE INTO media VALUES (?, ?, ?, ?)",
                        (mid, pid, "image", str(path))
                    )

                elif typ == "video":
                    vdir = pdir / "videos"
                    vdir.mkdir(parents=True, exist_ok=True)

                    subprocess.run([
                        "yt-dlp",
                        "-o", str(vdir / "%(id)s.%(ext)s"),
                        f"https://facebook.com/watch/?v={val}"
                    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

                    await db.execute(
                        "INSERT OR IGNORE INTO media VALUES (?, ?, ?, ?)",
                        (mid, pid, "video", str(vdir))
                    )

                await db.commit()

            except Exception as e:
                print("[MEDIA ERROR]", e)

            q.task_done()

# ================= SCRAPER =================

async def scrape(ctx, db, url, mq):
    raw = get_page_name(url)
    safe = safe_name(raw, url)

    print("[PAGE]", url, "->", safe)

    base = BASE / safe
    try:
        base.mkdir(parents=True, exist_ok=True)
    except:
        safe = hashlib.sha1(url.encode()).hexdigest()[:16]
        base = BASE / safe
        base.mkdir(parents=True, exist_ok=True)

    existing = await load_existing(db, safe)

    page = await ctx.new_page()
    seen = set()

    new_count = 0
    ign_db = 0
    ign_seen = 0

    async def handle(resp):
        nonlocal new_count, ign_db, ign_seen

        if "graphql" not in resp.url:
            return

        await THROTTLE.wait()

        async with API_SEM:
            await asyncio.sleep(random.uniform(0.05, 0.15))  # small jitter

            text = decode(await resp.body(), resp.headers)
            if not text:
                return

            for chunk in split_chunks(text):
                if "1675004" in chunk:
                    THROTTLE.hard_hit()
                    return

                try:
                    data = json.loads(chunk)
                except:
                    continue

                posts, media = extract(data)

                for pid, txt, ts in posts:
                    if not pid:
                        continue

                    short = pid[:32]

                    if pid in seen:
                        ign_seen += 1
                        print(f"[IGN-SEEN] {short}")
                        continue

                    if pid in existing:
                        ign_db += 1
                        print(f"[IGN-DB] {short}")
                        seen.add(pid)
                        continue

                    seen.add(pid)
                    new_count += 1

                    sid = short_id(pid)
                    pdir = base / sid

                    (pdir / "images").mkdir(parents=True, exist_ok=True)
                    (pdir / "videos").mkdir(parents=True, exist_ok=True)

                    print(f"[NEW] {short} {ts}")

                    await db.execute(
                        "INSERT OR IGNORE INTO posts VALUES (?, ?, ?, ?, ?)",
                        (pid, safe, txt, ts, sid)
                    )

                    for mid, mpid, typ, val in media:
                        if mpid == pid:
                            await mq.put((mid, pid, typ, val, pdir))

                await db.commit()

    page.on("response", handle)

    await page.goto(url)

    for i in range(SCROLL_LIMIT):
        await THROTTLE.wait()

        async with API_SEM:
            print(f"[SCROLL {safe}] {i}")
            await page.mouse.wheel(0, 30000)

        if i % 50 == 0:
            print(f"[STATS {safe}] new={new_count} db={ign_db} seen={ign_seen}")

        await page.wait_for_timeout(300 + random.randint(0, 300))

    await page.close()

# ================= MAIN =================

async def main():
    db = await init_db()

    urls = [x.strip() for x in open("pages.txt") if x.strip()]
    mq = asyncio.Queue()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        ctx = await browser.new_context(storage_state="fb_state.json")

        workers = [
            asyncio.create_task(scrape(ctx, db, u, mq))
            for u in urls[:SCRAPER_THREADS]
        ]

        media_workers = [
            asyncio.create_task(media_worker(mq, db))
            for _ in range(MEDIA_WORKERS)
        ]

        await asyncio.gather(*workers)
        await mq.join()

        for w in media_workers:
            w.cancel()

        await browser.close()

asyncio.run(main())