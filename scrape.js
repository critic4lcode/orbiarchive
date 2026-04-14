const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const util = require('util');
const execAsync = util.promisify(exec);

const LOG_FILE = path.join(process.cwd(), 'scrape.log');
const MISSING_FILES_LOG = path.join(process.cwd(), 'missing_files.log');

function ts() {
  return new Date().toISOString();
}

function log(msg) {
  const line = `[${ts()}] ${msg}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

function logWarn(msg) {
  const line = `[${ts()}] WARN: ${msg}`;
  console.warn(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

function logError(msg) {
  const line = `[${ts()}] ERROR: ${msg}`;
  console.error(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

function logMissingFile(url, reason, context) {
  const line = `[${ts()}] ${context ? `[${context}] ` : ''}${url} | ${reason}\n`;
  fs.appendFileSync(MISSING_FILES_LOG, line);
}

const BASE_URL = 'https://posztok.hu';

function resolveDataDir() {
  const dataDirArg = process.argv.find((a) => a.startsWith('--data-dir='));
  if (dataDirArg) return path.resolve(dataDirArg.split('=')[1]);
  if (process.env.DATA_DIR) return path.resolve(process.env.DATA_DIR);
  return path.join(__dirname, 'data');
}

const DATA_DIR = resolveDataDir();

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function fetchPage(url) {
  try {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Failed to fetch ${url}: ${response.statusText}`);
    return await response.text();
  } catch (error) {
    console.error(`Error fetching ${url}:`, error);
    return null;
  }
}

async function downloadFile(url, dest, context) {
  if (fs.existsSync(dest)) return null;
  try {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Failed to download ${url}: ${response.statusText}`);
    const buffer = Buffer.from(await response.arrayBuffer());
    fs.writeFileSync(dest, buffer);
    return null;
  } catch (error) {
    const msg = `Error downloading ${url}: ${error.message}`;
    logError(msg);
    logMissingFile(url, error.message, context);
    return { url, error: error.message };
  }
}

async function downloadVideo(url, destDir, filename, context) {
  const destPath = path.join(destDir, `${filename}.mp4`);
  if (fs.existsSync(destPath)) return null;
  try {
    if (url.includes('facebook.com') || url.includes('youtube.com') || url.includes('reel')) {
      log(`  Downloading video: ${url}`);
      await execAsync(`yt-dlp -o "${destPath}" "${url}" --quiet --no-warnings --no-playlist`);
    }
    return null;
  } catch (error) {
    const msg = `Failed to download video ${url}: ${error.message}`;
    logError(msg);
    logMissingFile(url, error.message, context);
    return { url, error: error.message };
  }
}

function savePosts(userDir, source, allPosts) {
  const finalPath = path.join(userDir, `${source.slug}.json`);
  const tmpPath = `${finalPath}.tmp`;
  const result = {
    metadata: source,
    scraped_at: new Date().toISOString(),
    posts: allPosts,
  };
  fs.writeFileSync(tmpPath, JSON.stringify(result, null, 2));
  fs.renameSync(tmpPath, finalPath);
}

async function fetchFullContent(continuationUrl) {
  const url = continuationUrl.startsWith('http') ? continuationUrl : BASE_URL + continuationUrl;
  const html = await fetchPage(url);
  if (!html) return null;
  const $ = cheerio.load(html);
  const body = $('article.post-details .wordbreak').first();
  if (!body.length) return null;
  return body.text().trim();
}

function parsePosts(html) {
  const $ = cheerio.load(html);
  const posts = [];

  $('article.post').each((i, el) => {
    const $post = $(el);
    const metaDiv = $post.find('div[id^="arepl-"]');
    if (!metaDiv.length) return;

    const postData = {
      id: metaDiv.attr('data-id'),
      source_slug: metaDiv.attr('data-sn'),
      source_name: metaDiv.attr('data-n'),
      date: metaDiv.attr('data-d'),
      original_url: metaDiv.attr('data-oau'),
      local_url: metaDiv.attr('data-f'),
      content: $post.find('.leadtextborder.wordbreak').text().trim(),
      media: [],
      links: [],
    };

    $post.find('figure img.imgfit').each((j, img) => {
      const imgUrl = $(img).attr('src');
      postData.media.push({
        type: 'image',
        url: imgUrl.startsWith('http') ? imgUrl : BASE_URL + imgUrl,
      });
    });

    $post.find('.video-wrapper .fb-video').each((j, vid) => {
      postData.media.push({
        type: 'video',
        url: $(vid).attr('data-href'),
      });
    });

    $post.find('.leadtextborder.wordbreak a').each((j, link) => {
      postData.links.push({
        text: $(link).text().trim(),
        url: $(link).attr('href'),
      });
    });

    const moreLink = $post.find('.pt-1.clearfix .float-right a').first();
    if (moreLink.length && moreLink.text().trim().startsWith('Folytatódik')) {
      postData.continuation_url = moreLink.attr('href');
    }

    posts.push(postData);
  });

  return posts;
}

async function getSources() {
  log(`Fetching sources from ${BASE_URL}/`);
  const html = await fetchPage(`${BASE_URL}/`);
  if (!html) return [];
  const $ = cheerio.load(html);
  const sources = [];

  $('.ma-ps').each((i, el) => {
    const $el = $(el);
    sources.push({
      slug: $el.attr('data-s'),
      name: $el.attr('data-n'),
      orig_url: $el.attr('data-o'),
      handler: $el.attr('data-h') || 'facebook',
    });
  });

  if (fs.existsSync('posztok_links.txt')) {
    const validLinks = fs
      .readFileSync('posztok_links.txt', 'utf-8')
      .split('\n')
      .map((l) => l.trim())
      .filter((l) => l.startsWith('https://posztok.hu/s/'))
      .map((l) => l.split('/').pop());

    const validSet = new Set(validLinks);
    const filtered = sources.filter((s) => validSet.has(s.slug));
    log(`Filtered to ${filtered.length} sources from posztok_links.txt`);
    return filtered;
  }

  log(`Found ${sources.length} sources`);
  return sources;
}

function parseHungarianDate(dateStr) {
  if (!dateStr) return null;
  const now = new Date();
  let date;

  if (dateStr.startsWith('Ma')) {
    date = new Date();
    const timeMatch = dateStr.match(/(\d{1,2}):(\d{2})/);
    if (timeMatch) {
      date.setHours(parseInt(timeMatch[1]), parseInt(timeMatch[2]), 0, 0);
    }
  } else if (dateStr.startsWith('Tegnap')) {
    date = new Date();
    date.setDate(date.getDate() - 1);
    const timeMatch = dateStr.match(/(\d{1,2}):(\d{2})/);
    if (timeMatch) {
      date.setHours(parseInt(timeMatch[1]), parseInt(timeMatch[2]), 0, 0);
    }
  } else {
    const parts = dateStr.match(/(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{1,2}):(\d{2})/);
    if (parts) {
      date = new Date(
        parseInt(parts[1]),
        parseInt(parts[2]) - 1,
        parseInt(parts[3]),
        parseInt(parts[4]),
        parseInt(parts[5]),
      );
    } else {
      date = new Date(dateStr);
    }
  }
  return isNaN(date.getTime()) ? dateStr : date.toISOString();
}

async function scrapeUser(source) {
  const userDir = path.join(DATA_DIR, source.slug);
  const mediaDir = path.join(userDir, 'media');

  if (!fs.existsSync(userDir)) fs.mkdirSync(userDir, { recursive: true });
  if (!fs.existsSync(mediaDir)) fs.mkdirSync(mediaDir, { recursive: true });

  log(`Scraping user archive: ${source.name} (${source.slug})...`);
  const url = `${BASE_URL}/s/${source.slug}`;
  const html = await fetchPage(url);
  if (!html) return;

  const jsonPath = path.join(userDir, `${source.slug}.json`);
  let allPosts = [];
  let resumeLastId = null;
  if (fs.existsSync(jsonPath)) {
    try {
      const existing = JSON.parse(fs.readFileSync(jsonPath, 'utf-8'));
      if (Array.isArray(existing.posts) && existing.posts.length > 0) {
        allPosts = existing.posts;
        resumeLastId = allPosts
          .map((p) => parseInt(p.id))
          .filter((n) => !isNaN(n))
          .reduce((min, n) => (n < min ? n : min), Infinity);
        log(`  Resuming from existing archive: ${allPosts.length} posts, lastId=${resumeLastId}`);
      }
    } catch (e) {
      logWarn(`Could not parse existing ${source.slug}.json, starting fresh: ${e.message}`);
    }
  }

  if (resumeLastId === null) {
    allPosts = parsePosts(html);
    for (const post of allPosts) {
      if (post.continuation_url) {
        await sleep(100);
        const full = await fetchFullContent(post.continuation_url);
        if (full) post.content = full;
      }
      post.date_iso = parseHungarianDate(post.date);
    }
    const firstPageMediaTasks = [];
    for (const post of allPosts) {
      post.download_errors = post.download_errors || [];
      for (const m of post.media) {
        if (m.type === 'image') {
          const ext = path.extname(m.url.split('?')[0]) || '.jpg';
          const filename = `${post.id}_${Math.random().toString(36).substring(7)}${ext}`;
          const dest = path.join(mediaDir, filename);
          firstPageMediaTasks.push(
            downloadFile(m.url, dest, source.slug).then((err) => {
              if (err) post.download_errors.push(err);
            }),
          );
          m.local_path = `media/${filename}`;
        } else if (m.type === 'video') {
          const vidFilename = `${post.id}_video`;
          firstPageMediaTasks.push(
            downloadVideo(m.url, mediaDir, vidFilename, source.slug).then((err) => {
              if (err) post.download_errors.push(err);
            }),
          );
          m.local_path = `media/${vidFilename}.mp4`;
        }
      }
    }
    await Promise.all(firstPageMediaTasks);
    savePosts(userDir, source, allPosts);
  }

  const sourceIdMatch = html.match(/initInfiniteScrollForSource\((\d+)\)/);
  const lastPostIdMatch = html.match(/document\.lastPostId = (\d+);/);

  if (sourceIdMatch && (lastPostIdMatch || resumeLastId !== null)) {
    const sourceId = sourceIdMatch[1];
    let lastId = resumeLastId !== null ? String(resumeLastId) : lastPostIdMatch[1];

    log(`  Archive crawl started for ${source.slug}. sourceId: ${sourceId}, lastId: ${lastId}`);

    let hasMore = true;
    while (hasMore) {
      await sleep(150);
      const pagUrl = `${BASE_URL}/next-source/${sourceId}?lastId=${lastId}`;
      const pagHtml = await fetchPage(pagUrl);

      if (!pagHtml || pagHtml.trim() === '' || pagHtml.includes('post-sources-placeholder')) {
        hasMore = false;
        break;
      }

      const newPosts = parsePosts(pagHtml);
      if (newPosts.length === 0) {
        hasMore = false;
        break;
      }

      for (const post of newPosts) {
        if (post.continuation_url) {
          await sleep(100);
          const full = await fetchFullContent(post.continuation_url);
          if (full) post.content = full;
        }
      }

      const mediaTasks = [];
      for (const post of newPosts) {
        post.date_iso = parseHungarianDate(post.date);
        post.download_errors = post.download_errors || [];
        for (const m of post.media) {
          if (m.type === 'image') {
            const ext = path.extname(m.url.split('?')[0]) || '.jpg';
            const filename = `${post.id}_${Math.random().toString(36).substring(7)}${ext}`;
            const dest = path.join(mediaDir, filename);
            mediaTasks.push(
              downloadFile(m.url, dest, source.slug).then((err) => {
                if (err) post.download_errors.push(err);
              }),
            );
            m.local_path = `media/${filename}`;
          } else if (m.type === 'video') {
            const vidFilename = `${post.id}_video`;
            mediaTasks.push(
              downloadVideo(m.url, mediaDir, vidFilename, source.slug).then((err) => {
                if (err) post.download_errors.push(err);
              }),
            );
            m.local_path = `media/${vidFilename}.mp4`;
          }
        }
      }
      await Promise.all(mediaTasks);

      allPosts = allPosts.concat(newPosts);
      lastId = newPosts[newPosts.length - 1].id;
      savePosts(userDir, source, allPosts);
      const errorCount = allPosts.reduce((n, p) => n + (p.download_errors ? p.download_errors.length : 0), 0);
      process.stdout.write(`\r    Total posts collected: ${allPosts.length}, download errors so far: ${errorCount}...`);
    }
    log(`\n  Finished archive for ${source.slug}.`);
  }

  savePosts(userDir, source, allPosts);
  const totalErrors = allPosts.reduce((n, p) => n + (p.download_errors ? p.download_errors.length : 0), 0);
  log(`  Archive saved to ${source.slug}/${source.slug}.json (${allPosts.length} posts, ${totalErrors} download errors)`);
}

async function checkYtDlp() {
  try {
    const { stdout } = await execAsync('yt-dlp --version');
    log(`yt-dlp detected (version ${stdout.trim()}).`);
    return true;
  } catch {
    logWarn(
      'yt-dlp not found on PATH. Video posts will be skipped. Install with `brew install yt-dlp` or see https://github.com/yt-dlp/yt-dlp.',
    );
    process.exit(1);
  }
}

async function withConcurrency(tasks, limit) {
  const results = [];
  const executing = new Set();
  for (const task of tasks) {
    const p = Promise.resolve().then(task).finally(() => executing.delete(p));
    executing.add(p);
    results.push(p);
    if (executing.size >= limit) {
      await Promise.race(executing);
    }
  }
  return Promise.all(results);
}

async function main() {
  await checkYtDlp();
  log(`Starting scrape. Log: ${LOG_FILE}, Missing files log: ${MISSING_FILES_LOG}`);
  const allSources = await getSources();
  const groupArg = process.argv[2];

  if (!groupArg || (groupArg !== '1' && groupArg !== '2')) {
    log('Please specify group 1 or 2: bun run scrape.js 1');
    return;
  }

  const concurrencyArg = process.argv.find((a) => a.startsWith('--concurrency='));
  const concurrency = concurrencyArg ? parseInt(concurrencyArg.split('=')[1]) : 3;

  const mid = Math.ceil(allSources.length / 2);
  const sources = groupArg === '1' ? allSources.slice(0, mid) : allSources.slice(mid);

  log(`Group ${groupArg}: Processing ${sources.length} sources with concurrency=${concurrency}:`);
  sources.forEach((s, i) => log(`  ${i + 1}. ${s.name} (${s.slug})`));

  const tasks = sources.map((source) => async () => {
    await scrapeUser(source);
    await sleep(500);
  });

  await withConcurrency(tasks, concurrency);

  log(`Group ${groupArg} archive completed.`);
}

main();
