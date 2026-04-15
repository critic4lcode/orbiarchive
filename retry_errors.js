const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const util = require('util');
const execAsync = util.promisify(exec);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const { acquireLock, releaseLock } = require('./lock');

const REGISTERED_USERS_ERROR = 'This video is only available for registered users';

function resolveDataDir() {
  const dataDirArg = process.argv.find((a) => a.startsWith('--data-dir='));
  if (dataDirArg) return path.resolve(dataDirArg.split('=')[1]);
  if (process.env.DATA_DIR) return path.resolve(process.env.DATA_DIR);
  return path.join(__dirname, 'data');
}

function resolveCookiesPath() {
  const cookiesArg = process.argv.find((a) => a.startsWith('--cookies='));
  if (cookiesArg) return path.resolve(cookiesArg.split('=')[1]);
  const defaultPath = path.join(process.cwd(), 'cookies.txt');
  return fs.existsSync(defaultPath) ? defaultPath : null;
}

function findJsonFiles(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...findJsonFiles(full));
    } else if (entry.isFile() && entry.name.endsWith('.json') && !entry.name.endsWith('.tmp')) {
      results.push(full);
    }
  }
  return results;
}

async function retryVideo(url, destPath, cookiesPath) {
  const cookiesArg = cookiesPath ? `--cookies "${cookiesPath}"` : '';
  await execAsync(
    `yt-dlp -o "${destPath}" "${url}" --quiet --no-warnings --no-playlist ${cookiesArg}`,
  );
}

async function processJsonFile(jsonPath, cookiesPath) {
  const lockPath = `${jsonPath}.lock`;

  // First pass: check without lock to skip files with nothing to retry
  let data;
  try {
    data = JSON.parse(fs.readFileSync(jsonPath, 'utf-8'));
  } catch (e) {
    console.error(`Skipping ${jsonPath}: could not parse JSON (${e.message})`);
    return;
  }
  if (!Array.isArray(data.posts)) return;

  const hasRetryable = data.posts.some(
    (p) =>
      Array.isArray(p.download_errors) &&
      p.download_errors.some((e) => e.error && e.error.includes(REGISTERED_USERS_ERROR)),
  );
  if (!hasRetryable) return;

  const slug = data.metadata?.slug || path.basename(path.dirname(jsonPath));
  const mediaDir = path.join(path.dirname(jsonPath), 'media');

  // Collect video download tasks outside the lock so we don't hold it during yt-dlp
  const tasks = [];
  for (const post of data.posts) {
    if (!Array.isArray(post.download_errors) || post.download_errors.length === 0) continue;
    const retryErrors = post.download_errors.filter(
      (e) => e.error && e.error.includes(REGISTERED_USERS_ERROR),
    );
    for (const errEntry of retryErrors) {
      if (!errEntry.url) continue;
      tasks.push({ post, errEntry, url: errEntry.url });
    }
  }

  // Download videos without holding the lock
  const results = new Map(); // url -> { success: bool, destPath }
  for (const { post, url } of tasks) {
    const vidFilename = `${post.id}_video.mp4`;
    const destPath = path.join(mediaDir, vidFilename);
    if (results.has(url)) continue; // deduplicate same url across posts
    if (fs.existsSync(destPath)) {
      results.set(url, { success: true, destPath, vidFilename });
      continue;
    }
    console.log(`  [${slug}] Retrying video for post ${post.id}: ${url}`);
    try {
      await retryVideo(url, destPath, cookiesPath);
      console.log(`  [${slug}] Success: ${vidFilename}`);
      results.set(url, { success: true, destPath, vidFilename });
    } catch (e) {
      console.error(`  [${slug}] Still failed for post ${post.id}: ${e.message}`);
      results.set(url, { success: false, vidFilename });
    }
  }

  // Acquire lock, re-read the latest JSON, apply results, write back
  await acquireLock(lockPath);
  try {
    let freshData;
    try {
      freshData = JSON.parse(fs.readFileSync(jsonPath, 'utf-8'));
    } catch (e) {
      console.error(`  [${slug}] Could not re-read JSON under lock: ${e.message}`);
      return;
    }

    let changed = false;
    for (const post of freshData.posts) {
      if (!Array.isArray(post.download_errors) || post.download_errors.length === 0) continue;
      for (let i = post.download_errors.length - 1; i >= 0; i--) {
        const errEntry = post.download_errors[i];
        if (!errEntry.error || !errEntry.error.includes(REGISTERED_USERS_ERROR)) continue;
        const result = results.get(errEntry.url);
        if (!result || !result.success) continue;
        post.download_errors.splice(i, 1);
        for (const m of post.media || []) {
          if (m.type === 'video' && m.url === errEntry.url && !m.local_path) {
            m.local_path = `media/${result.vidFilename}`;
          }
        }
        changed = true;
      }
    }

    if (changed) {
      const tmpPath = `${jsonPath}.tmp`;
      fs.writeFileSync(tmpPath, JSON.stringify(freshData, null, 2));
      fs.renameSync(tmpPath, jsonPath);
      console.log(`  [${slug}] Updated ${path.basename(jsonPath)}`);
    }
  } finally {
    releaseLock(lockPath);
  }
}

async function main() {
  const dataDir = resolveDataDir();
  const cookiesPath = resolveCookiesPath();

  if (!cookiesPath) {
    console.warn(
      'WARNING: No cookies.txt found. Retries will likely fail for registered-user-only videos.\n' +
        'Run: yt-dlp --cookies-from-browser chrome --cookies cookies.txt',
    );
  } else {
    console.log(`Using cookies: ${cookiesPath}`);
  }

  if (!fs.existsSync(dataDir)) {
    console.error(`ERROR: Data dir does not exist: ${dataDir}`);
    process.exit(1);
  }

  console.log(`Scanning for JSON files in: ${dataDir}`);
  const jsonFiles = findJsonFiles(dataDir);
  console.log(`Found ${jsonFiles.length} JSON file(s).`);

  for (const jsonFile of jsonFiles) {
    await processJsonFile(jsonFile, cookiesPath);
  }

  console.log('Done.');
}

main();
