const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const util = require('util');
const execAsync = util.promisify(exec);

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
  let data;
  try {
    data = JSON.parse(fs.readFileSync(jsonPath, 'utf-8'));
  } catch (e) {
    console.error(`Skipping ${jsonPath}: could not parse JSON (${e.message})`);
    return;
  }

  if (!Array.isArray(data.posts)) return;

  const slug = data.metadata?.slug || path.basename(path.dirname(jsonPath));
  const mediaDir = path.join(path.dirname(jsonPath), 'media');
  let changed = false;

  for (const post of data.posts) {
    if (!Array.isArray(post.download_errors) || post.download_errors.length === 0) continue;

    const retryErrors = post.download_errors.filter(
      (e) => e.error && e.error.includes(REGISTERED_USERS_ERROR),
    );
    if (retryErrors.length === 0) continue;

    for (const errEntry of retryErrors) {
      const url = errEntry.url;
      if (!url) continue;

      const vidFilename = `${post.id}_video.mp4`;
      const destPath = path.join(mediaDir, vidFilename);

      if (fs.existsSync(destPath)) {
        console.log(`  [${slug}] Already exists, skipping: ${vidFilename}`);
        post.download_errors = post.download_errors.filter((e) => e !== errEntry);
        changed = true;
        continue;
      }

      console.log(`  [${slug}] Retrying video for post ${post.id}: ${url}`);
      try {
        await retryVideo(url, destPath, cookiesPath);
        console.log(`  [${slug}] Success: ${vidFilename}`);
        post.download_errors = post.download_errors.filter((e) => e !== errEntry);

        // Update local_path on the matching media entry if missing
        for (const m of post.media || []) {
          if (m.type === 'video' && m.url === url && !m.local_path) {
            m.local_path = `media/${vidFilename}`;
          }
        }
        changed = true;
      } catch (e) {
        console.error(`  [${slug}] Still failed for post ${post.id}: ${e.message}`);
      }
    }
  }

  if (changed) {
    const tmpPath = `${jsonPath}.tmp`;
    fs.writeFileSync(tmpPath, JSON.stringify(data, null, 2));
    fs.renameSync(tmpPath, jsonPath);
    console.log(`  [${slug}] Updated ${path.basename(jsonPath)}`);
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
