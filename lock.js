const fs = require('fs');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function acquireLock(lockPath, timeoutMs = 30000, retryMs = 100) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const fd = fs.openSync(lockPath, 'wx');
      fs.closeSync(fd);
      return true;
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
      await sleep(retryMs);
    }
  }
  throw new Error(`Could not acquire lock: ${lockPath} (timeout ${timeoutMs}ms)`);
}

function releaseLock(lockPath) {
  try { fs.unlinkSync(lockPath); } catch {}
}

module.exports = { acquireLock, releaseLock };
