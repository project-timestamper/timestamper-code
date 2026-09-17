import {
  appendHash,
  fetchOnce,
  formatDuration,
  hashStream,
  loadDoneKeys,
  withRetries
} from './util.js'

const HASH_RETRIES = 5
const RETRY_DELAY_MS = 10000

export const normalizeDirUrl = (url) => (url.endsWith('/') ? url : `${url}/`)

/**
 * Parse an HTTPS directory index (Apache / nginx / Bootstrap-style).
 * Directories are hrefs ending in `/`; everything else under `root` is a file.
 */
export const listDirectory = async (dirUrl, rootUrl) => {
  const root = normalizeDirUrl(rootUrl)
  const response = await withRetries(normalizeDirUrl(dirUrl), () =>
    fetchOnce(normalizeDirUrl(dirUrl))
  )
  const html = await response.text()
  const hrefs = [...html.matchAll(/href="([^"]+)"/g)].map((match) => match[1])
  const dirs = []
  const files = []

  for (const href of hrefs) {
    if (!href || href.startsWith('?') || href.startsWith('#') || href.startsWith('../')) {
      continue
    }

    const absolute = new URL(href, normalizeDirUrl(dirUrl))
    if (!absolute.href.startsWith(root)) {
      continue
    }

    if (href.endsWith('/')) {
      dirs.push(absolute.href)
    } else {
      files.push(absolute.href)
    }
  }

  return { dirs: [...new Set(dirs)], files: [...new Set(files)] }
}

/** BFS over directory indexes; returns sorted unique leaf file URLs. */
export const spiderFiles = async (rootUrls) => {
  const files = []
  const seenDirs = new Set()

  for (const rootUrl of rootUrls) {
    const root = normalizeDirUrl(rootUrl)
    const queue = [root]
    console.log('spider root', root)

    while (queue.length > 0) {
      const dirUrl = queue.shift()
      if (seenDirs.has(dirUrl)) {
        continue
      }
      seenDirs.add(dirUrl)
      console.log('listing', dirUrl)
      const { dirs, files: leafFiles } = await listDirectory(dirUrl, root)
      files.push(...leafFiles)
      for (const dir of dirs) {
        if (!seenDirs.has(dir)) {
          queue.push(dir)
        }
      }
    }
  }

  return [...new Set(files)].sort()
}

export const hashHttpsFile = async (url) =>
  withRetries(url, async () => {
    const response = await fetchOnce(url)
    return hashStream(response.body)
  }, {
    retries: HASH_RETRIES,
    delayMs: RETRY_DELAY_MS
  })

/**
 * Spider roots, then SHA-256 each leaf. Appends `key\\thash` lines; resumes via
 * keys already present in `outputPath`.
 *
 * @param {object} opts
 * @param {string[]} opts.roots
 * @param {string} opts.outputPath
 * @param {(fileUrl: string) => string} [opts.keyFn] default: full URL
 * @param {(fileUrl: string) => boolean} [opts.filter] keep when true; default keep all
 */
export const collectUrlHashes = async ({
  roots,
  outputPath,
  keyFn = (url) => url,
  filter = () => true
} = {}) => {
  const done = loadDoneKeys(outputPath)
  console.log('already hashed:', done.size)

  const files = (await spiderFiles(roots)).filter(filter)
  const todoFiles = files.filter((url) => !done.has(keyFn(url)))
  console.log('listed:', files.length, 'todo:', todoFiles.length)

  const start = Date.now()
  let hashed = 0
  let attempted = 0
  const skipped = files.length - todoFiles.length

  for (const url of todoFiles) {
    const key = keyFn(url)
    attempted++
    console.log(`fetching ${attempted}/${todoFiles.length}`, key)
    try {
      const digest = await hashHttpsFile(url)
      appendHash(outputPath, key, digest)
      done.add(key)
      hashed++
      console.log(key, digest)
      if (hashed % 10 === 0) {
        const elapsed = Date.now() - start
        const remaining = todoFiles.length - attempted
        const eta = attempted > 0 ? remaining * (elapsed / attempted) : 0
        console.log(
          `progress: ${hashed} hashed, ${attempted}/${todoFiles.length} attempted, ${skipped} skipped, ` +
          `elapsed ${formatDuration(elapsed)}, eta ${formatDuration(eta)}`
        )
      }
    } catch (e) {
      console.error(key, e.message)
    }
  }

  return hashed
}
