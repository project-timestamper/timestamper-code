import { createHash } from 'node:crypto'
import { once } from 'node:events'
import fs from 'node:fs'
import path from 'node:path'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { createGunzip } from 'node:zlib'
import minimist from 'minimist'
import esMain from 'es-main'

const ROOT_URLS = [
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/integrated_sv_map/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/historical_data/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/changelog_details/'
]
const FTP_BASE = 'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/'
const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
const DEFAULT_OUTPUT = 'human_genome_hashes.txt'
const HASH_RETRIES = 5
const RETRY_DELAY_MS = 10000

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

const sidecarPath = (outputPath, suffix) => {
  if (outputPath.endsWith('_hashes.txt')) {
    return outputPath.replace(/_hashes\.txt$/, `_${suffix}`)
  }
  return `${outputPath}.${suffix}`
}

const normalizeDirUrl = (url) => (url.endsWith('/') ? url : `${url}/`)

const listDirectory = async (dirUrl, rootUrl) => {
  const root = normalizeDirUrl(rootUrl)
  const response = await fetch(normalizeDirUrl(dirUrl), {
    headers: { 'User-Agent': USER_AGENT }
  })
  if (!response.ok) {
    throw new Error(`list failed: ${response.status} ${dirUrl}`)
  }
  const html = await response.text()
  const hrefs = [...html.matchAll(/href="([^"]+)"/g)].map(match => match[1])
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

const spiderFiles = async (rootUrls = ROOT_URLS) => {
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

const relativeKey = (fileUrl) => {
  if (fileUrl.startsWith(FTP_BASE)) {
    return fileUrl.slice(FTP_BASE.length)
  }
  return fileUrl
}

const pathnameOf = (url) => {
  try {
    return new URL(url).pathname
  } catch {
    return url
  }
}

const isGzipUrl = (url) => pathnameOf(url).endsWith('.gz')

const isIndexUrl = (url) => /\.(?:tbi|csi)$/i.test(pathnameOf(url))

// Concatenated gzip / BGZF: keep reading members until the input ends.
const inflateGzip = async (body, onChunk) => {
  const unzip = createGunzip()
  const done = new Promise((resolve, reject) => {
    unzip.once('end', resolve)
    unzip.once('error', reject)
  })
  done.catch(() => {})

  unzip.on('data', onChunk)

  try {
    for await (const chunk of body) {
      if (!unzip.write(chunk)) {
        await Promise.race([once(unzip, 'drain'), done])
      }
    }
    unzip.end()
    await done
  } finally {
    unzip.destroy()
  }
}

const hashStream = async (body, { gunzip = false } = {}) => {
  const hash = createHash('sha256')

  if (!gunzip) {
    for await (const chunk of body) {
      hash.update(chunk)
    }
  } else {
    await inflateGzip(body, (chunk) => hash.update(chunk))
  }

  return hash.digest('hex')
}

const cacheFilePath = (cacheDir, key) =>
  path.join(cacheDir, key.replaceAll('/', '__'))

const fetchExpectedSize = async (url) => {
  const response = await fetch(url, { method: 'HEAD', headers: { 'User-Agent': USER_AGENT } })
  if (!response.ok) {
    throw new Error(`head failed: ${response.status} ${url}`)
  }
  const contentLength = response.headers.get('content-length')
  return contentLength != null ? Number(contentLength) : null
}

const downloadToFile = async (url, destPath, expectedSize) => {
  const response = await fetch(url, { headers: { 'User-Agent': USER_AGENT } })
  if (!response.ok) {
    throw new Error(`status: ${response.status} ${url}`)
  }
  if (!response.body) {
    throw new Error(`no body: ${url}`)
  }

  const headerSize = response.headers.get('content-length')
  const expected = headerSize != null ? Number(headerSize) : expectedSize
  const tmpPath = `${destPath}.part`
  fs.mkdirSync(path.dirname(destPath), { recursive: true })
  if (fs.existsSync(tmpPath)) {
    fs.unlinkSync(tmpPath)
  }

  await pipeline(Readable.fromWeb(response.body), fs.createWriteStream(tmpPath))
  const written = fs.statSync(tmpPath).size
  if (expected != null && written !== expected) {
    fs.unlinkSync(tmpPath)
    throw new Error(`incomplete download: ${written}/${expected} bytes ${url}`)
  }

  fs.renameSync(tmpPath, destPath)
  return written
}

const ensureDownloaded = async (url, localPath) => {
  const expectedSize = await fetchExpectedSize(url)
  if (fs.existsSync(localPath)) {
    const { size } = fs.statSync(localPath)
    if (expectedSize == null || size === expectedSize) {
      return { cached: true, bytes: size }
    }
    fs.unlinkSync(localPath)
  }

  const bytes = await downloadToFile(url, localPath, expectedSize)
  return { cached: false, bytes }
}

const hashLocalFile = async (filePath, { gunzip = false } = {}) =>
  hashStream(fs.createReadStream(filePath), { gunzip })

const hashFileOnce = async (url, key, cacheDir) => {
  const localPath = cacheFilePath(cacheDir, key)
  const { cached, bytes } = await ensureDownloaded(url, localPath)
  console.log(cached ? 'using download' : 'downloaded', key, `${(bytes / (1024 * 1024)).toFixed(1)} MB`)
  console.log('hashing', key)
  const digest = await hashLocalFile(localPath, { gunzip: isGzipUrl(url) })
  fs.unlinkSync(localPath)
  return digest
}

const isNonRetryableError = (e) =>
  /status: (?:40[134]|410|451)\b/.test(e.message)

const withRetries = async (label, fn) => {
  let lastError
  for (let attempt = 1; attempt <= HASH_RETRIES; attempt++) {
    try {
      return await fn()
    } catch (e) {
      lastError = e
      if (isNonRetryableError(e)) {
        throw e
      }
      const cause = e.cause ? ` (${e.cause.message || e.cause})` : ''
      console.error(`retry ${attempt}/${HASH_RETRIES}`, label, `${e.message}${cause}`)
      if (attempt < HASH_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt)
      }
    }
  }
  throw lastError
}

const hashFile = async (url, key, cacheDir) =>
  withRetries(url, () => hashFileOnce(url, key, cacheDir))

const loadDoneKeys = (filePath) => {
  const done = new Set()
  if (!fs.existsSync(filePath)) {
    return done
  }
  const fd = fs.openSync(filePath, 'r')
  const bufSize = 1024 * 1024
  const buf = Buffer.alloc(bufSize)
  let leftover = ''
  try {
    while (true) {
      const bytesRead = fs.readSync(fd, buf, 0, bufSize, null)
      if (bytesRead === 0) {
        break
      }
      leftover += buf.toString('utf8', 0, bytesRead)
      const lines = leftover.split('\n')
      leftover = lines.pop()
      for (const line of lines) {
        if (!line) {
          continue
        }
        const [key, digest] = line.split('\t')
        if (key && digest) {
          done.add(key)
        }
      }
    }
    if (leftover) {
      const [key, digest] = leftover.split('\t')
      if (key && digest) {
        done.add(key)
      }
    }
  } finally {
    fs.closeSync(fd)
  }
  return done
}

const appendHash = (filePath, key, digest) => {
  fs.appendFileSync(filePath, `${key}\t${digest}\n`)
}

const formatDuration = (ms) => {
  const totalSec = Math.max(0, Math.round(ms / 1000))
  const h = Math.floor(totalSec / 3600)
  const m = Math.floor((totalSec % 3600) / 60)
  const s = totalSec % 60
  if (h > 0) {
    return `${h}h ${m}m ${s}s`
  }
  if (m > 0) {
    return `${m}m ${s}s`
  }
  return `${s}s`
}

export const collectHumanGenomeHashes = async (outputPath = DEFAULT_OUTPUT) => {
  const done = loadDoneKeys(outputPath)
  const cacheDir = sidecarPath(outputPath, 'cache')
  console.log('already hashed:', done.size)

  const files = (await spiderFiles(ROOT_URLS)).filter((url) => !isIndexUrl(url))
  const todoFiles = files.filter(url => !done.has(relativeKey(url)))
  console.log('listed:', files.length, 'todo:', todoFiles.length)

  const start = Date.now()
  let hashed = 0
  let attempted = 0
  const skipped = files.length - todoFiles.length

  for (const url of todoFiles) {
    const key = relativeKey(url)
    attempted++
    console.log(`fetching ${attempted}/${todoFiles.length}`, key)
    try {
      const digest = await hashFile(url, key, cacheDir)
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

const main = async () => {
  const args = minimist(process.argv.slice(2), {
    default: { output: DEFAULT_OUTPUT },
    alias: { o: 'output' }
  })

  const hashed = await collectHumanGenomeHashes(args.output)
  console.log('hashed this run:', hashed)
}

if (esMain(import.meta)) {
  main()
}
