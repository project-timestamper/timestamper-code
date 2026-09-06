import fs from 'node:fs'
import path from 'node:path'
import { spawn } from 'node:child_process'
import minimist from 'minimist'
import esMain from 'es-main'
import * as tar from 'tar'
import unzipper from 'unzipper'
import {
  appendHash,
  appendLine,
  drainStream,
  formatDuration,
  hashStream,
  loadDoneKeys,
  loadLineSet,
  sidecarPath,
  streamToBuffer,
  withRetries
} from './util.js'

const API_BASE = 'https://publication-bdds.apps.epo.org/bdds/bdds-bff-service/prod/api'
const PRODUCT_ID = 32
const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
const DEFAULT_OUTPUT = 'epo_hashes.txt'
const DEFAULT_CACHE_DIR = 'epo_cache'
const FETCH_RETRIES = 5
const RETRY_DELAY_MS = 10000

const downloadUrl = (deliveryId, itemId) =>
  `${API_BASE}/public/products/${PRODUCT_ID}/delivery/${deliveryId}/item/${itemId}/download`

const isPdfPath = (p) => /\.pdf$/i.test(p)
const isZipPath = (p) => /\.zip$/i.test(p)
const isTarPath = (p) => /\.tar$/i.test(p)

const pdfKey = (entryPath) => path.basename(entryPath).replace(/\.pdf$/i, '')

const cachePathFor = (cacheDir, item) => {
  const safeName = path.basename(item.itemName).replace(/[^\w.-]+/g, '_')
  return path.join(cacheDir, `${item.itemId}_${safeName}`)
}

const listDeliveryItems = async () => {
  const response = await fetch(`${API_BASE}/public/products/${PRODUCT_ID}`, {
    headers: { Accept: 'application/json', 'User-Agent': USER_AGENT }
  })
  if (!response.ok) {
    throw new Error(`catalog failed: ${response.status}`)
  }
  const product = await response.json()
  const items = []
  for (const delivery of product.deliveries || []) {
    for (const item of delivery.items || []) {
      items.push({
        deliveryId: delivery.deliveryId,
        itemId: item.itemId,
        itemName: item.itemName,
        fileSize: item.fileSize
      })
    }
  }
  return { name: product.name, items }
}

const runCurl = (args) =>
  new Promise((resolve, reject) => {
    const child = spawn('curl', args, { stdio: ['ignore', 'inherit', 'inherit'] })
    child.on('error', reject)
    child.on('close', (code, signal) => {
      if (signal) {
        reject(new Error(`curl killed (${signal})`))
        return
      }
      resolve(code ?? 1)
    })
  })

/** Content-Length after redirects, or null if missing. */
const getContentLength = async (url) => {
  const headers = await new Promise((resolve, reject) => {
    const chunks = []
    const child = spawn(
      'curl',
      ['-sI', '-L', '-A', USER_AGENT, '--connect-timeout', '30', url],
      { stdio: ['ignore', 'pipe', 'pipe'] }
    )
    child.stdout.on('data', (d) => chunks.push(d))
    child.stderr.on('data', () => {})
    child.on('error', reject)
    child.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`curl HEAD exit ${code}`))
        return
      }
      resolve(Buffer.concat(chunks).toString('utf8'))
    })
  })
  const matches = [...headers.matchAll(/content-length:\s*(\d+)/gi)]
  if (matches.length === 0) {
    return null
  }
  return Number(matches[matches.length - 1][1])
}

const localFileSize = (destPath) =>
  (fs.existsSync(destPath) ? fs.statSync(destPath).size : 0)

const removeCacheFile = async (destPath) => {
  try {
    await fs.promises.unlink(destPath)
    return true
  } catch (e) {
    if (e.code === 'ENOENT') {
      return false
    }
    console.warn('could not remove cache file', destPath, e.message)
    return false
  }
}

const curlDownloadArgs = (url, destPath, { resume } = { resume: false }) => {
  const args = [
    '-fL',
    '--retry', '5',
    '--retry-delay', '5',
    '-A', USER_AGENT,
    '--connect-timeout', '30'
  ]
  if (resume) {
    args.push('-C', '-')
  }
  args.push('-o', destPath, url)
  return args
}

/**
 * Download an archive with curl. Prefer -C - resume when possible; if the server
 * rejects ranges (curl 33) or the file is still short of Content-Length, delete
 * the partial and download from scratch.
 */
const downloadWithCurl = async (url, destPath) => {
  await fs.promises.mkdir(path.dirname(destPath), { recursive: true })

  let expected = null
  try {
    expected = await getContentLength(url)
  } catch (e) {
    console.warn('could not read Content-Length:', e.message)
  }
  if (expected != null) {
    console.log('expected size', expected, 'bytes')
  }

  const isComplete = () => {
    const size = localFileSize(destPath)
    if (size <= 0) {
      return false
    }
    if (expected != null) {
      return size === expected
    }
    return false
  }

  if (isComplete()) {
    console.log('cache already complete', destPath)
    return
  }

  const sizeBefore = localFileSize(destPath)
  if (sizeBefore > 0) {
    console.log('resuming download from', sizeBefore, 'bytes ->', destPath)
    const code = await runCurl(curlDownloadArgs(url, destPath, { resume: true }))
    if (isComplete()) {
      console.log('download complete', localFileSize(destPath), 'bytes')
      return
    }
    // No Content-Length: trust a clean curl exit (ranges worked and finished).
    if (expected == null && code === 0 && localFileSize(destPath) > 0) {
      console.log('download complete', localFileSize(destPath), 'bytes')
      return
    }
    // curl 33 = ranges not supported / 416 on incomplete file; or still short.
    console.warn(
      `resume incomplete (curl exit ${code}, size ${localFileSize(destPath)}` +
      `${expected != null ? `, expected ${expected}` : ''}); re-downloading from scratch`
    )
    await removeCacheFile(destPath)
  }

  console.log('downloading ->', destPath)
  const code = await runCurl(curlDownloadArgs(url, destPath, { resume: false }))
  if (code !== 0) {
    await removeCacheFile(destPath)
    throw new Error(`curl exit ${code}`)
  }

  const size = localFileSize(destPath)
  if (size <= 0) {
    await removeCacheFile(destPath)
    throw new Error(`empty download: ${destPath}`)
  }
  if (expected != null && size !== expected) {
    await removeCacheFile(destPath)
    throw new Error(`size mismatch: got ${size}, expected ${expected}`)
  }
  console.log('download complete', size, 'bytes')
}

const recordPdf = async (entryPath, body, outputPath, done) => {
  const key = pdfKey(entryPath)
  if (done.has(key)) {
    await drainStream(body)
    return false
  }
  const digest = await hashStream(body)
  appendHash(outputPath, key, digest)
  done.add(key)
  console.log(key, digest)
  return true
}

const hashPdfsInZipBuffer = async (zipBuf, outputPath, done) => {
  let hashed = 0
  const directory = await unzipper.Open.buffer(zipBuf)
  for (const entry of directory.files) {
    if (entry.type === 'Directory') {
      continue
    }
    if (isPdfPath(entry.path)) {
      if (await recordPdf(entry.path, entry.stream(), outputPath, done)) {
        hashed++
      }
    } else if (isZipPath(entry.path)) {
      const nestedBuf = await streamToBuffer(entry.stream())
      hashed += await hashPdfsInZipBuffer(nestedBuf, outputPath, done)
    }
  }
  return hashed
}

// Stream a zip body from HTTP and hash PDFs; nested zips are buffered in memory.
const hashPdfsInZipStream = async (input, outputPath, done) => {
  let hashed = 0
  let chain = Promise.resolve()
  let failed = null

  await new Promise((resolve, reject) => {
    const parser = unzipper.Parse()
    input.pipe(parser)

    parser.on('entry', (entry) => {
      chain = chain.then(async () => {
        if (failed) {
          entry.autodrain()
          return
        }
        try {
          if (isPdfPath(entry.path)) {
            if (await recordPdf(entry.path, entry, outputPath, done)) {
              hashed++
            }
            return
          }
          if (isZipPath(entry.path)) {
            const nestedBuf = await streamToBuffer(entry)
            hashed += await hashPdfsInZipBuffer(nestedBuf, outputPath, done)
            return
          }
          entry.autodrain()
        } catch (e) {
          failed = e
          try {
            entry.autodrain()
          } catch {
            // ignore
          }
        }
      })
    })

    parser.on('finish', () => {
      chain.then(() => (failed ? reject(failed) : resolve()), reject)
    })
    parser.on('error', reject)
    input.on('error', reject)
  })

  return hashed
}

const hashPdfsInTarStream = async (input, outputPath, done) => {
  let hashed = 0
  let chain = Promise.resolve()
  let failed = null
  const parser = new tar.Parser()

  await new Promise((resolve, reject) => {
    parser.on('entry', (entry) => {
      input.pause()
      chain = chain.then(async () => {
        if (failed) {
          entry.resume()
          input.resume()
          return
        }
        try {
          if (entry.type !== 'File') {
            entry.resume()
            return
          }
          if (isPdfPath(entry.path)) {
            if (await recordPdf(entry.path, entry, outputPath, done)) {
              hashed++
            }
            return
          }
          if (isZipPath(entry.path)) {
            const nestedBuf = await streamToBuffer(entry)
            hashed += await hashPdfsInZipBuffer(nestedBuf, outputPath, done)
            return
          }
          entry.resume()
        } catch (e) {
          failed = e
          try {
            entry.resume()
          } catch {
            // ignore
          }
        } finally {
          input.resume()
        }
      })
    })

    const finish = () => {
      chain.then(() => (failed ? reject(failed) : resolve()), reject)
    }

    parser.on('end', finish)
    parser.on('finish', finish)
    parser.on('error', reject)
    input.on('error', reject)
    input.pipe(parser)
  })

  return hashed
}

const hashPdfsFromFile = async (filePath, itemName, outputPath, done) => {
  const input = fs.createReadStream(filePath)
  try {
    if (isZipPath(itemName)) {
      return await hashPdfsInZipStream(input, outputPath, done)
    }
    if (isTarPath(itemName)) {
      return await hashPdfsInTarStream(input, outputPath, done)
    }
    throw new Error(`unsupported archive type: ${itemName}`)
  } finally {
    await new Promise((resolve) => {
      if (input.destroyed) {
        resolve()
        return
      }
      input.once('close', resolve)
      input.destroy()
    })
  }
}

const hashPdfsFromDownload = async (item, outputPath, done, cacheDir) => {
  const url = downloadUrl(item.deliveryId, item.itemId)
  const destPath = cachePathFor(cacheDir, item)
  await downloadWithCurl(url, destPath)
  try {
    const hashed = await hashPdfsFromFile(destPath, item.itemName, outputPath, done)
    // Free disk once every PDF in the archive has been hashed.
    if (await removeCacheFile(destPath)) {
      console.log('removed cache file', destPath)
    }
    return hashed
  } catch (e) {
    // Corrupt or truncated archive: drop cache so the next retry is a clean fetch.
    console.warn('parse failed; removing cache file', destPath)
    await removeCacheFile(destPath)
    throw e
  }
}

const parseSizeGB = (fileSize) => {
  const m = String(fileSize).match(/([\d.]+)\s*(GB|MB|KB|B)/i)
  if (!m) {
    return Number.POSITIVE_INFINITY
  }
  const n = Number(m[1])
  const unit = m[2].toUpperCase()
  if (unit === 'GB') {
    return n
  }
  if (unit === 'MB') {
    return n / 1024
  }
  if (unit === 'KB') {
    return n / (1024 * 1024)
  }
  return n / (1024 ** 3)
}

export const collectEpoHashes = async ({
  outputPath = DEFAULT_OUTPUT,
  cacheDir = DEFAULT_CACHE_DIR,
  limit = Infinity,
  start = 0
} = {}) => {
  const completedPath = sidecarPath(outputPath, 'completed.txt')
  const done = loadDoneKeys(outputPath)
  const completed = loadLineSet(completedPath)
  console.log('already hashed pdfs:', done.size)
  console.log('completed archives:', completed.size)
  console.log('cache dir:', cacheDir)

  const { name, items } = await listDeliveryItems()
  console.log('product:', name)
  console.log('catalog archives:', items.length)

  items.sort((a, b) => parseSizeGB(a.fileSize) - parseSizeGB(b.fileSize))

  const todo = items
    .filter((item) => !completed.has(String(item.itemId)))
    .filter((item) => isZipPath(item.itemName) || isTarPath(item.itemName))
    .slice(start, start + (Number.isFinite(limit) ? limit : items.length))
  console.log('todo archives:', todo.length)

  const runStart = Date.now()
  let archivesDone = 0
  let pdfsHashed = 0

  for (const item of todo) {
    archivesDone++
    console.log(
      `archive ${archivesDone}/${todo.length}`,
      item.itemName,
      item.fileSize,
      `(delivery ${item.deliveryId}, item ${item.itemId})`
    )
    try {
      const n = await withRetries(
        item.itemName,
        () => hashPdfsFromDownload(item, outputPath, done, cacheDir),
        { retries: FETCH_RETRIES, delayMs: RETRY_DELAY_MS }
      )
      pdfsHashed += n
      appendLine(completedPath, item.itemId)
      completed.add(String(item.itemId))
      console.log('pdfs hashed from archive:', n)

      const elapsed = Date.now() - runStart
      const remaining = todo.length - archivesDone
      const eta = archivesDone > 0 ? remaining * (elapsed / archivesDone) : 0
      console.log(
        `progress: ${pdfsHashed} pdfs this run, ${archivesDone}/${todo.length} archives, ` +
        `elapsed ${formatDuration(elapsed)}, eta ${formatDuration(eta)}`
      )
    } catch (e) {
      console.error('archive failed', item.itemName, e.message)
    }
  }

  return pdfsHashed
}

const main = async () => {
  const args = minimist(process.argv.slice(2), {
    default: { output: DEFAULT_OUTPUT, cache: DEFAULT_CACHE_DIR, limit: 0, start: 0 },
    alias: { o: 'output', n: 'limit', c: 'cache' },
    string: ['output', 'cache']
  })
  const limit = args.limit > 0 ? args.limit : Infinity
  const hashed = await collectEpoHashes({
    outputPath: args.output,
    cacheDir: args.cache,
    limit,
    start: args.start
  })
  console.log('hashed this run:', hashed)
}

if (esMain(import.meta)) {
  main()
}
