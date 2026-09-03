import path from 'node:path'
import { Readable } from 'node:stream'
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
const FETCH_RETRIES = 5
const RETRY_DELAY_MS = 10000

const downloadUrl = (deliveryId, itemId) =>
  `${API_BASE}/public/products/${PRODUCT_ID}/delivery/${deliveryId}/item/${itemId}/download`

const isPdfPath = (p) => /\.pdf$/i.test(p)
const isZipPath = (p) => /\.zip$/i.test(p)
const isTarPath = (p) => /\.tar$/i.test(p)

const pdfKey = (entryPath) => path.basename(entryPath).replace(/\.pdf$/i, '')

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

const openDownloadStream = async (item) => {
  const url = downloadUrl(item.deliveryId, item.itemId)
  const response = await fetch(url, { headers: { 'User-Agent': USER_AGENT } })
  if (!response.ok) {
    throw new Error(`status: ${response.status} ${url}`)
  }
  if (!response.body) {
    throw new Error(`no body: ${url}`)
  }
  return Readable.fromWeb(response.body)
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

const hashPdfsFromDownload = async (item, outputPath, done) => {
  const input = await openDownloadStream(item)
  if (isZipPath(item.itemName)) {
    return hashPdfsInZipStream(input, outputPath, done)
  }
  if (isTarPath(item.itemName)) {
    return hashPdfsInTarStream(input, outputPath, done)
  }
  throw new Error(`unsupported archive type: ${item.itemName}`)
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
  limit = Infinity,
  start = 0
} = {}) => {
  const completedPath = sidecarPath(outputPath, 'completed.txt')
  const done = loadDoneKeys(outputPath)
  const completed = loadLineSet(completedPath)
  console.log('already hashed pdfs:', done.size)
  console.log('completed archives:', completed.size)

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
        () => hashPdfsFromDownload(item, outputPath, done),
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
    default: { output: DEFAULT_OUTPUT, limit: 0, start: 0 },
    alias: { o: 'output', n: 'limit' },
    string: ['output']
  })
  const limit = args.limit > 0 ? args.limit : Infinity
  const hashed = await collectEpoHashes({
    outputPath: args.output,
    limit,
    start: args.start
  })
  console.log('hashed this run:', hashed)
}

if (esMain(import.meta)) {
  main()
}
