import fs from 'node:fs'
import path from 'node:path'
import { spawn } from 'node:child_process'
import minimist from 'minimist'
import esMain from 'es-main'
import {
  appendHash,
  appendLine,
  formatDuration,
  loadDoneKeys,
  loadLineSet,
  sidecarPath,
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

const workDirFor = (cacheDir, item) =>
  path.join(cacheDir, `work_${item.itemId}`)

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

const runCommand = (command, args) =>
  new Promise((resolve, reject) => {
    console.log('+', command, args.join(' '))
    const child = spawn(command, args, { stdio: ['ignore', 'inherit', 'inherit'] })
    child.on('error', reject)
    child.on('close', (code, signal) => {
      if (signal) {
        reject(new Error(`${command} killed (${signal})`))
        return
      }
      resolve(code ?? 1)
    })
  })

const runCommandCapture = (command, args) =>
  new Promise((resolve, reject) => {
    const chunks = []
    const errChunks = []
    const child = spawn(command, args, { stdio: ['ignore', 'pipe', 'pipe'] })
    child.stdout.on('data', (d) => chunks.push(d))
    child.stderr.on('data', (d) => errChunks.push(d))
    child.on('error', reject)
    child.on('close', (code, signal) => {
      if (signal) {
        reject(new Error(`${command} killed (${signal})`))
        return
      }
      const stdout = Buffer.concat(chunks).toString('utf8')
      const stderr = Buffer.concat(errChunks).toString('utf8')
      if (code !== 0) {
        reject(new Error(`${command} exit ${code}: ${stderr || stdout}`))
        return
      }
      resolve(stdout)
    })
  })

/** Prefer sha256sum (Linux); fall back to shasum -a 256 (macOS). */
let sha256Tool = null
const getSha256Tool = async () => {
  if (sha256Tool) {
    return sha256Tool
  }
  try {
    await runCommandCapture('sha256sum', ['/dev/null'])
    sha256Tool = { command: 'sha256sum', argsFor: (file) => [file] }
    return sha256Tool
  } catch {
    // ignore
  }
  try {
    await runCommandCapture('shasum', ['-a', '256', '/dev/null'])
    sha256Tool = { command: 'shasum', argsFor: (file) => ['-a', '256', file] }
    return sha256Tool
  } catch {
    // ignore
  }
  throw new Error('neither sha256sum nor shasum found')
}

const sha256File = async (filePath) => {
  const tool = await getSha256Tool()
  const stdout = await runCommandCapture(tool.command, tool.argsFor(filePath))
  const digest = stdout.trim().split(/\s+/)[0]
  if (!/^[0-9a-f]{64}$/i.test(digest)) {
    throw new Error(`bad sha256 output for ${filePath}: ${stdout.trim()}`)
  }
  return digest.toLowerCase()
}

const runCurl = (args) => runCommand('curl', args)

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

const removePath = async (target) => {
  try {
    await fs.promises.rm(target, { recursive: true, force: true })
    return true
  } catch (e) {
    console.warn('could not remove', target, e.message)
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
    if (expected == null && code === 0 && localFileSize(destPath) > 0) {
      console.log('download complete', localFileSize(destPath), 'bytes')
      return
    }
    console.warn(
      `resume incomplete (curl exit ${code}, size ${localFileSize(destPath)}` +
      `${expected != null ? `, expected ${expected}` : ''}); re-downloading from scratch`
    )
    await removePath(destPath)
  }

  console.log('downloading ->', destPath)
  const code = await runCurl(curlDownloadArgs(url, destPath, { resume: false }))
  if (code !== 0) {
    await removePath(destPath)
    throw new Error(`curl exit ${code}`)
  }

  const size = localFileSize(destPath)
  if (size <= 0) {
    await removePath(destPath)
    throw new Error(`empty download: ${destPath}`)
  }
  if (expected != null && size !== expected) {
    await removePath(destPath)
    throw new Error(`size mismatch: got ${size}, expected ${expected}`)
  }
  console.log('download complete', size, 'bytes')
}

async function * walkFiles (dir) {
  const entries = await fs.promises.readdir(dir, { withFileTypes: true })
  for (const ent of entries) {
    const full = path.join(dir, ent.name)
    if (ent.isDirectory()) {
      yield * walkFiles(full)
    } else if (ent.isFile()) {
      yield full
    }
  }
}

const extractZip = async (zipPath, destDir) => {
  await fs.promises.mkdir(destDir, { recursive: true })
  // unzip: 0 = ok, 1 = success with warnings (e.g. backslashes)
  const code = await runCommand('unzip', ['-q', '-o', zipPath, '-d', destDir])
  if (code > 1) {
    throw new Error(`unzip exit ${code}: ${zipPath}`)
  }
}

const extractTar = async (tarPath, destDir) => {
  await fs.promises.mkdir(destDir, { recursive: true })
  const code = await runCommand('tar', ['-xf', tarPath, '-C', destDir])
  if (code !== 0) {
    throw new Error(`tar exit ${code}: ${tarPath}`)
  }
}

const extractArchive = async (archivePath, workDir) => {
  if (isZipPath(archivePath)) {
    await extractZip(archivePath, workDir)
    return
  }
  if (isTarPath(archivePath)) {
    await extractTar(archivePath, workDir)
    return
  }
  throw new Error(`unsupported archive type: ${archivePath}`)
}

/** Unzip nested .zip files in place until none remain. Skip corrupt zips. */
const expandNestedZips = async (workDir) => {
  for (;;) {
    const zips = []
    for await (const filePath of walkFiles(workDir)) {
      if (isZipPath(filePath)) {
        zips.push(filePath)
      }
    }
    if (zips.length === 0) {
      return
    }
    console.log('expanding nested zips:', zips.length)
    for (const zipPath of zips) {
      const destDir = zipPath.replace(/\.zip$/i, '')
      try {
        await extractZip(zipPath, destDir)
      } catch (e) {
        // EPO packages sometimes include truncated per-doc zips; don't fail the archive.
        console.warn('skipping corrupt nested zip:', zipPath, e.message)
      }
      await removePath(zipPath)
    }
  }
}

const hashPdfFile = async (filePath, outputPath, done) => {
  const key = pdfKey(filePath)
  if (done.has(key)) {
    return false
  }
  const digest = await sha256File(filePath)
  appendHash(outputPath, key, digest)
  done.add(key)
  console.log(key, digest)
  return true
}

const hashPdfsInWorkDir = async (workDir, outputPath, done) => {
  let hashed = 0
  for await (const filePath of walkFiles(workDir)) {
    if (!isPdfPath(filePath)) {
      continue
    }
    if (await hashPdfFile(filePath, outputPath, done)) {
      hashed++
    }
  }
  return hashed
}

const hashPdfsFromArchiveFile = async (archivePath, workDir, outputPath, done) => {
  await removePath(workDir)
  console.log('extracting', archivePath, '->', workDir)
  await extractArchive(archivePath, workDir)
  await expandNestedZips(workDir)
  const hashed = await hashPdfsInWorkDir(workDir, outputPath, done)
  await removePath(workDir)
  console.log('removed work dir', workDir)
  return hashed
}

const hashPdfsFromDownload = async (item, outputPath, done, cacheDir) => {
  const url = downloadUrl(item.deliveryId, item.itemId)
  const destPath = cachePathFor(cacheDir, item)
  const workDir = workDirFor(cacheDir, item)
  await downloadWithCurl(url, destPath)
  try {
    const hashed = await hashPdfsFromArchiveFile(destPath, workDir, outputPath, done)
    if (await removePath(destPath)) {
      console.log('removed cache file', destPath)
    }
    return hashed
  } catch (e) {
    await removePath(workDir)
    // Keep a size-complete archive for extract-only retries; drop obvious junk.
    console.warn('extract/hash failed; keeping archive for retry:', destPath)
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
