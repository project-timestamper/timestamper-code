import fs from 'node:fs'
import minimist from 'minimist'
import esMain from 'es-main'
import bencode from 'bencode'
import {
  appendLine,
  formatDuration,
  loadLineSet,
  sidecarPath,
  streamToBuffer,
  withRetries
} from './util.js'
import { fetchTorrentsIndex, normalizeEntry } from './annas_archive_torrents.js'

/**
 * Download Anna's Archive .torrent files and extract per-content-file MD5s
 * (not torrent piece hashes).
 *
 * MD5 sources:
 *   - explicit `md5sum` / `md5` fields (zlib)
 *   - 32-hex filename / path leaf (most Libgen collections)
 *
 * Output (tab-delimited):
 *   url \t path-inside-torrent \t md5
 *
 * Resume: torrent URLs already processed are tracked in a sidecar file so
 * torrents with zero MD5s are not re-downloaded forever.
 *
 * By default only collections known to carry content MD5s are processed.
 * Pass --all to walk every torrent in the index.
 *
 * Usage:
 *   node annas_hashes.js
 *   node annas_hashes.js --output annas_content_hashes.txt
 *   node annas_hashes.js --base https://annas-archive.gd --limit 20
 *   node annas_hashes.js --all
 */

const DEFAULT_BASE = 'https://annas-archive.gd'
const DEFAULT_OUTPUT = 'annas_content_hashes.txt'
const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
const FETCH_RETRIES = 5
const RETRY_DELAY_MS = 10000

/** URL path substrings for collections with extractable content MD5s. */
const DEFAULT_COLLECTION_MATCHERS = [
  '/managed_by_aa/zlib/',
  '/external/libgen_li_comics/',
  '/external/libgen_li_fic/',
  '/external/libgen_li_magazines/',
  '/external/libgen_li_non_fic/',
  '/external/libgen_li_standarts/',
  '/external/libgen_rs_fic/',
  '/external/libgen_rs_non_fic/'
]

const headers = { 'User-Agent': USER_AGENT }

const HEX_RE = /^[0-9a-f]+$/i

const bufferToUtf8 = (buf) => {
  try {
    return buf.toString('utf8')
  } catch {
    return null
  }
}

/** Normalize a bencoded hash field to lowercase hex, or null. */
const normalizeHashValue = (value) => {
  if (value == null) {
    return null
  }
  if (Buffer.isBuffer(value)) {
    const asText = bufferToUtf8(value)
    if (asText && HEX_RE.test(asText) && [32, 40, 64].includes(asText.length)) {
      return asText.toLowerCase()
    }
    if ([16, 20, 32].includes(value.length)) {
      return value.toString('hex')
    }
    return null
  }
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (HEX_RE.test(trimmed) && [32, 40, 64].includes(trimmed.length)) {
      return trimmed.toLowerCase()
    }
  }
  return null
}

const keyLooksLike = (key, names) => names.includes(key.toLowerCase())

/**
 * Pull content digests from a torrent file dict (md5sum / sha1 / sha256, etc.).
 * Ignores piece hashes — those live on info.pieces / file tree piece layers.
 */
const extractContentHashes = (fileDict) => {
  const hashes = {
    md5: null,
    md5Source: null,
    sha1: null,
    sha256: null,
    other: []
  }

  for (const [rawKey, rawValue] of Object.entries(fileDict)) {
    const key = rawKey.toString()
    const lower = key.toLowerCase()
    if (lower === 'pieces' || lower === 'piece layers' || lower === 'pieces root') {
      continue
    }
    const hex = normalizeHashValue(rawValue)
    if (!hex) {
      continue
    }
    if (keyLooksLike(key, ['md5', 'md5sum'])) {
      hashes.md5 = hex
      hashes.md5Source = 'field'
    } else if (keyLooksLike(key, ['sha1', 'sha-1'])) {
      hashes.sha1 = hex
    } else if (keyLooksLike(key, ['sha256', 'sha-256', 'sha2'])) {
      hashes.sha256 = hex
    } else if (
      lower.includes('md5') ||
      lower.includes('sha') ||
      lower.includes('hash') ||
      lower.includes('digest')
    ) {
      hashes.other.push(`${key}=${hex}`)
    }
  }

  return hashes
}

/** Libgen-style torrents often use the MD5 as the filename (optionally with ext). */
const md5FromPath = (pathStr) => {
  if (!pathStr) {
    return null
  }
  const parts = pathStr.split('/')
  const leaf = parts[parts.length - 1]
  if (HEX_RE.test(leaf) && leaf.length === 32) {
    return leaf.toLowerCase()
  }
  const stem = leaf.replace(/\.[^.]+$/, '')
  if (HEX_RE.test(stem) && stem.length === 32) {
    return stem.toLowerCase()
  }
  return null
}

const filePathString = (fileDict, torrentName) => {
  const pathValue = fileDict.path || fileDict['path.utf-8']
  let relative
  if (Array.isArray(pathValue)) {
    relative = pathValue.map((p) => p.toString()).join('/')
  } else if (pathValue != null) {
    relative = pathValue.toString()
  } else if (fileDict.name != null) {
    relative = fileDict.name.toString()
  } else {
    relative = ''
  }
  if (!torrentName) {
    return relative
  }
  if (!relative) {
    return torrentName
  }
  if (relative === torrentName) {
    return relative
  }
  return `${torrentName}/${relative}`
}

const listTorrentFiles = (decoded) => {
  const info = decoded.info
  if (info == null) {
    return { torrentName: '', files: [] }
  }
  const torrentName = info.name != null ? info.name.toString() : ''
  if (Array.isArray(info.files)) {
    return { torrentName, files: info.files }
  }
  return { torrentName, files: [info] }
}

const matchesDefaultCollections = (url) =>
  DEFAULT_COLLECTION_MATCHERS.some((needle) => url.includes(needle))

const fetchTorrentBuffer = async (url) => {
  return withRetries(`fetch torrent ${url}`, async () => {
    const response = await fetch(url, { headers })
    if (!response.ok) {
      throw new Error(`status: ${response.status} ${url}`)
    }
    return streamToBuffer(response.body)
  }, { retries: FETCH_RETRIES, delayMs: RETRY_DELAY_MS })
}

const analyzeTorrentFiles = (decoded) => {
  const { torrentName, files } = listTorrentFiles(decoded)
  const rows = []
  let withMd5 = 0
  let withoutMd5 = 0
  let md5FromField = 0
  let md5FromPathCount = 0
  let withSha1 = 0
  let withSha256 = 0
  const otherKeys = new Set()

  for (const fileDict of files) {
    const path = filePathString(fileDict, torrentName)
    const hashes = extractContentHashes(fileDict)
    if (!hashes.md5) {
      const pathMd5 = md5FromPath(path)
      if (pathMd5) {
        hashes.md5 = pathMd5
        hashes.md5Source = 'path'
      }
    }
    if (hashes.md5) {
      withMd5++
      if (hashes.md5Source === 'field') md5FromField++
      if (hashes.md5Source === 'path') md5FromPathCount++
      rows.push({ path, md5: hashes.md5 })
    } else {
      withoutMd5++
    }
    if (hashes.sha1) withSha1++
    if (hashes.sha256) withSha256++
    for (const other of hashes.other) {
      otherKeys.add(other.split('=')[0])
    }
  }

  return {
    rows,
    withMd5,
    withoutMd5,
    md5FromField,
    md5FromPath: md5FromPathCount,
    withSha1,
    withSha256,
    otherKeys: [...otherKeys],
    fileCount: files.length
  }
}

const run = async (argv = process.argv.slice(2)) => {
  const args = minimist(argv, {
    string: ['output', 'base'],
    boolean: ['help', 'all'],
    alias: { o: 'output', b: 'base', h: 'help', n: 'limit', a: 'all' },
    default: {
      output: DEFAULT_OUTPUT,
      base: DEFAULT_BASE,
      all: false
    }
  })

  if (args.help) {
    console.log(
      'Usage: node annas_hashes.js [--base URL] [--output FILE] [--limit N] [--all]'
    )
    return
  }

  const base = args.base.replace(/\/$/, '')
  const outputPath = args.output
  const processedPath = sidecarPath(outputPath, 'processed_urls.txt')
  const limit = args.limit != null ? Number(args.limit) : null
  const processAll = Boolean(args.all)
  const started = Date.now()

  console.log('fetching torrent index')
  const entries = await fetchTorrentsIndex(base)
  console.log('index entries', entries.length)
  console.log(
    processAll
      ? 'processing all collections'
      : `processing MD5 collections: ${DEFAULT_COLLECTION_MATCHERS.join(', ')}`
  )

  const done = loadLineSet(processedPath)
  console.log('already processed', done.size, 'torrent urls in', processedPath)

  let torrentsProcessed = 0
  let torrentsSkipped = 0
  let torrentsFiltered = 0
  let torrentsInvalid = 0
  let torrentsFailed = 0
  let rowsWritten = 0
  let filesWithMd5 = 0
  let filesWithoutMd5 = 0
  let filesWithSha1 = 0
  let filesWithSha256 = 0

  for (const entry of entries) {
    if (limit != null && torrentsProcessed >= limit) {
      break
    }
    const row = normalizeEntry(entry, base)
    if (!row) {
      torrentsInvalid++
      continue
    }
    if (!processAll && !matchesDefaultCollections(row.url)) {
      torrentsFiltered++
      continue
    }
    if (done.has(row.url)) {
      torrentsSkipped++
      continue
    }

    let buffer
    try {
      buffer = await fetchTorrentBuffer(row.url)
    } catch (err) {
      torrentsFailed++
      console.error('failed torrent', row.url, err.message)
      continue
    }

    let decoded
    try {
      decoded = bencode.decode(buffer)
    } catch (err) {
      torrentsFailed++
      console.error('bencode failed', row.url, err.message)
      continue
    }

    const analysis = analyzeTorrentFiles(decoded)
    for (const fileRow of analysis.rows) {
      appendLine(outputPath, `${row.url}\t${fileRow.path}\t${fileRow.md5}`)
      rowsWritten++
    }
    appendLine(processedPath, row.url)
    done.add(row.url)
    torrentsProcessed++
    filesWithMd5 += analysis.withMd5
    filesWithoutMd5 += analysis.withoutMd5
    filesWithSha1 += analysis.withSha1
    filesWithSha256 += analysis.withSha256

    const md5Detail = [
      analysis.md5FromField ? `${analysis.md5FromField} from md5sum field` : null,
      analysis.md5FromPath ? `${analysis.md5FromPath} from path` : null
    ].filter(Boolean).join(', ')

    const otherNote = analysis.otherKeys.length
      ? `; other hash-like keys: ${analysis.otherKeys.join(', ')}`
      : ''

    console.log(
      `torrent ${row.url}: ${analysis.fileCount} files; ` +
      `${analysis.withMd5} with MD5` +
      (md5Detail ? ` (${md5Detail})` : '') +
      `, ${analysis.withoutMd5} without MD5; ` +
      `content SHA-1: ${analysis.withSha1}, content SHA-256: ${analysis.withSha256}` +
      otherNote
    )

    if (torrentsProcessed % 50 === 0) {
      console.log(
        `progress: processed ${torrentsProcessed}, skipped ${torrentsSkipped}, ` +
        `filtered ${torrentsFiltered}, failed ${torrentsFailed}, rows written ${rowsWritten}`
      )
    }
  }

  console.log(
    `done: torrents processed ${torrentsProcessed}, skipped ${torrentsSkipped}, ` +
    `filtered ${torrentsFiltered}, invalid ${torrentsInvalid}, failed ${torrentsFailed}; ` +
    `rows written ${rowsWritten}; ` +
    `files with MD5 ${filesWithMd5}, without MD5 ${filesWithoutMd5}; ` +
    `content SHA-1 ${filesWithSha1}, content SHA-256 ${filesWithSha256}; ` +
    `elapsed ${formatDuration(Date.now() - started)}`
  )
}

if (esMain(import.meta)) {
  run().catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
}

export {
  analyzeTorrentFiles,
  extractContentHashes,
  md5FromPath,
  normalizeHashValue,
  run
}
