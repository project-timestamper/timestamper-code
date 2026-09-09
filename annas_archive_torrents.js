import minimist from 'minimist'
import esMain from 'es-main'
import {
  appendHash,
  formatDuration,
  loadDoneKeys,
  withRetries
} from './util.js'

/**
 * Collect BitTorrent info hashes (SHA-1 / btih) for every torrent listed on
 * Anna's Archive torrents pages.
 *
 * The public HTML at /torrents is fed by /dyn/torrents.json, which already
 * includes each .torrent URL and its 40-hex btih — no need to download the
 * torrent files themselves.
 *
 * Output: tab-delimited url\thash lines (resume-friendly).
 *
 * Usage:
 *   node annas_archive_torrents.js
 *   node annas_archive_torrents.js --output annas_archive_torrent_hashes.txt
 *   node annas_archive_torrents.js --base https://annas-archive.gd
 */

const DEFAULT_BASE = 'https://annas-archive.gd'
const DEFAULT_OUTPUT = 'annas_archive_torrent_hashes.txt'
const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
const FETCH_RETRIES = 5
const RETRY_DELAY_MS = 10000
const BTIH_RE = /^[0-9a-f]{40}$/i

const headers = { 'User-Agent': USER_AGENT, Accept: 'application/json' }

const torrentsJsonUrl = (base) => new URL('/dyn/torrents.json', base).href

const fetchTorrentsIndex = async (base) => {
  const url = torrentsJsonUrl(base)
  return withRetries(`fetch ${url}`, async () => {
    const response = await fetch(url, { headers })
    if (!response.ok) {
      throw new Error(`status: ${response.status} ${url}`)
    }
    const data = await response.json()
    if (!Array.isArray(data)) {
      throw new Error(`expected array from ${url}, got ${typeof data}`)
    }
    return data
  }, { retries: FETCH_RETRIES, delayMs: RETRY_DELAY_MS })
}

const normalizeEntry = (entry, base) => {
  if (!entry || typeof entry !== 'object') {
    return null
  }
  const btih = String(entry.btih || '').trim().toLowerCase()
  if (!BTIH_RE.test(btih)) {
    return null
  }
  let url = String(entry.url || '').trim()
  if (!url) {
    return null
  }
  try {
    url = new URL(url, base).href
  } catch {
    return null
  }
  return { url, btih }
}

const run = async (argv = process.argv.slice(2)) => {
  const args = minimist(argv, {
    string: ['output', 'base'],
    alias: { o: 'output', b: 'base' },
    default: {
      output: DEFAULT_OUTPUT,
      base: DEFAULT_BASE
    }
  })

  const base = args.base.replace(/\/$/, '')
  const outputPath = args.output
  const started = Date.now()

  console.log('fetching', torrentsJsonUrl(base))
  const entries = await fetchTorrentsIndex(base)
  console.log('index entries', entries.length)

  const done = loadDoneKeys(outputPath)
  console.log('already have', done.size, 'urls in', outputPath)

  let written = 0
  let skipped = 0
  let invalid = 0

  for (const entry of entries) {
    const row = normalizeEntry(entry, base)
    if (!row) {
      invalid++
      continue
    }
    if (done.has(row.url)) {
      skipped++
      continue
    }
    appendHash(outputPath, row.url, row.btih)
    done.add(row.url)
    written++
    if (written % 1000 === 0) {
      console.log(`wrote ${written} (skipped ${skipped}, invalid ${invalid})`)
    }
  }

  console.log(
    `done: wrote ${written}, skipped ${skipped}, invalid ${invalid}, ` +
    `total keys ${done.size}, elapsed ${formatDuration(Date.now() - started)}`
  )
}

if (esMain(import.meta)) {
  run().catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
}

export { fetchTorrentsIndex, normalizeEntry, run }
