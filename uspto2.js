import { createHash } from 'node:crypto'
import fs from 'node:fs'
import { appendFile } from 'node:fs/promises'

const SERIES = {
  utility: { prefix: '', stop: 12_700_000 },
  design: { prefix: 'D', stop: 1_140_000 },
  plant: { prefix: 'PP', stop: 37_550 },
  reissue: { prefix: 'RE', stop: 50_980 },
  // Pre-1836 patents, retroactively numbered; many destroyed in the 1836 fire.
  X: { prefix: 'X', stop: 9_957 },
  // Statutory Invention Registrations (program ended 2013).
  H: { prefix: 'H', stop: 2_294 },
  // Additions of Improvements (historical).
  AI: { prefix: 'AI', stop: 318 }
}

// USPTO ids are at least 7 chars: prefix + zero-padded number.
const formatId = (prefix, n) =>
  prefix + String(n).padStart(Math.max(0, 7 - prefix.length), '0')

const SERIES_BY_PREFIX = Object.entries(SERIES)
  .sort((a, b) => b[1].prefix.length - a[1].prefix.length)

const parseId = (id) => {
  for (const [series, { prefix, stop }] of SERIES_BY_PREFIX) {
    const rest = prefix === '' ? id : id.startsWith(prefix) ? id.slice(prefix.length) : null
    if (rest === null || !/^\d+$/.test(rest)) {
      continue
    }
    const n = Number(rest)
    if (n >= 1 && n <= stop) {
      return { series, n }
    }
  }
  return null
}

const BASE_URL = 'https://image-ppubs.uspto.gov/dirsearch-public/print/downloadPdf/'
const HASHES_PATH = 'uspto_hashes.txt'
const ATTEMPTED_PATH = 'patents_attempted.txt'
const MISSING_DELAY_MS = 1000
const RATE_LIMIT_DELAY_MS = 60_000
const RATE_LIMIT_DELAY_CAP_MS = 10 * 60_000
const RATE_LIMIT_RETRIES = 10
const RATE_LIMIT_STATUSES = new Set([403, 429, 503])

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const progress = {
  startedAt: Date.now(),
  total: Object.values(SERIES).reduce((sum, { stop }) => sum + stop, 0),
  done: 0,
  hashed: 0,
  previous: 0
}

const formatDuration = (ms) => {
  const s = Math.max(0, Math.round(ms / 1000))
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m ${s % 60}s`
}

const createAttempted = () => {
  const bits = {}
  for (const [series, { stop }] of Object.entries(SERIES)) {
    bits[series] = new Uint8Array(Math.ceil((stop + 1) / 8))
  }
  return bits
}

const hasAttempted = (bits, series, n) =>
  (bits[series][n >> 3] & (1 << (n & 7))) !== 0

const markAttempted = (bits, series, n) => {
  bits[series][n >> 3] |= 1 << (n & 7)
}

const loadAttempted = () => {
  const bits = createAttempted()
  let count = 0
  if (!fs.existsSync(ATTEMPTED_PATH)) {
    return { bits, count }
  }
  for (const line of fs.readFileSync(ATTEMPTED_PATH, 'utf8').split('\n')) {
    if (!line) {
      continue
    }
    const parsed = parseId(line)
    if (!parsed) {
      continue
    }
    const { series, n } = parsed
    if (!hasAttempted(bits, series, n)) {
      markAttempted(bits, series, n)
      count++
    }
  }
  return { bits, count }
}

const appendAttempted = async (id, series, n, bits) => {
  if (hasAttempted(bits, series, n)) {
    return
  }
  markAttempted(bits, series, n)
  await appendFile(ATTEMPTED_PATH, `${id}\n`, 'utf8')
}

const parseRetryAfterMs = (res) => {
  const value = res.headers.get('retry-after')
  if (!value) {
    return null
  }
  const seconds = Number(value)
  if (Number.isFinite(seconds)) {
    return Math.max(0, seconds * 1000)
  }
  const dateMs = Date.parse(value)
  if (Number.isFinite(dateMs)) {
    return Math.max(0, dateMs - Date.now())
  }
  return null
}

async function fetchOnce (url) {
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
    }
  })
  const buffer = res.status === 200
    ? Buffer.from(await res.arrayBuffer())
    : undefined
  return { status: res.status, buffer, retryAfterMs: parseRetryAfterMs(res) }
}

async function processOne ({ id, series, n, bits }) {
  const url = BASE_URL + id

  for (let attempt = 0; attempt <= RATE_LIMIT_RETRIES; attempt++) {
    let status
    let buffer
    let retryAfterMs
    try {
      ({ status, buffer, retryAfterMs } = await fetchOnce(url))
    } catch (err) {
      console.error(id, 'error', err)
      return 'error'
    }

    if (status === 200) {
      const sha256 = createHash('sha256').update(buffer).digest('hex')
      await appendFile(HASHES_PATH, `${id}\t${sha256}\n`, 'utf8')
      await appendAttempted(id, series, n, bits)
      return 'ok'
    }

    if (status === 404) {
      await appendAttempted(id, series, n, bits)
      await sleep(MISSING_DELAY_MS)
      return 'missing'
    }

    if (RATE_LIMIT_STATUSES.has(status)) {
      const delay = retryAfterMs ?? Math.min(
        RATE_LIMIT_DELAY_CAP_MS,
        RATE_LIMIT_DELAY_MS * 2 ** attempt
      )
      console.error(
        id,
        `rate-limited HTTP ${status}, sleeping ${formatDuration(delay)} ` +
        `(attempt ${attempt + 1}/${RATE_LIMIT_RETRIES + 1})`
      )
      await sleep(delay)
      continue
    }

    // Not recorded in attempted — next run will try again.
    console.error(id, 'error', `HTTP ${status}`)
    return 'error'
  }

  console.error(id, 'error', 'rate-limited retries exhausted')
  return 'error'
}

async function runSeries ({ series, prefix, stop, bits, progress }) {
  console.log(`Starting: series=${series} range=[1, ${stop}]`)

  for (let n = 1; n <= stop; n++) {
    const id = formatId(prefix, n)
    if (hasAttempted(bits, series, n)) {
      progress.previous++
    } else {
      const outcome = await processOne({ id, series, n, bits })
      if (outcome === 'ok') {
        progress.hashed++
      }
    }

    progress.done++
    if (progress.done % 500 === 0 || n === stop) {
      const elapsed = Date.now() - progress.startedAt
      const worked = progress.done - progress.previous
      const remaining = progress.total - progress.done
      const eta = worked > 0 ? remaining * (elapsed / worked) : 0
      console.log(
        `[${series}] hashed=${progress.hashed} previous=${progress.previous} ` +
        `elapsed ${formatDuration(elapsed)}, eta ${formatDuration(eta)} ` +
        `(${progress.done}/${progress.total})`
      )
    }
  }

  console.log(`[${series}] DONE`)
}

const { bits, count } = loadAttempted()
console.log(`Loaded ${count} attempted ids from ${ATTEMPTED_PATH}`)

for (const [series, { prefix, stop }] of Object.entries(SERIES)) {
  await runSeries({ series, prefix, stop, bits, progress })
}
