import fs from 'node:fs'
import minimist from 'minimist'
import esMain from 'es-main'
import { pMapIterable } from 'p-map'
import {
  appendHash,
  appendLine,
  formatDuration,
  log,
  logError,
  sidecarPath,
  USER_AGENT,
  withRetries
} from './util.js'

/**
 * SHA-1 of each aggregate .cdx.gz on mediatype:web items.
 *
 * The digest is the one in item metadata, including files that are not
 * downloadable. Per-WARC *.os.cdx.gz files are omitted.
 *
 * Advanced search lists identifiers in order, --page-size at a time.
 * --concurrency metadata requests stay in flight. Hashes are written
 * in identifier order. Resume is the last identifier written.
 *
 * Output: identifier/filename \t sha1
 *
 *   node wayback.js
 *   node wayback.js --output wayback_hashes.txt --limit 20
 *
 * Sidecars: wayback_resume.json, wayback_skipped.txt.
 * Delete wayback_resume.json to start over.
 */

const SEARCH = 'https://archive.org/advancedsearch.php'
const METADATA = 'https://archive.org/metadata'
const DEFAULT_OUTPUT = 'wayback_hashes.txt'
const PAGE_SIZE = 1000
const CONCURRENCY = 8
const SEARCH_RETRIES = 8
const METADATA_RETRIES = 5
const RETRY_DELAY_MS = 5000
/** Cap hung metadata fetches so they cannot hold a concurrency slot forever. */
const METADATA_TIMEOUT_MS = 5000
/** Advanced search (1000 rows + range query) is slower; keep a higher ceiling. */
const SEARCH_TIMEOUT_MS = 60000
const LOG_EVERY = 100
const SHA1_RE = /^[0-9a-f]{40}$/

const isTimeoutError = (e) =>
  e && (e.name === 'TimeoutError' ||
    (e.name === 'AbortError' && /timeout/i.test(String(e.message || ''))))

const escapeIdentifier = (id) => id.replace(/\\/g, '\\\\').replace(/"/g, '\\"')

// Archive.org sorts identifiers case-insensitively. JavaScript does not,
// so "ACC…" sorts before "acb…" here and after it in search results.
const identifierFollows = (id, after) => {
  const folded = id.toLowerCase()
  const foldedAfter = after.toLowerCase()
  if (folded !== foldedAfter) return folded > foldedAfter
  return id !== after
}

const pageQuery = (after) => {
  if (!after) return 'mediatype:web'
  const id = escapeIdentifier(after)
  return `mediatype:web AND identifier:["${id}" TO *] AND NOT identifier:"${id}"`
}

/** Recent latency samples for p50/p95 (ring buffer; avoids unbounded memory). */
const LATENCY_WINDOW = 10000

const createMetaStats = () => ({
  attempts: 0,
  httpOk: 0,
  httpErrors: 0,
  timeouts: 0,
  loggedTimeouts: 0,
  badMetadata: 0,
  totalMs: 0,
  byStatus: Object.create(null),
  itemsOk: 0,
  itemsFailed: 0,
  latencies: [],
  latencyIdx: 0
})

const isTimeoutMessage = (message) => /timeout after\b/.test(String(message || ''))

const bump = (map, key) => {
  map[key] = (map[key] || 0) + 1
}

const recordLatency = (stats, ms) => {
  const buf = stats.latencies
  if (buf.length < LATENCY_WINDOW) {
    buf.push(ms)
    return
  }
  buf[stats.latencyIdx] = ms
  stats.latencyIdx = (stats.latencyIdx + 1) % LATENCY_WINDOW
}

const recordMetaAttempt = (stats, { ms, status, httpOk, kind }) => {
  stats.attempts++
  stats.totalMs += ms
  recordLatency(stats, ms)
  if (status) bump(stats.byStatus, String(status))
  if (httpOk) stats.httpOk++
  else {
    stats.httpErrors++
    if (kind) bump(stats.byStatus, kind)
  }
}

/** Nearest-rank percentile on a sorted ascending array. */
const percentileMs = (sorted, p) => {
  if (sorted.length === 0) return 0
  const rank = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, Math.min(sorted.length - 1, rank))]
}

const formatMetaStats = (stats) => {
  const avgMs = stats.attempts > 0 ? Math.round(stats.totalMs / stats.attempts) : 0
  const sorted = stats.latencies.slice().sort((a, b) => a - b)
  const p50 = percentileMs(sorted, 50)
  const p95 = percentileMs(sorted, 95)
  const httpErrPct = stats.attempts > 0
    ? ((100 * stats.httpErrors) / stats.attempts).toFixed(1)
    : '0.0'
  const items = stats.itemsOk + stats.itemsFailed
  const itemFailPct = items > 0
    ? ((100 * stats.itemsFailed) / items).toFixed(1)
    : '0.0'
  const statuses = Object.entries(stats.byStatus)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([k, n]) => `${k}:${n}`)
    .join(' ')
  const timeoutDelta = stats.timeouts - stats.loggedTimeouts
  stats.loggedTimeouts = stats.timeouts
  return (
    `http err ${httpErrPct}% (${stats.httpErrors}/${stats.attempts}), ` +
    `avg ${avgMs}ms p50 ${p50}ms p95 ${p95}ms, ` +
    `timeouts +${timeoutDelta} (${stats.timeouts} total), ` +
    `item fail ${itemFailPct}% (${stats.itemsFailed}/${items || 0}), ` +
    `bad meta ${stats.badMetadata}` +
    (statuses ? `, statuses ${statuses}` : '')
  )
}

const fetchJson = async (url, stats = null, timeoutMs = METADATA_TIMEOUT_MS) => {
  const started = Date.now()
  let status = 0
  try {
    const response = await fetch(url, {
      headers: { 'User-Agent': USER_AGENT, Accept: 'application/json' },
      signal: AbortSignal.timeout(timeoutMs)
    })
    status = response.status
    if (response.status === 429 || response.status === 503) {
      const err = new Error(`status: ${response.status} ${url}`)
      const retryAfter = Number(response.headers.get('retry-after'))
      if (Number.isFinite(retryAfter) && retryAfter > 0) err.retryAfterMs = retryAfter * 1000
      throw err
    }
    if (!response.ok) throw new Error(`status: ${response.status} ${url}`)
    const data = await response.json()
    if (stats) recordMetaAttempt(stats, { ms: Date.now() - started, status, httpOk: true })
    return data
  } catch (e) {
    if (stats) {
      const timedOut = isTimeoutError(e)
      if (timedOut) stats.timeouts++
      const kind = status ? null : (timedOut ? 'timeout' : 'network')
      recordMetaAttempt(stats, { ms: Date.now() - started, status, httpOk: false, kind })
    }
    if (isTimeoutError(e)) {
      const err = new Error(`timeout after ${timeoutMs}ms ${url}`)
      err.cause = e
      throw err
    }
    throw e
  }
}

const searchPage = async (after, rows) => {
  const params = new URLSearchParams()
  params.set('q', pageQuery(after))
  params.append('fl[]', 'identifier')
  params.append('sort[]', 'identifier asc')
  params.set('rows', String(rows))
  params.set('page', '1')
  params.set('output', 'json')
  const url = `${SEARCH}?${params}`
  const data = await withRetries(
    `search ${after || '(start)'}`,
    () => fetchJson(url, null, SEARCH_TIMEOUT_MS),
    {
      retries: SEARCH_RETRIES,
      delayMs: RETRY_DELAY_MS,
      isNonRetryable: (e) => /status: (?:400|401|404)\b/.test(e.message)
    }
  )
  if (data.error) throw new Error(String(data.error))
  const docs = data.response && data.response.docs
  if (!Array.isArray(docs) || typeof data.response.numFound !== 'number') {
    throw new Error(`bad search response ${url}`)
  }
  return {
    total: data.response.numFound,
    ids: docs.map((doc) => doc.identifier).filter((id) => typeof id === 'string' && id !== '')
  }
}

/** Aggregate CDX rows for one item. Logs and skips a file with no usable SHA-1. */
const cdxRows = (identifier, files) => {
  const rows = []
  for (const file of files) {
    const name = file && file.name
    if (!name) continue
    const lower = name.toLowerCase()
    if (!lower.endsWith('.cdx.gz') || lower.endsWith('.os.cdx.gz')) continue
    if (/[\t\r\n]/.test(name) || /[\t\r\n]/.test(identifier)) {
      logError('bad name', identifier, name)
      continue
    }
    const sha1 = String(file.sha1 || '').trim().toLowerCase()
    if (!SHA1_RE.test(sha1)) {
      logError('no sha1', identifier, name)
      continue
    }
    rows.push({ path: `${identifier}/${name}`, sha1 })
  }
  return rows
}

const metadataRows = async (identifier, stats) => {
  const url = `${METADATA}/${encodeURIComponent(identifier)}/files`
  const data = await fetchJson(url, stats)
  if (!data || !Array.isArray(data.result)) {
    if (stats) stats.badMetadata++
    throw new Error(`bad metadata ${url}`)
  }
  return cdxRows(identifier, data.result)
}

const resumePathFor = (outputPath) => sidecarPath(outputPath, 'resume.json')

const loadResume = (path) => {
  if (!fs.existsSync(path)) {
    return { after: '', itemsDone: 0, hashesWritten: 0, total: 0, done: false }
  }
  return JSON.parse(fs.readFileSync(path, 'utf8'))
}

const saveResume = (path, state) => {
  const tmp = `${path}.tmp`
  fs.writeFileSync(tmp, `${JSON.stringify(state)}\n`)
  fs.renameSync(tmp, path)
}

const logProgress = (state, runItems, started, stats) => {
  const elapsed = Date.now() - started
  const remaining = Math.max(0, state.total - state.itemsDone)
  const eta = runItems > 0 ? remaining * (elapsed / runItems) : 0
  log(
    `progress: ${state.itemsDone}/${state.total || '?'} items, ` +
    `${state.hashesWritten} hashes, ` +
    `elapsed ${formatDuration(elapsed)}, eta ${formatDuration(eta)}`
  )
  if (stats && stats.attempts > 0) log(`stats: ${formatMetaStats(stats)}`)
}

export const collectWaybackCdxHashes = async ({
  outputPath = DEFAULT_OUTPUT,
  limit = Infinity,
  pageSize = PAGE_SIZE,
  concurrency = CONCURRENCY
} = {}) => {
  const resumePath = resumePathFor(outputPath)
  const skipPath = sidecarPath(outputPath, 'skipped.txt')
  const state = loadResume(resumePath)

  log('output:', outputPath)
  log('query: mediatype:web')
  log('page size:', pageSize, 'concurrency:', concurrency)
  if (state.done && state.itemsDone >= state.total) {
    log('already complete:', state.itemsDone, 'items,', state.hashesWritten, 'hashes')
    return { itemsDone: state.itemsDone, hashesWritten: state.hashesWritten, complete: true }
  }
  if (state.done) {
    state.done = false
    saveResume(resumePath, state)
  }
  if (state.after) log('resume after', state.after)
  if (state.total) log('total items:', state.total)

  const started = Date.now()
  let runItems = 0
  const stats = createMetaStats()

  const commit = (id, rows, failed = false) => {
    for (const row of rows) appendHash(outputPath, row.path, row.sha1)
    state.hashesWritten += rows.length
    state.after = id
    state.itemsDone++
    runItems++
    if (failed) stats.itemsFailed++
    else stats.itemsOk++
    saveResume(resumePath, state)
    if (runItems % LOG_EVERY === 0) logProgress(state, runItems, started, stats)
  }

  for (;;) {
    if (runItems >= limit) break
    const page = await searchPage(state.after, pageSize)
    if (!state.after && !state.total) {
      state.total = page.total
      saveResume(resumePath, state)
      log('total items:', state.total)
    }
    const ids = page.ids.filter((id) => !state.after || identifierFollows(id, state.after))
    if (page.ids.length > 0 && ids.length === 0) {
      throw new Error(`search did not advance after ${state.after}`)
    }
    if (ids.length === 0) {
      const remaining = state.total - state.itemsDone
      if (remaining > 0) {
        throw new Error(`search returned no identifiers after ${state.after || '(start)'}, ${remaining} items remaining`)
      }
      state.done = true
      saveResume(resumePath, state)
      break
    }
    log(
      `page: ${ids.length} identifiers`,
      state.after ? `after ${state.after}` : 'from start'
    )

    const pending = ids.slice(0, limit - runItems)
    const results = pMapIterable(pending, async (id) => {
      try {
        const rows = await withRetries(id, () => metadataRows(id, stats), {
          retries: METADATA_RETRIES,
          delayMs: RETRY_DELAY_MS,
          // bad metadata is a successful HTTP response with the wrong shape;
          // retrying usually just stalls concurrency slots.
          isNonRetryable: (e) =>
            /bad metadata\b/.test(e.message) ||
            /status: (?:40[134]|410|451)\b/.test(e.message),
          // Timeout storms are tracked in stats; per-attempt logs are too noisy.
          shouldLogRetry: (e) => !isTimeoutMessage(e.message)
        })
        return { id, rows }
      } catch (e) {
        return { id, rows: [], error: e }
      }
    }, { concurrency, backpressure: Infinity })
    for await (const result of results) {
      if (result.error) {
        if (!isTimeoutMessage(result.error.message)) {
          logError('failed', result.id, result.error.message)
        }
        appendLine(skipPath, `${result.id}\t${result.error.message.replace(/[\t\r\n]+/g, ' ')}`)
      }
      commit(result.id, result.rows, Boolean(result.error))
    }

    if (runItems >= limit) break
    logProgress(state, runItems, started, stats)
    if (state.total > 0 && state.itemsDone >= state.total) {
      state.done = true
      saveResume(resumePath, state)
      break
    }
  }

  if (stats.attempts > 0) log(`stats: ${formatMetaStats(stats)}`)
  return {
    itemsDone: state.itemsDone,
    hashesWritten: state.hashesWritten,
    complete: state.done === true,
    stats
  }
}

const main = async () => {
  const args = minimist(process.argv.slice(2), {
    default: { output: DEFAULT_OUTPUT, 'page-size': PAGE_SIZE, concurrency: CONCURRENCY },
    alias: { o: 'output', n: 'limit' },
    string: ['output']
  })
  const limit = args.limit == null ? Infinity : Number(args.limit)
  if (limit !== Infinity && (!Number.isInteger(limit) || limit <= 0)) {
    throw new Error(`invalid --limit: ${args.limit}`)
  }
  const pageSize = Number(args['page-size'])
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 10000) {
    throw new Error(`invalid --page-size: ${args['page-size']}`)
  }
  const concurrency = Number(args.concurrency)
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new Error(`invalid --concurrency: ${args.concurrency}`)
  }

  const started = Date.now()
  const result = await collectWaybackCdxHashes({
    outputPath: args.output,
    limit,
    pageSize,
    concurrency
  })
  log(
    `done: ${result.itemsDone} items, ${result.hashesWritten} hashes, ` +
    `complete ${result.complete}, elapsed ${formatDuration(Date.now() - started)}`
  )
}

if (esMain(import.meta)) {
  main().catch((err) => {
    logError(err)
    process.exitCode = 1
  })
}
