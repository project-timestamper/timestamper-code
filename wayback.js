import fs from 'node:fs'
import minimist from 'minimist'
import esMain from 'es-main'
import { pMapIterable } from 'p-map'
import {
  appendHash,
  appendLine,
  formatDuration,
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
const LOG_EVERY = 100
const SHA1_RE = /^[0-9a-f]{40}$/

const escapeIdentifier = (id) => id.replace(/\\/g, '\\\\').replace(/"/g, '\\"')

const pageQuery = (after) => {
  if (!after) return 'mediatype:web'
  const id = escapeIdentifier(after)
  return `mediatype:web AND identifier:["${id}" TO *] AND NOT identifier:"${id}"`
}

const fetchJson = async (url) => {
  const response = await fetch(url, {
    headers: { 'User-Agent': USER_AGENT, Accept: 'application/json' }
  })
  if (response.status === 429 || response.status === 503) {
    const err = new Error(`status: ${response.status} ${url}`)
    const retryAfter = Number(response.headers.get('retry-after'))
    if (Number.isFinite(retryAfter) && retryAfter > 0) err.retryAfterMs = retryAfter * 1000
    throw err
  }
  if (!response.ok) throw new Error(`status: ${response.status} ${url}`)
  return response.json()
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
  const data = await withRetries(`search ${after || '(start)'}`, () => fetchJson(url), {
    retries: SEARCH_RETRIES,
    delayMs: RETRY_DELAY_MS,
    isNonRetryable: (e) => /status: (?:400|401|404)\b/.test(e.message)
  })
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
      console.error('bad name', identifier, name)
      continue
    }
    const sha1 = String(file.sha1 || '').trim().toLowerCase()
    if (!SHA1_RE.test(sha1)) {
      console.error('no sha1', identifier, name)
      continue
    }
    rows.push({ path: `${identifier}/${name}`, sha1 })
  }
  return rows
}

const metadataRows = async (identifier) => {
  const url = `${METADATA}/${encodeURIComponent(identifier)}/files`
  const data = await fetchJson(url)
  if (!data || !Array.isArray(data.result)) throw new Error(`bad metadata ${url}`)
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

const logProgress = (state, runItems, started) => {
  const elapsed = Date.now() - started
  const remaining = Math.max(0, state.total - state.itemsDone)
  const eta = runItems > 0 ? remaining * (elapsed / runItems) : 0
  console.log(
    `progress: ${state.itemsDone}/${state.total || '?'} items, ` +
    `${state.hashesWritten} hashes, ` +
    `elapsed ${formatDuration(elapsed)}, eta ${formatDuration(eta)}`
  )
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

  console.log('output:', outputPath)
  console.log('query: mediatype:web')
  console.log('page size:', pageSize, 'concurrency:', concurrency)
  if (state.done && state.itemsDone >= state.total) {
    console.log('already complete:', state.itemsDone, 'items,', state.hashesWritten, 'hashes')
    return { itemsDone: state.itemsDone, hashesWritten: state.hashesWritten, complete: true }
  }
  if (state.done) {
    state.done = false
    saveResume(resumePath, state)
  }
  if (state.after) console.log('resume after', state.after)
  if (state.total) console.log('total items:', state.total)

  const started = Date.now()
  let runItems = 0

  const commit = (id, rows) => {
    for (const row of rows) appendHash(outputPath, row.path, row.sha1)
    state.hashesWritten += rows.length
    state.after = id
    state.itemsDone++
    runItems++
    saveResume(resumePath, state)
    if (runItems % LOG_EVERY === 0) logProgress(state, runItems, started)
  }

  for (;;) {
    if (runItems >= limit) break
    const page = await searchPage(state.after, pageSize)
    if (!state.after && !state.total) {
      state.total = page.total
      saveResume(resumePath, state)
      console.log('total items:', state.total)
    }
    const ids = page.ids.filter((id) => !state.after || id > state.after)
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
    console.log(
      `page: ${ids.length} identifiers`,
      state.after ? `after ${state.after}` : 'from start'
    )

    const pending = ids.slice(0, limit - runItems)
    const results = pMapIterable(pending, async (id) => {
      try {
        const rows = await withRetries(id, () => metadataRows(id), {
          retries: METADATA_RETRIES,
          delayMs: RETRY_DELAY_MS,
          isNonRetryable: (e) => /status: (?:40[134]|410|451)\b/.test(e.message)
        })
        return { id, rows }
      } catch (e) {
        return { id, rows: [], error: e }
      }
    }, { concurrency, backpressure: Infinity })
    for await (const result of results) {
      if (result.error) {
        console.error('failed', result.id, result.error.message)
        appendLine(skipPath, `${result.id}\t${result.error.message.replace(/[\t\r\n]/g, ' ')}`)
      }
      commit(result.id, result.rows)
    }

    if (runItems >= limit) break
    logProgress(state, runItems, started)
    if (state.total > 0 && state.itemsDone >= state.total) {
      state.done = true
      saveResume(resumePath, state)
      break
    }
  }

  return {
    itemsDone: state.itemsDone,
    hashesWritten: state.hashesWritten,
    complete: state.done === true
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
  console.log(
    `done: ${result.itemsDone} items, ${result.hashesWritten} hashes, ` +
    `complete ${result.complete}, elapsed ${formatDuration(Date.now() - started)}`
  )
}

if (esMain(import.meta)) {
  main().catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
}
