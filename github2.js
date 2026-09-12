import fs from 'node:fs'
import minimist from 'minimist'
import esMain from 'es-main'
import {
  appendHash,
  formatDuration,
  loadDoneKeys,
  sidecarPath,
  sleep,
  withRetries
} from './util.js'

/**
 * Collect as-of-D default-branch SHAs for public repos with at least
 * --min-stars, scanning created-at with an adaptive window.
 *
 * Search: GET /search/repositories (stars:>=N created:>=T created:<T+W)
 *   If total_count > 1000, halve W (min 1s) and re-probe.
 *   Otherwise page the window (≤1000 hits, even if <500) and advance T.
 *   If that window had <500 hits and T is still before D, double W for next T.
 * SHA: GET /repos/{owner}/{repo}/commits?until=D&per_page=1
 *
 *   GITHUB_TOKEN=… node github2.js --until 2026-09-12T00:00:00Z
 *   GITHUB_TOKEN=… node github2.js --limit 20
 *
 * Sidecars: github2_until.txt (D), github2_resume.json (T, W, page).
 */

const SEARCH = 'https://api.github.com/search/repositories'
const DEFAULT_OUTPUT = 'github2_hashes.txt'
const MIN_STARS = 10
const MIN_WINDOW_MS = 1000
const LOW = 500
const HIGH = 1000
const DEFAULT_WINDOW_MS = 30 * 24 * 60 * 60 * 1000
const SEARCH_START_MS = Date.parse('2008-01-01T00:00:00Z')
const SEARCH_PAUSE_MS = 2000
const PER_PAGE = 100
const CORE_LIMIT = 5000
const DEFAULT_CORE_INTERVAL_MS = Math.ceil(3600 * 1000 / CORE_LIMIT)
const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'

const fmt = (ms) => new Date(ms).toISOString().replace(/\.\d{3}Z$/, 'Z')

const rateLimitFrom = (res) => {
  const remaining = Number(res.headers.get('x-ratelimit-remaining'))
  const resetMs = Number(res.headers.get('x-ratelimit-reset')) * 1000
  const retryAfter = Number(res.headers.get('retry-after'))
  return {
    resource: res.headers.get('x-ratelimit-resource') || '',
    remaining: Number.isFinite(remaining) ? remaining : NaN,
    resetMs: Number.isFinite(resetMs) ? resetMs : NaN,
    retryAfterMs: retryAfter > 0 ? retryAfter * 1000 : 0
  }
}

const retryWaitMs = (rateLimit) => {
  if (rateLimit.retryAfterMs > 0) return rateLimit.retryAfterMs
  if (rateLimit.remaining === 0 && rateLimit.resetMs > Date.now()) {
    return rateLimit.resetMs - Date.now() + 1000
  }
  return 60000
}

const paceCore = async (rateLimit) => {
  if (rateLimit?.resource && rateLimit.resource !== 'core') return
  const timeLeft = rateLimit.resetMs - Date.now()
  let waitMs = DEFAULT_CORE_INTERVAL_MS
  if (rateLimit.remaining === 0 && timeLeft > 0) {
    waitMs = timeLeft + 1000
  } else if (rateLimit.remaining > 0 && timeLeft > 0) {
    waitMs = timeLeft / rateLimit.remaining
  }
  if (waitMs >= 2000) {
    console.log(
      'core pace',
      Math.ceil(waitMs / 1000), 's',
      'remaining', rateLimit.remaining,
      'reset', Number.isFinite(rateLimit.resetMs) ? new Date(rateLimit.resetMs).toISOString() : '-'
    )
  }
  if (waitMs > 0) await sleep(waitMs)
}

const canonicalUntil = (value) => {
  const ms = Date.parse(value)
  if (!Number.isFinite(ms)) throw new Error(`invalid --until: ${value}`)
  return new Date(ms).toISOString()
}

const resolveUntil = (outputPath, requested, resuming) => {
  const untilPath = sidecarPath(outputPath, 'until.txt')
  const saved = fs.existsSync(untilPath)
    ? fs.readFileSync(untilPath, 'utf8').trim()
    : ''
  if (resuming && !saved && !requested) {
    throw new Error(`resume needs --until (missing ${untilPath})`)
  }
  const until = canonicalUntil(requested || saved || new Date().toISOString())
  if (saved && until !== canonicalUntil(saved)) {
    throw new Error(`--until ${until} != ${canonicalUntil(saved)} (${untilPath})`)
  }
  if (!saved) fs.writeFileSync(untilPath, `${until}\n`)
  return until
}

const resumePathFor = (outputPath) => sidecarPath(outputPath, 'resume.json')

const loadResume = (outputPath) => {
  const resumePath = resumePathFor(outputPath)
  if (!fs.existsSync(resumePath)) {
    return { createdFrom: SEARCH_START_MS, windowMs: DEFAULT_WINDOW_MS, page: 1 }
  }
  try {
    const saved = JSON.parse(fs.readFileSync(resumePath, 'utf8'))
    const createdFrom = Date.parse(saved.createdFrom)
    const windowMs = Number(saved.windowMs)
    const page = Number(saved.page)
    return {
      createdFrom: Number.isFinite(createdFrom) ? createdFrom : SEARCH_START_MS,
      windowMs: Number.isInteger(windowMs) && windowMs >= MIN_WINDOW_MS
        ? windowMs
        : DEFAULT_WINDOW_MS,
      page: Number.isInteger(page) && page >= 1 ? page : 1
    }
  } catch {
    return { createdFrom: SEARCH_START_MS, windowMs: DEFAULT_WINDOW_MS, page: 1 }
  }
}

const saveResume = (outputPath, { createdFrom, windowMs, page }) => {
  fs.writeFileSync(resumePathFor(outputPath), `${JSON.stringify({
    createdFrom: fmt(createdFrom),
    windowMs,
    page
  })}\n`)
}

const githubHeaders = (token) => ({
  Authorization: `Bearer ${token}`,
  Accept: 'application/vnd.github+json',
  'User-Agent': USER_AGENT
})

const githubGet = async (token, url, { skipStatuses = [] } = {}) =>
  withRetries(`GET ${url}`, async () => {
    const res = await fetch(url, { headers: githubHeaders(token) })
    const rateLimit = rateLimitFrom(res)
    if (skipStatuses.includes(res.status)) {
      return { skip: true, status: res.status, rateLimit }
    }
    if (res.status === 403 || res.status === 429) {
      const err = new Error(`github ${res.status}`)
      err.retryAfterMs = retryWaitMs(rateLimit)
      throw err
    }
    if (!res.ok) throw new Error(`status: ${res.status} ${url}`)
    return { json: await res.json(), skip: false, rateLimit }
  })

const searchQuery = (minStars, startMs, endMs) =>
  `stars:>=${minStars} created:${fmt(startMs)}..${fmt(endMs)}`

let lastSearchMs = 0

const searchPage = async (token, q, page, perPage) => {
  const waitMs = SEARCH_PAUSE_MS - (Date.now() - lastSearchMs)
  if (waitMs > 0) await sleep(waitMs)
  lastSearchMs = Date.now()
  const url = new URL(SEARCH)
  url.searchParams.set('q', q)
  url.searchParams.set('per_page', String(perPage))
  url.searchParams.set('page', String(page))
  const { json } = await githubGet(token, url)
  if (json.incomplete_results) {
    const err = new Error('github search incomplete_results')
    err.retryAfterMs = 10000
    throw err
  }
  return json
}

const asOfDSha = async (token, owner, name, until) => {
  const url = new URL(`https://api.github.com/repos/${owner}/${name}/commits`)
  url.searchParams.set('until', until)
  url.searchParams.set('per_page', '1')
  const { json, skip, rateLimit } = await githubGet(token, url, { skipStatuses: [404, 409] })
  await paceCore(rateLimit)
  if (skip || !Array.isArray(json) || !json[0]?.sha) return null
  return String(json[0].sha).toLowerCase()
}

const run = async (argv = process.argv.slice(2)) => {
  const args = minimist(argv, {
    string: ['output', 'until'],
    alias: { o: 'output', n: 'limit' },
    default: {
      output: DEFAULT_OUTPUT,
      'min-stars': MIN_STARS
    }
  })

  const token = process.env.GITHUB_TOKEN
  if (!token) throw new Error('Set GITHUB_TOKEN')

  const minStars = Number(args['min-stars'])
  if (!Number.isInteger(minStars) || minStars < 0) {
    throw new Error(`invalid --min-stars: ${args['min-stars']}`)
  }
  const limit = args.limit == null ? Infinity : Number(args.limit)
  if (limit !== Infinity && (!Number.isInteger(limit) || limit <= 0)) {
    throw new Error(`invalid --limit: ${args.limit}`)
  }

  const outputPath = args.output
  const done = loadDoneKeys(outputPath)
  const until = resolveUntil(outputPath, args.until, done.size > 0)
  const nowMs = Date.parse(until)
  let { createdFrom, windowMs, page } = loadResume(outputPath)
  const started = Date.now()

  console.log('until', until, 'min-stars', minStars, 'have', done.size, outputPath)
  console.log('resume', fmt(createdFrom), 'windowMs', windowMs, 'page', page)

  while (createdFrom < nowMs && done.size < limit) {
    const end = Math.min(createdFrom + windowMs, nowMs)
    const q = searchQuery(minStars, createdFrom, end)
    const probe = await searchPage(token, q, 1, 1)
    const n = Number(probe.total_count) || 0
    console.log('window', fmt(createdFrom), '..', fmt(end), 'count', n, 'W', windowMs)

    if (n > HIGH) {
      if (end - createdFrom <= MIN_WINDOW_MS) {
        throw new Error(`count ${n} in 1s window ${fmt(createdFrom)}`)
      }
      const prev = windowMs
      windowMs = Math.max(MIN_WINDOW_MS, Math.floor(windowMs / 2))
      console.log('window shrink', prev, '->', windowMs, 'ms', `(count ${n} > ${HIGH})`)
      page = 1
      saveResume(outputPath, { createdFrom, windowMs, page })
      continue
    }

    const lastPage = Math.ceil(n / PER_PAGE)
    for (let p = page; p <= lastPage && done.size < limit; p++) {
      const batch = await searchPage(token, q, p, PER_PAGE)
      const items = batch.items || []
      console.log(
        'page', p, '/', lastPage,
        'size', items.length,
        'per_page', PER_PAGE,
        'total', n
      )
      for (const repo of items) {
        if (done.size >= limit) break
        const url = repo.html_url
        if (!url || done.has(url)) continue
        const owner = repo.owner?.login
        const name = repo.name
        if (!owner || !name) continue
        const sha = await asOfDSha(token, owner, name, until)
        if (!sha) continue
        appendHash(outputPath, url, sha)
        done.add(url)
        console.log(url, sha, 'stars', repo.stargazers_count)
      }
      page = p + 1
      saveResume(outputPath, { createdFrom, windowMs, page })
    }

    createdFrom = end
    page = 1
    if (n < LOW && createdFrom < nowMs) {
      const prev = windowMs
      windowMs *= 2
      console.log('window grow', prev, '->', windowMs, 'ms', `(count ${n} < ${LOW})`)
    }
    saveResume(outputPath, { createdFrom, windowMs, page })
  }

  console.log(`done: ${done.size} keys, elapsed ${formatDuration(Date.now() - started)}`)
}

if (esMain(import.meta)) {
  run().catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
}

export { run }
