import fs from 'node:fs'
import minimist from 'minimist'
import esMain from 'es-main'
import { cdxDigestToHex } from './gutenberg-wayback.js'
import {
  appendHash,
  formatDuration,
  loadDoneKeys,
  sidecarPath,
  sleep,
  withRetries
} from './util.js'

const CDX_API = 'https://web.archive.org/cdx/search/cdx'
const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
const DEFAULT_OUTPUT = 'wayback_youtube_hashes.txt'
const CDX_RETRIES = 8
const CDX_RETRY_DELAY_MS = 12000
const CDX_PAGE_LIMIT = 15000
const PAGE_PAUSE_MS = 1000

/** Primary YouTube media host in the Wayback Machine. */
const CDX_URL = '*.googlevideo.com/videoplayback*'

const headers = { 'User-Agent': USER_AGENT, Accept: '*/*' }

const isAvMime = (mimetype) => {
  const mime = String(mimetype || '').toLowerCase()
  return mime.startsWith('video/') || mime.startsWith('audio/')
}

const loadSeenDigests = (filePath) => {
  const digests = new Set()
  if (!fs.existsSync(filePath)) {
    return digests
  }
  for (const line of fs.readFileSync(filePath, 'utf8').split('\n')) {
    if (!line) {
      continue
    }
    const digest = line.split('\t')[1]
    if (digest) {
      digests.add(digest)
    }
  }
  return digests
}

const fetchText = async (url) => {
  const response = await fetch(url, { headers })
  if (response.status === 429 || response.status === 503) {
    const err = new Error(`status: ${response.status} ${url}`)
    const retryAfter = Number(response.headers.get('retry-after'))
    if (Number.isFinite(retryAfter) && retryAfter > 0) {
      err.retryAfterMs = retryAfter * 1000
    }
    throw err
  }
  if (!response.ok) {
    throw new Error(`status: ${response.status} ${url}`)
  }
  const text = await response.text()
  if (!text.trim()) {
    throw new Error(`status: empty body ${url}`)
  }
  return text
}

/**
 * Page CDX with resumeKey, writing matching rows as url\\tdigest (SHA-1 hex).
 * Filters status 200 and mimetype video/* or audio/* (server + client).
 */
export const collectWaybackYoutubeHashes = async ({
  outputPath = DEFAULT_OUTPUT,
  cdxUrl = CDX_URL,
  pageLimit = CDX_PAGE_LIMIT
} = {}) => {
  const resumePath = sidecarPath(outputPath, 'resume.txt')
  const doneKeys = loadDoneKeys(outputPath)
  const seenDigests = loadSeenDigests(outputPath)
  let resumeKey = fs.existsSync(resumePath)
    ? fs.readFileSync(resumePath, 'utf8').trim() || undefined
    : undefined

  console.log('output:', outputPath)
  console.log('cdx url:', cdxUrl)
  console.log('already recorded urls:', doneKeys.size)
  console.log('already recorded digests:', seenDigests.size)
  if (resumeKey) {
    console.log('resuming with resumeKey')
  }

  const runStart = Date.now()
  let pages = 0
  let scanned = 0
  let written = 0

  for (;;) {
    const params = new URLSearchParams()
    params.set('url', cdxUrl)
    params.set('output', 'json')
    params.set('fl', 'timestamp,original,mimetype,statuscode,digest,length')
    params.set('limit', String(pageLimit))
    params.set('showResumeKey', 'true')
    params.append('filter', 'statuscode:200')
    params.append('filter', 'mimetype:(video|audio)/.*')
    if (resumeKey) {
      params.set('resumeKey', resumeKey)
    }

    const endpoint = `${CDX_API}?${params}`
    const text = await withRetries(
      cdxUrl,
      () => fetchText(endpoint),
      {
        retries: CDX_RETRIES,
        delayMs: CDX_RETRY_DELAY_MS,
        isNonRetryable: (e) => /status: (?:40[134]|410|451)\b/.test(e.message)
      }
    )

    let data
    try {
      data = JSON.parse(text)
    } catch {
      throw new Error(`cdx non-json response: ${text.slice(0, 120)}`)
    }
    if (!Array.isArray(data) || data.length === 0) {
      break
    }

    let nextResume
    let body = data
    if (
      data.length >= 2 &&
      Array.isArray(data[data.length - 2]) &&
      data[data.length - 2].length === 0 &&
      Array.isArray(data[data.length - 1]) &&
      data[data.length - 1].length === 1
    ) {
      nextResume = data[data.length - 1][0]
      body = data.slice(0, -2)
    }

    const start = body[0] && body[0][0] === 'timestamp' ? 1 : 0
    let pageMatches = 0
    for (let i = start; i < body.length; i++) {
      const row = body[i]
      if (!Array.isArray(row) || row.length < 5) {
        continue
      }
      scanned++
      const [, original, mimetype, , digest] = row
      if (String(mimetype || '').includes('warc/revisit')) {
        continue
      }
      if (!isAvMime(mimetype)) {
        continue
      }
      const hex = cdxDigestToHex(digest)
      if (!hex) {
        continue
      }
      if (seenDigests.has(hex) || doneKeys.has(original)) {
        continue
      }
      appendHash(outputPath, original, hex)
      doneKeys.add(original)
      seenDigests.add(hex)
      written++
      pageMatches++
    }

    pages++
    const elapsed = Date.now() - runStart
    console.log(
      `page ${pages}: +${pageMatches} written ` +
      `(scanned ${scanned}, total written ${written}, ` +
      `elapsed ${formatDuration(elapsed)})`
    )

    if (!nextResume) {
      if (fs.existsSync(resumePath)) {
        fs.unlinkSync(resumePath)
      }
      break
    }
    resumeKey = nextResume
    fs.writeFileSync(resumePath, `${resumeKey}\n`)
    await sleep(PAGE_PAUSE_MS)
  }

  return { scanned, written, pages }
}

const main = async () => {
  const args = minimist(process.argv.slice(2), {
    default: { output: DEFAULT_OUTPUT, url: CDX_URL, limit: CDX_PAGE_LIMIT },
    alias: { o: 'output', u: 'url', l: 'limit' },
    string: ['output', 'url']
  })

  const { scanned, written, pages } = await collectWaybackYoutubeHashes({
    outputPath: args.output,
    cdxUrl: args.url,
    pageLimit: Number(args.limit) || CDX_PAGE_LIMIT
  })
  console.log(`done: ${written} digests written, ${scanned} rows scanned, ${pages} pages`)
}

if (esMain(import.meta)) {
  main()
}
