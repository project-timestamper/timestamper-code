import fs from 'node:fs'
import minimist from 'minimist'
import esMain from 'es-main'
import {
  appendHash,
  cdxDigestToHex,
  formatDuration,
  sidecarPath,
  sleep,
  withRetries
} from './util.js'

const CDX_API = 'https://web.archive.org/cdx/search/cdx'
const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
const DEFAULT_OUTPUT = 'wayback_youtube_hashes.txt'
const CDX_RETRIES = 8
const CDX_RETRY_DELAY_MS = 12000
const CDX_PAGE_LIMIT = 200000
const PAGE_PAUSE_MS = 1000

/** Primary YouTube media host in the Wayback Machine. */
const CDX_URL = '*.googlevideo.com/videoplayback*'

const headers = { 'User-Agent': USER_AGENT, Accept: 'text/plain' }

const isAvMime = (mimetype) => {
  const mime = String(mimetype || '').toLowerCase()
  return mime.startsWith('video/') || mime.startsWith('audio/')
}

/** Yield lines from a web ReadableStream without buffering the whole body. */
async function * readLines (body) {
  const decoder = new TextDecoder()
  let buf = ''
  for await (const chunk of body) {
    buf += decoder.decode(chunk, { stream: true })
    let idx
    while ((idx = buf.indexOf('\n')) >= 0) {
      yield buf.slice(0, idx).replace(/\r$/, '')
      buf = buf.slice(idx + 1)
    }
  }
  buf += decoder.decode()
  if (buf.length > 0) {
    yield buf.replace(/\r$/, '')
  }
}

/**
 * Parse one CDX text line for fl=timestamp,original,mimetype,statuscode,digest,length.
 * Trailing fields are fixed; original may theoretically contain spaces.
 */
const parseCdxLine = (line) => {
  const parts = line.split(' ')
  if (parts.length < 6) {
    return null
  }
  const length = parts[parts.length - 1]
  const digest = parts[parts.length - 2]
  const statuscode = parts[parts.length - 3]
  const mimetype = parts[parts.length - 4]
  const timestamp = parts[0]
  const original = parts.slice(1, -4).join(' ')
  return { timestamp, original, mimetype, statuscode, digest, length }
}

const fetchCdxResponse = async (url) => {
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
  if (!response.body) {
    throw new Error(`status: no body ${url}`)
  }
  return response
}

/**
 * Stream one CDX page (newline-delimited). After rows, a blank line then resumeKey.
 * Writes matches immediately; returns { pageScanned, pageWritten, nextResume }.
 */
const streamCdxPage = async (endpoint, outputPath) => {
  const response = await fetchCdxResponse(endpoint)
  let pageScanned = 0
  let pageWritten = 0
  let nextResume
  let afterBlank = false
  let sawRow = false

  for await (const line of readLines(response.body)) {
    if (!afterBlank && line === '') {
      afterBlank = true
      continue
    }
    if (afterBlank) {
      if (line) {
        nextResume = line
      }
      continue
    }
    if (!line) {
      continue
    }
    sawRow = true
    pageScanned++
    const row = parseCdxLine(line)
    if (!row) {
      continue
    }
    if (String(row.mimetype || '').includes('warc/revisit')) {
      continue
    }
    if (!isAvMime(row.mimetype)) {
      continue
    }
    const hex = cdxDigestToHex(row.digest)
    if (!hex) {
      continue
    }
    appendHash(outputPath, row.original, hex)
    pageWritten++
  }

  if (!sawRow && !nextResume) {
    throw new Error(`status: empty body ${endpoint}`)
  }

  return { pageScanned, pageWritten, nextResume }
}

/**
 * Page CDX with resumeKey, appending matching rows as url\\tdigest (SHA-1 hex).
 * Uses default newline-delimited CDX text (not JSON) and streams each page.
 * No in-memory dedupe — unique digests later with:
 *   sort -t $'\t' -k2,2 -u wayback_youtube_hashes.txt -o wayback_youtube_hashes.uniq.txt
 */
export const collectWaybackYoutubeHashes = async ({
  outputPath = DEFAULT_OUTPUT,
  cdxUrl = CDX_URL,
  pageLimit = CDX_PAGE_LIMIT
} = {}) => {
  const resumePath = sidecarPath(outputPath, 'resume.txt')
  let resumeKey = fs.existsSync(resumePath)
    ? fs.readFileSync(resumePath, 'utf8').trim() || undefined
    : undefined

  console.log('output:', outputPath)
  console.log('cdx url:', cdxUrl)
  console.log('format: newline-delimited CDX text')
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
    params.set('fl', 'timestamp,original,mimetype,statuscode,digest,length')
    params.set('limit', String(pageLimit))
    params.set('showResumeKey', 'true')
    params.append('filter', 'statuscode:200')
    params.append('filter', 'mimetype:(video|audio)/.*')
    if (resumeKey) {
      params.set('resumeKey', resumeKey)
    }

    const endpoint = `${CDX_API}?${params}`
    const { pageScanned, pageWritten, nextResume } = await withRetries(
      cdxUrl,
      () => streamCdxPage(endpoint, outputPath),
      {
        retries: CDX_RETRIES,
        delayMs: CDX_RETRY_DELAY_MS,
        isNonRetryable: (e) => /status: (?:40[134]|410|451)\b/.test(e.message)
      }
    )

    scanned += pageScanned
    written += pageWritten
    pages++
    const elapsed = Date.now() - runStart
    console.log(
      `page ${pages}: +${pageWritten} written ` +
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
