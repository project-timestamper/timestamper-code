import fs from 'node:fs'
import readline from 'node:readline'
import minimist from 'minimist'
import esMain from 'es-main'
import {
  USER_AGENT,
  appendHash,
  formatDuration,
  sidecarPath,
  withRetries
} from './util.js'

/**
 * List gs://arxiv-dataset and record every PDF, PostScript, and HTML object.
 *
 * Output (tab-delimited, resumable):
 *   name \t md5
 *
 * `name` is the object name. `md5` is lowercase hex from the object's
 * base64 `md5Hash` (no file bodies are downloaded).
 *
 * `arxiv/pdf/` is a second copy of `arxiv/arxiv/pdf/` with the same MD5s,
 * and it stops at 2025-08. Those objects are skipped.
 *
 *   node arxiv.js
 *   node arxiv.js --output arxiv_hashes.txt
 *   node arxiv.js --limit 20
 */

const LIST_URL = 'https://storage.googleapis.com/storage/v1/b/arxiv-dataset/o'
const DEFAULT_OUTPUT = 'arxiv_hashes.txt'
const PAGE_SIZE = 1000
const FETCH_RETRIES = 5
const RETRY_DELAY_MS = 10000
const DUPLICATE_PDF_PREFIX = 'arxiv/pdf/'

const NEW_PAPER_ID = /(?:^|\/)(\d{4}\.\d{4,5})v\d+/
const OLD_PAPER_ID = /\/([^/]+)\/(?:pdf|ps|html)\/\d{4}\/(\d{7})v\d+/

const kindOf = (name) => {
  if (name.startsWith(DUPLICATE_PDF_PREFIX)) return null
  if (name.includes('/pdf/')) return 'pdf'
  if (name.includes('/ps/')) return 'ps'
  if (name.includes('/html/')) return 'html'
  return null
}

/** arXiv id without version. Old ids keep the archive (`hep-th/9901001`). */
const paperIdOf = (name) => {
  const modern = name.match(NEW_PAPER_ID)
  if (modern) return modern[1]
  const legacy = name.match(OLD_PAPER_ID)
  if (legacy) return `${legacy[1]}/${legacy[2]}`
  return null
}

const md5Hex = (b64) => {
  if (!b64) return null
  const hex = Buffer.from(b64, 'base64').toString('hex')
  if (!/^[0-9a-f]{32}$/.test(hex)) return null
  return hex
}

const readResume = (resumePath) => {
  if (!fs.existsSync(resumePath)) return ''
  return fs.readFileSync(resumePath, 'utf8').trim()
}

const listUrl = (startOffset, pageToken) => {
  const url = new URL(LIST_URL)
  url.searchParams.set('prefix', 'arxiv/')
  url.searchParams.set('maxResults', String(PAGE_SIZE))
  url.searchParams.set('fields', 'nextPageToken,items(name,md5Hash)')
  if (startOffset) url.searchParams.set('startOffset', startOffset)
  if (pageToken) url.searchParams.set('pageToken', pageToken)
  return url.href
}

const fetchList = async (url) => {
  const response = await fetch(url, {
    headers: { 'User-Agent': USER_AGENT, Accept: 'application/json' }
  })
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
  return response.json()
}

const tally = async (outputPath) => {
  const counts = { pdf: 0, ps: 0, html: 0, unparsed: 0 }
  const papers = new Set()
  if (!fs.existsSync(outputPath)) return { ...counts, papers: 0 }

  const rl = readline.createInterface({
    input: fs.createReadStream(outputPath),
    crlfDelay: Infinity
  })
  for await (const line of rl) {
    if (!line) continue
    const tab = line.indexOf('\t')
    const name = tab < 0 ? line : line.slice(0, tab)
    const kind = kindOf(name)
    if (kind) counts[kind]++
    const paperId = paperIdOf(name)
    if (paperId) papers.add(paperId)
    else counts.unparsed++
  }
  return { ...counts, papers: papers.size }
}

export const collectArxivHashes = async ({
  outputPath = DEFAULT_OUTPUT,
  limit = Infinity
} = {}) => {
  const resumePath = sidecarPath(outputPath, 'resume.txt')
  let startOffset = readResume(resumePath)
  let pageToken
  let written = 0
  let missingMd5 = 0
  let pages = 0
  const runStart = Date.now()

  console.log('output:', outputPath)
  if (startOffset) console.log('resume after', startOffset)

  for (;;) {
    if (written >= limit) break
    const url = listUrl(startOffset, pageToken)
    const page = await withRetries(url, () => fetchList(url), {
      retries: FETCH_RETRIES,
      delayMs: RETRY_DELAY_MS
    })
    const items = page.items || []
    pages++
    for (const item of items) {
      if (!item.name || item.name <= startOffset) continue
      const kind = kindOf(item.name)
      if (kind && written < limit) {
        const digest = md5Hex(item.md5Hash)
        if (!digest) {
          missingMd5++
          console.error('missing md5', item.name)
        } else {
          appendHash(outputPath, item.name, digest)
          written++
        }
      }
      fs.writeFileSync(resumePath, `${item.name}\n`)
      if (written >= limit) break
    }
    const elapsed = Date.now() - runStart
    console.log(
      `page ${pages}: +${items.length} listed, ${written} written this run, ` +
      `elapsed ${formatDuration(elapsed)}`
    )
    if (!page.nextPageToken || written >= limit) break
    pageToken = page.nextPageToken
  }

  return { written, pages, missingMd5 }
}

const main = async () => {
  const args = minimist(process.argv.slice(2), {
    default: { output: DEFAULT_OUTPUT },
    alias: { o: 'output', n: 'limit' },
    string: ['output']
  })
  const limit = args.limit == null ? Infinity : Number(args.limit)
  if (limit !== Infinity && (!Number.isInteger(limit) || limit <= 0)) {
    throw new Error(`invalid --limit: ${args.limit}`)
  }

  const started = Date.now()
  const { written, pages, missingMd5 } = await collectArxivHashes({
    outputPath: args.output,
    limit
  })
  const counts = await tally(args.output)
  console.log(
    `done: ${written} written this run, ${pages} pages, ` +
    `${missingMd5} missing md5, elapsed ${formatDuration(Date.now() - started)}`
  )
  console.log(`pdfs: ${counts.pdf}`)
  console.log(`ps: ${counts.ps}`)
  console.log(`html: ${counts.html}`)
  console.log(`papers: ${counts.papers}`)
  if (counts.unparsed) console.log(`unparsed: ${counts.unparsed}`)
}

if (esMain(import.meta)) {
  main().catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
}
