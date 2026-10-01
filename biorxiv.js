import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { createHash, randomBytes } from 'node:crypto'
import { pipeline } from 'node:stream/promises'
import {
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client
} from '@aws-sdk/client-s3'
import minimist from 'minimist'
import esMain from 'es-main'
import unzipper from 'unzipper'
import {
  appendHash,
  formatDuration,
  sidecarPath,
  withRetries
} from './util.js'

/**
 * Hash every bioRxiv and medRxiv MECA package, and every file inside it.
 *
 * Buckets (requester-pays, us-east-1), bioRxiv first:
 *   s3://biorxiv-src-monthly
 *   s3://medrxiv-src-monthly
 *
 * One .meca at a time. The object is written to a temp file, SHA-256 hashed
 * as stored, then each zip member is hashed uncompressed. Nested zips are
 * one file and are not opened. Directory entries are skipped.
 *
 * Output (tab-delimited):
 *   {doi}v{version} \t sha256
 *   {doi}v{version}/{path-inside-zip} \t sha256
 *
 * DOI and version come from the JATS XML. If they cannot be read, the id
 * is s3://bucket/key so the package is still recorded.
 *
 * Resume sidecar is `count \t bucket \t key`, written after each package.
 * `--limit` is how many .meca packages to process this run.
 *
 * Logs the cumulative number of .meca files complete every 10,000, and
 * again at the end. A download that still fails stops the run and leaves
 * the cursor put. A package that downloads but is not a readable zip is
 * not counted complete, and the cursor moves past it.
 *
 * Uses the AWS SDK default credential chain. Run on an EC2 instance in
 * us-east-1 so the download stays in-region.
 *
 *   node biorxiv.js
 *   node biorxiv.js --output biorxiv_hashes.txt
 *   node biorxiv.js --limit 1
 */

const BUCKETS = ['biorxiv-src-monthly', 'medrxiv-src-monthly']
const REGION = 'us-east-1'
const DEFAULT_OUTPUT = 'biorxiv_hashes.txt'
const PAGE_SIZE = 1000
const LOG_EVERY = 10000
const FETCH_RETRIES = 5
const RETRY_DELAY_MS = 10000
const XML_SNIFF_BYTES = 2 * 1024 * 1024

const memberPath = (raw) =>
  String(raw || '').replace(/\\/g, '/').replace(/^\/+/, '').replace(/[\t\r\n]/g, '')

/** `{doi}v{version}` from a JATS snippet, or null when either part is missing. */
export const paperIdFromXml = (xml) => {
  if (!xml) return null
  let doi = null
  for (const match of xml.matchAll(/<article-id\b([^>]*)>([^<]*)<\/article-id>/gi)) {
    if (/pub-id-type\s*=\s*["']doi["']/i.test(match[1])) {
      doi = match[2].trim().replace(/^doi:\s*/i, '')
      break
    }
  }
  if (!doi) return null

  let version = null
  const articleVersion = xml.match(/<article-version\b[^>]*>\s*(\d+)\s*<\/article-version>/i)
  if (articleVersion) version = articleVersion[1]
  if (!version) {
    const meta = xml.match(
      /<meta-name>\s*(?:article[-_ ]?)?version\s*<\/meta-name>\s*<meta-value>\s*(\d+)\s*<\/meta-value>/i
    )
    if (meta) version = meta[1]
  }

  const trailing = doi.match(/v(\d+)$/i)
  if (trailing && (!version || trailing[1] === version)) {
    doi = doi.slice(0, -trailing[0].length)
    version = version || trailing[1]
  }
  if (!version) return null
  return `${doi}v${version}`.replace(/[\t\r\n]/g, '')
}

const hashFileStream = async (stream, { sniff = false } = {}) => {
  const hash = createHash('sha256')
  const sniffed = []
  let sniffedBytes = 0
  try {
    for await (const chunk of stream) {
      hash.update(chunk)
      if (sniff && sniffedBytes < XML_SNIFF_BYTES) {
        const take = Math.min(chunk.length, XML_SNIFF_BYTES - sniffedBytes)
        sniffed.push(chunk.subarray(0, take))
        sniffedBytes += take
      }
    }
  } catch (err) {
    stream.destroy?.()
    throw err
  }
  const xml = sniff
    ? Buffer.concat(sniffed).subarray(0, XML_SNIFF_BYTES).toString('utf8')
    : null
  return { digest: hash.digest('hex'), xml }
}

const hashFile = (filePath) =>
  hashFileStream(fs.createReadStream(filePath)).then((result) => result.digest)

/**
 * SHA-256 of the zip bytes, then of each member. `fallbackId` is used when
 * the JATS DOI and version cannot be read.
 */
export const rowsForMeca = async (filePath, fallbackId) => {
  const zipDigest = await hashFile(filePath)
  const directory = await unzipper.Open.file(filePath)
  const files = await directory.files
  const members = []
  let paperId = null
  for (const file of files) {
    const rawPath = file.path || ''
    if (file.type === 'Directory' || /[/\\]$/.test(rawPath)) continue
    const rel = memberPath(rawPath)
    if (!rel) continue
    const sniff = rel.toLowerCase().endsWith('.xml')
    const { digest, xml } = await hashFileStream(file.stream(), { sniff })
    members.push({ rel, digest })
    if (!paperId && xml) paperId = paperIdFromXml(xml)
  }
  const prefix = paperId || fallbackId
  const rows = [{ id: prefix, digest: zipDigest }]
  for (const member of members) {
    rows.push({ id: `${prefix}/${member.rel}`, digest: member.digest })
  }
  return { rows, unparsed: !paperId }
}

const send = async (client, command) => {
  try {
    return await client.send(command)
  } catch (err) {
    const status = err.$metadata?.httpStatusCode
    if (!status) throw err
    const wrapped = new Error(`status: ${status} ${err.message}`)
    wrapped.cause = err
    throw wrapped
  }
}

const downloadTo = async (client, bucket, key, dest) => {
  const response = await send(client, new GetObjectCommand({
    Bucket: bucket,
    Key: key,
    RequestPayer: 'requester'
  }))
  if (!response.Body) {
    throw new Error(`no body: s3://${bucket}/${key}`)
  }
  await pipeline(response.Body, fs.createWriteStream(dest))
  if (response.ContentLength != null) {
    const size = fs.statSync(dest).size
    if (size !== Number(response.ContentLength)) {
      throw new Error(
        `short read s3://${bucket}/${key}: ${size} of ${response.ContentLength}`
      )
    }
  }
}

const listPage = (client, bucket, { startAfter, continuationToken }) =>
  send(client, new ListObjectsV2Command({
    Bucket: bucket,
    MaxKeys: PAGE_SIZE,
    RequestPayer: 'requester',
    ...(startAfter ? { StartAfter: startAfter } : {}),
    ...(continuationToken ? { ContinuationToken: continuationToken } : {})
  }))

async function * listKeys (client, bucket, startAfter) {
  let continuationToken
  let first = true
  for (;;) {
    const page = await withRetries(`list s3://${bucket}`, () => listPage(client, bucket, {
      startAfter: first ? startAfter : undefined,
      continuationToken
    }), {
      retries: FETCH_RETRIES,
      delayMs: RETRY_DELAY_MS
    })
    first = false
    for (const item of page.Contents || []) {
      if (item.Key) yield item.Key
    }
    if (!page.IsTruncated || !page.NextContinuationToken) return
    continuationToken = page.NextContinuationToken
  }
}

const readResume = (resumePath) => {
  if (!fs.existsSync(resumePath)) return null
  const line = fs.readFileSync(resumePath, 'utf8').trim()
  if (!line) return null
  const tab1 = line.indexOf('\t')
  const tab2 = line.indexOf('\t', tab1 + 1)
  if (tab1 <= 0 || tab2 < 0) {
    throw new Error(`bad resume file: ${resumePath}`)
  }
  const count = Number(line.slice(0, tab1))
  const bucket = line.slice(tab1 + 1, tab2)
  const key = line.slice(tab2 + 1)
  if (!Number.isInteger(count) || count < 0 || !bucket || !key) {
    throw new Error(`bad resume file: ${resumePath}`)
  }
  if (!BUCKETS.includes(bucket)) {
    throw new Error(`resume bucket is not a collection bucket: ${bucket}`)
  }
  return { count, bucket, key }
}

const writeResume = (resumePath, count, bucket, key) => {
  fs.writeFileSync(resumePath, `${count}\t${bucket}\t${key}\n`)
}

const logComplete = (completed, bucket, key, started) => {
  console.log(
    `meca files complete: ${completed}, ` +
    `elapsed ${formatDuration(Date.now() - started)}, ` +
    `last ${bucket}/${key}`
  )
}

export const collectBiorxivHashes = async ({
  outputPath = DEFAULT_OUTPUT,
  limit = Infinity
} = {}) => {
  const resumePath = sidecarPath(outputPath, 'resume.txt')
  const resume = readResume(resumePath)
  let completed = resume ? resume.count : 0
  let processed = 0
  let hashes = 0
  let failed = 0
  let unparsed = 0
  const started = Date.now()
  const s3 = new S3Client({ region: REGION })
  const startIndex = resume ? BUCKETS.indexOf(resume.bucket) : 0

  console.log('output:', outputPath)
  if (resume) console.log('resume after', `${resume.bucket}/${resume.key}`)
  console.log(`meca files complete: ${completed}`)

  let stop = false
  for (let i = startIndex; i < BUCKETS.length && !stop; i++) {
    const bucket = BUCKETS[i]
    const startAfter = resume && bucket === resume.bucket ? resume.key : undefined
    for await (const key of listKeys(s3, bucket, startAfter)) {
      if (startAfter && key <= startAfter) continue
      if (!key.toLowerCase().endsWith('.meca')) continue
      if (processed >= limit) {
        stop = true
        break
      }
      processed++

      const dest = path.join(
        os.tmpdir(),
        `biorxiv-${process.pid}-${randomBytes(8).toString('hex')}.meca`
      )
      const fallbackId = `s3://${bucket}/${key}`
      let downloaded = false
      let outcome = null
      try {
        await withRetries(fallbackId, () => downloadTo(s3, bucket, key, dest), {
          retries: FETCH_RETRIES,
          delayMs: RETRY_DELAY_MS
        })
        downloaded = true
        outcome = await rowsForMeca(dest, fallbackId)
      } catch (err) {
        if (!downloaded) throw err
        failed++
        console.error(`failed ${fallbackId}: ${err.message}`)
        writeResume(resumePath, completed, bucket, key)
        continue
      } finally {
        await fs.promises.rm(dest, { force: true })
      }

      for (const row of outcome.rows) {
        appendHash(outputPath, row.id, row.digest)
      }
      hashes += outcome.rows.length
      if (outcome.unparsed) unparsed++
      completed++
      writeResume(resumePath, completed, bucket, key)
      if (completed % LOG_EVERY === 0) {
        logComplete(completed, bucket, key, started)
      }
    }
  }

  return {
    completed,
    written: completed - (resume ? resume.count : 0),
    hashes,
    failed,
    unparsed
  }
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
  const { completed, written, hashes, failed, unparsed } = await collectBiorxivHashes({
    outputPath: args.output,
    limit
  })
  console.log(
    `done: ${written} meca files this run, ${completed} meca files complete, ` +
    `${hashes} hashes written, ${failed} failed, ` +
    `elapsed ${formatDuration(Date.now() - started)}`
  )
  if (unparsed) console.log(`unparsed: ${unparsed}`)
}

if (esMain(import.meta)) {
  main().catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
}
