import { once } from 'node:events'
import fs from 'node:fs'
import { spawn } from 'node:child_process'
import { Readable } from 'node:stream'
import readline from 'node:readline'
import { createGunzip } from 'node:zlib'
import minimist from 'minimist'
import esMain from 'es-main'
import { base32 } from 'rfc4648'

const DATA_BASE = 'https://data.commoncrawl.org/'
const COLLINFO_URL = 'https://index.commoncrawl.org/collinfo.json'
const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
const DEFAULT_OUTPUT = 'common_crawl_hashes.txt'
const FETCH_RETRIES = 5
const RETRY_DELAY_MS = 10000

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const sidecarPath = (outputPath, suffix) => {
  if (outputPath.endsWith('_hashes.txt')) {
    return outputPath.replace(/_hashes\.txt$/, `_${suffix}.txt`)
  }
  return `${outputPath}.${suffix}`
}

const formatDuration = (ms) => {
  const totalSec = Math.max(0, Math.round(ms / 1000))
  const h = Math.floor(totalSec / 3600)
  const m = Math.floor((totalSec % 3600) / 60)
  const s = totalSec % 60
  if (h > 0) {
    return `${h}h ${m}m ${s}s`
  }
  if (m > 0) {
    return `${m}m ${s}s`
  }
  return `${s}s`
}

const digestToHex = (digest) => {
  if (!digest) {
    return null
  }
  const trimmed = digest.trim()
  if (/^[0-9a-f]{40}$/i.test(trimmed)) {
    return trimmed.toLowerCase()
  }
  try {
    const bytes = Buffer.from(base32.parse(trimmed, { loose: true }))
    if (bytes.length !== 20) {
      return null
    }
    return bytes.toString('hex')
  } catch {
    return null
  }
}

const extractDigest = (line) => {
  if (!line) {
    return null
  }
  const jsonStart = line.indexOf('{')
  if (jsonStart !== -1) {
    try {
      const obj = JSON.parse(line.slice(jsonStart))
      return digestToHex(obj.digest)
    } catch {
      return null
    }
  }
  const fields = line.split(' ')
  // Classic CDX: urlkey timestamp original mimetype statuscode digest ...
  if (fields.length >= 6) {
    return digestToHex(fields[5])
  }
  return null
}

const loadLineSet = (filePath) => {
  const values = new Set()
  if (!fs.existsSync(filePath)) {
    return values
  }
  const fd = fs.openSync(filePath, 'r')
  const bufSize = 1024 * 1024
  const buf = Buffer.alloc(bufSize)
  let leftover = ''
  try {
    while (true) {
      const bytesRead = fs.readSync(fd, buf, 0, bufSize, null)
      if (bytesRead === 0) {
        break
      }
      leftover += buf.toString('utf8', 0, bytesRead)
      const lines = leftover.split('\n')
      leftover = lines.pop()
      for (const line of lines) {
        const value = line.trim()
        if (value) {
          values.add(value)
        }
      }
    }
    if (leftover.trim()) {
      values.add(leftover.trim())
    }
  } finally {
    fs.closeSync(fd)
  }
  return values
}

const fetchOnce = async (url) => {
  const response = await fetch(url, { headers: { 'User-Agent': USER_AGENT } })
  if (!response.ok) {
    throw new Error(`status: ${response.status} ${url}`)
  }
  if (!response.body) {
    throw new Error(`no body: ${url}`)
  }
  return response
}

const withRetries = async (label, fn) => {
  let lastError
  for (let attempt = 1; attempt <= FETCH_RETRIES; attempt++) {
    try {
      return await fn()
    } catch (e) {
      lastError = e
      const cause = e.cause ? ` (${e.cause.message || e.cause})` : ''
      console.error(`retry ${attempt}/${FETCH_RETRIES}`, label, `${e.message}${cause}`)
      if (attempt < FETCH_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt)
      }
    }
  }
  throw lastError
}

const inflateGzipBuffer = async (body) => {
  const unzip = createGunzip()
  const chunks = []
  unzip.on('data', (chunk) => chunks.push(chunk))
  const done = new Promise((resolve, reject) => {
    unzip.once('end', resolve)
    unzip.once('error', reject)
  })
  done.catch(() => {})
  try {
    for await (const chunk of body) {
      if (!unzip.write(chunk)) {
        await Promise.race([once(unzip, 'drain'), done])
      }
    }
    unzip.end()
    await done
  } finally {
    unzip.destroy()
  }
  return Buffer.concat(chunks)
}

const gzipDecodeStream = (webStream) => {
  const input = Readable.fromWeb(webStream)
  const proc = spawn('gzip', ['-cd'], { stdio: ['pipe', 'pipe', 'pipe'] })
  input.pipe(proc.stdin)
  proc.stdin.on('error', () => {})
  proc.stderr.on('data', (chunk) => {
    const msg = chunk.toString().trim()
    if (msg) {
      console.error('gzip:', msg)
    }
  })
  return { stdout: proc.stdout, proc }
}

const latestCrawlId = async () => {
  const response = await withRetries(COLLINFO_URL, () => fetchOnce(COLLINFO_URL))
  const collinfo = await response.json()
  if (!Array.isArray(collinfo) || collinfo.length === 0) {
    throw new Error('empty collinfo.json')
  }
  return collinfo[0].id
}

const listCdxShards = async (crawl) => {
  const pathsUrl = `${DATA_BASE}crawl-data/${crawl}/cc-index.paths.gz`
  const response = await withRetries(pathsUrl, () => fetchOnce(pathsUrl))
  const buf = await inflateGzipBuffer(response.body)
  return buf
    .toString('utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => /\/indexes\/cdx-\d+\.gz$/.test(line))
}

const shardName = (path) => path.split('/').pop()

const processShard = async (shardPath, tmpPath, onProgress) => {
  const url = DATA_BASE + shardPath
  const response = await withRetries(url, () => fetchOnce(url))
  const { stdout, proc } = gzipDecodeStream(response.body)
  const rl = readline.createInterface({ input: stdout, crlfDelay: Infinity })
  const out = fs.createWriteStream(tmpPath)

  let lines = 0
  let digests = 0
  try {
    for await (const line of rl) {
      lines++
      const hex = extractDigest(line)
      if (hex) {
        if (!out.write(`${hex}\n`)) {
          await once(out, 'drain')
        }
        digests++
      }
      if (lines % 100000 === 0) {
        onProgress(lines, digests)
      }
    }
    out.end()
    await once(out, 'finish')
    const [exit] = await once(proc, 'close')
    if (exit !== 0 && exit !== null) {
      throw new Error(`gzip -cd exited ${exit} for ${shardPath}`)
    }
  } catch (e) {
    out.destroy()
    try {
      proc.kill('SIGTERM')
    } catch {
      // ignore
    }
    throw e
  }

  return { lines, digests }
}

const appendFile = async (fromPath, toPath) => {
  const input = fs.createReadStream(fromPath)
  const output = fs.createWriteStream(toPath, { flags: 'a' })
  input.pipe(output)
  await once(output, 'finish')
}

export const collectCommonCrawlDigests = async ({
  crawl,
  outputPath = DEFAULT_OUTPUT,
  limit = Infinity,
  start = 0
} = {}) => {
  const crawlId = crawl || await latestCrawlId()
  const completedPath = sidecarPath(outputPath, 'completed')
  const completed = loadLineSet(completedPath)
  console.log('crawl:', crawlId)
  console.log('already completed shards:', completed.size)

  const shards = await listCdxShards(crawlId)
  const selected = shards.slice(start, start + (Number.isFinite(limit) ? limit : shards.length))
  const todo = selected.filter((path) => !completed.has(shardName(path)))
  console.log('shards listed:', shards.length, 'selected:', selected.length, 'todo:', todo.length)

  const startMs = Date.now()
  let hashed = 0
  let attempted = 0
  let skipped = selected.length - todo.length

  for (const path of todo) {
    const name = shardName(path)
    const tmpPath = `${outputPath}.${name}.tmp`
    attempted++
    try {
      if (fs.existsSync(tmpPath)) {
        fs.unlinkSync(tmpPath)
      }
      console.log('shard', name)
      const { lines, digests } = await processShard(path, tmpPath, (nLines, nDigests) => {
        console.log(`  ${name} lines=${nLines} digests=${nDigests}`)
      })
      await appendFile(tmpPath, outputPath)
      fs.unlinkSync(tmpPath)
      fs.appendFileSync(completedPath, `${name}\n`)
      completed.add(name)
      hashed += digests
      const elapsed = Date.now() - startMs
      const remaining = todo.length - attempted
      const eta = attempted > 0 ? remaining * (elapsed / attempted) : 0
      console.log(
        `done ${name} lines=${lines} digests=${digests} ` +
        `progress: ${hashed} hashed, ${attempted}/${todo.length} attempted, ${skipped} skipped, ` +
        `elapsed ${formatDuration(elapsed)}, eta ${formatDuration(eta)}`
      )
    } catch (e) {
      console.error(name, e.message)
      if (fs.existsSync(tmpPath)) {
        fs.unlinkSync(tmpPath)
      }
    }
  }

  return { crawl: crawlId, hashed }
}

const main = async () => {
  const args = minimist(process.argv.slice(2), {
    default: { output: DEFAULT_OUTPUT, start: 0 },
    alias: { o: 'output', c: 'crawl', n: 'limit' },
    string: ['crawl', 'output']
  })

  const limit = args.limit === undefined ? Infinity : Number(args.limit)
  const result = await collectCommonCrawlDigests({
    crawl: args.crawl,
    outputPath: args.output,
    limit,
    start: Number(args.start) || 0
  })
  console.log('hashed this run:', result.hashed, 'crawl:', result.crawl)
}

if (esMain(import.meta)) {
  main()
}
