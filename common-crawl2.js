import { createHash } from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import readline from 'node:readline'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import minimist from 'minimist'
import esMain from 'es-main'
import {
  appendLine,
  fetchOnce,
  formatDuration,
  loadLineSet,
  withRetries
} from './util.js'

/**
 * Hash Common Crawl ZipNum CDX blocks (SHA-256 of each compressed gzip
 * member) into one binary hashlist per shard:
 *
 *   ../timestamper/docs/common_crawl_blocks/<CRAWL>/cdx-NNNNN
 *   ../timestamper/docs/common_crawl_blocks/completed_crawls.txt
 *
 * Shard files are named from cluster.idx `part` with `.gz` stripped
 * (e.g. cdx-00066.gz → cdx-00066). Verify uses showPagedIndex `part`.
 *
 * Each crawl directory is cleared at start (no mid-crawl resume). Each shard
 * is downloaded to disk, hashed by cluster.idx ranges, written, then deleted.
 *
 *   node common-crawl2.js --crawl CC-MAIN-2026-34
 *   node common-crawl2.js --all
 *   node common-crawl2.js --crawl CC-MAIN-2026-34 --limit 1
 */

const DATA_BASE = 'https://data.commoncrawl.org/'
const COLLINFO_URL = 'https://index.commoncrawl.org/collinfo.json'
const DEFAULT_OUT = path.resolve('../timestamper/docs/common_crawl_blocks')
const COMPLETED_CRAWLS_FILE = 'completed_crawls.txt'

/** Map CDX `part` (e.g. cdx-00066.gz) to on-disk hashlist name. */
const shardHashlistName = (part) =>
  part.endsWith('.gz') ? part.slice(0, -3) : part

const listCrawls = async () => {
  const response = await withRetries(COLLINFO_URL, () => fetchOnce(COLLINFO_URL))
  const collinfo = await response.json()
  if (!Array.isArray(collinfo) || collinfo.length === 0) {
    throw new Error('empty collinfo.json')
  }
  return collinfo.map((c) => c.id)
}

/** @returns {Map<string, { offset: number, length: number }[]>} */
const loadClusterIdx = async (crawl) => {
  const url = `${DATA_BASE}cc-index/collections/${crawl}/indexes/cluster.idx`
  const response = await withRetries(url, () => fetchOnce(url))
  const rl = readline.createInterface({
    input: Readable.fromWeb(response.body),
    crlfDelay: Infinity
  })
  const byPart = new Map()
  for await (const line of rl) {
    if (!line) continue
    const tab = line.indexOf('\t')
    if (tab < 0) continue
    const rest = line.slice(tab + 1).split('\t')
    const part = rest[0]
    const offset = Number(rest[1])
    const length = Number(rest[2])
    if (!part || !Number.isFinite(offset) || !Number.isFinite(length)) continue
    if (!byPart.has(part)) byPart.set(part, [])
    byPart.get(part).push({ offset, length })
  }
  for (const blocks of byPart.values()) {
    blocks.sort((a, b) => a.offset - b.offset)
  }
  return byPart
}

const resetCrawlDir = (dir) => {
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true, force: true })
  }
  fs.mkdirSync(dir, { recursive: true })
}

const downloadShard = async (url, destPath) => {
  await withRetries(url, async () => {
    fs.mkdirSync(path.dirname(destPath), { recursive: true })
    const response = await fetchOnce(url)
    const expected = Number(response.headers.get('content-length'))
    const out = fs.createWriteStream(destPath)
    try {
      await pipeline(Readable.fromWeb(response.body), out)
    } catch (e) {
      out.destroy()
      if (fs.existsSync(destPath)) fs.unlinkSync(destPath)
      throw e
    }
    const size = fs.statSync(destPath).size
    if (Number.isFinite(expected) && expected > 0 && size !== expected) {
      fs.unlinkSync(destPath)
      throw new Error(`size mismatch: got ${size}, expected ${expected}`)
    }
  })
}

/** @returns {Buffer[]} SHA-256 digests (32 bytes each), block order */
const hashBlocksFromFile = (filePath, blocks) => {
  if (blocks.length === 0) return []
  if (blocks[0].offset !== 0) {
    throw new Error(`first block offset ${blocks[0].offset} != 0`)
  }
  const fileSize = fs.statSync(filePath).size
  let end = 0
  for (let i = 0; i < blocks.length; i++) {
    const { offset, length } = blocks[i]
    if (i > 0 && offset !== blocks[i - 1].offset + blocks[i - 1].length) {
      throw new Error(`non-contiguous blocks at index ${i}`)
    }
    end = offset + length
  }
  if (end !== fileSize) {
    throw new Error(`blocks cover ${end} bytes but file is ${fileSize}`)
  }

  const fd = fs.openSync(filePath, 'r')
  const hashes = []
  try {
    for (const { offset, length } of blocks) {
      const buf = Buffer.allocUnsafe(length)
      const read = fs.readSync(fd, buf, 0, length, offset)
      if (read !== length) {
        throw new Error(`short read at ${offset}: got ${read}, want ${length}`)
      }
      hashes.push(createHash('sha256').update(buf).digest())
    }
  } finally {
    fs.closeSync(fd)
  }
  return hashes
}

const writeShardHashlist = (outDir, part, digests) => {
  const name = shardHashlistName(part)
  fs.writeFileSync(path.join(outDir, name), Buffer.concat(digests))
  return name
}

const processShard = async (crawl, part, blocks, outDir) => {
  const url = `${DATA_BASE}cc-index/collections/${crawl}/indexes/${part}`
  const tmpPath = path.join(outDir, `${part}.tmp`)
  if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)

  console.log('  downloading', part)
  await downloadShard(url, tmpPath)
  try {
    console.log('  hashing', blocks.length, 'blocks')
    const digests = hashBlocksFromFile(tmpPath, blocks)
    const name = writeShardHashlist(outDir, part, digests)
    console.log('  wrote', name)
    return digests.length
  } finally {
    if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
  }
}

export const collectCrawlBlockHashes = async ({
  crawl,
  outRoot = DEFAULT_OUT,
  limit = Infinity
} = {}) => {
  const outDir = path.join(outRoot, crawl)
  resetCrawlDir(outDir)

  console.log('crawl:', crawl)
  console.log('output:', outDir)

  console.log('loading cluster.idx…')
  const byPart = await loadClusterIdx(crawl)
  const parts = [...byPart.keys()].sort()
  const selected = Number.isFinite(limit) ? parts.slice(0, limit) : parts
  console.log('shards:', parts.length, 'selected:', selected.length)

  const startMs = Date.now()
  let blocksHashed = 0

  for (let i = 0; i < selected.length; i++) {
    const part = selected[i]
    const blocks = byPart.get(part)
    console.log('shard', part, `(${blocks.length} blocks) [${i + 1}/${selected.length}]`)
    try {
      const n = await processShard(crawl, part, blocks, outDir)
      blocksHashed += n
      const elapsed = Date.now() - startMs
      const done = i + 1
      const eta = done > 0 ? (selected.length - done) * (elapsed / done) : 0
      console.log(
        `done ${part} blocks=${n} ` +
        `progress: ${blocksHashed} hashed, ${done}/${selected.length} shards, ` +
        `elapsed ${formatDuration(elapsed)}, eta ${formatDuration(eta)}`
      )
    } catch (e) {
      console.error(part, e.message)
      throw e
    }
  }

  return {
    crawl,
    blocksHashed,
    shardsWritten: selected.length,
    outDir,
    complete: !Number.isFinite(limit) || limit >= parts.length
  }
}

const main = async () => {
  const args = minimist(process.argv.slice(2), {
    boolean: ['all'],
    default: { out: DEFAULT_OUT },
    alias: { c: 'crawl', o: 'out', n: 'limit' },
    string: ['crawl', 'out']
  })

  const outRoot = args.out
  fs.mkdirSync(outRoot, { recursive: true })
  const completedPath = path.join(outRoot, COMPLETED_CRAWLS_FILE)
  const completedCrawls = loadLineSet(completedPath)

  let crawls
  if (args.all) {
    const all = await listCrawls()
    crawls = all.filter((id) => !completedCrawls.has(id))
    console.log('all crawls:', all.length, 'already done:', completedCrawls.size, 'remaining:', crawls.length)
  } else if (args.crawl) {
    crawls = [args.crawl]
  } else {
    console.error('Usage: node common-crawl2.js --crawl CC-MAIN-2026-34')
    console.error('       node common-crawl2.js --all')
    process.exitCode = 1
    return
  }

  const limit = args.limit === undefined ? Infinity : Number(args.limit)
  const failures = []
  for (const crawl of crawls) {
    try {
      const result = await collectCrawlBlockHashes({
        crawl,
        outRoot,
        limit
      })
      console.log(
        'finished', result.crawl,
        'blocks hashed:', result.blocksHashed,
        'shards:', result.shardsWritten
      )
      if (result.complete && !completedCrawls.has(crawl)) {
        appendLine(completedPath, crawl)
        completedCrawls.add(crawl)
        console.log('recorded completed crawl:', crawl)
      }
    } catch (err) {
      console.error('FAILED', crawl, err.message || err)
      failures.push({ crawl, error: err.message || String(err) })
    }
  }
  if (failures.length > 0) {
    console.error('failed crawls:', failures.length)
    for (const f of failures) {
      console.error(' ', f.crawl, f.error)
    }
    process.exitCode = 1
  }
}

if (esMain(import.meta)) {
  main().catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
}
