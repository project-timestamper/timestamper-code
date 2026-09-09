import fs from 'node:fs'
import path from 'node:path'
import readline from 'node:readline'
import minimist from 'minimist'
import esMain from 'es-main'
import { stampPartitionDir } from './partition.js'

/**
 * Stream TSV column `n` into ../timestamper/docs/<name>, partitioned by hex
 * prefix length `s`, then stamp. Avoids loading the full hash list in memory.
 *
 *   node partition-column.js hashes.txt -n 2 -s 4 -c annas_literature_hashes
 */

const DOCS_ROOT = path.resolve('../timestamper/docs')
const FLUSH_BYTES = 32 * 1024 * 1024

const run = async () => {
  const args = minimist(process.argv.slice(2), {
    alias: { n: 'column', s: 'prefix', c: 'name' }
  })
  const [inputPath] = args._
  const column = Number(args.column)
  const prefixLength = Number(args.prefix)
  const outDir = path.join(DOCS_ROOT, String(args.name))

  if (!inputPath || !Number.isInteger(column) || !Number.isInteger(prefixLength) || !args.name) {
    console.error('Usage: node partition-column.js <file> -n <column> -s <prefix> -c <name>')
    process.exitCode = 1
    return
  }

  fs.mkdirSync(outDir, { recursive: true })
  for (const name of fs.readdirSync(outDir)) {
    if (new RegExp(`^[0-9A-Fa-f]{${prefixLength}}(\\.ots)?$`).test(name)) {
      fs.unlinkSync(path.join(outDir, name))
    }
  }

  const pending = new Map() // prefix -> Buffer[]
  let pendingBytes = 0
  let kept = 0

  const flush = () => {
    for (const [prefix, chunks] of pending) {
      fs.appendFileSync(path.join(outDir, prefix), Buffer.concat(chunks))
    }
    pending.clear()
    pendingBytes = 0
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(inputPath),
    crlfDelay: Infinity
  })

  for await (const line of rl) {
    const hash = line.split('\t')[column]?.trim().toLowerCase()
    if (!hash || hash.length < prefixLength || hash.length % 2 || /[^0-9a-f]/.test(hash)) {
      continue
    }
    const prefix = hash.slice(0, prefixLength).toUpperCase()
    const bytes = Buffer.from(hash, 'hex')
    if (!pending.has(prefix)) pending.set(prefix, [])
    pending.get(prefix).push(bytes)
    pendingBytes += bytes.length
    kept++
    if (pendingBytes >= FLUSH_BYTES) flush()
    if (kept % 1_000_000 === 0) console.log('kept', kept)
  }
  flush()

  console.log('wrote', kept, 'hashes; stamping…')
  console.log('stamped', await stampPartitionDir(outDir))
}

if (esMain(import.meta)) {
  run().catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
}
