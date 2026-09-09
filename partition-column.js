import fs from 'node:fs'
import path from 'node:path'
import minimist from 'minimist'
import esMain from 'es-main'
import { readColumn } from './column.js'
import { makePartitions } from './partition.js'

/**
 * Extract column `n` from a TSV hash list, partition by hex prefix length `s`,
 * and write OpenTimestamps partitions under ../timestamper/docs/<collection>.
 *
 * Usage:
 *   node partition-column.js <input.tsv> --column 1 --prefix 3 --name annas_archive_torrents
 *   node partition-column.js annas_archive_torrent_hashes.txt -n 1 -s 3 -c annas_archive_torrents
 */

const DOCS_ROOT = path.resolve('../timestamper/docs')
const HEX_RE = /^[0-9a-f]+$/i

const usage = () => {
  console.error(`Usage: node partition-column.js <input.tsv> --column <n> --prefix <s> --name <collection>

  --column, -n   0-based TSV column containing hashes (e.g. 1 for url\\thash)
  --prefix, -s   hex prefix length for partition filenames
  --name, -c     collection directory name under ${DOCS_ROOT}
`)
}

const run = async (argv = process.argv.slice(2)) => {
  const args = minimist(argv, {
    string: ['name'],
    boolean: ['help'],
    alias: {
      n: 'column',
      s: 'prefix',
      c: 'name',
      h: 'help'
    }
  })

  if (args.help || args._.length !== 1 ||
      args.column === undefined || args.prefix === undefined || !args.name) {
    usage()
    process.exitCode = 1
    return
  }

  const inputPath = args._[0]
  const column = Number(args.column)
  const prefixLength = Number(args.prefix)
  const collection = String(args.name)

  if (!Number.isInteger(column) || column < 0) {
    throw new Error(`invalid --column: ${args.column}`)
  }
  if (!Number.isInteger(prefixLength) || prefixLength < 1) {
    throw new Error(`invalid --prefix: ${args.prefix}`)
  }
  if (!collection || collection.includes('/') || collection.includes('..')) {
    throw new Error(`invalid --name: ${args.name}`)
  }
  if (!fs.existsSync(inputPath)) {
    throw new Error(`input not found: ${inputPath}`)
  }

  const outDir = path.join(DOCS_ROOT, collection)
  console.log('reading column', column, 'from', inputPath)
  const raw = await readColumn(inputPath, column)

  const hashes = []
  let skipped = 0
  for (const value of raw) {
    const hash = String(value).trim().toLowerCase()
    if (!hash || !HEX_RE.test(hash)) {
      skipped++
      continue
    }
    if (hash.length < prefixLength) {
      skipped++
      continue
    }
    hashes.push(hash)
  }

  console.log(`hashes ${hashes.length} (skipped ${skipped})`)
  console.log('writing partitions to', outDir, `prefix=${prefixLength}`)
  fs.mkdirSync(outDir, { recursive: true })
  await makePartitions(outDir, hashes, prefixLength)
  console.log('done')
}

if (esMain(import.meta)) {
  run().catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
}

export { run }
