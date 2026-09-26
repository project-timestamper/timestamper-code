import fs from 'node:fs'
import path from 'node:path'
import minimist from 'minimist'
import esMain from 'es-main'
import { stampFilePaths } from './partition.js'

/**
 * OpenTimestamps each shard hash list under common_crawl_blocks.
 * All pending files go in one Merkle tree / calendar submit. Skips shards
 * that already have a sibling .ots.
 *
 *   node stamp-cc-blocks.js
 *   node stamp-cc-blocks.js --root ../timestamper/docs/common_crawl_blocks
 *   node stamp-cc-blocks.js --crawl CC-MAIN-2026-39
 */

const DEFAULT_ROOT = path.resolve('../timestamper/docs/common_crawl_blocks')

const listShardFiles = (dir) =>
  fs.readdirSync(dir)
    .filter((name) => name.startsWith('cdx-') && !name.endsWith('.ots'))
    .sort()

const run = async (argv = process.argv.slice(2)) => {
  const args = minimist(argv, {
    string: ['root', 'crawl'],
    default: { root: DEFAULT_ROOT }
  })
  const root = args.root
  if (!fs.existsSync(root)) {
    throw new Error(`missing ${root}`)
  }

  const crawls = (args.crawl ? [args.crawl] : fs.readdirSync(root))
    .filter((name) => name.startsWith('CC-MAIN-'))
    .filter((name) => fs.statSync(path.join(root, name)).isDirectory())
    .sort()

  const todo = []
  let skipped = 0
  for (const crawl of crawls) {
    const dir = path.join(root, crawl)
    for (const name of listShardFiles(dir)) {
      const filePath = path.join(dir, name)
      if (fs.existsSync(`${filePath}.ots`)) skipped++
      else todo.push(filePath)
    }
  }

  console.log('root', root, 'crawls', crawls.length, 'todo', todo.length, 'already had ots', skipped)
  if (todo.length === 0) {
    console.log('done stamped 0')
    return
  }
  console.log('hashing and submitting', todo.length, 'files…')
  const stamped = await stampFilePaths(todo)
  console.log('done stamped', stamped, 'already had ots', skipped)
}

if (esMain(import.meta)) {
  run().catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
}

export { run }
