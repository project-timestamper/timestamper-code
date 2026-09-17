import minimist from 'minimist'
import esMain from 'es-main'
import { collectUrlHashes, normalizeDirUrl } from './ftp-https.js'

/**
 * Spider https://files.wwpdb.org/ (HTTPS directory indexes) and SHA-256 every
 * leaf file. Output is url\\thash (full URL as key), resumable.
 *
 *   node wwpdb.js
 *   node wwpdb.js -o wwpdb_hashes.txt
 *   node wwpdb.js --root https://files.wwpdb.org/pub/pdb/doc/
 */

const DEFAULT_ROOT = 'https://files.wwpdb.org/pub/'
const DEFAULT_OUTPUT = 'wwpdb_hashes.txt'

export const collectWwpdbHashes = async ({
  roots = [DEFAULT_ROOT],
  outputPath = DEFAULT_OUTPUT
} = {}) =>
  collectUrlHashes({
    roots: roots.map(normalizeDirUrl),
    outputPath,
    keyFn: (url) => url
  })

const main = async () => {
  const args = minimist(process.argv.slice(2), {
    default: { output: DEFAULT_OUTPUT },
    alias: { o: 'output', r: 'root' },
    string: ['output', 'root']
  })

  const roots = args.root
    ? (Array.isArray(args.root) ? args.root : [args.root])
    : [DEFAULT_ROOT]

  const hashed = await collectWwpdbHashes({
    roots,
    outputPath: args.output
  })
  console.log('hashed this run:', hashed)
}

if (esMain(import.meta)) {
  main().catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
}
