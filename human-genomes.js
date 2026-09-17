import minimist from 'minimist'
import esMain from 'es-main'
import { collectUrlHashes } from './ftp-https.js'

const ROOT_URLS = [
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/integrated_sv_map/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/historical_data/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/changelog_details/'
]
const FTP_BASE = 'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/'
const DEFAULT_OUTPUT = 'human_genome_hashes.txt'

const relativeKey = (fileUrl) => {
  if (fileUrl.startsWith(FTP_BASE)) {
    return fileUrl.slice(FTP_BASE.length)
  }
  return fileUrl
}

const pathnameOf = (url) => {
  try {
    return new URL(url).pathname
  } catch {
    return url
  }
}

const isIndexUrl = (url) => /\.(?:tbi|csi)$/i.test(pathnameOf(url))

export const collectHumanGenomeHashes = async (outputPath = DEFAULT_OUTPUT) =>
  collectUrlHashes({
    roots: ROOT_URLS,
    outputPath,
    keyFn: relativeKey,
    filter: (url) => !isIndexUrl(url)
  })

const main = async () => {
  const args = minimist(process.argv.slice(2), {
    default: { output: DEFAULT_OUTPUT },
    alias: { o: 'output' }
  })

  const hashed = await collectHumanGenomeHashes(args.output)
  console.log('hashed this run:', hashed)
}

if (esMain(import.meta)) {
  main()
}
