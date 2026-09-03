import fs from 'node:fs'
import minimist from 'minimist'
import esMain from 'es-main'
import {
  appendHash,
  formatDuration,
  hashStream,
  loadDoneKeys,
  withRetries
} from './util.js'

const ROOT_URLS = [
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/integrated_sv_map/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/historical_data/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/changelog_details/'
]
const FTP_BASE = 'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/'
const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
const DEFAULT_OUTPUT = 'human_genome_hashes.txt'
const HASH_RETRIES = 5
const RETRY_DELAY_MS = 10000

const normalizeDirUrl = (url) => (url.endsWith('/') ? url : `${url}/`)

const listDirectory = async (dirUrl, rootUrl) => {
  const root = normalizeDirUrl(rootUrl)
  const response = await fetch(normalizeDirUrl(dirUrl), {
    headers: { 'User-Agent': USER_AGENT }
  })
  if (!response.ok) {
    throw new Error(`list failed: ${response.status} ${dirUrl}`)
  }
  const html = await response.text()
  const hrefs = [...html.matchAll(/href="([^"]+)"/g)].map(match => match[1])
  const dirs = []
  const files = []

  for (const href of hrefs) {
    if (!href || href.startsWith('?') || href.startsWith('#') || href.startsWith('../')) {
      continue
    }

    const absolute = new URL(href, normalizeDirUrl(dirUrl))
    if (!absolute.href.startsWith(root)) {
      continue
    }

    if (href.endsWith('/')) {
      dirs.push(absolute.href)
    } else {
      files.push(absolute.href)
    }
  }

  return { dirs: [...new Set(dirs)], files: [...new Set(files)] }
}

const spiderFiles = async (rootUrls = ROOT_URLS) => {
  const files = []
  const seenDirs = new Set()

  for (const rootUrl of rootUrls) {
    const root = normalizeDirUrl(rootUrl)
    const queue = [root]
    console.log('spider root', root)

    while (queue.length > 0) {
      const dirUrl = queue.shift()
      if (seenDirs.has(dirUrl)) {
        continue
      }
      seenDirs.add(dirUrl)
      console.log('listing', dirUrl)
      const { dirs, files: leafFiles } = await listDirectory(dirUrl, root)
      files.push(...leafFiles)
      for (const dir of dirs) {
        if (!seenDirs.has(dir)) {
          queue.push(dir)
        }
      }
    }
  }

  return [...new Set(files)].sort()
}

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

const hashFileOnce = async (url) => {
  const response = await fetch(url, { headers: { 'User-Agent': USER_AGENT } })
  if (!response.ok) {
    throw new Error(`status: ${response.status} ${url}`)
  }
  if (!response.body) {
    throw new Error(`no body: ${url}`)
  }
  return hashStream(response.body)
}

const hashFile = async (url) =>
  withRetries(url, () => hashFileOnce(url), {
    retries: HASH_RETRIES,
    delayMs: RETRY_DELAY_MS
  })

export const collectHumanGenomeHashes = async (outputPath = DEFAULT_OUTPUT) => {
  const done = loadDoneKeys(outputPath)
  console.log('already hashed:', done.size)

  const files = (await spiderFiles(ROOT_URLS)).filter((url) => !isIndexUrl(url))
  const todoFiles = files.filter(url => !done.has(relativeKey(url)))
  console.log('listed:', files.length, 'todo:', todoFiles.length)

  const start = Date.now()
  let hashed = 0
  let attempted = 0
  const skipped = files.length - todoFiles.length

  for (const url of todoFiles) {
    const key = relativeKey(url)
    attempted++
    console.log(`fetching ${attempted}/${todoFiles.length}`, key)
    try {
      const digest = await hashFile(url)
      appendHash(outputPath, key, digest)
      done.add(key)
      hashed++
      console.log(key, digest)
      if (hashed % 10 === 0) {
        const elapsed = Date.now() - start
        const remaining = todoFiles.length - attempted
        const eta = attempted > 0 ? remaining * (elapsed / attempted) : 0
        console.log(
          `progress: ${hashed} hashed, ${attempted}/${todoFiles.length} attempted, ${skipped} skipped, ` +
          `elapsed ${formatDuration(elapsed)}, eta ${formatDuration(eta)}`
        )
      }
    } catch (e) {
      console.error(key, e.message)
    }
  }

  return hashed
}

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
