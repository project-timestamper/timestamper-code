import { createHash } from 'node:crypto'
import fs from 'node:fs'
import { unlink } from 'node:fs/promises'
import path from 'node:path'
import esMain from 'es-main'
import minimist from 'minimist'
import puppeteer from 'puppeteer-extra'
import StealthPlugin from 'puppeteer-extra-plugin-stealth'
import * as tar from 'tar'

puppeteer.use(StealthPlugin())

const DEFAULT_OUTPUT = 'uspto_hashes.txt'
const DEFAULT_DOWNLOAD_DIR = path.join(process.env.HOME ?? '', 'Downloads')

const sidecarPath = (outputPath, suffix) => {
  if (outputPath.endsWith('_hashes.txt')) {
    return outputPath.replace(/_hashes\.txt$/, `_${suffix}.txt`)
  }
  return `${outputPath}.${suffix}`
}

const completedPathFor = (outputPath) => sidecarPath(outputPath, 'completed')

const sleep = async (ms) => {
  return new Promise(resolve => setTimeout(resolve, ms))
}

const setupBrowser = async () => {
  const browser = await puppeteer.launch({
    headless: false,
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
  })
  const page = await browser.newPage()
  await page.setViewport({ width: 1920, height: 1080 })
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
  return { browser, page }
}

var page1 = null

const clickLinkWithText = async (page, text) => {
  await page.evaluate((text) => {
    const links = Array.from(document.querySelectorAll('a'))
    const link = links.find(a => a.innerText.trim() === text)
    if (link) {
      link.scrollIntoView({ behavior: 'smooth', block: 'center' })
      window.setTimeout(() => link.click(), 3000)
      return true
    }
    return false
  }, text)
}

export const run = async () => {
  const { page } = await setupBrowser()
  page1 = page
  await page.goto('https://data.uspto.gov/bulkdata/datasets/', {
    waitUntil: 'networkidle2',
    timeout: 60000
  })
  await sleep(3000)
  await clickLinkWithText(page, 'Patent Grant Multi-page PDF Images')
  await sleep(2000) // Wait for navigation after click
}

export const gotoPage = async (page, url) => {
  await page.goto(url, {
    waitUntil: 'networkidle2',
    timeout: 60000
  })
  // Wait a bit more for any JavaScript to finish executing
  await sleep(2000)
}

const loadLineSet = (filePath, { keyed = false } = {}) => {
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
        if (!line) {
          continue
        }
        if (keyed) {
          const [key, digest] = line.split('\t')
          if (key && digest) {
            values.add(key)
          }
        } else {
          values.add(line.trim())
        }
      }
    }
    if (leftover) {
      if (keyed) {
        const [key, digest] = leftover.split('\t')
        if (key && digest) {
          values.add(key)
        }
      } else {
        values.add(leftover.trim())
      }
    }
  } finally {
    fs.closeSync(fd)
  }
  return values
}

const loadDoneKeys = (filePath) => loadLineSet(filePath, { keyed: true })

export const loadCompletedArchives = (outputPath = DEFAULT_OUTPUT) =>
  loadLineSet(completedPathFor(outputPath))

const appendHash = (filePath, key, digest) => {
  fs.appendFileSync(filePath, `${key}\t${digest}\n`)
}

const archiveId = (tarPath) => path.basename(tarPath)

const markArchiveCompleted = (outputPath, tarPath, completed) => {
  const id = archiveId(tarPath)
  if (completed.has(id)) {
    return false
  }
  completed.add(id)
  fs.appendFileSync(completedPathFor(outputPath), `${id}\n`)
  return true
}

const normalizeEntryPath = (entryPath) => entryPath.replace(/^\.?\//, '')

const isPdfEntry = (entry) => entry.type === 'File' && /\.pdf$/i.test(entry.path)

const entryKey = (tarPath, entryPath) =>
  `${path.basename(tarPath)}/${normalizeEntryPath(entryPath)}`

// Stream each PDF member out of the tar (no extract-to-disk) and SHA-256 it.
export const hashPdfEntriesFromTar = async (tarPath, {
  outputPath = DEFAULT_OUTPUT,
  done = null
} = {}) => {
  const doneKeys = done ?? loadDoneKeys(outputPath)
  const pending = []
  let hashed = 0
  let skipped = 0
  let pdfs = 0

  await tar.list({
    file: tarPath,
    onReadEntry (entry) {
      if (!isPdfEntry(entry)) {
        entry.resume()
        return
      }

      pdfs++
      const key = entryKey(tarPath, entry.path)
      if (doneKeys.has(key)) {
        skipped++
        entry.resume()
        return
      }

      const hash = createHash('sha256')
      pending.push(new Promise((resolve, reject) => {
        entry.on('data', (chunk) => hash.update(chunk))
        entry.on('error', reject)
        entry.on('end', () => {
          const digest = hash.digest('hex')
          appendHash(outputPath, key, digest)
          doneKeys.add(key)
          hashed++
          console.log(key, digest)
          resolve()
        })
      }))
    }
  })

  await Promise.all(pending)
  return { pdfs, hashed, skipped }
}

export const processTarFile = async (tarPath, {
  outputPath = DEFAULT_OUTPUT,
  deleteTar = true,
  completed = null
} = {}) => {
  const resolved = path.resolve(tarPath)
  const id = archiveId(resolved)
  const completedArchives = completed ?? loadCompletedArchives(outputPath)

  if (completedArchives.has(id)) {
    console.log('skip completed', id)
    if (deleteTar && fs.existsSync(resolved)) {
      await unlink(resolved)
      console.log('deleted leftover', resolved)
    }
    return { pdfs: 0, hashed: 0, skipped: 0, completed: true }
  }

  console.log('processing', resolved)
  try {
    const result = await hashPdfEntriesFromTar(resolved, { outputPath })
    if (result.pdfs === 0) {
      throw new Error('no PDF entries found')
    }
    console.log(
      `done ${id}: ${result.hashed} hashed, ` +
      `${result.skipped} skipped, ${result.pdfs} pdfs`
    )
    markArchiveCompleted(outputPath, resolved, completedArchives)
    if (deleteTar) {
      await unlink(resolved)
      console.log('deleted', resolved)
    }
    return { ...result, completed: true }
  } catch (err) {
    // Leave the archive off the completed list so the next run retries it.
    console.error('failed', id, err.message)
    throw err
  }
}

export const processTarDir = async (dirPath, {
  outputPath = DEFAULT_OUTPUT,
  deleteTar = true
} = {}) => {
  const completed = loadCompletedArchives(outputPath)
  const names = fs.readdirSync(dirPath)
    .filter(name => /\.tar(\.gz)?$/i.test(name))
    .sort()
  const results = []
  for (const name of names) {
    if (completed.has(name)) {
      console.log('skip completed', name)
      const leftover = path.join(dirPath, name)
      if (deleteTar && fs.existsSync(leftover)) {
        await unlink(leftover)
        console.log('deleted leftover', leftover)
      }
      results.push({ pdfs: 0, hashed: 0, skipped: 0, completed: true })
      continue
    }
    try {
      results.push(await processTarFile(path.join(dirPath, name), {
        outputPath,
        deleteTar,
        completed
      }))
    } catch (err) {
      results.push({ pdfs: 0, hashed: 0, skipped: 0, completed: false, error: err.message })
    }
  }
  return results
}

// Use when choosing what to download: only archives not already completed.
export const shouldProcessArchive = (tarNameOrPath, outputPath = DEFAULT_OUTPUT) => {
  const completed = loadCompletedArchives(outputPath)
  return !completed.has(archiveId(tarNameOrPath))
}

if (esMain(import.meta)) {
  const args = minimist(process.argv.slice(2), {
    boolean: ['keep'],
    string: ['output', 'tar', 'dir'],
    default: {
      output: DEFAULT_OUTPUT,
      dir: DEFAULT_DOWNLOAD_DIR
    }
  })
  const deleteTar = !args.keep

  if (args.tar) {
    await processTarFile(args.tar, { outputPath: args.output, deleteTar })
  } else if (args._[0]) {
    const target = args._[0]
    const stat = fs.statSync(target)
    if (stat.isDirectory()) {
      await processTarDir(target, { outputPath: args.output, deleteTar })
    } else {
      await processTarFile(target, { outputPath: args.output, deleteTar })
    }
  } else if (process.argv.includes('--dir')) {
    await processTarDir(args.dir, { outputPath: args.output, deleteTar })
  } else {
    await run()
  }
}
