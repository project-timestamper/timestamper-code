import fs from 'node:fs'
import { createHash } from 'node:crypto'
import { installIntoGlobal } from 'iterator-helpers-polyfill'
import { execSync } from 'node:child_process'
import { base32 } from 'rfc4648'
installIntoGlobal()

export const strToDate = (s) => {
  const year = s.substring(0, 4)
  const month = s.substring(4, 6)
  const day = s.substring(6, 8)
  return new Date(year, month - 1, day)
}
export const readJson = (file) => JSON.parse(fs.readFileSync(file).toString())

export const findFirstInstance = async (path, content, { start, end, accumulate } = { start: 0, end: Infinity, accumulate: false }) => {
  let i = 0
  const pieces = []
  const stream = fs.createReadStream(path, { start, end })
  let tailPiece = Buffer.alloc(0)
  for await (const chunk of stream) {
    const examine = Buffer.concat([tailPiece, chunk])
    const loc = examine.indexOf(content)
    if (loc > -1) {
      const position = i + loc - tailPiece.length + start
      const result = { position }
      if (accumulate) {
        pieces.push(chunk.slice(0, loc - tailPiece.length))
        result.buf = Buffer.concat(pieces)
      }
      return result
    } else {
      if (accumulate) {
        pieces.push(chunk)
      }
    }
    i += chunk.length
    tailPiece = chunk.slice(chunk.length - content.length + 1)
  }
  return { position: -1 }
}

export const readFilePart = async (path, position, length) => {
  const fh = await fs.promises.open(path, 'r')
  const buffer = Buffer.alloc(length)
  await fh.read({ position, length, buffer })
  fh.close()
  return buffer
}

export const moveToUpperCase = async (dir) => {
  const filenames = await fs.promises.readdir(dir)
  for (const filename of filenames) {
    let fixedFilename
    if (filename.endsWith('.ots')) {
      const [a, b] = filename.split('.')
      fixedFilename = `${a.toUpperCase()}.${b}`
    } else {
      fixedFilename = filename.toUpperCase()
    }
    if (fixedFilename !== filename) {
      execSync(`git mv ${filename} ${fixedFilename}`,
        { cwd: dir })
    }
  }
}

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

export const sidecarPath = (outputPath, suffix) => {
  if (outputPath.endsWith('_hashes.txt')) {
    return outputPath.replace(/_hashes\.txt$/, `_${suffix}`)
  }
  return `${outputPath}.${suffix}`
}

export const formatDuration = (ms) => {
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

export const hashStream = async (body, algorithm = 'sha256') => {
  const hash = createHash(algorithm)
  for await (const chunk of body) {
    hash.update(chunk)
  }
  return hash.digest('hex')
}

export const drainStream = async (body) => {
  for await (const _chunk of body) {
    // discard
  }
}

export const streamToBuffer = async (body) => {
  const chunks = []
  for await (const chunk of body) {
    chunks.push(chunk)
  }
  return Buffer.concat(chunks)
}

const forEachLineSync = (filePath, onLine) => {
  if (!fs.existsSync(filePath)) {
    return
  }
  const fd = fs.openSync(filePath, 'r')
  const bufSize = 1024 * 1024
  const buf = Buffer.alloc(bufSize)
  let leftover = ''
  try {
    for (;;) {
      const bytesRead = fs.readSync(fd, buf, 0, bufSize, null)
      if (bytesRead === 0) {
        break
      }
      leftover += buf.toString('utf8', 0, bytesRead)
      const lines = leftover.split('\n')
      leftover = lines.pop()
      for (const line of lines) {
        onLine(line)
      }
    }
    if (leftover) {
      onLine(leftover)
    }
  } finally {
    fs.closeSync(fd)
  }
}

export const loadLineSet = (filePath) => {
  const values = new Set()
  forEachLineSync(filePath, (line) => {
    const value = line.trim()
    if (value) {
      values.add(value)
    }
  })
  return values
}

/** Load keys from `key\\tdigest` lines (hash list resume files). */
export const loadDoneKeys = (filePath) => {
  const done = new Set()
  forEachLineSync(filePath, (line) => {
    if (!line) {
      return
    }
    const tab = line.indexOf('\t')
    if (tab <= 0) {
      return
    }
    const key = line.slice(0, tab)
    const digest = line.slice(tab + 1)
    if (key && digest) {
      done.add(key)
    }
  })
  return done
}

export const appendHash = (filePath, key, digest) => {
  fs.appendFileSync(filePath, `${key}\t${digest}\n`)
}

export const appendLine = (filePath, line) => {
  fs.appendFileSync(filePath, `${line}\n`)
}

const defaultNonRetryable = (e) =>
  /status: (?:40[134]|410|451)\b/.test(e.message)

export const withRetries = async (label, fn, {
  retries = 5,
  delayMs = 10000,
  isNonRetryable = defaultNonRetryable
} = {}) => {
  let lastError
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fn()
    } catch (e) {
      lastError = e
      if (isNonRetryable(e)) {
        throw e
      }
      const cause = e.cause ? ` (${e.cause.message || e.cause})` : ''
      console.error(`retry ${attempt}/${retries}`, label, `${e.message}${cause}`)
      if (attempt < retries) {
        const waitMs = e.retryAfterMs > 0 ? e.retryAfterMs : delayMs * attempt
        await sleep(waitMs)
      }
    }
  }
  throw lastError
}

/** SHA-1 of empty payload in CDX base32 form. */
const EMPTY_DIGEST_B32 = '3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ'

/** Convert Wayback CDX digest (base32 SHA-1, optional sha1: prefix) to lowercase hex. */
export const cdxDigestToHex = (digest) => {
  if (!digest) {
    return null
  }
  let value = String(digest).trim()
  if (value.toLowerCase().startsWith('sha1:')) {
    value = value.slice(5)
  }
  if (/^[0-9a-f]{40}$/i.test(value)) {
    return value.toLowerCase()
  }
  const raw = value.toUpperCase().replace(/=+$/, '')
  if (raw === EMPTY_DIGEST_B32) {
    return null
  }
  try {
    const bytes = base32.parse(raw, { loose: true })
    if (bytes.length !== 20) {
      return null
    }
    return Buffer.from(bytes).toString('hex')
  } catch {
    return null
  }
}
