import fs from 'node:fs'
import { createHash } from 'node:crypto'
import { installIntoGlobal } from 'iterator-helpers-polyfill'
import { execSync } from 'node:child_process'
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

export const loadLineSet = (filePath) => {
  const values = new Set()
  if (!fs.existsSync(filePath)) {
    return values
  }
  for (const line of fs.readFileSync(filePath, 'utf8').split('\n')) {
    const value = line.trim()
    if (value) {
      values.add(value)
    }
  }
  return values
}

/** Load keys from `key\\tdigest` lines (hash list resume files). */
export const loadDoneKeys = (filePath) => {
  const done = new Set()
  if (!fs.existsSync(filePath)) {
    return done
  }
  for (const line of fs.readFileSync(filePath, 'utf8').split('\n')) {
    if (!line) {
      continue
    }
    const [key, digest] = line.split('\t')
    if (key && digest) {
      done.add(key)
    }
  }
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
        await sleep(delayMs * attempt)
      }
    }
  }
  throw lastError
}
