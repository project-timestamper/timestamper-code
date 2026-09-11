import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import OpenTimestamps from 'opentimestamps'

const Timestamp = OpenTimestamps.Timestamp
const Context = OpenTimestamps.Context
const Utils = OpenTimestamps.Utils

/** Default cache dir — matches Python otsclient AppDirs('ots', 'opentimestamps'). */
export const defaultCachePath = () => {
  if (process.platform === 'darwin') {
    return path.join(os.homedir(), 'Library', 'Caches', 'ots')
  }
  if (process.platform === 'win32') {
    const base = process.env.LOCALAPPDATA || path.join(os.homedir(), 'AppData', 'Local')
    return path.join(base, 'ots', 'Cache')
  }
  return path.join(os.homedir(), '.cache', 'ots')
}

const toBytes = (commitment) => {
  if (Array.isArray(commitment)) return commitment
  return Array.from(commitment)
}

const commitmentKey = (commitment) => Utils.bytesToHex(toBytes(commitment))

/**
 * Persistent + in-memory cache of upgraded Timestamp trees, keyed by commitment.
 * On-disk layout matches Python otsclient.cache.TimestampCache.
 */
export class TimestampCache {
  constructor (cachePath = defaultCachePath()) {
    this.path = cachePath
    this.memory = new Map()
    if (this.path) {
      fs.mkdirSync(this.path, { recursive: true })
      const versionPath = path.join(this.path, 'version')
      if (!fs.existsSync(versionPath)) {
        fs.writeFileSync(versionPath, '1.0\n')
      }
    }
  }

  commitmentPath (commitment) {
    const hex = commitmentKey(commitment)
    return path.join(
      this.path,
      hex.slice(0, 2),
      hex.slice(2, 4),
      hex.slice(4, 6),
      hex.slice(6, 8),
      hex
    )
  }

  get (commitment) {
    const key = commitmentKey(commitment)
    if (this.memory.has(key)) {
      return this.memory.get(key)
    }
    if (!this.path || key.length > 128) {
      return undefined
    }
    const file = this.commitmentPath(commitment)
    if (!fs.existsSync(file)) {
      return undefined
    }
    const buf = fs.readFileSync(file)
    const stamp = Timestamp.deserialize(
      new Context.StreamDeserialization(buf),
      toBytes(commitment)
    )
    this.memory.set(key, stamp)
    return stamp
  }

  save (timestamp) {
    const key = commitmentKey(timestamp.msg)
    this.memory.set(key, timestamp)
    if (!this.path || key.length > 128) {
      return
    }
    const file = this.commitmentPath(timestamp.msg)
    fs.mkdirSync(path.dirname(file), { recursive: true })
    const ctx = new Context.StreamSerialization()
    timestamp.serialize(ctx)
    fs.writeFileSync(file, Buffer.from(ctx.getOutput()))
  }

  merge (newTimestamp) {
    let existing = this.get(newTimestamp.msg)
    if (!existing) {
      existing = new Timestamp(toBytes(newTimestamp.msg))
    }
    existing.merge(newTimestamp)
    this.save(existing)
    return existing
  }
}

/** Depth-first walk of a timestamp tree. */
export function * walkStamp (stamp) {
  yield stamp
  for (const sub of stamp.ops.values()) {
    yield * walkStamp(sub)
  }
}
