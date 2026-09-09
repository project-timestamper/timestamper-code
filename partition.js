import path from 'node:path'
import { createHash } from 'node:crypto'
import OpenTimestamps from 'opentimestamps'
import fs from 'node:fs'

export const partitionByPrefix = (hashes, prefixLength) => {
  const result = {}
  for (const hash of hashes) {
    const prefix = hash.slice(0, prefixLength).toUpperCase()
    if (result[prefix] === undefined) {
      result[prefix] = []
    }
    result[prefix].push(hash)
  }
  return result
}

/** List partition data filenames in `dir` (hex prefixes, excluding .ots). */
export const listPartitionFiles = (dir) =>
  fs.readdirSync(dir)
    .filter((name) => /^[0-9A-Fa-f]+$/.test(name))
    .sort()

/**
 * SHA-256 each partition file in `dir`, calendar-stamp, and write `<prefix>.ots`.
 * Partition data files must already exist on disk.
 */
export const stampPartitionDir = async (dir) => {
  const prefixes = listPartitionFiles(dir)
  const opObject = new OpenTimestamps.Ops.OpSHA256()
  const detaches = []
  for (const prefix of prefixes) {
    const data = fs.readFileSync(path.join(dir, prefix))
    const digest = createHash('sha256').update(data).digest()
    detaches.push(OpenTimestamps.DetachedTimestampFile.fromHash(opObject, digest))
  }
  await OpenTimestamps.stamp(detaches)
  for (let i = 0; i < prefixes.length; ++i) {
    fs.writeFileSync(
      path.join(dir, `${prefixes[i]}.ots`),
      Buffer.from(detaches[i].serializeToBytes())
    )
  }
  return prefixes.length
}

export const savePartitions = async (dir, partitionMap) => {
  for (const [prefix, items] of Object.entries(partitionMap)) {
    const data = Buffer.from(items.join(''), 'hex')
    fs.writeFileSync(path.join(dir, prefix), data)
  }
  await stampPartitionDir(dir)
}

export const makePartitions = async (dir, hashes, prefixLength) => {
  const partitionMap = partitionByPrefix(hashes, prefixLength)
  await savePartitions(dir, partitionMap)
}
