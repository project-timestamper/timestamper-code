import fs from 'node:fs'
import path from 'node:path'
import OpenTimestamps from 'opentimestamps'
import { createClient, putObject } from './r2.js'
import pMap from 'p-map'
import { execSync } from 'node:child_process'
import DetachedTimestampFile from 'opentimestamps/src/detached-timestamp-file.js'
import { TimestampCache, walkStamp } from './ots-cache.js'

export const stampHashes = async (hashList, hashType) => {
  const opObject = hashType === 'sha1' ? new OpenTimestamps.Ops.OpSHA1() : new OpenTimestamps.Ops.OpSHA256()
  const detaches = hashList.map(hash => {
    const binaryHash = Buffer.from(hash, 'hex')
    return OpenTimestamps.DetachedTimestampFile.fromHash(opObject, binaryHash)
  })
  await OpenTimestamps.stamp(detaches)
  return detaches
}

export const writeHashes = (hashList, detaches, outDir) => {
  const detachesSerialized = detaches.map(d => d.serializeToBytes())
  fs.mkdirSync(outDir, { recursive: true })
  for (const i in hashList) {
    const filename = path.join(outDir, `${hashList[i]}.ots`)
    fs.writeFileSync(filename, Buffer.from(detachesSerialized[i]))
    console.log(filename)
  }
}

const zipMap = (keys, values) => {
  const result = {}
  for (const i in keys) {
    result[keys[i]] = values[i]
  }
  return result
}

const stampAndWriteHashes = async (hashList, outDir) => {
  const detaches = await stampHashes(hashList)
  writeHashes(hashList, detaches, outDir)
}

const stampAndCollectHashes = async (hashList) => {
  const hashType = hashList[0].length === 40 ? 'sha1' : 'sha256'
  const detaches = (await stampHashes(hashList, hashType)).map(
    detach => detach.serializeToBytes())
  return zipMap(hashList, detaches)
}

const uploadHashes = async (hashPairs) => {
  let uploadCount = 0
  const client = createClient()
  await pMap(hashPairs,
    async ([hash, otsFile]) => {
      uploadCount++
      if (uploadCount % 1000 === 0) {
        console.log(uploadCount)
      }
      await putObject(client, `ots/${hash}.ots`, otsFile)
    },
    { concurrency: 64, stopOnError: false })
  client.destroy()
}

export const saveHashes = async (filePath, hashPairs) => {
  let i = 0
  for (const [hash, detach] of hashPairs) {
    ++i
    if (i % 1000 === 0) {
      console.log(i, '/', hashPairs.length)
    }
    fs.writeFileSync(path.join(filePath, 'ots', hash + '.ots'), Buffer.from(detach))
  }
}

export const stampAndUploadHashes = async (hashList) => {
  const hashToDetachMap = await stampAndCollectHashes(hashList)
  await uploadHashes(Object.entries(hashToDetachMap))
}

export const stampAndSaveHashes = async (filePath, hashList) => {
  const hashToDetachMap = await stampAndCollectHashes(hashList)
  await saveHashes(filePath, Object.entries(hashToDetachMap))
}

const Notary = OpenTimestamps.Notary
const Calendar = OpenTimestamps.Calendar

/**
 * Upgrade a timestamp tree using a commitment cache (Python otsclient style).
 * Shared calendar tips are fetched once, then reused for later .ots files.
 */
export const upgradeTimestampCached = async (timestamp, cache) => {
  let changed = false
  const attestationsBefore = timestamp.getAttestations().size

  for (const subStamp of walkStamp(timestamp)) {
    const cached = cache.get(subStamp.msg)
    if (cached) subStamp.merge(cached)
  }
  if (timestamp.getAttestations().size > attestationsBefore) {
    changed = true
    console.log('Got attestation(s) from cache')
  }

  const whitelist = Calendar.DEFAULT_CALENDAR_WHITELIST
  const existingAttestations = timestamp.getAttestations()
  const jobs = []

  // Collect calendar requests up front (like the library) so one completing
  // attestation does not skip the other calendars for this tip.
  for (const subStamp of timestamp.directlyVerified()) {
    if (subStamp.isTimestampComplete()) continue
    for (const attestation of subStamp.attestations) {
      if (!(attestation instanceof Notary.PendingAttestation)) continue
      if (!whitelist.contains(attestation.uri)) {
        console.log(
          'Ignoring attestation from calendar ' + attestation.uri +
          ': Calendar not in whitelist'
        )
        continue
      }
      jobs.push({ subStamp, uri: attestation.uri, commitment: subStamp.msg })
    }
  }

  await Promise.all(jobs.map(async ({ subStamp, uri, commitment }) => {
    const calendar = new Calendar.RemoteCalendar(uri)
    try {
      const upgradedStamp = await calendar.getTimestamp(commitment)
      const attsFromRemote = upgradedStamp.getAttestations()
      if (attsFromRemote.size > 0) {
        console.log('Got 1 attestation(s) from ' + calendar.url)
      }
      const newAttestations = [...attsFromRemote].filter(a => !existingAttestations.has(a))
      if (newAttestations.length === 0) return
      cache.merge(upgradedStamp)
      subStamp.merge(upgradedStamp)
      for (const a of newAttestations) existingAttestations.add(a)
      changed = true
    } catch (err) {
      console.log('Calendar ' + calendar.url + ': ' + (err.message || err))
    }
  }))

  return changed
}

export const upgrade = async (filePath, cache = new TimestampCache()) => {
  const buf = await fs.promises.readFile(filePath)
  const detachedOts = DetachedTimestampFile.deserialize(buf)
  await upgradeTimestampCached(detachedOts.timestamp, cache)
  await fs.promises.writeFile(filePath, Buffer.from(detachedOts.serializeToBytes(), 'binary'))
}

export const upgradeAll = async (dir, cachePath) => {
  const cache = new TimestampCache(cachePath)
  const files = (await fs.promises.readdir(dir)).filter(f => f.endsWith('.ots')).sort()
  let i = 0
  for (const file of files) {
    i++
    if (i <= 5 || i % 100 === 0 || i === files.length) {
      console.log(`[${i}/${files.length}]`, file)
    }
    await upgrade(path.join(dir, file), cache)
  }
}

export const countHashes = async (dir, bytesPerHash) => {
  const files = await fs.promises.readdir(dir)
  let total = 0
  for (const file of files) {
    if (!file.endsWith('.ots')) {
      const stat = await fs.promises.stat(path.join(dir, file))
      const count = stat.size / bytesPerHash
      total += count
      console.log(file, stat.size, count)
    }
  }
  return total
}
