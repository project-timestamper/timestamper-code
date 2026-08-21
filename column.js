import fs from 'node:fs'
import readline from 'node:readline'

/**
 * Read column `n` (0-based) from a tab-separated text file.
 * @param {string} filePath
 * @param {number} n
 * @returns {Promise<string[]>}
 */
export const readColumn = async (filePath, n) => {
  const values = []
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity
  })
  for await (const line of rl) {
    if (line.length === 0) {
      continue
    }
    const fields = line.split('\t')
    values.push(fields[n] ?? '')
  }
  return values
}
