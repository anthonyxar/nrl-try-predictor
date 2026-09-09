// Shared fetch helper with a timeout, since plain fetch() has no built-in
// ceiling — if a request never resolves (e.g. a sleeping Render backend
// mid cold-start), it would otherwise leave the UI spinning forever.
const DEFAULT_TIMEOUT_MS = 45000

export function fetchJson(url, { timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs)

  return fetch(url, { signal: controller.signal })
    .then(res => {
      if (!res.ok) {
        const err = new Error(`Request failed (${res.status})`)
        err.status = res.status
        throw err
      }
      return res.json()
    })
    .catch(e => {
      if (e.name === 'AbortError') {
        const timeoutErr = new Error(
          'The server is taking longer than expected to respond — it may be waking up from sleep. Please try again in a moment.'
        )
        timeoutErr.isTimeout = true
        throw timeoutErr
      }
      throw e
    })
    .finally(() => clearTimeout(timeoutId))
}
