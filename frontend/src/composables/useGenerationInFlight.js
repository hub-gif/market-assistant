import { storeToRefs } from 'pinia'
import { useTaskStore } from '../stores/task'

/**
 * 与 Pinia `useTaskStore` 同步；进行中列表持久化在 localStorage，跨标签页可见。
 */
export function generationInFlightKey() {
  return storeToRefs(useTaskStore()).inFlightKeys
}

export function clearGenerationInFlightState() {
  useTaskStore().clearAll()
}

export function clearRegenerateReportInFlightOnly() {
  useTaskStore().clearRegenerateReportOnly()
}

/**
 * @param {string} key
 * @param {() => Promise<T>} fn
 * @returns {Promise<T>}
 */
export async function withGenerationInFlight(key, fn) {
  return useTaskStore().withInFlight(key, fn)
}
