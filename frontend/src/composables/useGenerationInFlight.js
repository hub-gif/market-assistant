import { ref } from 'vue'

/**
 * 长耗时生成类 POST/GET 的「进行中」标记（模块级，路由切换不丢）。
 * key 示例：`strategy-draft:12`、`regenerate-report:12`、`preview-report:12`
 */
const inFlightKey = ref(null)

export function generationInFlightKey() {
  return inFlightKey
}

/**
 * @param {string} key
 * @param {() => Promise<T>} fn
 * @returns {Promise<T>}
 */
export async function withGenerationInFlight(key, fn) {
  inFlightKey.value = key
  try {
    return await fn()
  } finally {
    if (inFlightKey.value === key) {
      inFlightKey.value = null
    }
  }
}
