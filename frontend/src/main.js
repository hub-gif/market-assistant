import { createApp } from 'vue'
import { createPinia } from 'pinia'
import './style.css'
import 'github-markdown-css/github-markdown-light.css'
import './styles/ui.css'
import App from './App.vue'
import router from './router'
import { useTaskStore } from './stores/task'

const pinia = createPinia()
const app = createApp(App)
app.use(pinia)
app.use(router)

if (typeof window !== 'undefined') {
  window.addEventListener('storage', (e) => {
    if (e.key === 'ma_tasks_inflight' || e.key === 'ma_tasks_inflight_ts') {
      useTaskStore().hydrateFromLocalStorage()
    }
  })
}

app.mount('#app')
