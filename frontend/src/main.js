import { createApp } from 'vue'
import './style.css'
import 'github-markdown-css/github-markdown-light.css'
import './styles/ui.css'
import App from './App.vue'
import router from './router'

createApp(App).use(router).mount('#app')
