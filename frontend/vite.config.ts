import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// In the container the built assets are served by nginx, which proxies /api to
// the backend (see default.conf.template).  This dev server does the same, so the
// frontend code never needs to know the address of the backend.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 4000,
    proxy: {
      '/api': process.env.VITE_API_PROXY ?? 'http://localhost:8000',
    },
  },
})
