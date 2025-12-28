import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [sveltekit()],
  server: {
    port: 5173,
    strictPort: false,
    proxy: {
      // Proxy auth endpoints to Bun backend
      '/auth': {
        target: 'http://localhost:3000',
        changeOrigin: true,
      },
      // Proxy admin API endpoints to Bun backend
      '/admin/api': {
        target: 'http://localhost:3000',
        changeOrigin: true,
      },
      // Proxy admin WebSocket to Bun backend
      '/admin/ws': {
        target: 'ws://localhost:3000',
        ws: true,
      },
    },
  },
  build: {
    target: 'esnext',
  },
});
