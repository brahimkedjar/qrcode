import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    port: 5173,
    proxy: {
      // convenient local dev proxy to Nest server
      '/api': {
        target: 'http://localhost:4000',
        changeOrigin: true
      }
    }
  },
  define: {
    // Make Next-style env reachable for the copied component without edits
    // Leave empty by default so component builds '/api/...' correctly
    'process.env.NEXT_PUBLIC_API_URL': JSON.stringify(process.env.VITE_API_URL || '')
  }
});
