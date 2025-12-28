import adapter from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: vitePreprocess(),
  kit: {
    adapter: adapter({
      pages: 'dist',
      assets: 'dist',
      fallback: 'index.html',
      precompress: false,
      strict: true,
    }),
    paths: {
      base: '/admin',
    },
    alias: {
      $lib: './src/lib',
      $components: './src/lib/components',
      $stores: './src/lib/stores',
      $theme: './src/lib/theme',
    },
  },
};

export default config;
