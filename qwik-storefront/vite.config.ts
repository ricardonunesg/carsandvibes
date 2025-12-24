import { qwikVite } from '@qwik.dev/core/optimizer';
import { qwikRouter } from '@qwik.dev/router/vite';
import { i18nPlugin } from 'compiled-i18n/vite';
import { defineConfig } from 'vite';
import tsconfigPaths from 'vite-tsconfig-paths';

export default defineConfig(async (config) => {
  return {
    // Qwik SSR target
    ssr: {
      target: 'webworker',
    },

    build: {
      sourcemap: config.mode === 'development',
    },

    plugins: [
      qwikRouter(),
      qwikVite(),
      tsconfigPaths(),
      i18nPlugin({
        locales: ['en', 'de', 'es'],
      }),
    ],

    /**
     * 👉 IMPORTANTE PARA PRODUÇÃO COM NGINX
     * Permite acesso via domínio externo (senão dá "Blocked request")
     */
    preview: {
      host: true,
      port: 4173,
      allowedHosts: [
        'carsandvibes.duckdns.org',
      ],
      headers: {
        'Cache-Control': 'public, max-age=600',
      },
    },
  };
});
