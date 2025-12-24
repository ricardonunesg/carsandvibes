// ~/carsandvibes/imports/scripts/clear-facets.ts
import { bootstrap, Logger } from '@vendure/core';
import {
  ProductService,
  ProductVariantService,
  RequestContextService,
} from '@vendure/core';
import { config as vendureConfig } from '../../src/vendure-config';

async function main() {
  // Clonar config e impedir listen de HTTP (evita conflito com PM2 na 3000)
  const config = {
    ...vendureConfig,
    apiOptions: {
      ...vendureConfig.apiOptions,
      port: 0,
    },
  };

  const app = await bootstrap(config);

  const productService = app.get(ProductService);
  const variantService = app.get(ProductVariantService);
  const ctxService = app.get(RequestContextService);

  const ctx = await ctxService.create({ apiType: 'admin' });

  const take = 200;

  // ------------------------
  // 1) LIMPAR PRODUCTS
  // ------------------------
  Logger.info(`A limpar facetValueIds de TODOS os PRODUCTS...`);
  let clearedProducts = 0;
  let skip = 0;

  for (;;) {
    const page = await productService.findAll(ctx, { take, skip });
    if (!page.items.length) break;

    for (const p of page.items) {
      await productService.update(ctx, { id: p.id, facetValueIds: [] });
      clearedProducts++;
      if (clearedProducts % 200 === 0) Logger.info(`Products limpos: ${clearedProducts}`);
    }

    skip += take;
    if (page.items.length < take) break;
  }

  Logger.info(`✅ Products limpos (total): ${clearedProducts}`);

  // ------------------------
  // 2) LIMPAR VARIANTS
  // ------------------------
  Logger.info(`A limpar facetValueIds de TODAS as VARIANTS...`);
  let clearedVariants = 0;
  skip = 0;

  for (;;) {
    const page = await variantService.findAll(ctx, { take, skip });
    if (!page.items.length) break;

    for (const v of page.items) {
      // ✅ Nesta versão, ProductVariantService.update espera um ARRAY de inputs
      await variantService.update(ctx, [{ id: v.id, facetValueIds: [] }]);
      clearedVariants++;
      if (clearedVariants % 500 === 0) Logger.info(`Variants limpas: ${clearedVariants}`);
    }

    skip += take;
    if (page.items.length < take) break;
  }

  Logger.info(`✅ Variants limpas (total): ${clearedVariants}`);
  Logger.info(`🎉 Concluído. Todas as facetValues foram removidas de products e variants.`);

  await app.close();
}

main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error('❌ Erro:', e);
  process.exit(1);
});
