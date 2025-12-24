// ~/carsandvibes/imports/scripts/apply-brand-omp.ts
import { bootstrap, Logger } from '@vendure/core';
import {
  ProductService,
  ProductVariantService,
  RequestContextService,
} from '@vendure/core';
import { config as vendureConfig } from '../../src/vendure-config';

const FACET_VALUE_ID = '88'; // OMP

async function main() {
  const config = {
    ...vendureConfig,
    apiOptions: {
      ...vendureConfig.apiOptions,
      port: 0, // não abre porta
    },
  };

  const app = await bootstrap(config);

  const productService = app.get(ProductService);
  const variantService = app.get(ProductVariantService);
  const ctxService = app.get(RequestContextService);
  const ctx = await ctxService.create({ apiType: 'admin' });

  const take = 200;

  // ------------------------
  // PRODUCTS
  // ------------------------
  Logger.info('A aplicar BRAND=OMP (id 88) a TODOS os PRODUCTS...');
  let updatedProducts = 0;
  let skip = 0;

  for (;;) {
    const page = await productService.findAll(ctx, { take, skip });
    if (!page.items.length) break;

    for (const p of page.items) {
      await productService.update(ctx, {
        id: p.id,
        facetValueIds: [FACET_VALUE_ID],
      });
      updatedProducts++;
      if (updatedProducts % 200 === 0) Logger.info(`Products atualizados: ${updatedProducts}`);
    }

    skip += take;
    if (page.items.length < take) break;
  }

  Logger.info(`✅ Products atualizados (total): ${updatedProducts}`);

  // ------------------------
  // VARIANTS (batch)
  // ------------------------
  Logger.info('A aplicar BRAND=OMP (id 88) a TODAS as VARIANTS...');
  let updatedVariants = 0;
  skip = 0;

  for (;;) {
    const page = await variantService.findAll(ctx, { take, skip });
    if (!page.items.length) break;

    const inputs = page.items.map(v => ({
      id: v.id,
      facetValueIds: [FACET_VALUE_ID],
    }));

    await variantService.update(ctx, inputs);

    updatedVariants += inputs.length;
    if (updatedVariants % 500 === 0) Logger.info(`Variants atualizadas: ${updatedVariants}`);

    skip += take;
    if (page.items.length < take) break;
  }

  Logger.info(`✅ Variants atualizadas (total): ${updatedVariants}`);
  Logger.info('🎉 Concluído. BRAND=OMP aplicado a todos os products e variants.');

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
