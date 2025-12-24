import { bootstrap, Logger } from '@vendure/core';
import {
  ProductVariantService,
  CollectionService,
  RequestContextService,
} from '@vendure/core';
import { config as vendureConfig } from '../../src/vendure-config';
import * as XLSX from 'xlsx';
import path from 'path';

const EXCEL_PATH = path.resolve(
  process.env.EXCEL_PATH ?? '/root/carsandvibes/imports/source/PHC OMP database for categories.xlsx',
);

const SHEET_NAME = process.env.SHEET_NAME ?? 'PHC Racing Force Database';

// ✅ idioma para ler o "name" das collections corretamente
const LANG_CODE = process.env.LANG_CODE ?? 'pt';

// SKU matching priority: 1) Mpn 2) Ref PHC
const SKU_FIELDS = ['Mpn', 'Ref PHC'] as const;

// Category fields (deepest wins)
const CAT_FIELDS = ['Categoryn1', 'Categoryn2', 'Categoryn3', 'Categoryn4'] as const;

function norm(s: unknown): string {
  return String(s ?? '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

function toStr(s: unknown): string {
  return String(s ?? '').trim();
}

async function main() {
  const config = {
    ...vendureConfig,
    apiOptions: {
      ...vendureConfig.apiOptions,
      port: 0, // não abrir HTTP
    },
  };

  const app = await bootstrap(config);

  // ✅ ctx com languageCode
  const ctx = await app.get(RequestContextService).create({
    apiType: 'admin',
    languageCode: LANG_CODE as any,
  });

  const collectionService = app.get(CollectionService);
  const variantService = app.get(ProductVariantService);

  // 1) Load ALL collections (existing)
  Logger.info(`A carregar Collections existentes (LANG_CODE=${LANG_CODE})...`);
  const allCollections: any[] = [];
  {
    const take = 1000;
    let skip = 0;
    for (;;) {
      const page = await collectionService.findAll(ctx, { take, skip });
      allCollections.push(...page.items);
      skip += take;
      if (page.items.length < take) break;
    }
  }

  // Build lookup by (parentId + normalized name/slug) -> collectionId
  const byParentAndKey = new Map<string, string>();
  const rootId = null;

  for (const c of allCollections) {
    const parentId = c.parent ? String(c.parent.id) : rootId;
    const parentKey = parentId ?? 'ROOT';
    const id = String(c.id);

    // index por NAME
    const nameKey = `${parentKey}::${norm(c.name)}`;
    if (norm(c.name)) byParentAndKey.set(nameKey, id);

    // index por SLUG (se existir)
    const slug = (c as any).slug;
    const slugKey = `${parentKey}::${norm(slug)}`;
    if (norm(slug)) byParentAndKey.set(slugKey, id);
  }

  function findDeepestCollectionId(categories: string[]): string | null {
    let parentId: string | null = null;
    let found: string | null = null;

    for (const nameOrSlug of categories) {
      const key = `${parentId ?? 'ROOT'}::${norm(nameOrSlug)}`;
      const id = byParentAndKey.get(key);
      if (!id) break; // para no primeiro nível que não existir
      found = id;
      parentId = id;
    }
    return found;
  }

  // 2) Read Excel rows
  Logger.info(`A ler Excel: ${EXCEL_PATH} (sheet: ${SHEET_NAME})`);
  const wb = XLSX.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) {
    throw new Error(
      `Sheet "${SHEET_NAME}" não existe. Sheets: ${wb.SheetNames.join(', ')}`
    );
  }

  const rows = XLSX.utils.sheet_to_json<Record<string, any>>(ws, { defval: '' });
  Logger.info(`Linhas no Excel: ${rows.length}`);

  // 3) For each row, find product by SKU and assign to deepest existing collection
  const collectionToProductIds = new Map<string, Set<string>>();
  const missingCollections: Array<{ sku: string; cats: string[] }> = [];
  const missingProducts: Array<{ skuTried: string[]; cats: string[] }> = [];

  let processed = 0;

  for (const r of rows) {
    processed++;

    // categories (do nível 1 ao 4)
    const cats = CAT_FIELDS.map(f => toStr(r[f])).filter(Boolean);
    if (!cats.length) continue;

    const collectionId = findDeepestCollectionId(cats);
    if (!collectionId) {
      const skuTry = SKU_FIELDS.map(f => toStr(r[f])).filter(Boolean);
      missingCollections.push({ sku: skuTry[0] ?? '(sem sku)', cats });
      continue;
    }

    // find product by variant sku (try Mpn first, then Ref PHC)
    let productId: string | null = null;
    const tried: string[] = [];

    for (const f of SKU_FIELDS) {
      const sku = toStr(r[f]);
      if (!sku) continue;
      tried.push(sku);

      const variant = await variantService.findBySku(ctx, sku);
      if (variant) {
        productId = String((variant as any).productId ?? (variant as any).product?.id);
        if (productId) break;
      }
    }

    if (!productId) {
      missingProducts.push({ skuTried: tried, cats });
      continue;
    }

    if (!collectionToProductIds.has(collectionId)) {
      collectionToProductIds.set(collectionId, new Set<string>());
    }
    collectionToProductIds.get(collectionId)!.add(productId);

    if (processed % 500 === 0) {
      Logger.info(`Processadas: ${processed}`);
    }
  }

  Logger.info(`Processadas total: ${processed}`);
  Logger.info(`Collections alvo com products: ${collectionToProductIds.size}`);
  Logger.info(`Sem collection match: ${missingCollections.length}`);
  Logger.info(`Sem product match (por SKU): ${missingProducts.length}`);

  // 4) Apply assignments in batches per collection
  Logger.info('A aplicar associações Products -> Collections...');
  let totalAssigned = 0;

  for (const [collectionId, productSet] of collectionToProductIds.entries()) {
    const productIds = Array.from(productSet);
    const chunkSize = 200;

    for (let i = 0; i < productIds.length; i += chunkSize) {
      const chunk = productIds.slice(i, i + chunkSize);
      await collectionService.addProductsToCollection(ctx, collectionId, chunk);
      totalAssigned += chunk.length;
    }
  }

  Logger.info(`✅ Concluído. Products associados (contagem de operações): ${totalAssigned}`);

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
