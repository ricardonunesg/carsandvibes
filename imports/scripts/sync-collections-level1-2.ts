import { bootstrap, Logger, TransactionalConnection } from '@vendure/core';
import {
  CollectionService,
  ProductService,
  ProductVariantService,
  RequestContextService,
  FacetService,
  FacetValueService,
} from '@vendure/core';
import { Collection, ProductVariant } from '@vendure/core/dist/entity';
import { config as vendureConfig } from '../../src/vendure-config';
import * as path from 'path';
import * as XLSX from 'xlsx';

const FILE_PATH =
  process.env.CATEGORIES_XLSX ??
  path.resolve(process.cwd(), 'imports/source/combined_categories_with_sku.xlsx');

// Facet para “sem match”
const DISCONTINUED_FACET_CODE = 'catalog_status';
const DISCONTINUED_FACET_NAME = 'Catalog Status';
const DISCONTINUED_VALUE_CODE = 'discontinued';
const DISCONTINUED_VALUE_NAME = 'Discontinued';

function slugify(input: string): string {
  return input
    .toString()
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '')
    .slice(0, 60);
}

function cellStr(v: any): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}

async function main() {
  // ⚠️ Para evitar erros no fim e jobs: não precisamos de plugins aqui
  const config = {
    ...vendureConfig,
    apiOptions: { ...vendureConfig.apiOptions, port: 0 },
    // Importantíssimo: não disparar apply-filters jobs
    catalogOptions: { ...(vendureConfig.catalogOptions ?? {}), collectionFilters: [] },
    // corta plugins para não haver job queue/scheduler/email/adminui etc durante o script
    plugins: [],
  } as any;

  const app = await bootstrap(config);

  const collectionService = app.get(CollectionService);
  const productService = app.get(ProductService);
  const variantService = app.get(ProductVariantService);
  const ctxService = app.get(RequestContextService);
  const connection = app.get(TransactionalConnection);
  const facetService = app.get(FacetService);
  const facetValueService = app.get(FacetValueService);

  const ctx = await ctxService.create({ apiType: 'admin' });

  // Root collection (da channel atual)
  const collectionRepo = connection.getRepository(ctx, Collection);
  const root = await collectionRepo.findOne({
    where: { isRoot: true as any },
  });
  if (!root) {
    throw new Error('Não encontrei a Root Collection (isRoot=true).');
  }

  Logger.info(`📄 A ler Excel: ${FILE_PATH}`);
  const wb = XLSX.readFile(FILE_PATH);
  const mergedSheet = wb.Sheets['MERGED'];
  const noMatchSheet = wb.Sheets['NO_MATCH_IN_CATFILE'];

  if (!mergedSheet) throw new Error('Sheet "MERGED" não existe no Excel.');
  if (!noMatchSheet) throw new Error('Sheet "NO_MATCH_IN_CATFILE" não existe no Excel.');

  const mergedRows: any[] = XLSX.utils.sheet_to_json(mergedSheet, { defval: null });
  const noMatchRows: any[] = XLSX.utils.sheet_to_json(noMatchSheet, { defval: null });

  // --- Cache collections
  const level1ByName = new Map<string, string>(); // name -> id
  const level2ByKey = new Map<string, string>(); // `${L1}>>${L2}` -> id

  async function ensureCollection(name: string, parentId: string): Promise<string> {
    const slug = slugify(name);

    // tenta encontrar pela combinação name+parent (mais robusto)
    const existing = await collectionRepo.findOne({
      where: {
        name: name as any,
        parent: { id: parentId } as any,
      } as any,
      relations: ['parent'],
    });

    if (existing) return String(existing.id);

    const created = await collectionService.create(ctx, {
      parentId,
      name,
      slug,
      isPrivate: false,
      // opcional: description
      description: '',
    } as any);

    return String(created.id);
  }

  // --- 1) Criar/garantir collections e mapear SKU -> (L1,L2)
  Logger.info('🧩 A garantir Collections (Categoryn1/Categoryn2) ...');

  const skuToCats: Array<{ sku: string; l1: string; l2: string }> = [];

  for (const r of mergedRows) {
    const sku = cellStr(r['SKU']) ?? cellStr(r['SKU_norm']);
    // preferimos os níveis _prod (do teu ficheiro principal)
    const l1 = cellStr(r['Categoryn1_prod']) ?? cellStr(r['Categoryn1_phc']);
    const l2 = cellStr(r['Categoryn2_prod']) ?? cellStr(r['Categoryn2_phc']);

    if (!sku || !l1 || !l2) continue;

    skuToCats.push({ sku, l1, l2 });
  }

  // Garantir collections únicas
  const uniqueL1 = Array.from(new Set(skuToCats.map(x => x.l1)));
  for (const l1 of uniqueL1) {
    const id = await ensureCollection(l1, String(root.id));
    level1ByName.set(l1, id);
  }

  const uniqueL2 = Array.from(new Set(skuToCats.map(x => `${x.l1}>>${x.l2}`)));
  for (const key of uniqueL2) {
    const [l1, l2] = key.split('>>');
    const parentId = level1ByName.get(l1)!;
    const id = await ensureCollection(l2, parentId);
    level2ByKey.set(key, id);
  }

  Logger.info(`✅ Collections OK: L1=${level1ByName.size} | L2=${level2ByKey.size}`);

  // --- helpers: find productId by SKU via repository (rápido e confiável)
  const variantRepo = connection.getRepository(ctx, ProductVariant);

  async function getProductIdBySku(sku: string): Promise<string | null> {
    const v = await variantRepo.findOne({
      where: { sku: sku as any } as any,
      relations: ['product'],
    });
    if (!v?.product?.id) return null;
    return String(v.product.id);
  }

  // --- 2) Atribuir produtos às collections (nível 2 + nível 1)
  Logger.info('📦 A atribuir Products às Collections (L2 + L1)...');

  let assigned = 0;
  let missingSku = 0;

  // Para reduzir chamadas repetidas, cache SKU->productId
  const productIdCache = new Map<string, string | null>();

  for (const row of skuToCats) {
    const { sku, l1, l2 } = row;

    let productId = productIdCache.get(sku);
    if (productId === undefined) {
      productId = await getProductIdBySku(sku);
      productIdCache.set(sku, productId ?? null);
    }
    if (!productId) {
      missingSku++;
      continue;
    }

    const l1Id = level1ByName.get(l1)!;
    const l2Id = level2ByKey.get(`${l1}>>${l2}`)!;

    // add ao nível 2
    await collectionService.addProductsToCollection(ctx, l2Id, [productId]);
    // add ao nível 1 também (para o nível 1 listar tudo)
    await collectionService.addProductsToCollection(ctx, l1Id, [productId]);

    assigned++;
    if (assigned % 500 === 0) Logger.info(`Atribuídos: ${assigned}`);
  }

  Logger.info(`✅ Atribuição feita. Assigned=${assigned} | SKUs sem variant no Vendure=${missingSku}`);

  // --- 3) Garantir Facet “DISCONTINUADO”
  Logger.info('🏷️ A garantir Facet/FacetValue de DISCONTINUADO...');

  let facet = await facetService.findOneByCode(ctx, DISCONTINUED_FACET_CODE);
  if (!facet) {
    facet = await facetService.create(ctx, {
      code: DISCONTINUED_FACET_CODE,
      name: DISCONTINUED_FACET_NAME,
      isPrivate: false,
      values: [],
    } as any);
  }

  // garantir value
  const fvList = await facetValueService.findAll(ctx, { take: 200, skip: 0 });
  let discontinuedValue = fvList.items.find(
    (v: any) => v.code === DISCONTINUED_VALUE_CODE && String(v.facetId ?? v.facet?.id) === String(facet!.id),
  );

  if (!discontinuedValue) {
    discontinuedValue = await facetValueService.create(ctx, facet.id as any, {
      code: DISCONTINUED_VALUE_CODE,
      name: DISCONTINUED_VALUE_NAME,
    } as any);
  }

  const discontinuedValueId = String(discontinuedValue.id);

  // --- 4) Aplicar “DISCONTINUADO” aos SKUs sem match
  Logger.info('🚫 A aplicar DISCONTINUADO aos SKUs sem match...');

  const noMatchSkus = noMatchRows
    .map(r => cellStr(r['SKU']) ?? cellStr(r['SKU_norm']))
    .filter((x): x is string => Boolean(x));

  let flagged = 0;
  let notFound = 0;

  for (const sku of noMatchSkus) {
    const productId = await getProductIdBySku(sku);
    if (!productId) {
      notFound++;
      continue;
    }

    // aplicar ao PRODUCT (mantém outras facets? aqui sobrescreve; como tu limpaste antes e estás a controlar, é ok)
    await productService.update(ctx, {
      id: productId,
      facetValueIds: [discontinuedValueId],
    } as any);

    // aplicar também à VARIANT (para search/facets bater certo)
    const v = await variantRepo.findOne({ where: { sku: sku as any } as any });
    if (v?.id) {
      await variantService.update(ctx, [{ id: String(v.id), facetValueIds: [discontinuedValueId] }] as any);
    }

    flagged++;
    if (flagged % 200 === 0) Logger.info(`Discontinued aplicados: ${flagged}`);
  }

  Logger.info(`✅ DISCONTINUADO aplicado: ${flagged} | SKUs no-match sem variant no Vendure: ${notFound}`);

  Logger.info('🎉 Script terminado. Agora faz reindex no Admin API: mutation { reindex }');

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
