import {
  bootstrapWorker,
  LanguageCode,
  Logger,
  RequestContextService,
  TransactionalConnection,
  FacetService,
  FacetValueService,
  CollectionService,
  ProductService,
  ProductVariantService,
} from '@vendure/core';
import { In } from 'typeorm';
import * as xlsx from 'xlsx';
import { config } from '../../src/vendure-config';

type Row = {
  SKU_FROM_FILE1?: string;
  Categoryn1?: string;
  Categoryn2?: string;
  Categoryn3?: string;
  Categoryn4?: string;
};

function normName(s: string) {
  return String(s ?? '').trim();
}
function normCode(s: string) {
  return normName(s)
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/[^a-z0-9\-]/g, '')
    .replace(/\-+/g, '-')
    .replace(/^\-|\-$/g, '');
}
function keyPath(parts: string[]) {
  return parts.map(p => normName(p)).join(' | ');
}

async function main() {
  const EXCEL_PATH = process.env.EXCEL_PATH!;
  const SHEET_NAME = process.env.SHEET_NAME || 'PHC_WITH_SKU';
  const LANG_CODE = (process.env.LANG_CODE || 'pt') as LanguageCode;

  if (!EXCEL_PATH) {
    throw new Error('Falta EXCEL_PATH');
  }

  const { app } = await bootstrapWorker(config);
  const ctx = await app.get(RequestContextService).create({ apiType: 'admin' });

  const connection = app.get(TransactionalConnection);
  const facetService = app.get(FacetService);
  const facetValueService = app.get(FacetValueService);
  const collectionService = app.get(CollectionService);
  const productService = app.get(ProductService);
  const variantService = app.get(ProductVariantService);

  Logger.info(`A ler Excel: ${EXCEL_PATH} (sheet: ${SHEET_NAME})`);
  const wb = xlsx.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) throw new Error(`Sheet não encontrado: ${SHEET_NAME}`);
  const rows = xlsx.utils.sheet_to_json<Row>(ws, { defval: '' });

  Logger.info(`Linhas no Excel: ${rows.length}`);

  // 1) Criar / garantir facets cat1..cat4
  const facets = [
    { code: 'cat1', name: 'cat1' },
    { code: 'cat2', name: 'cat2' },
    { code: 'cat3', name: 'cat3' },
    { code: 'cat4', name: 'cat4' },
  ];

  const facetByCode = new Map<string, any>();
  for (const f of facets) {
    const existing = await facetService.findAll(ctx, { take: 0 }).then(r =>
      r.items.find(i => i.code === f.code),
    );
    if (existing) {
      facetByCode.set(f.code, existing);
      continue;
    }
    const created = await facetService.create(ctx, {
      code: f.code,
      translations: [{ languageCode: LANG_CODE, name: f.name }],
      isPrivate: false,
    });
    facetByCode.set(f.code, created);
    Logger.info(`Facet criada: ${f.code}`);
  }

  // 2) Criar / garantir facet values por PATH (evita colisões se nomes repetirem em níveis diferentes)
  // Map: "cat2" -> ( "CAT1 | CAT2" -> facetValueId )
  const facetValueIdByLevelKey = new Map<string, Map<string, string>>();

  async function ensureFacetValue(levelCode: string, displayName: string, uniqueKey: string) {
    if (!facetValueIdByLevelKey.has(levelCode)) facetValueIdByLevelKey.set(levelCode, new Map());
    const map = facetValueIdByLevelKey.get(levelCode)!;
    if (map.has(uniqueKey)) return map.get(uniqueKey)!;

    // procurar existente por code (baseado em uniqueKey)
    const fvCode = normCode(uniqueKey) || normCode(displayName);
    const facet = facetByCode.get(levelCode);

    // tenta achar por code
    const existing = await facetValueService.findAll(ctx, { take: 0 }).then(r =>
      r.items.find(i => i.code === fvCode && (i as any).facet?.code === levelCode),
    );

    if (existing) {
      map.set(uniqueKey, existing.id);
      return existing.id;
    }

    const created = await facetValueService.create(ctx, facet.id, {
      code: fvCode,
      translations: [{ languageCode: LANG_CODE, name: displayName }],
    });
    map.set(uniqueKey, created.id);
    return created.id;
  }

  // 3) Mapear SKU -> variantId + productId
  const skus = Array.from(
    new Set(rows.map(r => normName(r.SKU_FROM_FILE1 || '')).filter(Boolean)),
  );

  Logger.info(`SKUs únicos no Excel: ${skus.length}`);

  // buscar variants por SKU em batches
  const variantRepo = connection.rawConnection.getRepository('ProductVariant');
  const skuToVariant: Map<string, { variantId: string; productId: string }> = new Map();

  const BATCH = 1000;
  for (let i = 0; i < skus.length; i += BATCH) {
    const batch = skus.slice(i, i + BATCH);
    // @ts-ignore
    const found = await variantRepo.find({ where: { sku: In(batch) }, select: ['id', 'sku', 'productId'] });
    for (const v of found) {
      skuToVariant.set(v.sku, { variantId: String(v.id), productId: String(v.productId) });
    }
  }

  Logger.info(`SKUs mapeados para variantId: ${skuToVariant.size}`);

  // 4) Construir sets de facetValueIds por product e por variant
  const productFacetIds = new Map<string, Set<string>>();
  const variantFacetIds = new Map<string, Set<string>>();

  let noSkuMatch = 0;

  for (const r of rows) {
    const sku = normName(r.SKU_FROM_FILE1 || '');
    if (!sku) continue;

    const match = skuToVariant.get(sku);
    if (!match) {
      noSkuMatch++;
      continue;
    }

    const c1 = normName(r.Categoryn1 || '');
    const c2 = normName(r.Categoryn2 || '');
    const c3 = normName(r.Categoryn3 || '');
    const c4 = normName(r.Categoryn4 || '');

    const pId = match.productId;
    const vId = match.variantId;

    if (!productFacetIds.has(pId)) productFacetIds.set(pId, new Set());
    if (!variantFacetIds.has(vId)) variantFacetIds.set(vId, new Set());

    // nivel 1
    if (c1) {
      const id = await ensureFacetValue('cat1', c1, keyPath([c1]));
      productFacetIds.get(pId)!.add(id);
      variantFacetIds.get(vId)!.add(id);
    }
    // nivel 2
    if (c1 && c2) {
      const id = await ensureFacetValue('cat2', c2, keyPath([c1, c2]));
      productFacetIds.get(pId)!.add(id);
      variantFacetIds.get(vId)!.add(id);
    }
    // nivel 3
    if (c1 && c2 && c3) {
      const id = await ensureFacetValue('cat3', c3, keyPath([c1, c2, c3]));
      productFacetIds.get(pId)!.add(id);
      variantFacetIds.get(vId)!.add(id);
    }
    // nivel 4
    if (c1 && c2 && c3 && c4) {
      const id = await ensureFacetValue('cat4', c4, keyPath([c1, c2, c3, c4]));
      productFacetIds.get(pId)!.add(id);
      variantFacetIds.get(vId)!.add(id);
    }
  }

  Logger.info(`Sem variant match (por SKU): ${noSkuMatch}`);

  // 5) Aplicar facetValueIds aos PRODUCTS
  Logger.info(`A aplicar facets cat1..cat4 aos PRODUCTS... (total=${productFacetIds.size})`);
  let doneP = 0;
  for (const [productId, set] of productFacetIds) {
    await productService.update(ctx, {
      id: productId,
      facetValueIds: Array.from(set),
    } as any);
    doneP++;
    if (doneP % 200 === 0) Logger.info(`Products atualizados: ${doneP}`);
  }
  Logger.info(`✅ Products atualizados (total): ${doneP}`);

  // 6) Aplicar facetValueIds às VARIANTS (para garantir que os filtros de Collection pegam)
  Logger.info(`A aplicar facets cat1..cat4 às VARIANTS... (total=${variantFacetIds.size})`);
  let doneV = 0;
  for (const [variantId, set] of variantFacetIds) {
    await variantService.update(ctx, {
      id: variantId,
      facetValueIds: Array.from(set),
    } as any);
    doneV++;
    if (doneV % 500 === 0) Logger.info(`Variants atualizadas: ${doneV}`);
  }
  Logger.info(`✅ Variants atualizadas (total): ${doneV}`);

  // 7) Configurar filtros nas Collections para refinamento (cat1 -> cat2 -> cat3 -> cat4)
  // Ideia:
  // - depth 1: filter cat1
  // - depth 2: inheritFilters=true + filter cat2
  // - depth 3: inheritFilters=true + filter cat3
  // - depth 4: inheritFilters=true + filter cat4
  // Assim: cat2 mostra tudo do cat2, cat3 refina, cat4 refina mais.
  // (Collections são “variants”, é isso mesmo no Vendure.) :contentReference[oaicite:1]{index=1}

  Logger.info(`A carregar Collections (para aplicar filtros facet-value-filter)...`);
  const allCollections = await collectionService.findAll(ctx, { take: 0 });
  const byId = new Map<string, any>();
  for (const c of allCollections.items) byId.set(String(c.id), c);

  // build path names até ao root
  function getPathNames(c: any): string[] {
    const names: string[] = [];
    let cur: any = c;
    while (cur && cur.parent) {
      // parar no __root_collection__
      if (cur.parent?.name === '__root_collection__') {
        names.unshift(normName(cur.name));
        break;
      }
      names.unshift(normName(cur.name));
      cur = byId.get(String(cur.parent.id));
    }
    return names.filter(Boolean);
  }

  let updatedCollections = 0;
  for (const c of allCollections.items) {
    if (c.name === '__root_collection__') continue;

    const path = getPathNames(c);
    const depth = path.length; // 1..N (a partir de root)

    if (depth < 1 || depth > 4) continue; // só vamos até cat4

    const levelCode = `cat${depth}`;
    const uniqueKey = keyPath(path.slice(0, depth));

    const fvMap = facetValueIdByLevelKey.get(levelCode);
    const fvId = fvMap?.get(uniqueKey);
    if (!fvId) continue;

    // IMPORTANTE: facet-value-filter usa args facetValueNames + containsAny (docs)
    // Vamos usar "facetCode:valueCode" para evitar ambiguidades. :contentReference[oaicite:2]{index=2}
    // Aqui usamos o "code" do facet value (que criámos com base no uniqueKey).
    // Para isso precisamos de reconstruir o code:
    const fvCode = normCode(uniqueKey) || normCode(path[depth - 1]);
    const facetValueNameToken = `${levelCode}:${fvCode}`;

    await collectionService.update(ctx, {
      id: c.id,
      inheritFilters: depth > 1,
      filters: [
        {
          code: 'facet-value-filter',
          args: {
            facetValueNames: [facetValueNameToken],
            containsAny: false,
          },
        },
      ],
    } as any);

    updatedCollections++;
  }

  Logger.info(`✅ Collections atualizadas com filtros: ${updatedCollections}`);
  Logger.info('🎉 Concluído: facets aplicadas e filtros configurados (cat1..cat4 com inherit).');

  await app.close();
}

main().catch(err => {
  Logger.error(err);
  process.exit(1);
});
