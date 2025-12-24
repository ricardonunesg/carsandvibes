import 'dotenv/config';
import path from 'path';
import xlsx from 'xlsx';
import {
  bootstrap,
  DefaultLogger,
  LogLevel,
  RequestContext,
  LanguageCode,
  Collection,
  ProductVariant,
} from '@vendure/core';
import { config } from '../../src/vendure-config';

type Row = Record<string, any>;

function norm(s: any) {
  return String(s ?? '')
    .trim()
    .replace(/\s+/g, ' ')
    .toLowerCase();
}

// tenta encontrar a coluna pelo nome (case-insensitive, ignora espaços)
function pickCol(row: Row, wanted: string[]) {
  const keys = Object.keys(row);
  for (const w of wanted) {
    const wk = norm(w);
    const hit = keys.find(k => norm(k) === wk);
    if (hit) return hit;
  }
  // fallback: contém
  for (const w of wanted) {
    const wk = norm(w);
    const hit = keys.find(k => norm(k).includes(wk));
    if (hit) return hit;
  }
  return null;
}

async function main() {
  // evita EADDRINUSE (PM2 já está a servir 3000)
  process.env.PORT = '0';

  const EXCEL_PATH =
    process.env.EXCEL_PATH ||
    '/root/carsandvibes/imports/source/PHC_categories_with_SKU_from_file1.xlsx';
  const SHEET_NAME = process.env.SHEET_NAME || 'PHC_WITH_SKU';
  const LANG_CODE = (process.env.LANG_CODE as LanguageCode) || LanguageCode.pt;

  const app = await bootstrap({
    config: {
      ...config,
      logger: new DefaultLogger({ level: LogLevel.Info }),
    },
  });

  const connection = app.get('TransactionalConnection');
  const collectionRepo = connection.rawConnection.getRepository(Collection);
  const variantRepo = connection.rawConnection.getRepository(ProductVariant);
  const collectionService = app.get('CollectionService');

  const ctx = new RequestContext({
    apiType: 'admin',
    channel: (await connection.rawConnection
      .getRepository('Channel' as any)
      .findOne({ where: { code: 'default' } })) as any,
    languageCode: LANG_CODE,
    isAuthorized: true,
    authorizedAsOwnerOnly: false,
    session: {} as any,
    req: {} as any,
  });

  // 1) carregar collections nível 2 (filhas diretas do root)
  const root = await collectionRepo.findOne({
    where: { name: '__root_collection__' } as any,
    relations: ['parent', 'translations'],
  });

  if (!root) throw new Error('Não encontrei __root_collection__');

  const level2 = await collectionRepo.find({
    where: { parent: { id: (root as any).id } } as any,
    relations: ['parent', 'translations'],
  });

  // index por nome normalizado (ex: "DRIVER")
  const level2ByName = new Map<string, Collection>();
  for (const c of level2) {
    const t = (c as any).translations?.[0];
    const name = t?.name ?? (c as any).name;
    level2ByName.set(norm(name), c);
  }

  app
    .get('Logger')
    .info(
      `[Vendure Server] Collections nível 2 indexadas (keys): ${level2ByName.size}`,
    );

  // 2) ler excel
  const wb = xlsx.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) throw new Error(`Sheet não encontrada: ${SHEET_NAME}`);

  const rows = xlsx.utils.sheet_to_json<Row>(ws, { defval: '' });
  app
    .get('Logger')
    .info(
      `[Vendure Server] A ler Excel: ${EXCEL_PATH} (sheet: ${SHEET_NAME})`,
    );
  app.get('Logger').info(`[Vendure Server] Linhas no Excel: ${rows.length}`);

  // detectar colunas (SKU e Categoryn2)
  const sample = rows[0] ?? {};
  const skuCol = pickCol(sample, ['sku', 'SKU', 'Sku']);
  const cat2Col = pickCol(sample, ['Categoryn2', 'categoryn2', 'Category2', 'category2']);

  if (!skuCol) throw new Error('Não encontrei coluna SKU no Excel');
  if (!cat2Col) throw new Error('Não encontrei coluna Categoryn2 no Excel');

  // 3) mapear SKU -> variantId (em bulk, para ser rápido)
  const skuSet = new Set<string>();
  for (const r of rows) {
    const sku = String(r[skuCol] ?? '').trim();
    if (sku) skuSet.add(sku);
  }
  app
    .get('Logger')
    .info(`[Vendure Server] SKUs únicos no Excel: ${skuSet.size}`);

  const skuList = Array.from(skuSet);
  const skuToVariantId = new Map<string, number>();

  // query em batches (Postgres aguenta IN grande, mas vamos seguro)
  const BATCH = 1000;
  for (let i = 0; i < skuList.length; i += BATCH) {
    const batch = skuList.slice(i, i + BATCH);
    const found = await variantRepo
      .createQueryBuilder('v')
      .select(['v.id', 'v.sku'])
      .where('v.sku IN (:...skus)', { skus: batch })
      .getMany();

    for (const v of found) {
      skuToVariantId.set((v as any).sku, (v as any).id);
    }
  }

  app
    .get('Logger')
    .info(
      `[Vendure Server] SKUs mapeados para variantId: ${skuToVariantId.size}`,
    );

  // 4) agrupar: collectionId -> Set<variantId>
  const colIdToVariantIds = new Map<number, Set<number>>();
  let noCollectionMatch = 0;
  let noVariantMatch = 0;

  let processed = 0;
  for (const r of rows) {
    processed++;
    if (processed % 500 === 0) {
      app.get('Logger').info(`[Vendure Server] Processadas: ${processed}`);
    }

    const sku = String(r[skuCol] ?? '').trim();
    const cat2 = String(r[cat2Col] ?? '').trim();

    if (!sku || !cat2) continue;

    const c = level2ByName.get(norm(cat2));
    if (!c) {
      noCollectionMatch++;
      continue;
    }

    const variantId = skuToVariantId.get(sku);
    if (!variantId) {
      noVariantMatch++;
      continue;
    }

    const cid = (c as any).id as number;
    if (!colIdToVariantIds.has(cid)) colIdToVariantIds.set(cid, new Set());
    colIdToVariantIds.get(cid)!.add(variantId);
  }

  app.get('Logger').info(`[Vendure Server] Processadas total: ${processed}`);
  app
    .get('Logger')
    .info(
      `[Vendure Server] Collections alvo (nível 2) com variants: ${colIdToVariantIds.size}`,
    );
  app
    .get('Logger')
    .info(
      `[Vendure Server] Sem collection match (Categoryn2 não encontrado no nível 2): ${noCollectionMatch}`,
    );
  app
    .get('Logger')
    .info(`[Vendure Server] Sem variant match (por SKU): ${noVariantMatch}`);

  // 5) FORÇAR filtro manual nas collections nível 2 (muito importante)
  //    e aplicar join table
  const joinTable = 'collection_product_variants_product_variant';
  const colCol = 'collectionId';
  const varCol = 'productVariantId'; // em Vendure v3

  const runner = connection.rawConnection.createQueryRunner();

  const touchedCollectionIds: number[] = [];

  try {
    await runner.connect();
    await runner.startTransaction();

    // (a) atualizar filters + inheritFilters para todas as collections alvo
    for (const [collectionId] of colIdToVariantIds) {
      await runner.query(
        `UPDATE collection
         SET "inheritFilters" = false,
             "filters" = $2
         WHERE id = $1`,
        [
          collectionId,
          JSON.stringify([
            {
              code: 'variant-id-filter',
              args: {},
            },
          ]),
        ],
      );
      touchedCollectionIds.push(collectionId);
    }

    // (b) limpar apenas as collections alvo
    await runner.query(
      `DELETE FROM ${joinTable} WHERE "${colCol}" = ANY($1)`,
      [touchedCollectionIds],
    );

    // (c) inserir novamente
    let attempts = 0;
    for (const [collectionId, set] of colIdToVariantIds) {
      for (const variantId of set) {
        attempts++;
        await runner.query(
          `INSERT INTO ${joinTable} ("${colCol}", "${varCol}")
           VALUES ($1, $2)
           ON CONFLICT DO NOTHING`,
          [collectionId, variantId],
        );
      }
    }

    await runner.commitTransaction();

    app
      .get('Logger')
      .info(
        `[Vendure Server] ✅ Join table preenchida. Inserts tentados: ${attempts}`,
      );
  } catch (e) {
    await runner.rollbackTransaction();
    throw e;
  } finally {
    await runner.release();
  }

  // 6) recalcular collections (gera job "apply-collection-filters")
  app
    .get('Logger')
    .info(
      `[Vendure Server] A disparar triggerApplyFiltersJob() para ${touchedCollectionIds.length} collections...`,
    );

  await collectionService.triggerApplyFiltersJob(ctx, {
    collectionIds: touchedCollectionIds,
    applyToChangedVariantsOnly: false,
  });

  app.get('Logger').info(`[Vendure Server] 🎉 Concluído.`);

  await app.close();
}

main().catch(err => {
  // eslint-disable-next-line no-console
  console.error('❌ ERRO:', err);
  process.exit(1);
});
