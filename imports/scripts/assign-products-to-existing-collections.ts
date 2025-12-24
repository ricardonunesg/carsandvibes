import { bootstrap, Logger, TransactionalConnection } from '@vendure/core';
import { RequestContextService } from '@vendure/core';
import { config as vendureConfig } from '../../src/vendure-config';
import * as XLSX from 'xlsx';
import path from 'path';

const EXCEL_PATH = path.resolve(
  process.env.EXCEL_PATH ??
    '/root/carsandvibes/imports/source/PHC_categories_with_SKU_from_file1.xlsx',
);
const SHEET_NAME = process.env.SHEET_NAME ?? 'PHC_WITH_SKU';
const LANG_CODE = process.env.LANG_CODE ?? 'pt';

const SKU_FIELD = 'SKU_FROM_FILE1';
const CAT_FIELDS = ['Categoryn1', 'Categoryn2', 'Categoryn3', 'Categoryn4'] as const;

// Vendure guarda relação Collection <-> Variant nesta tabela
const JOIN_TABLE = 'collection_product_variants_product_variant';

function norm(s: unknown): string {
  return String(s ?? '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[^a-z0-9\s-]/g, '');
}

function toStr(s: unknown): string {
  return String(s ?? '').trim();
}

type ColRow = {
  id: string;
  parentId: string | null;
  name: string;
  slug: string;
};

async function main() {
  const config = {
    ...vendureConfig,
    apiOptions: { ...vendureConfig.apiOptions, port: 0 },
  };

  const app = await bootstrap(config);

  await app.get(RequestContextService).create({
    apiType: 'admin',
    languageCode: LANG_CODE as any,
  });

  const connection = app.get(TransactionalConnection);
  const qr = connection.rawConnection.createQueryRunner();

  // -----------------------------
  // 1) COLLECTIONS (da BD)
  // -----------------------------
  Logger.info(`A carregar Collections da BD (LANG_CODE=${LANG_CODE})...`);

  const collections: ColRow[] = await qr.query(
    `
    SELECT
      c.id::text AS "id",
      c."parentId"::text AS "parentId",
      COALESCE(ct.name, '') AS "name",
      COALESCE(ct.slug, '') AS "slug"
    FROM collection c
    LEFT JOIN collection_translation ct
      ON ct."baseId" = c.id
     AND ct."languageCode" = $1
    `,
    [LANG_CODE],
  );

  // root id (no teu caso é 1)
  let rootId: string | null = null;
  for (const c of collections) {
    if (norm(c.name) === '__root_collection__') {
      rootId = c.id;
      break;
    }
  }
  if (!rootId) {
    const roots = collections.filter(r => r.parentId == null);
    if (roots.length === 1) rootId = roots[0].id;
  }
  if (!rootId) throw new Error('Não consegui determinar o root collection id.');

  Logger.info(`Root collection id detetado: ${rootId}`);

  // mapa (parentId + key) -> collectionId
  const byParentAndKey = new Map<string, string>();

  for (const c of collections) {
    const parentKey = c.parentId ?? rootId;
    const id = c.id;

    const n = norm(c.name);
    if (n) byParentAndKey.set(`${parentKey}::${n}`, id);

    const s = norm(c.slug);
    if (s) byParentAndKey.set(`${parentKey}::${s}`, id);
  }

  function findDeepestCollectionId(categories: string[]): string | null {
    let parentId: string = rootId!;
    let found: string | null = null;

    for (const level of categories) {
      const key = `${parentId}::${norm(level)}`;
      const id = byParentAndKey.get(key);
      if (!id) break;
      found = id;
      parentId = id;
    }
    return found;
  }

  // -----------------------------
  // 2) EXCEL
  // -----------------------------
  Logger.info(`A ler Excel: ${EXCEL_PATH} (sheet: ${SHEET_NAME})`);
  const wb = XLSX.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) throw new Error(`Sheet "${SHEET_NAME}" não existe. Sheets: ${wb.SheetNames.join(', ')}`);

  const rows = XLSX.utils.sheet_to_json<Record<string, any>>(ws, { defval: '' });
  Logger.info(`Linhas no Excel: ${rows.length}`);

  // -----------------------------
  // 3) SKU -> variantId (da BD)
  // -----------------------------
  const skus = Array.from(new Set(rows.map(r => toStr(r[SKU_FIELD])).filter(Boolean)));
  Logger.info(`SKUs únicos no Excel: ${skus.length}`);

  const skuToVariantId = new Map<string, string>();

  for (let i = 0; i < skus.length; i += 500) {
    const chunk = skus.slice(i, i + 500);

    const res: Array<{ sku: string; variantId: string }> = await qr.query(
      `
      SELECT pv.sku as "sku", pv.id::text as "variantId"
      FROM product_variant pv
      WHERE pv.sku = ANY($1)
      `,
      [chunk],
    );

    for (const row of res) {
      if (row.sku && row.variantId) skuToVariantId.set(row.sku, row.variantId);
    }
  }

  Logger.info(`SKUs mapeados para variantId: ${skuToVariantId.size}`);

  // -----------------------------
  // 4) Build assignments (collection -> variantIds)
  // -----------------------------
  const collectionToVariantIds = new Map<string, Set<string>>();
  let missingCollections = 0;
  let missingVariants = 0;

  let processed = 0;

  for (const r of rows) {
    processed++;

    const cats = CAT_FIELDS.map(f => toStr(r[f])).filter(Boolean);
    if (!cats.length) continue;

    const collectionId = findDeepestCollectionId(cats);
    if (!collectionId) {
      missingCollections++;
      continue;
    }

    const sku = toStr(r[SKU_FIELD]);
    const variantId = skuToVariantId.get(sku);

    if (!sku || !variantId) {
      missingVariants++;
      continue;
    }

    if (!collectionToVariantIds.has(collectionId)) {
      collectionToVariantIds.set(collectionId, new Set<string>());
    }
    collectionToVariantIds.get(collectionId)!.add(variantId);

    if (processed % 500 === 0) Logger.info(`Processadas: ${processed}`);
  }

  Logger.info(`Processadas total: ${processed}`);
  Logger.info(`Collections alvo com variants: ${collectionToVariantIds.size}`);
  Logger.info(`Sem collection match: ${missingCollections}`);
  Logger.info(`Sem variant match (por SKU): ${missingVariants}`);

  // -----------------------------
  // 5) Apply assignments (via BD)
  // -----------------------------
  Logger.info('A aplicar associações Collections -> Variants (via BD)...');

  const cols: Array<{ column_name: string }> = await qr.query(
    `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name=$1
    ORDER BY ordinal_position
    `,
    [JOIN_TABLE],
  );

  const colNames = cols.map(c => c.column_name);

  if (!colNames.includes('collectionId')) {
    throw new Error(`A tabela ${JOIN_TABLE} não tem coluna collectionId. Colunas: ${colNames.join(', ')}`);
  }

  // coluna da variant (normalmente "productVariantId")
  const variantIdCol =
    colNames.find(c => c.toLowerCase().includes('variant') && c.toLowerCase().endsWith('id')) ??
    colNames.find(c => c !== 'collectionId' && c.toLowerCase().endsWith('id'));

  if (!variantIdCol) {
    throw new Error(`Não consegui identificar coluna de variantId na tabela ${JOIN_TABLE}. Colunas: ${colNames.join(', ')}`);
  }

  Logger.info(`Tabela join: ${JOIN_TABLE} | coluna variantId: ${variantIdCol}`);

  let totalAssigned = 0;

  for (const [collectionId, variantSet] of collectionToVariantIds.entries()) {
    const variantIds = Array.from(variantSet);

    for (let i = 0; i < variantIds.length; i += 500) {
      const chunk = variantIds.slice(i, i + 500);

      await qr.query(
        `
        INSERT INTO "${JOIN_TABLE}" ("collectionId", "${variantIdCol}")
        SELECT $1::int, x::int
        FROM unnest($2::int[]) as x
        WHERE NOT EXISTS (
          SELECT 1 FROM "${JOIN_TABLE}" j
          WHERE j."collectionId" = $1::int AND j."${variantIdCol}" = x::int
        )
        `,
        [Number(collectionId), chunk.map(x => Number(x))],
      );

      totalAssigned += chunk.length;
    }
  }

  Logger.info(`✅ Concluído. Variants associadas (tentativas): ${totalAssigned}`);

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
