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

// ✅ SKU column
const SKU_FIELD = 'SKU_FROM_FILE1';

// ✅ 3 níveis como pediste (Category1 mais largo, Category3 mais específico)
const CAT1_COL = 'Categoryn1';
const CAT2_COL = 'Categoryn2';
const CAT3_COL = 'Categoryn3';

// Facets a criar/usar
const FACETS = [
  { code: 'phc_cat1', name: 'PHC Category 1' },
  { code: 'phc_cat2', name: 'PHC Category 2' },
  { code: 'phc_cat3', name: 'PHC Category 3' },
] as const;

function toStr(v: unknown): string {
  return String(v ?? '').trim();
}

function slugifyCode(v: string): string {
  return v
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/[^a-z0-9-]/g, '')
    .replace(/-+/g, '-')
    .slice(0, 80);
}

async function main() {
  const config = {
    ...vendureConfig,
    apiOptions: { ...vendureConfig.apiOptions, port: 0 },
  };

  const app = await bootstrap(config);

  // só para garantir contexto admin
  await app.get(RequestContextService).create({
    apiType: 'admin',
    languageCode: LANG_CODE as any,
  });

  const connection = app.get(TransactionalConnection);
  const qr = connection.rawConnection.createQueryRunner();

  // -----------------------------
  // 0) Detect join table Product<->FacetValue
  // -----------------------------
  async function detectProductFacetJoin() {
    const candidates: Array<{ table_name: string }> = await qr.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name ILIKE 'product%facet%value%'
    `);

    for (const t of candidates) {
      const cols: Array<{ column_name: string }> = await qr.query(
        `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=$1
        `,
        [t.table_name],
      );
      const names = cols.map(c => c.column_name);

      if (names.includes('productId') && names.includes('facetValueId')) {
        return { table: t.table_name, facetCol: 'facetValueId' };
      }
      // fallback (caso raro)
      if (names.includes('productId') && names.includes('facetValuesId')) {
        return { table: t.table_name, facetCol: 'facetValuesId' };
      }
    }

    throw new Error(
      `Não encontrei join table Product<->FacetValue. Candidatas: ${candidates
        .map(x => x.table_name)
        .join(', ')}`,
    );
  }

  const join = await detectProductFacetJoin();
  Logger.info(`Join table Product<->FacetValue: ${join.table} (facet col: ${join.facetCol})`);

  // -----------------------------
  // 1) Ensure facet + facet values (via BD)
  // -----------------------------
  async function ensureFacet(code: string, name: string): Promise<number> {
    const existing: Array<{ id: number }> = await qr.query(
      `SELECT id FROM facet WHERE code = $1 LIMIT 1`,
      [code],
    );
    if (existing.length) return existing[0].id;

    // ✅ SEM "position" (no teu schema não existe)
    const inserted: Array<{ id: number }> = await qr.query(
      `
      INSERT INTO facet (code, "isPrivate", "createdAt", "updatedAt")
      VALUES ($1, false, NOW(), NOW())
      RETURNING id
      `,
      [code],
    );

    const facetId = inserted[0].id;

    await qr.query(
      `
      INSERT INTO facet_translation ("languageCode", name, "baseId")
      VALUES ($1, $2, $3)
      `,
      [LANG_CODE, name, facetId],
    );

    return facetId;
  }

  async function ensureFacetValue(facetId: number, valueName: string): Promise<number> {
    const code = slugifyCode(valueName);
    if (!code) throw new Error(`FacetValue inválido (vazio) para facetId=${facetId}`);

    const existing: Array<{ id: number }> = await qr.query(
      `SELECT id FROM facet_value WHERE "facetId" = $1 AND code = $2 LIMIT 1`,
      [facetId, code],
    );
    if (existing.length) return existing[0].id;

    const inserted: Array<{ id: number }> = await qr.query(
      `
      INSERT INTO facet_value (code, "facetId", "createdAt", "updatedAt")
      VALUES ($1, $2, NOW(), NOW())
      RETURNING id
      `,
      [code, facetId],
    );
    const facetValueId = inserted[0].id;

    await qr.query(
      `
      INSERT INTO facet_value_translation ("languageCode", name, "baseId")
      VALUES ($1, $2, $3)
      `,
      [LANG_CODE, valueName, facetValueId],
    );

    return facetValueId;
  }

  // -----------------------------
  // 2) Ensure facets
  // -----------------------------
  Logger.info('A garantir facets...');
  const facetIds: Record<string, number> = {};

  for (const f of FACETS) {
    const id = await ensureFacet(f.code, f.name);
    facetIds[f.code] = id;
    Logger.info(`Facet OK: ${f.code} (id=${id})`);
  }

  // -----------------------------
  // 3) Read Excel
  // -----------------------------
  Logger.info(`A ler Excel: ${EXCEL_PATH} (sheet: ${SHEET_NAME})`);
  const wb = XLSX.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) {
    throw new Error(`Sheet "${SHEET_NAME}" não existe. Sheets: ${wb.SheetNames.join(', ')}`);
  }

  const rows = XLSX.utils.sheet_to_json<Record<string, any>>(ws, { defval: '' });
  Logger.info(`Linhas no Excel: ${rows.length}`);

  // -----------------------------
  // 4) SKU -> productId (via BD)
  // -----------------------------
  const skus = Array.from(new Set(rows.map(r => toStr(r[SKU_FIELD])).filter(Boolean)));
  Logger.info(`SKUs únicos: ${skus.length}`);

  const skuToProductId = new Map<string, number>();

  for (let i = 0; i < skus.length; i += 500) {
    const chunk = skus.slice(i, i + 500);

    const res: Array<{ sku: string; productId: number }> = await qr.query(
      `
      SELECT pv.sku as "sku", pv."productId"::int as "productId"
      FROM product_variant pv
      WHERE pv.sku = ANY($1)
      `,
      [chunk],
    );

    for (const r of res) skuToProductId.set(r.sku, r.productId);
  }

  Logger.info(`SKUs encontrados no Vendure: ${skuToProductId.size}`);

  // -----------------------------
  // 5) Prepare product -> facetValueIds
  // -----------------------------
  const productToFacetValueIds = new Map<number, Set<number>>();
  const valueCache = new Map<string, number>(); // facetCode::valueName

  let missingSku = 0;
  let processed = 0;

  for (const r of rows) {
    processed++;

    const sku = toStr(r[SKU_FIELD]);
    const productId = skuToProductId.get(sku);

    if (!sku || !productId) {
      missingSku++;
      continue;
    }

    const c1 = toStr(r[CAT1_COL]);
    const c2 = toStr(r[CAT2_COL]);
    const c3 = toStr(r[CAT3_COL]);

    const wanted: Array<{ facetCode: string; valueName: string }> = [];
    if (c1) wanted.push({ facetCode: 'phc_cat1', valueName: c1 });
    if (c2) wanted.push({ facetCode: 'phc_cat2', valueName: c2 });
    if (c3) wanted.push({ facetCode: 'phc_cat3', valueName: c3 });

    if (!wanted.length) continue;

    if (!productToFacetValueIds.has(productId)) {
      productToFacetValueIds.set(productId, new Set<number>());
    }

    for (const w of wanted) {
      const cacheKey = `${w.facetCode}::${w.valueName}`;
      let facetValueId = valueCache.get(cacheKey);

      if (!facetValueId) {
        const facetId = facetIds[w.facetCode];
        facetValueId = await ensureFacetValue(facetId, w.valueName);
        valueCache.set(cacheKey, facetValueId);
      }

      productToFacetValueIds.get(productId)!.add(facetValueId);
    }

    if (processed % 500 === 0) Logger.info(`Processadas: ${processed}`);
  }

  Logger.info(`Processadas total: ${processed}`);
  Logger.info(`Products com facetValues para aplicar: ${productToFacetValueIds.size}`);
  Logger.info(`Linhas com SKU não encontrado no Vendure: ${missingSku}`);

  // -----------------------------
  // 6) Apply joins (idempotente)
  // -----------------------------
  Logger.info('A aplicar facetValues aos PRODUCTS (via BD)...');

  let inserts = 0;

  for (const [productId, fvSet] of productToFacetValueIds.entries()) {
    const fvIds = Array.from(fvSet);

    for (let i = 0; i < fvIds.length; i += 500) {
      const chunk = fvIds.slice(i, i + 500);

      await qr.query(
        `
        INSERT INTO "${join.table}" ("productId", "${join.facetCol}")
        SELECT $1::int, x::int
        FROM unnest($2::int[]) as x
        WHERE NOT EXISTS (
          SELECT 1 FROM "${join.table}" j
          WHERE j."productId" = $1::int AND j."${join.facetCol}" = x::int
        )
        `,
        [productId, chunk],
      );

      inserts += chunk.length;
    }
  }

  Logger.info(`✅ Concluído. FacetValue ligações tentadas: ${inserts}`);

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
