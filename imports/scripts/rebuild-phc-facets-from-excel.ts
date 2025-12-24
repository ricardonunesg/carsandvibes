import { bootstrap, Logger, TransactionalConnection } from '@vendure/core';
import { config as vendureConfig } from '../../src/vendure-config';
import * as XLSX from 'xlsx';

const EXCEL_PATH =
  process.env.EXCEL_PATH ?? '/root/carsandvibes/imports/source/PHC_categories_with_SKU_from_file1.xlsx';
const SHEET_NAME = process.env.SHEET_NAME ?? 'PHC_WITH_SKU';
const LANG_CODE = process.env.LANG_CODE ?? 'pt';

const SKU_FIELD = process.env.SKU_FIELD ?? 'SKU_FROM_FILE1';
const CAT1_COL = process.env.CAT1_COL ?? 'Categoryn1';
const CAT2_COL = process.env.CAT2_COL ?? 'Categoryn2';
const CAT3_COL = process.env.CAT3_COL ?? 'Categoryn3';

const FACETS = [
  { code: 'phc_cat1', name: 'PHC Category 1', col: CAT1_COL },
  { code: 'phc_cat2', name: 'PHC Category 2', col: CAT2_COL },
  { code: 'phc_cat3', name: 'PHC Category 3', col: CAT3_COL },
] as const;

function norm(v: unknown) {
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
  const app = await bootstrap({
    ...vendureConfig,
    apiOptions: { ...vendureConfig.apiOptions, port: 0 },
  });

  const qr = app.get(TransactionalConnection).rawConnection.createQueryRunner();

  // join table Product<->FacetValue
  const joinTable = 'product_facet_values_facet_value';
  const productIdCol = 'productId';
  const facetValueIdCol = 'facetValueId';

  Logger.info(`A fazer rebuild PHC facets a partir do Excel`);
  Logger.info(`Excel: ${EXCEL_PATH} | Sheet: ${SHEET_NAME} | LANG_CODE=${LANG_CODE}`);

  // 1) garantir facets existem (sem "position")
  async function ensureFacet(code: string, name: string): Promise<number> {
    const ex: Array<{ id: number }> = await qr.query(`SELECT id FROM facet WHERE code=$1 LIMIT 1`, [code]);
    if (ex.length) return ex[0].id;

    const ins: Array<{ id: number }> = await qr.query(
      `INSERT INTO facet (code, "isPrivate", "createdAt", "updatedAt")
       VALUES ($1, false, NOW(), NOW()) RETURNING id`,
      [code],
    );
    const facetId = ins[0].id;

    await qr.query(
      `INSERT INTO facet_translation ("languageCode", name, "baseId")
       VALUES ($1, $2, $3)`,
      [LANG_CODE, name, facetId],
    );

    // ligar facet ao channel 1
    await qr.query(
      `INSERT INTO facet_channels_channel ("facetId", "channelId")
       VALUES ($1, 1) ON CONFLICT DO NOTHING`,
      [facetId],
    );

    return facetId;
  }

  async function ensureFacetValue(facetId: number, valueName: string): Promise<number> {
    const code = slugifyCode(valueName);
    if (!code) throw new Error(`FacetValue inválido: "${valueName}"`);

    const ex: Array<{ id: number }> = await qr.query(
      `SELECT id FROM facet_value WHERE "facetId"=$1 AND code=$2 LIMIT 1`,
      [facetId, code],
    );
    if (ex.length) return ex[0].id;

    const ins: Array<{ id: number }> = await qr.query(
      `INSERT INTO facet_value (code, "facetId", "createdAt", "updatedAt")
       VALUES ($1, $2, NOW(), NOW()) RETURNING id`,
      [code, facetId],
    );
    const facetValueId = ins[0].id;

    await qr.query(
      `INSERT INTO facet_value_translation ("languageCode", name, "baseId")
       VALUES ($1, $2, $3)`,
      [LANG_CODE, valueName, facetValueId],
    );

    // ligar facetValue ao channel 1
    await qr.query(
      `INSERT INTO facet_value_channels_channel ("facetValueId", "channelId")
       VALUES ($1, 1) ON CONFLICT DO NOTHING`,
      [facetValueId],
    );

    return facetValueId;
  }

  const facetIds: Record<string, number> = {};
  for (const f of FACETS) {
    facetIds[f.code] = await ensureFacet(f.code, f.name);
    Logger.info(`Facet OK: ${f.code} (id=${facetIds[f.code]})`);
  }

  // 2) limpar PHC ligações existentes
  Logger.info(`A limpar ligações PHC (cat1/cat2/cat3) em products...`);
  const deleted = await qr.query(
    `
    DELETE FROM ${joinTable} j
    USING facet_value fv
    JOIN facet f ON f.id = fv."facetId"
    WHERE j."${facetValueIdCol}" = fv.id
      AND f.code IN ('phc_cat1','phc_cat2','phc_cat3')
    RETURNING 1
    `,
  );
  Logger.info(`Ligações removidas: ${deleted.length}`);

  // 3) ler Excel
  const wb = XLSX.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) throw new Error(`Sheet não encontrada. Sheets: ${wb.SheetNames.join(', ')}`);

  const rows = XLSX.utils.sheet_to_json<Record<string, any>>(ws, { defval: '' });
  Logger.info(`Linhas no Excel: ${rows.length}`);

  // 4) mapear sku->productId (só SKUs do excel)
  const skus = Array.from(new Set(rows.map(r => norm(r[SKU_FIELD])).filter(Boolean)));
  Logger.info(`SKUs únicos no Excel: ${skus.length}`);

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
  Logger.info(`SKUs em falta (Excel - Vendure): ${skus.length - skuToProductId.size}`);

  // 5) agregar por productId -> set facetValueIds
  const productToFacetValueIds = new Map<number, Set<number>>();
  const valueCache = new Map<string, number>(); // facetCode::valueName

  let missingSku = 0;
  let processed = 0;

  for (const r of rows) {
    processed++;
    const sku = norm(r[SKU_FIELD]);
    const productId = skuToProductId.get(sku);

    if (!sku || !productId) {
      missingSku++;
      continue;
    }

    if (!productToFacetValueIds.has(productId)) productToFacetValueIds.set(productId, new Set<number>());

    for (const f of FACETS) {
      const val = norm(r[f.col]);
      if (!val) continue;

      const key = `${f.code}::${val}`;
      let fvId = valueCache.get(key);
      if (!fvId) {
        fvId = await ensureFacetValue(facetIds[f.code], val);
        valueCache.set(key, fvId);
      }
      productToFacetValueIds.get(productId)!.add(fvId);
    }

    if (processed % 500 === 0) Logger.info(`Processadas: ${processed}`);
  }

  Logger.info(`Processadas total: ${processed}`);
  Logger.info(`Products afetados: ${productToFacetValueIds.size}`);
  Logger.info(`Linhas com SKU não encontrado: ${missingSku}`);

  // 6) inserir joins (idempotente)
  Logger.info(`A inserir ligações product<->facetValue...`);
  let inserts = 0;

  for (const [productId, set] of productToFacetValueIds.entries()) {
    const fvIds = Array.from(set);
    for (let i = 0; i < fvIds.length; i += 500) {
      const chunk = fvIds.slice(i, i + 500);
      await qr.query(
        `
        INSERT INTO ${joinTable} ("${productIdCol}", "${facetValueIdCol}")
        SELECT $1::int, x::int
        FROM unnest($2::int[]) AS x
        WHERE NOT EXISTS (
          SELECT 1 FROM ${joinTable} j
          WHERE j."${productIdCol}"=$1::int AND j."${facetValueIdCol}"=x::int
        )
        `,
        [productId, chunk],
      );
      inserts += chunk.length;
    }
  }

  Logger.info(`✅ Rebuild concluído. Ligações tentadas: ${inserts}`);
  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
