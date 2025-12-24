import 'dotenv/config';
import XLSX from 'xlsx';
import { bootstrap, Logger, TransactionalConnection } from '@vendure/core';
import { config as vendureConfig } from '../../src/vendure-config';

const EXCEL_PATH = process.env.EXCEL_PATH || '/root/carsandvibes/imports/source/PHC_categories_with_SKU_from_file1.xlsx';
const SHEET_NAME = process.env.SHEET_NAME || 'PHC_WITH_SKU';
const LANG_CODE = process.env.LANG_CODE || 'pt';

const SKU_FIELD = process.env.SKU_FIELD || 'SKU_FROM_FILE1';
const CAT1_COL = process.env.CAT1_COL || 'Categoryn1';
const CAT2_COL = process.env.CAT2_COL || 'Categoryn2';

// default channel
const CHANNEL_ID = Number(process.env.CHANNEL_ID || 1);

// join variant<->facetValue (se o teu DB tiver outro nome, ajustas por env)
const VARIANT_JOIN_TABLE = process.env.VARIANT_JOIN_TABLE || 'product_variant_facet_values_facet_value';
const VARIANT_JOIN_VARIANT_COL = process.env.VARIANT_JOIN_VARIANT_COL || 'productVariantId';
const VARIANT_JOIN_FV_COL = process.env.VARIANT_JOIN_FV_COL || 'facetValueId';

// excluir (já fizeste)
const EXCLUDE_CAT1_CODE = 'driver';
const EXCLUDE_CAT2_CODE = 'fia-driver-gear';

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
  const app = await bootstrap({ ...vendureConfig, apiOptions: { ...vendureConfig.apiOptions, port: 0 } });
  const qr = app.get(TransactionalConnection).rawConnection.createQueryRunner();

  Logger.info('============================================');
  Logger.info('Apply Category1+Category2 (except driver & fia-driver-gear)');
  Logger.info(`Excel: ${EXCEL_PATH}`);
  Logger.info(`Sheet: ${SHEET_NAME}`);
  Logger.info(`SKU_FIELD: ${SKU_FIELD} | CAT1: ${CAT1_COL} | CAT2: ${CAT2_COL}`);
  Logger.info(`Variant join: ${VARIANT_JOIN_TABLE} (${VARIANT_JOIN_VARIANT_COL}, ${VARIANT_JOIN_FV_COL})`);

  // -------- ensure facet (no "position") ----------
  async function ensureFacet(code: string, name: string): Promise<number> {
    const ex: Array<{ id: number }> = await qr.query(`SELECT id::int as id FROM facet WHERE code=$1 LIMIT 1`, [code]);
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

    await qr.query(
      `INSERT INTO facet_channels_channel ("facetId","channelId")
       VALUES ($1,$2) ON CONFLICT DO NOTHING`,
      [facetId, CHANNEL_ID],
    );

    return facetId;
  }

  async function ensureFacetValue(facetId: number, facetCode: string, valueName: string): Promise<number> {
    const code = slugifyCode(valueName);
    if (!code) throw new Error(`FacetValue inválido para "${valueName}"`);

    // skip os 2 que já fizeste
    if (facetCode === 'category1' && code === EXCLUDE_CAT1_CODE) return -1;
    if (facetCode === 'category2' && code === EXCLUDE_CAT2_CODE) return -1;

    const ex: Array<{ id: number }> = await qr.query(
      `SELECT id::int as id FROM facet_value WHERE "facetId"=$1 AND code=$2 LIMIT 1`,
      [facetId, code],
    );
    if (ex.length) return ex[0].id;

    const ins: Array<{ id: number }> = await qr.query(
      `INSERT INTO facet_value (code, "facetId", "createdAt", "updatedAt")
       VALUES ($1,$2,NOW(),NOW()) RETURNING id`,
      [code, facetId],
    );
    const facetValueId = ins[0].id;

    await qr.query(
      `INSERT INTO facet_value_translation ("languageCode", name, "baseId")
       VALUES ($1,$2,$3)`,
      [LANG_CODE, valueName, facetValueId],
    );

    await qr.query(
      `INSERT INTO facet_value_channels_channel ("facetValueId","channelId")
       VALUES ($1,$2) ON CONFLICT DO NOTHING`,
      [facetValueId, CHANNEL_ID],
    );

    return facetValueId;
  }

  // ensure facets
  const facetIdCat1 = await ensureFacet('category1', 'Category 1');
  const facetIdCat2 = await ensureFacet('category2', 'Category 2');

  Logger.info(`Facet category1 id=${facetIdCat1}`);
  Logger.info(`Facet category2 id=${facetIdCat2}`);

  // -------- read excel ----------
  const wb = XLSX.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) throw new Error(`Sheet "${SHEET_NAME}" não existe. Sheets: ${wb.SheetNames.join(', ')}`);

  const rows = XLSX.utils.sheet_to_json<Record<string, any>>(ws, { defval: '' });
  Logger.info(`Linhas no Excel: ${rows.length}`);

  // unique SKUs
  const allSkus = Array.from(new Set(rows.map(r => norm(r[SKU_FIELD])).filter(Boolean)));
  Logger.info(`SKUs únicos no Excel: ${allSkus.length}`);

  // SKU -> (variantId, productId)
  const skuToIds = new Map<string, { variantId: number; productId: number }>();
  for (let i = 0; i < allSkus.length; i += 500) {
    const chunk = allSkus.slice(i, i + 500);
    const res: Array<{ sku: string; variantId: number; productId: number }> = await qr.query(
      `
      SELECT pv.sku as sku, pv.id::int as "variantId", pv."productId"::int as "productId"
      FROM product_variant pv
      WHERE pv.sku = ANY($1)
      `,
      [chunk],
    );
    for (const r of res) skuToIds.set(r.sku, { variantId: r.variantId, productId: r.productId });
  }

  Logger.info(`SKUs encontrados no Vendure: ${skuToIds.size}`);
  Logger.info(`SKUs em falta: ${allSkus.length - skuToIds.size}`);

  // cache facetValueId
  const fvCache = new Map<string, number>(); // facetCode::valueName -> id

  // agregação: productId -> set(fvId), variantId -> set(fvId)
  const productToFvs = new Map<number, Set<number>>();
  const variantToFvs = new Map<number, Set<number>>();

  let processed = 0;
  let missingSkuLines = 0;

  for (const r of rows) {
    processed++;
    const sku = norm(r[SKU_FIELD]);
    const ids = skuToIds.get(sku);
    if (!sku || !ids) {
      missingSkuLines++;
      continue;
    }

    if (!productToFvs.has(ids.productId)) productToFvs.set(ids.productId, new Set());
    if (!variantToFvs.has(ids.variantId)) variantToFvs.set(ids.variantId, new Set());

    const c1 = norm(r[CAT1_COL]);
    const c2 = norm(r[CAT2_COL]);

    if (c1) {
      const key = `category1::${c1}`;
      let fv = fvCache.get(key);
      if (fv === undefined) {
        fv = await ensureFacetValue(facetIdCat1, 'category1', c1);
        fvCache.set(key, fv);
      }
      if (fv !== -1) {
        productToFvs.get(ids.productId)!.add(fv);
        variantToFvs.get(ids.variantId)!.add(fv);
      }
    }

    if (c2) {
      const key = `category2::${c2}`;
      let fv = fvCache.get(key);
      if (fv === undefined) {
        fv = await ensureFacetValue(facetIdCat2, 'category2', c2);
        fvCache.set(key, fv);
      }
      if (fv !== -1) {
        productToFvs.get(ids.productId)!.add(fv);
        variantToFvs.get(ids.variantId)!.add(fv);
      }
    }

    if (processed % 500 === 0) Logger.info(`Processadas: ${processed}`);
  }

  Logger.info(`Processadas total: ${processed}`);
  Logger.info(`Linhas com SKU não encontrado: ${missingSkuLines}`);
  Logger.info(`Products afetados: ${productToFvs.size}`);
  Logger.info(`Variants afetadas: ${variantToFvs.size}`);
  Logger.info(`FacetValues cacheados/criados (inclui os 2 excluídos como -1): ${fvCache.size}`);

  // -------- insert joins idempotente ----------
  Logger.info('A aplicar joins ao PRODUCT...');
  let prodLinks = 0;
  for (const [productId, set] of productToFvs) {
    const fvIds = Array.from(set);
    for (let i = 0; i < fvIds.length; i += 500) {
      const chunk = fvIds.slice(i, i + 500);
      await qr.query(
        `
        INSERT INTO product_facet_values_facet_value ("productId","facetValueId")
        SELECT $1::int, x::int
        FROM unnest($2::int[]) x
        ON CONFLICT DO NOTHING
        `,
        [productId, chunk],
      );
      prodLinks += chunk.length;
    }
  }
  Logger.info(`✅ Product links tentados: ${prodLinks}`);

  Logger.info('A aplicar joins à VARIANT...');
  let varLinks = 0;
  for (const [variantId, set] of variantToFvs) {
    const fvIds = Array.from(set);
    for (let i = 0; i < fvIds.length; i += 500) {
      const chunk = fvIds.slice(i, i + 500);
      await qr.query(
        `
        INSERT INTO "${VARIANT_JOIN_TABLE}" ("${VARIANT_JOIN_VARIANT_COL}","${VARIANT_JOIN_FV_COL}")
        SELECT $1::int, x::int
        FROM unnest($2::int[]) x
        ON CONFLICT DO NOTHING
        `,
        [variantId, chunk],
      );
      varLinks += chunk.length;
    }
  }
  Logger.info(`✅ Variant links tentados: ${varLinks}`);

  Logger.info('🎉 Concluído (Category1+Category2, exceto driver e fia-driver-gear).');
  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
