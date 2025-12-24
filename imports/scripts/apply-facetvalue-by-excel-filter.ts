import 'dotenv/config';
import XLSX from 'xlsx';
import { bootstrap, Logger, TransactionalConnection } from '@vendure/core';
import { config as vendureConfig } from '../../src/vendure-config';

const EXCEL_PATH = process.env.EXCEL_PATH!;
const SHEET_NAME = process.env.SHEET_NAME!;

// Excel
const SKU_FIELD = process.env.SKU_FIELD || 'SKU_FROM_FILE1';
const FILTER_COL = process.env.FILTER_COL!;        // ex: Categoryn1
const FILTER_VALUE = (process.env.FILTER_VALUE || '').trim(); // ex: DRIVER
const MATCH_MODE = (process.env.MATCH_MODE || 'exact').toLowerCase(); // exact | iexact

// Facet / Value
const FACET_CODE = process.env.FACET_CODE!;             // ex: category1
const FACET_VALUE_CODE = process.env.FACET_VALUE_CODE!; // ex: driver

// Onde aplicar
const APPLY_PRODUCTS = (process.env.APPLY_PRODUCTS ?? 'true').toLowerCase() === 'true';
const APPLY_VARIANTS = (process.env.APPLY_VARIANTS ?? 'true').toLowerCase() === 'true';

// Join variant<->facetValue (podes ajustar por env se necessário)
const VARIANT_JOIN_TABLE =
  process.env.VARIANT_JOIN_TABLE || 'product_variant_facet_values_facet_value';
const VARIANT_JOIN_VARIANT_COL =
  process.env.VARIANT_JOIN_VARIANT_COL || 'productVariantId';
const VARIANT_JOIN_FV_COL =
  process.env.VARIANT_JOIN_FV_COL || 'facetValueId';

function norm(v: unknown) {
  return String(v ?? '').trim();
}

function equalsByMode(a: string, b: string) {
  if (MATCH_MODE === 'iexact') return a.trim().toUpperCase() === b.trim().toUpperCase();
  return a.trim() === b.trim();
}

async function main() {
  if (!EXCEL_PATH || !SHEET_NAME) throw new Error('Falta EXCEL_PATH e/ou SHEET_NAME');
  if (!FILTER_COL) throw new Error('Falta FILTER_COL');
  if (!FILTER_VALUE) throw new Error('Falta FILTER_VALUE');
  if (!FACET_CODE || !FACET_VALUE_CODE) throw new Error('Falta FACET_CODE e/ou FACET_VALUE_CODE');

  // Boot sem porta para não bater com PM2
  const config = { ...vendureConfig, apiOptions: { ...vendureConfig.apiOptions, port: 0 } };
  const app = await bootstrap(config);

  const connection = app.get(TransactionalConnection);
  const qr = connection.rawConnection.createQueryRunner();

  Logger.info('=============================================');
  Logger.info('apply-facetvalue-by-excel-filter');
  Logger.info(`Excel: ${EXCEL_PATH}`);
  Logger.info(`Sheet: ${SHEET_NAME}`);
  Logger.info(`Filtro: ${FILTER_COL} ${MATCH_MODE === 'iexact' ? '(case-insensitive)' : ''} == "${FILTER_VALUE}"`);
  Logger.info(`SKU_FIELD: ${SKU_FIELD}`);
  Logger.info(`Facet: ${FACET_CODE} | FacetValue: ${FACET_VALUE_CODE}`);
  Logger.info(`Apply: products=${APPLY_PRODUCTS} | variants=${APPLY_VARIANTS}`);

  // 1) buscar facetValueId
  const fvRes: Array<{ id: number }> = await qr.query(
    `
    SELECT fv.id::int as id
    FROM facet_value fv
    JOIN facet f ON f.id = fv."facetId"
    WHERE f.code = $1
      AND fv.code = $2
    LIMIT 1
    `,
    [FACET_CODE, FACET_VALUE_CODE],
  );

  if (!fvRes.length) {
    throw new Error(`Não encontrei facetValue: facet=${FACET_CODE} valueCode=${FACET_VALUE_CODE}`);
  }
  const facetValueId = fvRes[0].id;
  Logger.info(`FacetValueId: ${facetValueId}`);

  // 2) ler excel
  const wb = XLSX.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) throw new Error(`Sheet "${SHEET_NAME}" não encontrada. Sheets: ${wb.SheetNames.join(', ')}`);

  const rows = XLSX.utils.sheet_to_json<Record<string, any>>(ws, { defval: '' });
  Logger.info(`Linhas no Excel: ${rows.length}`);

  // 3) SKUs filtrados
  const skus = Array.from(
    new Set(
      rows
        .filter(r => equalsByMode(norm(r[FILTER_COL]), FILTER_VALUE))
        .map(r => norm(r[SKU_FIELD]))
        .filter(Boolean),
    ),
  );

  Logger.info(`SKUs únicos que passam no filtro: ${skus.length}`);

  if (!skus.length) {
    Logger.info('Nada para fazer.');
    await app.close();
    return;
  }

  // 4) sku -> variantId/productId
  const found: Array<{ variantId: number; productId: number }> = [];
  for (let i = 0; i < skus.length; i += 500) {
    const chunk = skus.slice(i, i + 500);
    const res: Array<{ variantId: number; productId: number }> = await qr.query(
      `
      SELECT pv.id::int as "variantId", pv."productId"::int as "productId"
      FROM product_variant pv
      WHERE pv.sku = ANY($1)
      `,
      [chunk],
    );
    found.push(...res);
  }

  const variantIds = Array.from(new Set(found.map(x => x.variantId)));
  const productIds = Array.from(new Set(found.map(x => x.productId)));

  Logger.info(`Variants encontradas no Vendure: ${variantIds.length}`);
  Logger.info(`Products únicos encontrados: ${productIds.length}`);

  // 5) aplicar ao PRODUCT
  if (APPLY_PRODUCTS) {
    Logger.info('A aplicar ao PRODUCT...');
    let prodOps = 0;
    for (let i = 0; i < productIds.length; i += 500) {
      const chunk = productIds.slice(i, i + 500);
      await qr.query(
        `
        INSERT INTO product_facet_values_facet_value ("productId","facetValueId")
        SELECT x::int, $2::int
        FROM unnest($1::int[]) x
        ON CONFLICT DO NOTHING
        `,
        [chunk, facetValueId],
      );
      prodOps += chunk.length;
    }
    Logger.info(`✅ Products tentados: ${prodOps}`);
  }

  // 6) aplicar à VARIANT
  if (APPLY_VARIANTS) {
    Logger.info(`A aplicar à VARIANT (join ${VARIANT_JOIN_TABLE})...`);
    let varOps = 0;
    for (let i = 0; i < variantIds.length; i += 500) {
      const chunk = variantIds.slice(i, i + 500);
      await qr.query(
        `
        INSERT INTO "${VARIANT_JOIN_TABLE}" ("${VARIANT_JOIN_VARIANT_COL}", "${VARIANT_JOIN_FV_COL}")
        SELECT x::int, $2::int
        FROM unnest($1::int[]) x
        ON CONFLICT DO NOTHING
        `,
        [chunk, facetValueId],
      );
      varOps += chunk.length;
    }
    Logger.info(`✅ Variants tentadas: ${varOps}`);
  }

  Logger.info('🎉 Concluído.');
  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
