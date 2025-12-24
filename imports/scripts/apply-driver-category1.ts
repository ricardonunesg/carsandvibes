import 'dotenv/config';
import XLSX from 'xlsx';
import { bootstrap, Logger, TransactionalConnection } from '@vendure/core';
import { config as vendureConfig } from '../../src/vendure-config';

const EXCEL_PATH =
  process.env.EXCEL_PATH ||
  '/root/carsandvibes/imports/source/PHC_categories_with_SKU_from_file1.xlsx';

const SHEET_NAME = process.env.SHEET_NAME || 'PHC_WITH_SKU';

const SKU_FIELD = process.env.SKU_FIELD || 'SKU_FROM_FILE1';
const CAT1_FIELD = process.env.CAT1_FIELD || 'Categoryn1';
const CAT1_VALUE = (process.env.CAT1_VALUE || 'DRIVER').trim().toUpperCase();

const FACET_CODE = process.env.FACET_CODE || 'category1';
const FACET_VALUE_CODE = process.env.FACET_VALUE_CODE || 'driver';

// join table variante<->facetValue (podes forçar por env se quiseres)
const VARIANT_JOIN_TABLE =
  process.env.VARIANT_JOIN_TABLE || 'product_variant_facet_values_facet_value';
const VARIANT_JOIN_VARIANT_COL =
  process.env.VARIANT_JOIN_VARIANT_COL || 'productVariantId';
const VARIANT_JOIN_FV_COL =
  process.env.VARIANT_JOIN_FV_COL || 'facetValueId';

function norm(v: unknown) {
  return String(v ?? '').trim();
}

async function main() {
  // Port 0 para não bater com o PM2
  const config = { ...vendureConfig, apiOptions: { ...vendureConfig.apiOptions, port: 0 } };
  const app = await bootstrap(config);

  const connection = app.get(TransactionalConnection);
  const qr = connection.rawConnection.createQueryRunner();

  Logger.info('=============================================');
  Logger.info('A aplicar facet category1=driver via Excel');
  Logger.info(`Excel: ${EXCEL_PATH}`);
  Logger.info(`Sheet: ${SHEET_NAME}`);
  Logger.info(`Match: ${CAT1_FIELD} == ${CAT1_VALUE}`);
  Logger.info(`Facet: ${FACET_CODE} | FacetValue code: ${FACET_VALUE_CODE}`);

  // 1) buscar facetValueId (por code)
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
  Logger.info(`FacetValueId encontrado: ${facetValueId}`);

  // 2) ler excel
  const wb = XLSX.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) throw new Error(`Sheet "${SHEET_NAME}" não encontrada. Sheets: ${wb.SheetNames.join(', ')}`);

  const rows = XLSX.utils.sheet_to_json<Record<string, any>>(ws, { defval: '' });
  Logger.info(`Linhas no Excel: ${rows.length}`);

  // 3) skus com Categoryn1=DRIVER
  const skus = Array.from(
    new Set(
      rows
        .filter(r => norm(r[CAT1_FIELD]).toUpperCase() === CAT1_VALUE)
        .map(r => norm(r[SKU_FIELD]))
        .filter(Boolean),
    ),
  );
  Logger.info(`SKUs únicos com ${CAT1_FIELD}=${CAT1_VALUE}: ${skus.length}`);

  if (!skus.length) {
    Logger.info('Nada para fazer.');
    await app.close();
    return;
  }

  // 4) sku -> variantId, productId
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
  Logger.info(`SKUs não encontrados no Vendure: ${skus.length - found.length} (aprox)`);

  // 5) aplicar ao PRODUCT (idempotente)
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

  // 6) aplicar à VARIANT (idempotente)
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

  Logger.info('🎉 Concluído.');

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
