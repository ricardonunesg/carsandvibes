import { bootstrap, Logger, TransactionalConnection } from '@vendure/core';
import { config as vendureConfig } from '../../src/vendure-config';
import * as XLSX from 'xlsx';

/**
 * ENV obrigatórias:
 *  EXCEL_PATH
 *  SHEET_NAME
 *  SKU_FIELD        (ex: SKU_FROM_FILE1)
 *  CAT_FIELD        (ex: Categoryn2)
 *  CAT_VALUE        (ex: FIA DRIVER GEAR)  <-- NOVO
 *  FACET_VALUE_ID   (ex: 222)
 */

const EXCEL_PATH = process.env.EXCEL_PATH!;
const SHEET_NAME = process.env.SHEET_NAME!;
const SKU_FIELD = process.env.SKU_FIELD!;
const CAT_FIELD = process.env.CAT_FIELD!;
const CAT_VALUE = (process.env.CAT_VALUE ?? '').trim();
const FACET_VALUE_ID = Number(process.env.FACET_VALUE_ID);

function norm(v: unknown) {
  return String(v ?? '').trim();
}

async function main() {
  if (!EXCEL_PATH || !SHEET_NAME || !SKU_FIELD || !CAT_FIELD || !CAT_VALUE || !FACET_VALUE_ID) {
    throw new Error(
      'Faltam ENV. Precisas de EXCEL_PATH, SHEET_NAME, SKU_FIELD, CAT_FIELD, CAT_VALUE e FACET_VALUE_ID',
    );
  }

  Logger.info(`A aplicar facetValueId=${FACET_VALUE_ID} para ${CAT_FIELD} == "${CAT_VALUE}"`);

  const app = await bootstrap({
    ...vendureConfig,
    apiOptions: { ...vendureConfig.apiOptions, port: 0 },
  });

  const qr = app.get(TransactionalConnection).rawConnection.createQueryRunner();

  // Ler Excel
  const wb = XLSX.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) throw new Error(`Sheet não encontrada. Sheets: ${wb.SheetNames.join(', ')}`);

  const rows = XLSX.utils.sheet_to_json<Record<string, any>>(ws, { defval: '' });
  Logger.info(`Linhas no Excel: ${rows.length}`);

  // Extrair SKUs únicos apenas das linhas onde CAT_FIELD == CAT_VALUE
  const skus = Array.from(
    new Set(
      rows
        .filter(r => norm(r[CAT_FIELD]) === CAT_VALUE)
        .map(r => norm(r[SKU_FIELD]))
        .filter(Boolean),
    ),
  );

  Logger.info(`SKUs únicos no Excel com ${CAT_FIELD}="${CAT_VALUE}": ${skus.length}`);

  // Map SKU -> productId
  const productIds = new Set<number>();

  for (let i = 0; i < skus.length; i += 500) {
    const chunk = skus.slice(i, i + 500);

    const res: Array<{ productId: number }> = await qr.query(
      `
      SELECT DISTINCT pv."productId"::int AS "productId"
      FROM product_variant pv
      WHERE pv.sku = ANY($1)
      `,
      [chunk],
    );

    for (const r of res) productIds.add(r.productId);
  }

  Logger.info(`Products únicos encontrados no Vendure: ${productIds.size}`);

  // Aplicar join idempotente
  let total = 0;
  const productIdArray = Array.from(productIds);

  for (let i = 0; i < productIdArray.length; i += 500) {
    const chunk = productIdArray.slice(i, i + 500);

    await qr.query(
      `
      INSERT INTO product_facet_values_facet_value ("productId", "facetValueId")
      SELECT x::int, $2::int
      FROM unnest($1::int[]) AS x
      WHERE NOT EXISTS (
        SELECT 1
        FROM product_facet_values_facet_value j
        WHERE j."productId" = x::int
          AND j."facetValueId" = $2::int
      )
      `,
      [chunk, FACET_VALUE_ID],
    );

    total += chunk.length;
  }

  Logger.info(`✅ FacetValue aplicado (tentativas): ${total}`);

  await app.close();
  Logger.info('🎉 Script terminado com sucesso');
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
