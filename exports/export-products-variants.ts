import { bootstrap, TransactionalConnection, Logger } from '@vendure/core';
import { config as vendureConfig } from '../src/vendure-config';
import * as XLSX from 'xlsx';
import fs from 'fs';
import path from 'path';

const EXPORT_DIR = '/root/carsandvibes/exports';
const EXPORT_FILE = path.join(EXPORT_DIR, 'vendure_products_variants.xlsx');

const LANG_CODE = process.env.LANG_CODE ?? 'pt';

async function main() {
  const config = {
    ...vendureConfig,
    apiOptions: { ...vendureConfig.apiOptions, port: 0 },
  };

  const app = await bootstrap(config);
  const connection = app.get(TransactionalConnection);
  const qr = connection.rawConnection.createQueryRunner();

  Logger.info('📦 A ler products + variants + preços diretamente da BD...');

  // Vendure v3+: preços em product_variant_price (por channel)
  // Vamos buscar 1 preço "principal" por variant (o mais baixo channelId, ou seja, o primeiro).
  const rows = await qr.query(
    `
    WITH price_one AS (
      SELECT DISTINCT ON (pvp."variantId")
        pvp."variantId"::text              AS "variantId",
        pvp."price" / 100.0                AS "price",
        pvp."currencyCode"                 AS "currencyCode",
        pvp."channelId"::text              AS "channelId"
      FROM product_variant_price pvp
      ORDER BY pvp."variantId", pvp."channelId" ASC
    )
    SELECT
      p.id::text                          AS "productId",
      pt.name                             AS "productName",
      pt.slug                             AS "productSlug",
      p.enabled                           AS "productEnabled",
      pv.id::text                         AS "variantId",
      pv.sku                              AS "sku",
      pvt.name                            AS "variantName",
      po.price                            AS "price",
      po."currencyCode"                   AS "currencyCode",
      po."channelId"                      AS "channelId",
      pv.enabled                          AS "variantEnabled",
      pv."createdAt"                      AS "createdAt",
      pv."updatedAt"                      AS "updatedAt"
    FROM product p
    JOIN product_translation pt
      ON pt."baseId" = p.id AND pt."languageCode" = $1
    JOIN product_variant pv
      ON pv."productId" = p.id
    JOIN product_variant_translation pvt
      ON pvt."baseId" = pv.id AND pvt."languageCode" = $1
    LEFT JOIN price_one po
      ON po."variantId" = pv.id::text
    ORDER BY p.id::int, pv.id::int
    `,
    [LANG_CODE],
  );

  Logger.info(`🔢 Registos encontrados: ${rows.length}`);

  if (!fs.existsSync(EXPORT_DIR)) fs.mkdirSync(EXPORT_DIR, { recursive: true });

  const worksheet = XLSX.utils.json_to_sheet(rows);
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, 'products_variants');

  XLSX.writeFile(workbook, EXPORT_FILE);

  Logger.info(`✅ Excel criado em: ${EXPORT_FILE}`);

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
