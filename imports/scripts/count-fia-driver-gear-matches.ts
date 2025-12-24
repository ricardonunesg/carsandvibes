import { bootstrap, TransactionalConnection } from '@vendure/core';
import { config as vendureConfig } from '../../src/vendure-config';
import * as XLSX from 'xlsx';

const EXCEL_PATH = process.env.EXCEL_PATH!;
const SHEET_NAME = process.env.SHEET_NAME!;
const SKU_FIELD = process.env.SKU_FIELD ?? 'SKU_FROM_FILE1';
const CAT2_FIELD = process.env.CAT2_FIELD ?? 'Categoryn2';

async function main() {
  const app = await bootstrap({
    ...vendureConfig,
    apiOptions: { ...vendureConfig.apiOptions, port: 0 },
  });

  const qr = app.get(TransactionalConnection).rawConnection.createQueryRunner();

  const wb = XLSX.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) throw new Error(`Sheet não existe. Sheets: ${wb.SheetNames.join(', ')}`);

  const rows = XLSX.utils.sheet_to_json<Record<string, any>>(ws, { defval: '' });

  const skus = Array.from(
    new Set(
      rows
        .filter(r => String(r[CAT2_FIELD] ?? '').trim() === 'FIA DRIVER GEAR')
        .map(r => String(r[SKU_FIELD] ?? '').trim())
        .filter(Boolean),
    ),
  );

  console.log('SKUs únicos FIA DRIVER GEAR no Excel:', skus.length);

  const foundSkus = new Set<string>();
  const productIds = new Set<number>();

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
    for (const r of res) {
      foundSkus.add(r.sku);
      productIds.add(r.productId);
    }
  }

  console.log('SKUs desses que existem no Vendure:', foundSkus.size);
  console.log('Products únicos correspondentes:', productIds.size);

  await app.close();
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
