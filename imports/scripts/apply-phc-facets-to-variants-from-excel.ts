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
  { code: 'phc_cat1', col: CAT1_COL },
  { code: 'phc_cat2', col: CAT2_COL },
  { code: 'phc_cat3', col: CAT3_COL },
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

  // Descobrir join table Variant<->FacetValue
  async function detectVariantFacetJoin() {
    const candidates: Array<{ table_name: string }> = await qr.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema='public'
        AND table_name ILIKE 'product_variant%facet%value%'
    `);

    for (const t of candidates) {
      const cols: Array<{ column_name: string }> = await qr.query(
        `SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=$1`,
        [t.table_name],
      );
      const names = cols.map(c => c.column_name);

      // Vendure costuma usar "productVariantId" + "facetValueId"
      const variantCol = names.includes('productVariantId')
        ? 'productVariantId'
        : names.includes('variantId')
          ? 'variantId'
          : null;

      const facetCol = names.includes('facetValueId')
        ? 'facetValueId'
        : names.includes('facetValuesId')
          ? 'facetValuesId'
          : null;

      if (variantCol && facetCol) {
        return { table: t.table_name, variantCol, facetCol };
      }
    }

    throw new Error(
      `Não encontrei join table Variant<->FacetValue. Candidatas: ${candidates
        .map(x => x.table_name)
        .join(', ')}`,
    );
  }

  const join = await detectVariantFacetJoin();
  Logger.info(
    `Join table Variant<->FacetValue: ${join.table} | variantCol=${join.variantCol} | facetCol=${join.facetCol}`,
  );

  // Cache: facetCode::valueName -> facetValueId
  async function getFacetValueIdByName(facetCode: string, valueName: string): Promise<number> {
    const code = slugifyCode(valueName);
    const res: Array<{ id: number }> = await qr.query(
      `
      SELECT fv.id
      FROM facet_value fv
      JOIN facet f ON f.id = fv."facetId"
      WHERE f.code=$1 AND fv.code=$2
      LIMIT 1
      `,
      [facetCode, code],
    );
    if (!res.length) throw new Error(`FacetValue não existe: ${facetCode}="${valueName}"`);
    return res[0].id;
  }

  // 1) Ler Excel
  const wb = XLSX.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) throw new Error(`Sheet não encontrada. Sheets: ${wb.SheetNames.join(', ')}`);
  const rows = XLSX.utils.sheet_to_json<Record<string, any>>(ws, { defval: '' });
  Logger.info(`Linhas no Excel: ${rows.length}`);

  // 2) Map SKU -> variantId
  const skus = Array.from(new Set(rows.map(r => norm(r[SKU_FIELD])).filter(Boolean)));
  Logger.info(`SKUs únicos no Excel: ${skus.length}`);

  const skuToVariantId = new Map<string, number>();
  for (let i = 0; i < skus.length; i += 500) {
    const chunk = skus.slice(i, i + 500);
    const res: Array<{ sku: string; id: number }> = await qr.query(
      `SELECT sku, id::int FROM product_variant WHERE sku = ANY($1)`,
      [chunk],
    );
    for (const r of res) skuToVariantId.set(r.sku, r.id);
  }

  Logger.info(`SKUs encontrados no Vendure (variants): ${skuToVariantId.size}`);
  Logger.info(`SKUs em falta: ${skus.length - skuToVariantId.size}`);

  // 3) Agregar por variantId -> facetValueIds
  const variantToFacetValueIds = new Map<number, Set<number>>();
  const valueCache = new Map<string, number>();

  let missingSku = 0;
  let processed = 0;

  for (const r of rows) {
    processed++;
    const sku = norm(r[SKU_FIELD]);
    const variantId = skuToVariantId.get(sku);

    if (!sku || !variantId) {
      missingSku++;
      continue;
    }

    if (!variantToFacetValueIds.has(variantId)) variantToFacetValueIds.set(variantId, new Set());

    for (const f of FACETS) {
      const val = norm(r[f.col]);
      if (!val) continue;

      const key = `${f.code}::${val}`;
      let fvId = valueCache.get(key);
      if (!fvId) {
        fvId = await getFacetValueIdByName(f.code, val);
        valueCache.set(key, fvId);
      }
      variantToFacetValueIds.get(variantId)!.add(fvId);
    }

    if (processed % 500 === 0) Logger.info(`Processadas: ${processed}`);
  }

  Logger.info(`Processadas total: ${processed}`);
  Logger.info(`Variants afetadas: ${variantToFacetValueIds.size}`);
  Logger.info(`Linhas com SKU não encontrado: ${missingSku}`);

  // 4) Inserir joins (idempotente)
  Logger.info(`A inserir ligações variant<->facetValue...`);
  let inserts = 0;

  for (const [variantId, set] of variantToFacetValueIds.entries()) {
    const fvIds = Array.from(set);

    for (let i = 0; i < fvIds.length; i += 500) {
      const chunk = fvIds.slice(i, i + 500);

      await qr.query(
        `
        INSERT INTO "${join.table}" ("${join.variantCol}", "${join.facetCol}")
        SELECT $1::int, x::int
        FROM unnest($2::int[]) AS x
        WHERE NOT EXISTS (
          SELECT 1 FROM "${join.table}" j
          WHERE j."${join.variantCol}"=$1::int AND j."${join.facetCol}"=x::int
        )
        `,
        [variantId, chunk],
      );

      inserts += chunk.length;
    }
  }

  Logger.info(`✅ Concluído. Ligações tentadas: ${inserts}`);
  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
