import 'dotenv/config';
import * as xlsx from 'xlsx';
import { bootstrap, TransactionalConnection, Logger, LanguageCode } from '@vendure/core';
import { config } from '../../src/vendure-config';

type Row = Record<string, any>;

const EXCEL_PATH = process.env.EXCEL_PATH!;
const SHEET_NAME = process.env.SHEET_NAME ?? 'PHC_WITH_SKU';
const LANG = (process.env.LANG_CODE ?? 'pt') as LanguageCode;

const SKU_COL = process.env.SKU_COL ?? 'SKU_FROM_FILE1';
const C1 = process.env.CAT1_COL ?? 'Categoryn1';
const C2 = process.env.CAT2_COL ?? 'Categoryn2';
const C3 = process.env.CAT3_COL ?? 'Categoryn3';
const C4 = process.env.CAT4_COL ?? 'Categoryn4';

function clean(s: any) {
  return String(s ?? '').trim();
}
function slugify(s: string) {
  return s
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/[^a-z0-9\-]/g, '')
    .replace(/\-+/g, '-')
    .replace(/^\-|\-$/g, '');
}
function keyPath(parts: string[]) {
  return parts.map(p => clean(p)).filter(Boolean).join(' | ');
}

// cria código único por “path” (para evitar colisões de nomes repetidos)
function facetValueCodeFromPath(path: string) {
  const code = slugify(path);
  return code.length ? code : 'x';
}

async function main() {
  if (!EXCEL_PATH) throw new Error('Falta EXCEL_PATH');

  // evitar conflito com pm2
  process.env.PORT = '0';

  const app = await bootstrap({
    ...config,
    apiOptions: { ...config.apiOptions, port: 0 },
  });

  const connection = app.get(TransactionalConnection);
  const qr = connection.rawConnection.createQueryRunner();

  Logger.info(`A ler Excel: ${EXCEL_PATH} (sheet: ${SHEET_NAME})`);
  const wb = xlsx.readFile(EXCEL_PATH);
  const ws = wb.Sheets[SHEET_NAME];
  if (!ws) throw new Error(`Sheet não existe: ${SHEET_NAME}`);
  const rows = xlsx.utils.sheet_to_json<Row>(ws, { defval: '' });
  Logger.info(`Linhas no Excel: ${rows.length}`);

  // 1) garantir facets cat1..cat4 (por SQL)
  const facetCodes = ['cat1', 'cat2', 'cat3', 'cat4'] as const;

  const facetIdByCode = new Map<string, number>();

  for (const fc of facetCodes) {
    // facet
    const f = await qr.query(`SELECT id FROM facet WHERE code = $1 LIMIT 1`, [fc]);
    let facetId: number;

    if (f.length) {
      facetId = Number(f[0].id);
    } else {
      const ins = await qr.query(`INSERT INTO facet(code, "isPrivate", "createdAt", "updatedAt")
                                  VALUES($1, false, now(), now())
                                  RETURNING id`, [fc]);
      facetId = Number(ins[0].id);

      await qr.query(
        `INSERT INTO facet_translation("languageCode","name","createdAt","updatedAt","baseId")
         VALUES($1,$2,now(),now(),$3)`,
        [LANG, fc, facetId],
      );
    }
    facetIdByCode.set(fc, facetId);
  }

  Logger.info(`Facets garantidas: ${Array.from(facetIdByCode.entries()).map(([c,id])=>`${c}=${id}`).join(', ')}`);

  // 2) recolher valores únicos por nível (usamos PATH para unicidade)
  const valuesByFacet = new Map<string, Map<string, { name: string; code: string }>>();
  for (const fc of facetCodes) valuesByFacet.set(fc, new Map());

  for (const r of rows) {
    const a = clean(r[C1]);
    const b = clean(r[C2]);
    const c = clean(r[C3]);
    const d = clean(r[C4]);

    if (a) {
      const path = keyPath([a]);
      valuesByFacet.get('cat1')!.set(path, { name: a, code: facetValueCodeFromPath(path) });
    }
    if (a && b) {
      const path = keyPath([a, b]);
      valuesByFacet.get('cat2')!.set(path, { name: b, code: facetValueCodeFromPath(path) });
    }
    if (a && b && c) {
      const path = keyPath([a, b, c]);
      valuesByFacet.get('cat3')!.set(path, { name: c, code: facetValueCodeFromPath(path) });
    }
    if (a && b && c && d) {
      const path = keyPath([a, b, c, d]);
      valuesByFacet.get('cat4')!.set(path, { name: d, code: facetValueCodeFromPath(path) });
    }
  }

  Logger.info(
    `Valores únicos: cat1=${valuesByFacet.get('cat1')!.size}, cat2=${valuesByFacet.get('cat2')!.size}, cat3=${valuesByFacet.get('cat3')!.size}, cat4=${valuesByFacet.get('cat4')!.size}`,
  );

  // 3) criar facet_values + translations (por SQL) e guardar id por (facetCode + path)
  const facetValueIdByFacetPath = new Map<string, number>();

  for (const fc of facetCodes) {
    const facetId = facetIdByCode.get(fc)!;
    const map = valuesByFacet.get(fc)!;

    let created = 0;

    for (const [pathKey, v] of map.entries()) {
      // existe?
      const ex = await qr.query(
        `SELECT fv.id
         FROM facet_value fv
         WHERE fv.code = $1 AND fv."facetId" = $2
         LIMIT 1`,
        [v.code, facetId],
      );

      let fvId: number;
      if (ex.length) {
        fvId = Number(ex[0].id);
      } else {
        const ins = await qr.query(
          `INSERT INTO facet_value(code, "createdAt","updatedAt","facetId")
           VALUES($1, now(), now(), $2)
           RETURNING id`,
          [v.code, facetId],
        );
        fvId = Number(ins[0].id);

        await qr.query(
          `INSERT INTO facet_value_translation("languageCode","name","createdAt","updatedAt","baseId")
           VALUES($1,$2,now(),now(),$3)`,
          [LANG, v.name, fvId],
        );

        created++;
      }

      facetValueIdByFacetPath.set(`${fc}::${pathKey}`, fvId);
    }

    Logger.info(`FacetValues ${fc}: criados agora=${created} (total esperado=${map.size})`);
  }

  // 4) mapear SKU -> variantId + productId (por SQL)
  const skuSet = new Set<string>();
  for (const r of rows) {
    const sku = clean(r[SKU_COL]);
    if (sku) skuSet.add(sku);
  }
  const skus = Array.from(skuSet);
  Logger.info(`SKUs únicos no Excel: ${skus.length}`);

  const skuToIds = new Map<string, { variantId: number; productId: number }>();

  for (let i = 0; i < skus.length; i += 1000) {
    const batch = skus.slice(i, i + 1000);
    const found = await qr.query(
      `SELECT pv.sku, pv.id as "variantId", pv."productId" as "productId"
       FROM product_variant pv
       WHERE pv.sku = ANY($1)`,
      [batch],
    );
    for (const f of found) {
      skuToIds.set(String(f.sku), { variantId: Number(f.variantId), productId: Number(f.productId) });
    }
  }

  Logger.info(`SKUs encontrados como variants: ${skuToIds.size}`);

  // 5) construir relações product/variant -> facetValueIds e inserir em join tables
  // Tabelas join default:
  // product_facet_values_facet_value
  // product_variant_facet_values_facet_value
  const prodJoin = 'product_facet_values_facet_value';
  const varJoin = 'product_variant_facet_values_facet_value';

  // limpa apenas estes 4 facets (opcional, mas deixa o sistema limpo)
  // (remover tudo pode apagar outras facets tipo brand, por isso filtramos por facetId)
  const facetIds = Array.from(facetIdByCode.values());
  await qr.query(
    `DELETE FROM "${prodJoin}"
     WHERE "facetValueId" IN (SELECT id FROM facet_value WHERE "facetId" = ANY($1))`,
    [facetIds],
  );
  await qr.query(
    `DELETE FROM "${varJoin}"
     WHERE "facetValueId" IN (SELECT id FROM facet_value WHERE "facetId" = ANY($1))`,
    [facetIds],
  );
  Logger.info('Limpei relações antigas cat1..cat4 (product/variant facet joins).');

  let missingSku = 0;
  let insertedProd = 0;
  let insertedVar = 0;

  for (const r of rows) {
    const sku = clean(r[SKU_COL]);
    if (!sku) continue;

    const ids = skuToIds.get(sku);
    if (!ids) {
      missingSku++;
      continue;
    }

    const a = clean(r[C1]);
    const b = clean(r[C2]);
    const c = clean(r[C3]);
    const d = clean(r[C4]);

    const toInsert: number[] = [];

    if (a) {
      const k = keyPath([a]);
      toInsert.push(facetValueIdByFacetPath.get(`cat1::${k}`)!);
    }
    if (a && b) {
      const k = keyPath([a, b]);
      toInsert.push(facetValueIdByFacetPath.get(`cat2::${k}`)!);
    }
    if (a && b && c) {
      const k = keyPath([a, b, c]);
      toInsert.push(facetValueIdByFacetPath.get(`cat3::${k}`)!);
    }
    if (a && b && c && d) {
      const k = keyPath([a, b, c, d]);
      toInsert.push(facetValueIdByFacetPath.get(`cat4::${k}`)!);
    }

    // inserir product joins
    for (const fvId of toInsert) {
      await qr.query(
        `INSERT INTO "${prodJoin}"("productId","facetValueId")
         VALUES($1,$2) ON CONFLICT DO NOTHING`,
        [ids.productId, fvId],
      );
      insertedProd++;
      await qr.query(
        `INSERT INTO "${varJoin}"("productVariantId","facetValueId")
         VALUES($1,$2) ON CONFLICT DO NOTHING`,
        [ids.variantId, fvId],
      );
      insertedVar++;
    }
  }

  Logger.info(`Sem SKU match: ${missingSku}`);
  Logger.info(`✅ Inserções (tentativas) product facet join: ${insertedProd}`);
  Logger.info(`✅ Inserções (tentativas) variant facet join: ${insertedVar}`);

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
