import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import {
  bootstrapWorker,
  Logger,
  RequestContextService,
  AssetService,
  TransactionalConnection,
} from '@vendure/core';
import { config } from '../../src/vendure-config';

type CsvRow = {
  variant_sku: string;
  image_path: string;
  position?: string | number;
  is_cover?: string | number;
};

const CSV_PATH =
  process.env.CSV_PATH ||
  '/root/carsandvibes/imports/source/vendure_variant_images_mapping.csv';
const CSV_DELIMITER = process.env.CSV_DELIMITER || ';';

const MAX_SKUS = Number(process.env.MAX_SKUS || 0); // 0 = sem limite
const LOG_EVERY = Number(process.env.LOG_EVERY || 20);

// a tua tabela real
const VARIANT_ASSET_TABLE = 'product_variant_asset';

// se quiseres não apagar as ligações antigas (default = true)
const REPLACE_EXISTING =
  (process.env.REPLACE_EXISTING ?? 'true').toLowerCase() === 'true';

function norm(v: unknown) {
  return String(v ?? '').trim();
}
function toInt(v: unknown, fallback = 0) {
  const n = Number(norm(v));
  return Number.isFinite(n) ? n : fallback;
}
function guessMimeType(filePath: string) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.png') return 'image/png';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.gif') return 'image/gif';
  return 'application/octet-stream';
}

function parseCsvFile(filePath: string, delimiter: string): CsvRow[] {
  const raw = fs.readFileSync(filePath, 'utf-8');
  const lines = raw.split(/\r?\n/).filter(l => l.trim().length > 0);
  if (lines.length < 2) return [];

  const headers = lines[0].split(delimiter).map(h => h.trim());
  const out: CsvRow[] = [];

  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(delimiter);
    const obj: any = {};
    for (let c = 0; c < headers.length; c++) obj[headers[c]] = parts[c] ?? '';
    out.push(obj as CsvRow);
  }
  return out;
}

async function main() {
  if (!fs.existsSync(CSV_PATH)) throw new Error(`CSV não existe: ${CSV_PATH}`);

  const { app } = await bootstrapWorker(config);
  const ctx = await app.get(RequestContextService).create({
    apiType: 'admin',
    languageCode: 'pt',
  });

  const assetService = app.get(AssetService);
  const connection = app.get(TransactionalConnection);

  Logger.info('=============================================');
  Logger.info('Import images -> Vendure (variants) from Prestashop CSV (v4)');
  Logger.info(`CSV: ${CSV_PATH} (delimiter: "${CSV_DELIMITER}")`);
  Logger.info(`MAX_SKUS=${MAX_SKUS || 'no-limit'} | LOG_EVERY=${LOG_EVERY}`);
  Logger.info(`Variant Asset table: ${VARIANT_ASSET_TABLE}`);
  Logger.info(`REPLACE_EXISTING=${REPLACE_EXISTING}`);

  const rows = parseCsvFile(CSV_PATH, CSV_DELIMITER);
  Logger.info(`Linhas no CSV: ${rows.length}`);

  // agrupar por SKU
  const bySku = new Map<string, CsvRow[]>();
  for (const r of rows) {
    const sku = norm((r as any).variant_sku);
    const img = norm((r as any).image_path);
    if (!sku || !img) continue;
    if (!bySku.has(sku)) bySku.set(sku, []);
    bySku.get(sku)!.push(r);
  }
  Logger.info(`SKUs únicos no CSV: ${bySku.size}`);

  const skuList = Array.from(bySku.keys());
  const finalSkus = MAX_SKUS > 0 ? skuList.slice(0, MAX_SKUS) : skuList;

  // cache para não criar o mesmo asset várias vezes
  const assetIdByPath = new Map<string, number>();

  async function findVariantIdBySku(sku: string): Promise<number | null> {
    const res: Array<{ id: number }> = await connection.rawConnection.query(
      `SELECT id::int as id FROM product_variant WHERE sku=$1 LIMIT 1`,
      [sku],
    );
    return res.length ? res[0].id : null;
  }

  async function deleteExistingVariantAssets(variantId: number) {
    await connection.rawConnection.query(
      `DELETE FROM ${VARIANT_ASSET_TABLE} WHERE "productVariantId"=$1`,
      [variantId],
    );
  }

  async function insertVariantAssetIfMissing(
    variantId: number,
    assetId: number,
    position: number,
  ) {
    // Sem UNIQUE -> usamos WHERE NOT EXISTS
    await connection.rawConnection.query(
      `
      INSERT INTO ${VARIANT_ASSET_TABLE}
        ("productVariantId","assetId","position","createdAt","updatedAt")
      SELECT $1::int, $2::int, $3::int, NOW(), NOW()
      WHERE NOT EXISTS (
        SELECT 1 FROM ${VARIANT_ASSET_TABLE}
        WHERE "productVariantId"=$1::int AND "assetId"=$2::int
      )
      `,
      [variantId, assetId, position],
    );
  }

  let skuProcessed = 0;
  let skuMissing = 0;
  let assetsCreated = 0;
  let variantsUpdated = 0;
  let imagesMissingOnDisk = 0;
  let imagesTotal = 0;
  let linksAttempted = 0;

  for (const sku of finalSkus) {
    skuProcessed++;

    const list = bySku.get(sku)!;
    const ordered = [...list].sort(
      (a, b) => toInt((a as any).position, 9999) - toInt((b as any).position, 9999),
    );
    const coverRow =
      ordered.find(r => toInt((r as any).is_cover, 0) === 1) ?? ordered[0];

    const variantId = await findVariantIdBySku(sku);
    if (!variantId) {
      skuMissing++;
      continue;
    }

    const assetIdsInOrder: number[] = [];
    let featuredAssetId: number | undefined;

    for (const r of ordered) {
      const imgPath = norm((r as any).image_path);
      if (!imgPath) continue;

      imagesTotal++;

      if (!fs.existsSync(imgPath)) {
        imagesMissingOnDisk++;
        continue;
      }

      let assetId = assetIdByPath.get(imgPath);

      if (!assetId) {
        const filename = path.basename(imgPath);
        const mimetype = guessMimeType(imgPath);

        const created = await assetService.create(ctx, {
          file: {
            filename,
            mimetype,
            encoding: '7bit',
            createReadStream: () => fs.createReadStream(imgPath),
          } as any,
        });

        if ((created as any).id) {
          assetId = Number((created as any).id);
          assetIdByPath.set(imgPath, assetId);
          assetsCreated++;
        } else {
          continue;
        }
      }

      assetIdsInOrder.push(assetId);
      if (r === coverRow) featuredAssetId = assetId;
    }

    if (!assetIdsInOrder.length) continue;

    // opcional: limpar para ficar exatamente igual ao Prestashop
    if (REPLACE_EXISTING) {
      await deleteExistingVariantAssets(variantId);
    }

    // inserir ligações com position 1..n
    let pos = 0;
    for (const aid of assetIdsInOrder) {
      pos++;
      await insertVariantAssetIfMissing(variantId, aid, pos);
      linksAttempted++;
    }

    // set featuredAssetId na product_variant
    if (featuredAssetId) {
      await connection.rawConnection.query(
        `UPDATE product_variant SET "featuredAssetId"=$2 WHERE id=$1`,
        [variantId, featuredAssetId],
      );
    }

    variantsUpdated++;

    if (skuProcessed % LOG_EVERY === 0) {
      Logger.info(
        `Progress: ${skuProcessed}/${finalSkus.length} | missingSKU=${skuMissing} | variantsUpdated=${variantsUpdated} | assetsCreated=${assetsCreated} | imgMissingOnDisk=${imagesMissingOnDisk}/${imagesTotal} | links=${linksAttempted}`,
      );
    }
  }

  Logger.info('---------------------------------------------');
  Logger.info(`SKUs processados: ${skuProcessed}`);
  Logger.info(`SKUs sem match no Vendure: ${skuMissing}`);
  Logger.info(`Assets criados: ${assetsCreated}`);
  Logger.info(`Variants atualizadas: ${variantsUpdated}`);
  Logger.info(`Links variant<->asset (tentativas): ${linksAttempted}`);
  Logger.info(`Imagens inexistentes no disco: ${imagesMissingOnDisk} / ${imagesTotal}`);
  Logger.info('🎉 Concluído.');

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
