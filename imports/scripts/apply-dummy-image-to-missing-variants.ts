import 'dotenv/config';
import { bootstrapWorker, Logger, TransactionalConnection } from '@vendure/core';
import { AssetService } from '@vendure/core';
import path from 'path';
import fs from 'fs';
import { config } from '../../src/vendure-config';

const DUMMY_PATH =
  process.env.DUMMY_PATH ||
  '/root/carsandvibes/imports/assets/dummy.jpg';

const LOG_EVERY = Number(process.env.LOG_EVERY || 500);

async function main() {
  if (!fs.existsSync(DUMMY_PATH)) {
    throw new Error(`Dummy image não encontrada em ${DUMMY_PATH}`);
  }

  const { app } = await bootstrapWorker(config);
  const connection = app.get(TransactionalConnection);
  const assetService = app.get(AssetService);

  Logger.info('=============================================');
  Logger.info('Aplicar imagem dummy a variants sem imagem');
  Logger.info(`Dummy: ${DUMMY_PATH}`);

  // 1) criar (ou reutilizar) o asset dummy
  const dummyFile = fs.readFileSync(DUMMY_PATH);
  const dummyAsset = await assetService.createFromFile(
    {
      originalname: path.basename(DUMMY_PATH),
      mimetype: 'image/jpeg',
      buffer: dummyFile,
    },
    { ctx: await connection.createContext() },
  );

  Logger.info(`Dummy asset id=${dummyAsset.id}`);

  // 2) buscar variants sem assets
  const variants: Array<{ id: number }> =
    await connection.rawConnection.query(`
      SELECT pv.id
      FROM product_variant pv
      LEFT JOIN product_variant_asset pva
        ON pva."productVariantId" = pv.id
      WHERE pva.id IS NULL
    `);

  Logger.info(`Variants sem imagem: ${variants.length}`);

  let processed = 0;

  for (const v of variants) {
    processed++;

    // ligar asset
    await connection.rawConnection.query(
      `
      INSERT INTO product_variant_asset
        ("productVariantId","assetId","position","createdAt","updatedAt")
      VALUES ($1,$2,1,NOW(),NOW())
      `,
      [v.id, dummyAsset.id],
    );

    // definir como featured
    await connection.rawConnection.query(
      `
      UPDATE product_variant
      SET "featuredAssetId"=$2
      WHERE id=$1
      `,
      [v.id, dummyAsset.id],
    );

    if (processed % LOG_EVERY === 0) {
      Logger.info(`Progress: ${processed}/${variants.length}`);
    }
  }

  Logger.info('---------------------------------------------');
  Logger.info(`Variants atualizadas: ${processed}`);
  Logger.info('🎉 Concluído.');

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
