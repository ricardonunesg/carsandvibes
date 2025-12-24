import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import {
  bootstrapWorker,
  Logger,
  TransactionalConnection,
  RequestContextService,
  AssetService,
} from '@vendure/core';
import { config } from '../../src/vendure-config';

const DUMMY_PATH =
  process.env.DUMMY_PATH || '/root/carsandvibes/imports/assets/dummy.jpg';

const CHANNEL_ID = Number(process.env.CHANNEL_ID || 1);
const CHANNEL_TOKEN = process.env.CHANNEL_TOKEN || '23456'; // do teu default channel
const LOG_EVERY = Number(process.env.LOG_EVERY || 1000);

function guessMimeType(filePath: string) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.png') return 'image/png';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.gif') return 'image/gif';
  return 'application/octet-stream';
}

async function main() {
  if (!fs.existsSync(DUMMY_PATH)) {
    throw new Error(`Dummy image não encontrada: ${DUMMY_PATH}`);
  }

  const { app } = await bootstrapWorker(config);

  const connection = app.get(TransactionalConnection);
  const ctxService = app.get(RequestContextService);
  const assetService = app.get(AssetService);

  const ctx = await ctxService.create({
    apiType: 'admin',
    languageCode: 'pt',
    channelOrToken: CHANNEL_TOKEN, // garante channel certo
  });

  Logger.info('=============================================');
  Logger.info('Criar + aplicar imagem dummy às variants sem imagem');
  Logger.info(`Dummy: ${DUMMY_PATH}`);
  Logger.info(`CHANNEL_ID=${CHANNEL_ID} | CHANNEL_TOKEN=${CHANNEL_TOKEN}`);

  // 1) Criar asset dummy (sempre cria novo; simples e garantido)
  const filename = path.basename(DUMMY_PATH);
  const mimetype = guessMimeType(DUMMY_PATH);

  const created = await assetService.create(ctx, {
    file: {
      filename,
      mimetype,
      encoding: '7bit',
      createReadStream: () => fs.createReadStream(DUMMY_PATH),
    } as any,
  });

  const dummyAssetId = Number((created as any).id);
  if (!dummyAssetId) {
    throw new Error(`Falha a criar asset dummy. Resposta: ${JSON.stringify(created)}`);
  }

  Logger.info(`✅ Dummy asset criado: id=${dummyAssetId}`);

  // 2) Garantir asset no channel (BD-only)
  await connection.rawConnection.query(
    `
    INSERT INTO asset_channels_channel ("assetId","channelId")
    SELECT $1::int, $2::int
    WHERE NOT EXISTS (
      SELECT 1 FROM asset_channels_channel
      WHERE "assetId"=$1::int AND "channelId"=$2::int
    )
    `,
    [dummyAssetId, CHANNEL_ID],
  );

  // 3) Buscar variants sem imagem
  const variants: Array<{ id: number }> = await connection.rawConnection.query(`
    SELECT pv.id::int as id
    FROM product_variant pv
    LEFT JOIN product_variant_asset pva ON pva."productVariantId" = pv.id
    WHERE pva.id IS NULL
  `);

  Logger.info(`Variants sem imagem: ${variants.length}`);

  let processed = 0;

  for (const v of variants) {
    processed++;

    // inserir ligação (sem ON CONFLICT; usas WHERE NOT EXISTS)
    await connection.rawConnection.query(
      `
      INSERT INTO product_variant_asset
        ("productVariantId","assetId","position","createdAt","updatedAt")
      SELECT $1::int, $2::int, 1, NOW(), NOW()
      WHERE NOT EXISTS (
        SELECT 1 FROM product_variant_asset
        WHERE "productVariantId"=$1::int AND "assetId"=$2::int
      )
      `,
      [v.id, dummyAssetId],
    );

    // set featuredAssetId se estiver null
    await connection.rawConnection.query(
      `
      UPDATE product_variant
      SET "featuredAssetId"=$2
      WHERE id=$1 AND "featuredAssetId" IS NULL
      `,
      [v.id, dummyAssetId],
    );

    if (processed % LOG_EVERY === 0) {
      Logger.info(`Progress: ${processed}/${variants.length}`);
    }
  }

  Logger.info('---------------------------------------------');
  Logger.info(`🎉 Concluído. Variants atualizadas: ${processed}`);
  Logger.info(`Dummy asset id usado: ${dummyAssetId}`);

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
