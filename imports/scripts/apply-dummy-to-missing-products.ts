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

const DUMMY_ASSET_ID = Number(process.env.DUMMY_ASSET_ID || 0);
const DUMMY_PATH = process.env.DUMMY_PATH || ''; // opcional: se quiseres criar asset aqui
const CHANNEL_ID = Number(process.env.CHANNEL_ID || 1);
const CHANNEL_TOKEN = process.env.CHANNEL_TOKEN || '23456';
const LOG_EVERY = Number(process.env.LOG_EVERY || 500);

function guessMimeType(filePath: string) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.png') return 'image/png';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.gif') return 'image/gif';
  return 'application/octet-stream';
}

async function ensureDummyAsset(
  app: any,
  connection: TransactionalConnection,
): Promise<number> {
  // 1) se já passou id, usar
  if (DUMMY_ASSET_ID) {
    const exists: Array<{ id: number }> = await connection.rawConnection.query(
      `SELECT id::int as id FROM asset WHERE id=$1 LIMIT 1`,
      [DUMMY_ASSET_ID],
    );
    if (!exists.length) throw new Error(`DUMMY_ASSET_ID não existe: ${DUMMY_ASSET_ID}`);

    // garantir channel
    await connection.rawConnection.query(
      `
      INSERT INTO asset_channels_channel ("assetId","channelId")
      SELECT $1::int, $2::int
      WHERE NOT EXISTS (
        SELECT 1 FROM asset_channels_channel
        WHERE "assetId"=$1::int AND "channelId"=$2::int
      )
      `,
      [DUMMY_ASSET_ID, CHANNEL_ID],
    );

    return DUMMY_ASSET_ID;
  }

  // 2) senão: criar via AssetService (igual ao script anterior)
  if (!DUMMY_PATH) throw new Error('Define DUMMY_ASSET_ID ou DUMMY_PATH para criar o dummy asset.');
  if (!fs.existsSync(DUMMY_PATH)) throw new Error(`Dummy image não encontrada: ${DUMMY_PATH}`);

  const ctxService = app.get(RequestContextService);
  const assetService = app.get(AssetService);

  const ctx = await ctxService.create({
    apiType: 'admin',
    languageCode: 'pt',
    channelOrToken: CHANNEL_TOKEN,
  });

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

  const newId = Number((created as any).id);
  if (!newId) throw new Error(`Falha a criar dummy asset. Resposta: ${JSON.stringify(created)}`);

  await connection.rawConnection.query(
    `
    INSERT INTO asset_channels_channel ("assetId","channelId")
    SELECT $1::int, $2::int
    WHERE NOT EXISTS (
      SELECT 1 FROM asset_channels_channel
      WHERE "assetId"=$1::int AND "channelId"=$2::int
    )
    `,
    [newId, CHANNEL_ID],
  );

  return newId;
}

async function detectProductAssetJoin(connection: TransactionalConnection) {
  // tenta encontrar uma tabela que contenha "product" e "asset" no nome
  const candidates: Array<{ table_name: string }> = await connection.rawConnection.query(
    `
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
      AND table_name ILIKE '%product%asset%'
    ORDER BY table_name
    `,
  );

  // ignorar asset_channels_channel etc
  const filtered = candidates
    .map(c => c.table_name)
    .filter(t => !t.includes('channels') && !t.includes('variant'));

  // Vendure costuma ter product_asset
  const joinTable = filtered.includes('product_asset')
    ? 'product_asset'
    : filtered[0];

  if (!joinTable) {
    throw new Error(
      `Não encontrei tabela de ligação Product<->Asset. Candidatas: ${candidates
        .map(c => c.table_name)
        .join(', ')}`,
    );
  }

  const cols: Array<{ column_name: string }> = await connection.rawConnection.query(
    `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='public'
      AND table_name=$1
    `,
    [joinTable],
  );

  const colNames = cols.map(c => c.column_name);

  // descobrir nomes das colunas
  const productIdCol =
    colNames.find(c => c.toLowerCase() === 'productid') ||
    colNames.find(c => c.toLowerCase().includes('product') && c.toLowerCase().includes('id'));

  const assetIdCol =
    colNames.find(c => c.toLowerCase() === 'assetid') ||
    colNames.find(c => c.toLowerCase().includes('asset') && c.toLowerCase().includes('id'));

  const positionCol = colNames.find(c => c.toLowerCase() === 'position') || null;

  if (!productIdCol || !assetIdCol) {
    throw new Error(
      `Tabela ${joinTable} não tem colunas esperadas. Colunas: ${colNames.join(', ')}`,
    );
  }

  return { joinTable, productIdCol, assetIdCol, positionCol };
}

async function main() {
  const { app } = await bootstrapWorker(config);
  const connection = app.get(TransactionalConnection);

  Logger.info('=============================================');
  Logger.info('Aplicar dummy aos PRODUCTS sem imagem');

  const dummyAssetId = await ensureDummyAsset(app, connection);

  const { joinTable, productIdCol, assetIdCol, positionCol } =
    await detectProductAssetJoin(connection);

  Logger.info(`DummyAssetId=${dummyAssetId}`);
  Logger.info(`Join table product<->asset: ${joinTable} (${productIdCol}, ${assetIdCol}${positionCol ? ', position' : ''})`);

  // Products sem featuredAssetId
  const products: Array<{ id: number }> = await connection.rawConnection.query(
    `SELECT id::int as id FROM product WHERE "featuredAssetId" IS NULL`,
  );

  Logger.info(`Products sem imagem (featuredAssetId NULL): ${products.length}`);

  let processed = 0;

  for (const p of products) {
    processed++;

    // inserir ligação na join table (se não existir)
    if (positionCol) {
      await connection.rawConnection.query(
        `
        INSERT INTO "${joinTable}" ("${productIdCol}","${assetIdCol}","${positionCol}","createdAt","updatedAt")
        SELECT $1::int, $2::int, 1, NOW(), NOW()
        WHERE NOT EXISTS (
          SELECT 1 FROM "${joinTable}"
          WHERE "${productIdCol}"=$1::int AND "${assetIdCol}"=$2::int
        )
        `,
        [p.id, dummyAssetId],
      );
    } else {
      // caso raro: join table só com ids
      await connection.rawConnection.query(
        `
        INSERT INTO "${joinTable}" ("${productIdCol}","${assetIdCol}")
        SELECT $1::int, $2::int
        WHERE NOT EXISTS (
          SELECT 1 FROM "${joinTable}"
          WHERE "${productIdCol}"=$1::int AND "${assetIdCol}"=$2::int
        )
        `,
        [p.id, dummyAssetId],
      );
    }

    // set featured asset
    await connection.rawConnection.query(
      `UPDATE product SET "featuredAssetId"=$2 WHERE id=$1 AND "featuredAssetId" IS NULL`,
      [p.id, dummyAssetId],
    );

    if (processed % LOG_EVERY === 0) {
      Logger.info(`Progress: ${processed}/${products.length}`);
    }
  }

  Logger.info('---------------------------------------------');
  Logger.info(`🎉 Concluído. Products atualizados: ${processed}`);

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
