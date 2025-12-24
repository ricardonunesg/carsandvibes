import 'dotenv/config';
import { bootstrapWorker, Logger, TransactionalConnection } from '@vendure/core';
import { config } from '../../src/vendure-config';

const DUMMY_ASSET_ID = Number(process.env.DUMMY_ASSET_ID || 0);
const CHANNEL_ID = Number(process.env.CHANNEL_ID || 1);
const LOG_EVERY = Number(process.env.LOG_EVERY || 200);

async function detectCollectionAssetJoin(connection: TransactionalConnection) {
  // tenta encontrar tabela com "collection" + "asset"
  const candidates: Array<{ table_name: string }> = await connection.rawConnection.query(
    `
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
      AND table_name ILIKE '%collection%asset%'
    ORDER BY table_name
    `,
  );

  // remover coisas que não sejam join (por ex. channels)
  const filtered = candidates
    .map(c => c.table_name)
    .filter(t => !t.includes('channels'));

  const joinTable = filtered.includes('collection_asset') ? 'collection_asset' : filtered[0];

  if (!joinTable) {
    throw new Error(
      `Não encontrei tabela join collection<->asset. Candidatas: ${candidates
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
    ORDER BY ordinal_position
    `,
    [joinTable],
  );

  const colNames = cols.map(c => c.column_name);

  const collectionIdCol =
    colNames.find(c => c.toLowerCase() === 'collectionid') ||
    colNames.find(c => c.toLowerCase().includes('collection') && c.toLowerCase().includes('id'));

  const assetIdCol =
    colNames.find(c => c.toLowerCase() === 'assetid') ||
    colNames.find(c => c.toLowerCase().includes('asset') && c.toLowerCase().includes('id'));

  const positionCol = colNames.find(c => c.toLowerCase() === 'position') || null;

  if (!collectionIdCol || !assetIdCol) {
    throw new Error(
      `Tabela ${joinTable} não tem colunas esperadas. Colunas: ${colNames.join(', ')}`,
    );
  }

  return { joinTable, collectionIdCol, assetIdCol, positionCol };
}

async function main() {
  if (!DUMMY_ASSET_ID) {
    throw new Error('Define DUMMY_ASSET_ID (id do asset dummy).');
  }

  const { app } = await bootstrapWorker(config);
  const connection = app.get(TransactionalConnection);

  Logger.info('=============================================');
  Logger.info('Aplicar dummy às COLLECTIONS sem imagem (BD-only)');
  Logger.info(`DUMMY_ASSET_ID=${DUMMY_ASSET_ID}`);

  // garantir que o asset existe
  const assetExists: Array<{ id: number }> = await connection.rawConnection.query(
    `SELECT id::int as id FROM asset WHERE id=$1 LIMIT 1`,
    [DUMMY_ASSET_ID],
  );
  if (!assetExists.length) throw new Error(`Asset dummy não existe: ${DUMMY_ASSET_ID}`);

  // garantir asset no channel
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

  const { joinTable, collectionIdCol, assetIdCol, positionCol } =
    await detectCollectionAssetJoin(connection);

  Logger.info(
    `Join table collection<->asset: ${joinTable} (${collectionIdCol}, ${assetIdCol}${
      positionCol ? ', position' : ''
    })`,
  );

  // collections sem featuredAssetId
  const collections: Array<{ id: number }> = await connection.rawConnection.query(
    `SELECT id::int as id FROM collection WHERE "featuredAssetId" IS NULL`,
  );

  Logger.info(`Collections sem featuredAssetId: ${collections.length}`);

  let processed = 0;

  for (const c of collections) {
    processed++;

    // inserir ligação na join table (se não existir)
    if (positionCol) {
      await connection.rawConnection.query(
        `
        INSERT INTO "${joinTable}" ("${collectionIdCol}","${assetIdCol}","${positionCol}","createdAt","updatedAt")
        SELECT $1::int, $2::int, 1, NOW(), NOW()
        WHERE NOT EXISTS (
          SELECT 1 FROM "${joinTable}"
          WHERE "${collectionIdCol}"=$1::int AND "${assetIdCol}"=$2::int
        )
        `,
        [c.id, DUMMY_ASSET_ID],
      );
    } else {
      await connection.rawConnection.query(
        `
        INSERT INTO "${joinTable}" ("${collectionIdCol}","${assetIdCol}")
        SELECT $1::int, $2::int
        WHERE NOT EXISTS (
          SELECT 1 FROM "${joinTable}"
          WHERE "${collectionIdCol}"=$1::int AND "${assetIdCol}"=$2::int
        )
        `,
        [c.id, DUMMY_ASSET_ID],
      );
    }

    // set featuredAssetId
    await connection.rawConnection.query(
      `
      UPDATE collection
      SET "featuredAssetId"=$2
      WHERE id=$1 AND "featuredAssetId" IS NULL
      `,
      [c.id, DUMMY_ASSET_ID],
    );

    if (processed % LOG_EVERY === 0) {
      Logger.info(`Progress: ${processed}/${collections.length}`);
    }
  }

  Logger.info('---------------------------------------------');
  Logger.info(`🎉 Concluído. Collections atualizadas: ${processed}`);

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
