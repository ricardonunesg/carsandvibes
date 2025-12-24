import { bootstrapWorker, Logger, RequestContextService, TransactionalConnection } from '@vendure/core';
import { config } from '../../src/vendure-config';

async function main() {
  const { app } = await bootstrapWorker(config);

  const ctx = await app.get(RequestContextService).create({ apiType: 'admin' });
  const connection = app.get(TransactionalConnection);

  // tabela default do Vendure para membership de collections (variants)
  const table = 'collection_product_variants_product_variant';

  const before = await connection.rawConnection.query(`SELECT COUNT(*)::int AS n FROM "${table}"`);
  Logger.info(`Antes: ${before[0]?.n ?? 0} linhas em ${table}`);

  await connection.rawConnection.query(`TRUNCATE TABLE "${table}"`);

  const after = await connection.rawConnection.query(`SELECT COUNT(*)::int AS n FROM "${table}"`);
  Logger.info(`Depois: ${after[0]?.n ?? 0} linhas em ${table}`);

  Logger.info('✅ Membership limpo (collections -> variants).');

  await app.close();
}

main().catch(err => {
  Logger.error(err);
  process.exit(1);
});
