import { bootstrap, TransactionalConnection, Logger } from '@vendure/core';
import { config as vendureConfig } from '../../src/vendure-config';

async function main() {
  const app = await bootstrap({
    ...vendureConfig,
    apiOptions: { ...vendureConfig.apiOptions, port: 0 },
  });

  const connection = app.get(TransactionalConnection);
  await connection.rawConnection.query('TRUNCATE TABLE collection_product_variants_product_variant;');

  Logger.info('✅ Limpo: collection_product_variants_product_variant (TRUNCATE)');
  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
