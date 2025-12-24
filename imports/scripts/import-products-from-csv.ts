const fs = require('fs');
const { bootstrap, Importer, Logger } = require('@vendure/core');

const vendureConfig = require('../../dist/vendure-config').config || require('../../src/vendure-config').config;

const CSV_PATH = process.env.CSV_PATH || '/root/carsandvibes/imports/generated/vendure_products_import.csv';
const LANG_CODE = process.env.LANG_CODE || 'pt';

async function main() {
  const app = await bootstrap({
    ...vendureConfig,
    apiOptions: { ...vendureConfig.apiOptions, port: 0 },
  });

  const importer = app.get(Importer);
  const input = fs.createReadStream(CSV_PATH);

  Logger.info(`A importar CSV: ${CSV_PATH} (lang=${LANG_CODE})`);

  await new Promise((resolve, reject) => {
    importer.parseAndImport(input, LANG_CODE, true).subscribe({
      next: p => {
        if (p.type === 'progress') Logger.info(`Import: ${p.percentage}% ${p.message || ''}`);
        if (p.type === 'error') Logger.error(p.message || 'Erro no import');
      },
      error: err => reject(err),
      complete: () => resolve(),
    });
  });

  Logger.info('✅ Import concluído');
  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
