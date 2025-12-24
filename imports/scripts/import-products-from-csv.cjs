const fs = require('fs');
const { bootstrap, Importer, Logger } = require('@vendure/core');

let vendureConfig;
try {
  vendureConfig = require('../../dist/vendure-config').config;
} catch (e) {
  vendureConfig = require('../../src/vendure-config').config;
}

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

  let progressCount = 0;
  let errorCount = 0;
  let lastProgress = null;

  await new Promise((resolve, reject) => {
    importer.parseAndImport(input, LANG_CODE, true).subscribe({
      next: (p) => {
        if (p.type === 'progress') {
          lastProgress = `${p.percentage}% ${p.message || ''}`.trim();
          progressCount++;
          if (p.percentage % 10 === 0) Logger.info(`Import: ${lastProgress}`);
        }
        if (p.type === 'error') {
          errorCount++;
          Logger.error(`[IMPORT ERROR] ${p.message || 'erro sem mensagem'}`);
          if (p.error) Logger.error(String(p.error));
        }
        if (p.type === 'info') {
          Logger.info(`[IMPORT] ${p.message || ''}`);
        }
      },
      error: (err) => reject(err),
      complete: () => resolve(),
    });
  });

  Logger.info(`✅ Import terminado. progress events=${progressCount}, errors=${errorCount}, last=${lastProgress}`);

  await app.close();
}

main().catch(err => {
  console.error('❌ ERRO:', err);
  process.exit(1);
});
