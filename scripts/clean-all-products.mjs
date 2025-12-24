// /root/carsandvibes/scripts/clean-all-products.mjs
// Apaga TODOS os produtos do Vendure (3.5.0)

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ADMIN_API_URL =
  process.env.ADMIN_API_URL || 'http://127.0.0.1:3000/admin-api';

// ---------- COOKIE (mesmo esquema do importador) ----------
function cookieHeaderFromJar(jarPath) {
  if (process.env.COOKIE_HEADER) return process.env.COOKIE_HEADER;

  if (!jarPath) {
    throw new Error('COOKIE_JAR não definido e COOKIE_HEADER ausente');
  }
  if (!fs.existsSync(jarPath)) {
    throw new Error(`Cookie jar não encontrado: ${jarPath}`);
  }
  const text = fs.readFileSync(jarPath, 'utf8');
  const lines = text.split(/\r?\n/).filter(l => l.trim());
  const cookies = [];
  for (let line of lines) {
    if (line.startsWith('#HttpOnly_')) {
      line = line.replace(/^#HttpOnly_/, '');
    } else if (line.startsWith('#')) {
      continue;
    }
    const f = line.split(/\s+/);
    if (f.length >= 7) cookies.push({ name: f[5], value: f[6] });
  }
  if (!cookies.length) {
    throw new Error(
      'Cookie jar vazio – faz login outra vez com curl -c cookie-plain.jar ...',
    );
  }
  return cookies.map(c => `${c.name}=${c.value}`).join('; ');
}

const cookieJarPath =
  process.env.COOKIE_JAR || '/root/carsandvibes/cookie-plain.jar';
const COOKIE_HEADER = cookieHeaderFromJar(cookieJarPath);

// ---------- GQL HELPER ----------
async function gql(query, variables = {}) {
  const res = await fetch(ADMIN_API_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Cookie: COOKIE_HEADER,
    },
    body: JSON.stringify({ query, variables }),
  });

  const json = await res.json();
  if (json.errors && json.errors.length) {
    console.error('GraphQL errors:', json.errors);
    const err = new Error('GraphQL error');
    err.graphQLErrors = json.errors;
    throw err;
  }
  return json.data;
}

async function fetchOnePage(take = 50) {
  const data = await gql(
    `
    query AllProducts($take: Int!) {
      products(options: { take: $take }) {
        totalItems
        items { id slug name }
      }
    }
  `,
    { take },
  );
  return data.products;
}

async function deleteProduct(id) {
  const data = await gql(
    `
    mutation DeleteProduct($id: ID!) {
      deleteProduct(id: $id) {
        result
        message
      }
    }
  `,
    { id },
  );
  return data.deleteProduct;
}

// ---------- MAIN ----------
async function main() {
  console.log(`Endpoint: ${ADMIN_API_URL}`);
  console.log('⚠️  VAI APAGAR TODOS OS PRODUTOS!');

  let totalDeleted = 0;

  while (true) {
    const page = await fetchOnePage(50);
    const items = page.items;
    if (!items.length) {
      console.log('Nenhum produto restante. Fim da limpeza.');
      break;
    }

    console.log(
      `\n→ Página com ${items.length} produtos (total reportado: ${page.totalItems}).`,
    );

    for (const p of items) {
      console.log(`  - A apagar id=${p.id}, slug=${p.slug} (${p.name})...`);
      try {
        const res = await deleteProduct(p.id);
        if (res.result === 'DELETED') {
          console.log(`    ✅ DELETED (msg: ${res.message || 'ok'})`);
          totalDeleted++;
        } else {
          console.warn(
            `    [WARN] Resultado ao apagar id=${p.id}: ${res.result} (${res.message || ''})`,
          );
        }
      } catch (e) {
        console.error(
          `    ❌ Erro ao apagar produto id=${p.id}:`,
          e.graphQLErrors || e.message || e,
        );
      }
    }
  }

  console.log(`\n--- LIMPEZA CONCLUÍDA ---`);
  console.log(`Produtos apagados (aprox): ${totalDeleted}`);
}

main().catch(err => {
  console.error('Erro no script de limpeza:', err);
  process.exit(1);
});
