// /root/carsandvibes/scripts/clean-products.mjs
// Limpa produtos problemáticos por slug (Vendure 3.5.0)

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ADMIN_API_URL =
  process.env.ADMIN_API_URL || 'http://127.0.0.1:3000/admin-api';

// ---------- COOKIE (igual ao importador) ----------
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

// ---------- CONFIG: slugs a apagar ----------
const SLUGS_TO_DELETE = [
  'ia0-1879-a01', // ONE EVO AIR OVERALL
  'ia0-1876-a01', // ONE-S AIR OVERALL
];

// ---------- HELPERS ----------
async function getProductIdBySlug(slug) {
  const data = await gql(
    `
    query GetProductBySlug($slug: String!) {
      products(options: { filter: { slug: { eq: $slug } } }) {
        items { id slug }
      }
    }
  `,
    { slug },
  );
  return data.products.items[0]?.id ?? null;
}

async function deleteProductById(id) {
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
  console.log(`Produtos (slugs) a apagar:`, SLUGS_TO_DELETE);

  for (const slug of SLUGS_TO_DELETE) {
    console.log(`\n➡ A tratar slug="${slug}"...`);

    let id = null;
    try {
      id = await getProductIdBySlug(slug);
    } catch (e) {
      console.error(
        `  ❌ Erro ao procurar produto com slug=${slug}:`,
        e.message || e,
      );
      continue;
    }

    if (!id) {
      console.warn(
        `  [WARN] Nenhum produto encontrado com slug=${slug}. (Talvez já tenha sido apagado.)`,
      );
      continue;
    }

    console.log(`  → Encontrado produto id=${id}, slug=${slug}. A apagar...`);

    try {
      const result = await deleteProductById(id);
      if (result?.result === 'DELETED') {
        console.log(`  ✅ Produto id=${id} apagado com sucesso.`);
      } else {
        console.warn(
          `  [WARN] Resultado ao apagar id=${id}:`,
          result?.result,
          result?.message || '',
        );
      }
    } catch (e) {
      console.error(
        `  ❌ Erro FATAL ao apagar produto id=${id}:`,
        e.graphQLErrors || e.message || e,
      );
    }
  }

  console.log('\n--- LIMPEZA CONCLUÍDA ---');
}

main().catch(err => {
  console.error('Erro no script de limpeza:', err);
  process.exit(1);
});
