#!/usr/bin/env node
/**
 * Importa imagens para ProductVariants no Vendure com base num CSV (1 linha por imagem).
 *
 * Requisitos:
 *  - Node 18+ (fetch + FormData)
 *  - Admin API acessível (ex.: http://localhost:3000/admin-api)
 *  - Token de admin (Bearer) com permissões para criar assets e atualizar variants
 *
 * Uso:
 *  ADMIN_API_URL="http://localhost:3000/admin-api" \
 *  ADMIN_TOKEN="SEU_TOKEN_AQUI" \
 *  node imports/scripts/import-variant-assets.mjs
 */

import fs from "node:fs";
import path from "node:path";

const CSV_PATH = process.env.CSV_PATH || path.resolve("imports/source/vendure_variant_images_mapping.csv");
const ADMIN_API_URL = process.env.ADMIN_API_URL || "http://localhost:3000/admin-api";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN;

if (!ADMIN_TOKEN) {
  console.error("Falta ADMIN_TOKEN no ambiente. Ex.: export ADMIN_TOKEN='...'");
  process.exit(1);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function gqlRequest(query, variables) {
  const res = await fetch(ADMIN_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${ADMIN_TOKEN}`,
    },
    body: JSON.stringify({ query, variables }),
  });

  const json = await res.json();
  if (!res.ok || json.errors) {
    throw new Error(`GraphQL error: ${JSON.stringify(json.errors || json, null, 2)}`);
  }
  return json.data;
}

// GraphQL multipart upload (spec)
async function gqlUploadCreateAsset(filePath, fileName) {
  const query = `
    mutation CreateAssets($input: [CreateAssetInput!]!) {
      createAssets(input: $input) {
        ... on Asset { id preview }
        ... on MimeTypeError { errorCode message }
        ... on NoPermissionError { errorCode message }
      }
    }
  `;

  const operations = {
    query,
    variables: {
      input: [{ file: null }],
    },
  };

  // map: which variables get which file field
  const map = { "0": ["variables.input.0.file"] };

  const form = new FormData();
  form.append("operations", JSON.stringify(operations));
  form.append("map", JSON.stringify(map));

  const fileBlob = new Blob([fs.readFileSync(filePath)]);
  form.append("0", fileBlob, fileName || path.basename(filePath));

  const res = await fetch(ADMIN_API_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${ADMIN_TOKEN}`,
      // NÃO meter Content-Type aqui; o fetch define boundary automaticamente.
    },
    body: form,
  });

  const json = await res.json();
  if (!res.ok || json.errors) {
    throw new Error(`Upload GraphQL error: ${JSON.stringify(json.errors || json, null, 2)}`);
  }
  const result = json.data?.createAssets?.[0];
  if (!result?.id) {
    throw new Error(`Falha a criar asset: ${JSON.stringify(json.data, null, 2)}`);
  }
  return result; // {id, preview}
}

async function findVariantIdBySku(sku) {
  const query = `
    query FindVariant($sku: String!) {
      productVariants(options: { filter: { sku: { eq: $sku } }, take: 1 }) {
        items { id sku featuredAsset { id } assets { id } }
      }
    }
  `;
  const data = await gqlRequest(query, { sku });
  return data.productVariants.items[0] || null;
}

async function updateVariantAssets(variantId, assetIds, featuredAssetId) {
  const mutation = `
    mutation UpdateVariants($input: [UpdateProductVariantInput!]!) {
      updateProductVariants(input: $input) {
        id sku featuredAsset { id } assets { id }
      }
    }
  `;
  const input = [{
    id: variantId,
    assetIds,
    ...(featuredAssetId ? { featuredAssetId } : {}),
  }];

  const data = await gqlRequest(mutation, { input });
  return data.updateProductVariants?.[0];
}

function parseCsvLines(csvText) {
  const lines = csvText.split(/\r?\n/).filter(Boolean);
  const header = lines.shift();
  const cols = header.split(";").map(s => s.trim());

  // expected columns
  const idx = Object.fromEntries(cols.map((c, i) => [c, i]));

  const rows = [];
  for (const line of lines) {
    // como não tens quotes neste CSV, split simples chega
    const parts = line.split(";");
    const get = (name) => parts[idx[name]] ?? "";
    rows.push({
      variant_sku: get("variant_sku"),
      image_path: get("image_path"),
      position: Number(get("position") || 0),
      is_cover: String(get("is_cover") || "0") === "1",
      source: get("source"),
    });
  }
  return rows;
}

function groupByVariant(rows) {
  const map = new Map();
  for (const r of rows) {
    if (!r.variant_sku) continue;
    if (!map.has(r.variant_sku)) map.set(r.variant_sku, []);
    map.get(r.variant_sku).push(r);
  }
  // manter ordem: variant primeiro e depois fallback, e por position
  for (const [sku, arr] of map.entries()) {
    arr.sort((a, b) => {
      const sa = a.source === "variant" ? 0 : 1;
      const sb = b.source === "variant" ? 0 : 1;
      if (sa !== sb) return sa - sb;
      return (a.position ?? 0) - (b.position ?? 0);
    });
    map.set(sku, arr);
  }
  return map;
}

(async function main() {
  console.log(`CSV: ${CSV_PATH}`);
  console.log(`Admin API: ${ADMIN_API_URL}`);

  const csvText = fs.readFileSync(CSV_PATH, "utf8");
  const rows = parseCsvLines(csvText);
  const grouped = groupByVariant(rows);

  console.log(`Linhas CSV: ${rows.length}`);
  console.log(`SKUs (variants) únicos: ${grouped.size}`);

  const logMissing = [];
  const logErrors = [];

  let processed = 0;

  for (const [sku, images] of grouped.entries()) {
    processed += 1;

    try {
      const variant = await findVariantIdBySku(sku);

      if (!variant) {
        logMissing.push(sku);
        continue;
      }

      // Upload de todos os assets desta variante
      const createdAssetIds = [];
      let featuredAssetId = variant.featuredAsset?.id || null;

      // se houver cover no CSV, escolhemos o primeiro cover como featured
      const coverRow = images.find(x => x.is_cover);
      for (const img of images) {
        const p = img.image_path;
        if (!p || !fs.existsSync(p)) {
          console.warn(`[WARN] Ficheiro não existe: ${p} (sku=${sku})`);
          continue;
        }
        const asset = await gqlUploadCreateAsset(p, path.basename(p));
        createdAssetIds.push(asset.id);

        if (coverRow && img.image_path === coverRow.image_path) {
          featuredAssetId = asset.id;
        }
        // para não rebentar o server com muitas requests seguidas
        await sleep(20);
      }

      if (createdAssetIds.length === 0) {
        continue;
      }

      // juntar assets existentes + novos (e manter ordem: existentes primeiro)
      const existingAssetIds = (variant.assets || []).map(a => a.id);
      const finalAssetIds = [...existingAssetIds, ...createdAssetIds];

      const updated = await updateVariantAssets(variant.id, finalAssetIds, featuredAssetId);
      console.log(`[OK] ${sku} -> +${createdAssetIds.length} assets (featured=${featuredAssetId || "n/a"})`);

    } catch (err) {
      logErrors.push({ sku, error: String(err?.message || err) });
      console.error(`[ERR] ${sku}: ${err?.message || err}`);
    }
  }

  // Logs
  fs.writeFileSync("imports/logs/missing-variants.txt", logMissing.join("\n"), "utf8");
  fs.writeFileSync("imports/logs/errors.json", JSON.stringify(logErrors, null, 2), "utf8");

  console.log(`\nTerminado.`);
  console.log(`Missing variants: ${logMissing.length} (imports/logs/missing-variants.txt)`);
  console.log(`Errors: ${logErrors.length} (imports/logs/errors.json)`);
})();
