#!/usr/bin/env node
/**
 * apply-facets-to-all-variants.mjs
 *
 * - Lê o XLSX e calcula os slugs finais (igual ao import)
 * - Faz scan aos Products na BD via paginação e apanha só os que existem no XLSX
 * - Recolhe todos os variantIds desses produtos
 * - Garante 4 Facets (nav1..nav4) + 1 FacetValue por facet
 * - Aplica facetValueIds às variantes (bulk se der, senão single)
 * - Aplica também a brand OMP facetValueId (default: 88)
 * - Corre runPendingSearchIndexUpdates no fim
 */

import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { execFileSync } from "node:child_process";
import xlsx from "xlsx";

/**
 * ENV
 */
const ADMIN_API_URL = process.env.ADMIN_API_URL ?? "http://localhost:3000/admin-api";
const COOKIE_JAR =
  process.env.COOKIE_JAR ?? path.join(process.env.HOME ?? "/root", "carsandvibes/cookie-plain.jar");

const INPUT_XLSX =
  process.env.INPUT_XLSX ??
  path.join(
    process.env.HOME ?? "/root",
    "carsandvibes/imports/source/2026_RRP_OMP_23102025_variants_options_prepared.xlsx"
  );

const SHEET_NAME = (process.env.SHEET_NAME ?? "").trim(); // se vazio, usa 1ª sheet
const DRY_RUN = (process.env.DRY_RUN ?? "1") === "1";
const LOG_DIR = process.env.LOG_DIR ?? path.join(process.env.HOME ?? "/root", "carsandvibes/imports/logs");

// idiomas disponíveis: en, pt, fr, es
const LANGUAGE_CODE = (process.env.LANGUAGE_CODE ?? "pt").trim();
const ALSO_EN = (process.env.ALSO_EN ?? "0") === "1";

// nomes bonitos (estes aparecem no filtro do cliente)
const FACET3_NAME = (process.env.FACET3_NAME ?? "Tipo").trim();
const FACET4_NAME = (process.env.FACET4_NAME ?? "Aplicação").trim();

// label do valor (o que aparece como “opção” do filtro)
const FACET3_VALUE_NAME = (process.env.FACET3_VALUE_NAME ?? "Standard").trim();
const FACET4_VALUE_NAME = (process.env.FACET4_VALUE_NAME ?? "Standard").trim();

// OMP facetValueId (já tens 88)
const BRAND_FACETVALUE_ID = String(process.env.BRAND_FACETVALUE_ID ?? "88").trim();

// batch sizes
const PRODUCTS_PAGE_TAKE = Number(process.env.PRODUCTS_PAGE_TAKE ?? "200");
const UPDATE_BATCH = Number(process.env.UPDATE_BATCH ?? "200");

if (!fs.existsSync(INPUT_XLSX)) {
  console.error("XLSX não encontrado:", INPUT_XLSX);
  process.exit(1);
}
if (!fs.existsSync(COOKIE_JAR)) {
  console.error("COOKIE_JAR não encontrado:", COOKIE_JAR);
  process.exit(1);
}
fs.mkdirSync(LOG_DIR, { recursive: true });

function nowId() {
  return String(Date.now());
}

function writeJson(filePath, obj) {
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), "utf-8");
}

function curlGraphQL(query, variables = {}) {
  const payload = JSON.stringify({ query, variables });
  const out = execFileSync(
    "curl",
    ["-sS", ADMIN_API_URL, "-H", "Content-Type: application/json", "-b", COOKIE_JAR, "--data-binary", payload],
    { encoding: "utf-8" }
  );

  let json;
  try {
    json = JSON.parse(out);
  } catch (e) {
    const err = new Error("Resposta não-JSON do GraphQL");
    err.details = out.slice(0, 2000);
    throw err;
  }

  if (json.errors?.length) {
    const err = new Error("GraphQL errors");
    err.details = json.errors;
    err.response = json;
    throw err;
  }
  return json.data;
}

function safeGraphQL(query, variables = {}) {
  try {
    return { ok: true, data: curlGraphQL(query, variables) };
  } catch (e) {
    return { ok: false, err: e };
  }
}

/**
 * Slugify consistente
 */
function slugify(s) {
  const str = String(s ?? "")
    .trim()
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-+/g, "-");
  return str || "x";
}

/**
 * Normalização de valores do XLSX
 */
function normVal(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "number") {
    const s = String(v);
    return s.replace(/\.0+$/, "").trim();
  }
  return String(v).trim().replace(/\s+/g, " ");
}

function whoAmI() {
  const q = `query{ activeAdministrator{ id firstName lastName emailAddress } }`;
  const r = curlGraphQL(q);
  const a = r.activeAdministrator;
  const name = [a?.firstName, a?.lastName].filter(Boolean).join(" ") || "(sem nome)";
  const email = a?.emailAddress || "(sem emailAddress)";
  console.log(`Auth OK: ${name} <${email}> (id=${a?.id ?? "?"})`);
}

/**
 * Lê XLSX e calcula os slugs finais como no import:
 * finalSlug = slugify(ProductName) + "-" + slugify(ProductCode)
 */
function loadProductSlugsFromXlsx() {
  const wb = xlsx.readFile(INPUT_XLSX);
  const sheetName = SHEET_NAME || wb.SheetNames[0];
  const ws = wb.Sheets[sheetName];
  if (!ws) throw new Error(`Sheet não encontrada: ${sheetName}`);

  const rows = xlsx.utils.sheet_to_json(ws, { defval: "" });

  const byCode = new Map();
  for (const r of rows) {
    const productCode = normVal(r["ProductCode"]);
    const productName = normVal(r["ProductName"] || r["SHORT DESCRIPTION"] || r["Product Name"]);
    if (!productCode) continue;
    if (!byCode.has(productCode)) {
      byCode.set(productCode, { productCode, productName: productName || productCode });
    }
  }

  const slugs = [];
  for (const p of byCode.values()) {
    const baseSlug = slugify(p.productName);
    const finalSlug = `${baseSlug}-${slugify(p.productCode)}`;
    slugs.push(finalSlug);
  }
  return slugs;
}

/**
 * Pagina produtos do Vendure e apanha os que estão no set do XLSX.
 * Também recolhe os variantIds.
 *
 * Nota: no teu schema Product.variants é LIST, não connection.
 */
const Q_PRODUCTS_PAGE = `
query($take:Int!, $skip:Int!){
  products(options:{ take:$take, skip:$skip }){
    totalItems
    items{
      id
      slug
      variants{ id }
    }
  }
}
`;

async function collectVariantIdsForXlsxProducts(xlsxSlugsSet) {
  let skip = 0;
  let total = null;
  const found = new Map(); // slug -> {id, variantIds[]}

  while (true) {
    const data = curlGraphQL(Q_PRODUCTS_PAGE, { take: PRODUCTS_PAGE_TAKE, skip });
    const page = data.products;
    if (total === null) total = page.totalItems ?? 0;

    const items = page.items ?? [];
    for (const p of items) {
      if (!p?.slug) continue;
      if (!xlsxSlugsSet.has(p.slug)) continue;
      const vIds = (p.variants ?? []).map((v) => v.id).filter(Boolean);
      found.set(p.slug, { id: p.id, slug: p.slug, variantIds: vIds });
    }

    skip += items.length;
    if (skip >= total || items.length === 0) break;
  }

  // flatten
  const allVariantIds = [];
  let productsWithNoVariants = 0;

  for (const v of found.values()) {
    if (!v.variantIds.length) productsWithNoVariants += 1;
    allVariantIds.push(...v.variantIds);
  }

  return { foundProducts: found, allVariantIds, productsWithNoVariants };
}

/**
 * Facets + values
 *
 * Vamos criar:
 * nav1 -> value: catalogo
 * nav2 -> value: todos
 * nav3 -> value: standard (nome do facet = FACET3_NAME, nome do value = FACET3_VALUE_NAME)
 * nav4 -> value: standard (nome do facet = FACET4_NAME, nome do value = FACET4_VALUE_NAME)
 */
const Q_FACETS_PAGE = `
query($take:Int!, $skip:Int!){
  facets(options:{ take:$take, skip:$skip }){
    totalItems
    items{
      id
      code
      name
      values{ id code name }
    }
  }
}
`;

const M_CREATE_FACET = `
mutation($input: CreateFacetInput!){
  createFacet(input:$input){
    id code name values{ id code name }
  }
}
`;

const M_CREATE_FACET_VALUE = `
mutation($input: CreateFacetValueInput!){
  createFacetValue(input:$input){
    id code name
  }
}
`;

function loadAllFacets() {
  const take = 200;
  let skip = 0;
  let total = null;
  const out = [];
  while (true) {
    const data = curlGraphQL(Q_FACETS_PAGE, { take, skip });
    const page = data.facets;
    if (total === null) total = page.totalItems ?? 0;
    out.push(...(page.items ?? []));
    skip += (page.items ?? []).length;
    if (skip >= total || (page.items ?? []).length === 0) break;
  }
  return out;
}

function findFacetByCode(facets, code) {
  return facets.find((f) => f.code === code) || null;
}
function findFacetValueByCode(facet, valueCode) {
  return (facet?.values ?? []).find((v) => v.code === valueCode) || null;
}

function ensureFacet(facetsCache, code, displayName) {
  const existing = findFacetByCode(facetsCache, code);
  if (existing) return existing;

  if (DRY_RUN) {
    console.log(`[DRY] createFacet ${code} (${displayName})`);
    const fake = { id: `DRY_F_${code}`, code, name: displayName, values: [] };
    facetsCache.push(fake);
    return fake;
  }

  // required no teu erro: isPrivate é obrigatório
  const input = {
    code,
    isPrivate: false,
    translations: [
      { languageCode: LANGUAGE_CODE, name: displayName },
      ...(ALSO_EN ? [{ languageCode: "en", name: displayName }] : []),
    ],
  };

  const res = curlGraphQL(M_CREATE_FACET, { input });
  const created = res.createFacet;
  facetsCache.push(created);
  return created;
}

function ensureFacetValue(facetsCache, facetCode, facetDisplayName, valueCode, valueName) {
  const facet = ensureFacet(facetsCache, facetCode, facetDisplayName);
  const existing = findFacetValueByCode(facet, valueCode);
  if (existing) return existing;

  if (DRY_RUN) {
    console.log(`[DRY] createFacetValue ${facetCode}:${valueCode} (${valueName})`);
    const fake = { id: `DRY_FV_${facetCode}_${valueCode}`, code: valueCode, name: valueName };
    facet.values = facet.values || [];
    facet.values.push(fake);
    return fake;
  }

  const input = {
    facetId: facet.id,
    code: valueCode,
    translations: [
      { languageCode: LANGUAGE_CODE, name: valueName },
      ...(ALSO_EN ? [{ languageCode: "en", name: valueName }] : []),
    ],
  };

  const res = curlGraphQL(M_CREATE_FACET_VALUE, { input });
  const created = res.createFacetValue;
  facet.values = facet.values || [];
  facet.values.push(created);
  return created;
}

/**
 * Descobrir como atualizar facetValueIds nas variantes (bulk ou single),
 * porque o teu schema pode mudar.
 */
function unwrapNamedType(t) {
  let cur = t;
  while (cur) {
    if (cur.name) return cur.name;
    cur = cur.ofType;
  }
  return null;
}

function getMutationInputTypeName(mutationName) {
  const data = curlGraphQL(`
    query{
      __schema{
        mutationType{
          fields{
            name
            args{
              name
              type{ kind name ofType{ kind name ofType{ kind name ofType{ kind name }}}}
            }
          }
        }
      }
    }
  `);

  const fields = data.__schema?.mutationType?.fields ?? [];
  const m = fields.find((f) => f.name === mutationName);
  if (!m) throw new Error(`Mutation não encontrada: ${mutationName}`);

  const inputArg = (m.args ?? []).find((a) => a.name === "input") ?? (m.args ?? [])[0];
  if (!inputArg) throw new Error(`Mutation ${mutationName} não tem args?!`);

  const typeName = unwrapNamedType(inputArg.type);
  if (!typeName) throw new Error(`Não consegui resolver input type da mutation ${mutationName}`);
  return typeName;
}

function getInputFields(typeName) {
  const data = curlGraphQL(
    `
    query($name:String!){
      __type(name:$name){
        name
        inputFields{ name }
      }
    }
  `,
    { name: typeName }
  );
  return data.__type?.inputFields ?? [];
}

function pickField(fields, candidates) {
  const names = fields.map((f) => f.name);
  for (const c of candidates) if (names.includes(c)) return c;
  return null;
}

let _updatePlan = null;
function getUpdateVariantPlan() {
  if (_updatePlan) return _updatePlan;

  // bulk
  const bulkInputType = getMutationInputTypeName("updateProductVariants");
  const bulkFields = getInputFields(bulkInputType);

  const bulkIdsField = pickField(bulkFields, ["ids", "productVariantIds", "variantIds"]);
  const bulkFacetField = pickField(bulkFields, ["facetValueIds", "facetValues", "facetValueIdsToAdd"]);

  if (bulkIdsField && bulkFacetField) {
    _updatePlan = { mode: "bulk", inputType: bulkInputType, idsField: bulkIdsField, facetField: bulkFacetField };
    console.log(`Update plan: mode=${_updatePlan.mode} inputType=${_updatePlan.inputType}`);
    return _updatePlan;
  }

  // single
  const singleInputType = getMutationInputTypeName("updateProductVariant");
  const singleFields = getInputFields(singleInputType);

  const singleIdField = pickField(singleFields, ["id", "productVariantId", "variantId"]);
  const singleFacetField = pickField(singleFields, ["facetValueIds", "facetValues", "facetValueIdsToAdd"]);

  if (singleIdField && singleFacetField) {
    _updatePlan = { mode: "single", inputType: singleInputType, idField: singleIdField, facetField: singleFacetField };
    console.log(`Update plan: mode=${_updatePlan.mode} inputType=${_updatePlan.inputType}`);
    return _updatePlan;
  }

  throw new Error(
    `Não encontrei campos para aplicar facet values em variantes.\n` +
      `updateProductVariants(${bulkInputType}) fields=[${bulkFields.map((f) => f.name).join(", ")}]\n` +
      `updateProductVariant(${singleInputType}) fields=[${singleFields.map((f) => f.name).join(", ")}]`
  );
}

function updateVariantsFacetValues(variantIds, facetValueIds) {
  if (!variantIds.length) return;
  const plan = getUpdateVariantPlan();

  if (DRY_RUN) {
    console.log(`[DRY] apply facetValueIds=${facetValueIds.join(",")} to variants=${variantIds.length} (mode=${plan.mode})`);
    return;
  }

  if (plan.mode === "bulk") {
    const input = { [plan.idsField]: variantIds, [plan.facetField]: facetValueIds };

    const mutation = `
      mutation($input:${plan.inputType}!){
        updateProductVariants(input:$input){ id }
      }
    `;
    const res = safeGraphQL(mutation, { input });
    if (!res.ok) throw res.err;
    return;
  }

  const mutation = `
    mutation($input:${plan.inputType}!){
      updateProductVariant(input:$input){ id }
    }
  `;

  for (const id of variantIds) {
    const input = { [plan.idField]: id, [plan.facetField]: facetValueIds };
    const res = safeGraphQL(mutation, { input });
    if (!res.ok) throw res.err;
  }
}

const M_RUN_PENDING_INDEX = `mutation{ runPendingSearchIndexUpdates{ success } }`;

async function main() {
  whoAmI();

  const slugs = loadProductSlugsFromXlsx();
  console.log(`Produtos únicos (pai) no XLSX: ${slugs.length}`);

  const set = new Set(slugs);

  const { foundProducts, allVariantIds, productsWithNoVariants } = await collectVariantIdsForXlsxProducts(set);

  console.log(`Produtos encontrados na BD: ${foundProducts.size}/${slugs.length}`);
  if (productsWithNoVariants) {
    console.log(`ATENÇÃO: produtos sem variantes (não devem existir se já criaste defaults): ${productsWithNoVariants}`);
  }
  console.log(`Total variantes a tocar: ${allVariantIds.length}`);

  // carregar facets existentes
  const facetsCache = loadAllFacets();

  // garantir 4 facets + 1 value por facet
  const fv1 = ensureFacetValue(facetsCache, "nav1", "Navegação 1", "catalogo", "Catálogo");
  const fv2 = ensureFacetValue(facetsCache, "nav2", "Navegação 2", "todos", "Todos");
  const fv3 = ensureFacetValue(facetsCache, "nav3", FACET3_NAME, "standard", FACET3_VALUE_NAME);
  const fv4 = ensureFacetValue(facetsCache, "nav4", FACET4_NAME, "standard", FACET4_VALUE_NAME);

  const facetValueIdsToApply = [fv1.id, fv2.id, fv3.id, fv4.id, BRAND_FACETVALUE_ID];

  console.log(
    `Aplicar facetValueIds: ${facetValueIdsToApply.join(", ")} (inclui BRAND=${BRAND_FACETVALUE_ID})`
  );

  // aplicar em batches
  let touched = 0;
  for (let i = 0; i < allVariantIds.length; i += UPDATE_BATCH) {
    const batch = allVariantIds.slice(i, i + UPDATE_BATCH);
    updateVariantsFacetValues(batch, facetValueIdsToApply);
    touched += batch.length;
    if (DRY_RUN) continue;
    if (touched % 1000 === 0) console.log(`...progresso: ${touched}/${allVariantIds.length} variantes`);
  }

  // reindex / pending updates
  const idx = safeGraphQL(M_RUN_PENDING_INDEX);
  if (idx.ok) console.log(`runPendingSearchIndexUpdates: success=${idx.data.runPendingSearchIndexUpdates?.success}`);
  else console.log(`runPendingSearchIndexUpdates: erro (ignorado):`, idx.err?.details ?? idx.err);

  const summary = {
    DRY_RUN,
    productsInXlsx: slugs.length,
    productsFoundInDb: foundProducts.size,
    productsMissingInDb: slugs.length - foundProducts.size,
    variantsTouched: allVariantIds.length,
    appliedFacetValueIds: facetValueIdsToApply,
    errors: [],
  };

  const summaryPath = path.join(LOG_DIR, `apply-facets-to-all-variants-summary.${nowId()}.json`);
  writeJson(summaryPath, summary);
  console.log("DONE:", summary);
  console.log("Summary:", summaryPath);
}

main().catch((e) => {
  const logPath = path.join(LOG_DIR, `FATAL_apply-facets-to-all-variants_${nowId()}.json`);
  writeJson(logPath, { message: e?.message ?? String(e), details: e?.details ?? null, response: e?.response ?? null, stack: e?.stack ?? null });
  console.error("FATAL (top-level):", e?.details ?? e);
  console.error("Log:", logPath);
  process.exit(1);
});
