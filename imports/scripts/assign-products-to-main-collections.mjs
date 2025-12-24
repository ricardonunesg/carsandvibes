#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { execFileSync } from "node:child_process";
import xlsx from "xlsx";

console.log("START assign-products-to-main-collections");

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

const SHEET_NAME = (process.env.SHEET_NAME ?? "").trim();
const DRY_RUN = (process.env.DRY_RUN ?? "1") === "1";
const LIMIT_PRODUCTS = Number(process.env.LIMIT_PRODUCTS ?? "0");
const ONLY_PRODUCT_SLUG = (process.env.ONLY_PRODUCT_SLUG ?? "").trim();
const ONLY_PRODUCT_CODE = (process.env.ONLY_PRODUCT_CODE ?? "").trim();

const LOG_DIR =
  process.env.LOG_DIR ?? path.join(process.env.HOME ?? "/root", "carsandvibes/imports/logs");

const LANGUAGE_CODE = (process.env.LANGUAGE_CODE ?? "pt").trim(); // en/pt/fr/es
const ALSO_EN = (process.env.ALSO_EN ?? "0") === "1";

const FACET_CODE = (process.env.FACET_CODE ?? "category").trim();
const FACET_NAME = (process.env.FACET_NAME ?? "Category").trim();

// main collection “nível 1”
const MAIN_COLLECTION_SLUG = (process.env.MAIN_COLLECTION_SLUG ?? "store").trim();

// Se quiseres forçar mapping em vez de heurística:
// MAPPING_JSON='{"cockpit":["cockpit"],"driver":["driver","fia-driver-gear"]}'
const MAPPING_JSON = (process.env.MAPPING_JSON ?? "").trim();

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
 * Helpers
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

function normVal(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "number") return String(v).replace(/\.0+$/, "").trim();
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
 * XLSX -> lista de produtos pai (slugs finais iguais ao import)
 */
function loadParentProductSlugsFromXlsx() {
  const wb = xlsx.readFile(INPUT_XLSX);
  const sheetName = SHEET_NAME || wb.SheetNames[0];
  const ws = wb.Sheets[sheetName];
  if (!ws) throw new Error(`Sheet não encontrada: ${sheetName}`);

  const rows = xlsx.utils.sheet_to_json(ws, { defval: "" });

  const byCode = new Map();
  for (const r of rows) {
    const productCode = normVal(r["ProductCode"]);
    const productName = normVal(r["ProductName"] || r["SHORT DESCRIPTION"] || r["Product Name"]);
    const sku = normVal(r["VariantSKU"] || r["SKU"] || r["VariantSku"]);

    if (!productCode) continue;
    // só conta produtos que tenham pelo menos uma linha de variante
    if (!sku) continue;

    if (!byCode.has(productCode)) {
      const baseSlug = slugify(productName || productCode);
      const finalSlug = `${baseSlug}-${slugify(productCode)}`;
      byCode.set(productCode, {
        productCode,
        productName: productName || productCode,
        productSlug: finalSlug,
      });
    }
  }

  let products = [...byCode.values()];

  if (ONLY_PRODUCT_CODE) products = products.filter((p) => p.productCode === ONLY_PRODUCT_CODE);
  if (ONLY_PRODUCT_SLUG) products = products.filter((p) => p.productSlug === ONLY_PRODUCT_SLUG);
  if (LIMIT_PRODUCTS > 0) products = products.slice(0, LIMIT_PRODUCTS);

  return products;
}

/**
 * Queries / Mutations
 */
const Q_COLLECTIONS = `
query($take:Int!){
  collections(options:{take:$take}){
    totalItems
    items{ id slug name }
  }
}`;

const Q_COLLECTION_FILTERS = `
query{
  collectionFilters{
    code
    args { name type }
  }
}`;

const Q_FACETS = `
query($take:Int!){
  facets(options:{take:$take}){
    totalItems
    items{
      id code name
      values{ id code name }
    }
  }
}`;

const M_CREATE_FACET = `
mutation($input: CreateFacetInput!){
  createFacet(input:$input){
    id code name
    values{ id code name }
  }
}`;

const M_CREATE_FACET_VALUE = `
mutation($input: CreateFacetValueInput!){
  createFacetValue(input:$input){
    id code name
  }
}`;

const M_UPDATE_COLLECTION = `
mutation($input: UpdateCollectionInput!){
  updateCollection(input:$input){
    id slug name
  }
}`;

const Q_PRODUCT_BY_SLUG_WITH_VARIANTS = `
query($slug:String!){
  products(options:{take:1, filter:{slug:{eq:$slug}}}){
    items{
      id slug
      variants{ id }
    }
    totalItems
  }
}`;

/**
 * Introspection helpers p/ updateProductVariant(s)
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
  const r = curlGraphQL(`
    query {
      __schema {
        mutationType {
          fields {
            name
            args {
              name
              type { kind name ofType { kind name ofType { kind name ofType { kind name }}}}
            }
          }
        }
      }
    }
  `);

  const fields = r.__schema?.mutationType?.fields ?? [];
  const m = fields.find((f) => f.name === mutationName);
  if (!m) throw new Error(`Mutation não encontrada: ${mutationName}`);

  const inputArg = (m.args ?? []).find((a) => a.name === "input") ?? (m.args ?? [])[0];
  if (!inputArg) throw new Error(`Mutation ${mutationName} não tem args?!`);

  const typeName = unwrapNamedType(inputArg.type);
  if (!typeName) throw new Error(`Não consegui resolver input type da mutation ${mutationName}`);
  return typeName;
}

function getInputFields(typeName) {
  const r = curlGraphQL(
    `
    query($name:String!){
      __type(name:$name){
        name
        inputFields{ name type{ kind name ofType{ kind name ofType{ kind name }}}}
      }
    }
  `,
    { name: typeName }
  );
  const t = r.__type;
  return t?.inputFields ?? [];
}

function pickField(fields, candidates) {
  const names = fields.map((f) => f.name);
  for (const c of candidates) if (names.includes(c)) return c;
  return null;
}

let _updatePlan = null;
function getUpdateVariantPlanCached() {
  if (_updatePlan) return _updatePlan;

  // BULK
  const bulkInputType = getMutationInputTypeName("updateProductVariants");
  const bulkFields = getInputFields(bulkInputType);

  const bulkIdsField = pickField(bulkFields, ["ids", "productVariantIds", "variantIds"]);
  const bulkFacetField = pickField(bulkFields, ["facetValueIds", "facetValues", "facetValueIdsToAdd"]);

  if (bulkIdsField && bulkFacetField) {
    _updatePlan = { mode: "bulk", inputType: bulkInputType, idsField: bulkIdsField, facetField: bulkFacetField };
    console.log(`Update plan: mode=${_updatePlan.mode} inputType=${_updatePlan.inputType}`);
    return _updatePlan;
  }

  // SINGLE
  const singleInputType = getMutationInputTypeName("updateProductVariant");
  const singleFields = getInputFields(singleInputType);

  const singleIdField = pickField(singleFields, ["id", "productVariantId", "variantId"]);
  const singleFacetField = pickField(singleFields, ["facetValueIds", "facetValues", "facetValueIdsToAdd"]);

  if (singleIdField && singleFacetField) {
    _updatePlan = { mode: "single", inputType: singleInputType, idField: singleIdField, facetField: singleFacetField };
    console.log(`Update plan: mode=${_updatePlan.mode} inputType=${_updatePlan.inputType}`);
    return _updatePlan;
  }

  const bulkNames = bulkFields.map((f) => f.name).join(", ");
  const singleNames = singleFields.map((f) => f.name).join(", ");
  throw new Error(
    `Não encontrei campos para aplicar facet values.\n` +
      `updateProductVariants input=${bulkInputType} fields=[${bulkNames}]\n` +
      `updateProductVariant input=${singleInputType} fields=[${singleNames}]`
  );
}

function updateVariantsFacetValues(variantIds, facetValueIds) {
  if (!variantIds.length) return;

  const plan = getUpdateVariantPlanCached();

  if (DRY_RUN) {
    console.log(`[DRY] apply facetValueIds=${facetValueIds.join(",")} to variants=${variantIds.length} (mode=${plan.mode})`);
    return;
  }

  if (plan.mode === "bulk") {
    const input = { [plan.idsField]: variantIds, [plan.facetField]: facetValueIds };
    const mutation = `mutation($input:${plan.inputType}!){ updateProductVariants(input:$input){ id } }`;
    const res = safeGraphQL(mutation, { input });
    if (!res.ok) throw res.err;
    return;
  }

  const mutation = `mutation($input:${plan.inputType}!){ updateProductVariant(input:$input){ id } }`;
  for (const id of variantIds) {
    const input = { [plan.idField]: id, [plan.facetField]: facetValueIds };
    const res = safeGraphQL(mutation, { input });
    if (!res.ok) throw res.err;
  }
}

/**
 * Load collections (take=1000 máximo no teu schema)
 */
function loadCollections() {
  const r = curlGraphQL(Q_COLLECTIONS, { take: 1000 });
  return r.collections?.items ?? [];
}

/**
 * Facet + FacetValues
 */
function loadFacetByCode(code) {
  const r = curlGraphQL(Q_FACETS, { take: 1000 });
  const items = r.facets?.items ?? [];
  return items.find((f) => f.code === code) ?? null;
}

function ensureFacet() {
  const existing = loadFacetByCode(FACET_CODE);
  if (existing) return existing;

  if (DRY_RUN) {
    console.log(`[DRY] createFacet ${FACET_CODE} (${FACET_NAME})`);
    return { id: `DRY_FACET_${FACET_CODE}`, code: FACET_CODE, name: FACET_NAME, values: [] };
  }

  const input = {
    code: FACET_CODE,
    isPrivate: false,
    translations: [
      { languageCode: LANGUAGE_CODE, name: FACET_NAME },
      ...(ALSO_EN ? [{ languageCode: "en", name: FACET_NAME }] : []),
    ],
  };

  const r = curlGraphQL(M_CREATE_FACET, { input });
  return r.createFacet;
}

function ensureFacetValue(facetId, facetValueCode, facetValueName) {
  const facet = loadFacetByCode(FACET_CODE);
  const existing = facet?.values?.find((v) => v.code === facetValueCode);
  if (existing) return existing;

  if (DRY_RUN) {
    console.log(`[DRY] createFacetValue ${FACET_CODE}:${facetValueCode} (${facetValueName})`);
    return { id: `DRY_FV_${FACET_CODE}_${facetValueCode}`, code: facetValueCode, name: facetValueName };
  }

  const input = {
    facetId,
    code: facetValueCode,
    translations: [
      { languageCode: LANGUAGE_CODE, name: facetValueName },
      ...(ALSO_EN ? [{ languageCode: "en", name: facetValueName }] : []),
    ],
  };

  const r = curlGraphQL(M_CREATE_FACET_VALUE, { input });
  return r.createFacetValue;
}

/**
 * CollectionFilter: facet-value-filter
 */
function loadFacetValueFilterShape() {
  const r = curlGraphQL(Q_COLLECTION_FILTERS);
  const filters = r.collectionFilters ?? [];
  const fvf = filters.find((f) => f.code === "facet-value-filter");
  if (!fvf) throw new Error("Não encontrei collectionFilter facet-value-filter");
  return fvf;
}

function buildFacetValueFilterOperation(facetValueId) {
  const fvf = loadFacetValueFilterShape();
  const argNames = (fvf.args ?? []).map((a) => a.name);

  // Vendure normalmente: facetValueIds (lista) ou facetValueId (single)
  const args = [];

  if (argNames.includes("facetValueIds")) {
    // ConfigurableOperation arg values são strings, por isso passamos JSON string de array
    args.push({ name: "facetValueIds", value: JSON.stringify([facetValueId]) });
  } else if (argNames.includes("facetValueId")) {
    args.push({ name: "facetValueId", value: String(facetValueId) });
  } else {
    // fallback: mete o primeiro arg com o id
    if (!argNames.length) throw new Error("facet-value-filter não tem args no schema?!");
    args.push({ name: argNames[0], value: String(facetValueId) });
  }

  // alguns schemas têm “containsAny”
  if (argNames.includes("containsAny")) {
    args.push({ name: "containsAny", value: "true" });
  }

  return { code: "facet-value-filter", arguments: args };
}

function updateCollectionFilter(collectionId, collectionSlug, facetValueId) {
  const filterOp = buildFacetValueFilterOperation(facetValueId);

  if (DRY_RUN) {
    console.log(`[DRY] updateCollection ${collectionSlug} set facet-value-filter facetValueId=${facetValueId}`);
    return;
  }

  const input = {
    id: collectionId,
    filters: [filterOp],
  };

  const res = safeGraphQL(M_UPDATE_COLLECTION, { input });
  if (!res.ok) throw res.err;
}

/**
 * Product -> variants ids
 */
function loadProductVariantIdsBySlug(slug) {
  const r = curlGraphQL(Q_PRODUCT_BY_SLUG_WITH_VARIANTS, { slug });
  const item = r.products?.items?.[0];
  if (!item) return { productId: null, variantIds: [] };
  const variantIds = (item.variants ?? []).map((v) => v.id).filter(Boolean);
  return { productId: item.id, variantIds };
}

/**
 * Decide collection(s) to apply
 * - always MAIN_COLLECTION_SLUG
 * - plus heuristic match of second-level collection based on slug includes collection slug
 * - optional mapping JSON
 */
function buildMapping(collections) {
  if (!MAPPING_JSON) return null;
  try {
    const obj = JSON.parse(MAPPING_JSON);
    return obj;
  } catch {
    console.warn("MAPPING_JSON inválido, vou ignorar.");
    return null;
  }
}

function pickSecondLevelCollectionSlug(productSlug, collections, mappingObj) {
  // mapping override: key matches productSlug substring, returns array of collection slugs
  if (mappingObj) {
    for (const key of Object.keys(mappingObj)) {
      if (productSlug.includes(key)) {
        const arr = mappingObj[key];
        if (Array.isArray(arr) && arr.length) return arr[0];
      }
    }
  }

  // heuristic: best match collection slug contained in productSlug, excluding main
  const candidates = collections
    .map((c) => c.slug)
    .filter((s) => s && s !== MAIN_COLLECTION_SLUG);

  let best = "";
  for (const s of candidates) {
    if (productSlug.includes(s) && s.length > best.length) best = s;
  }
  return best || "";
}

/**
 * Main
 */
async function main() {
  whoAmI();

  const products = loadParentProductSlugsFromXlsx();
  console.log(`Produtos únicos (pai) no XLSX: ${products.length}`);

  const collections = loadCollections();
  const collectionsBySlug = new Map(collections.map((c) => [c.slug, c]));

  if (!collectionsBySlug.has(MAIN_COLLECTION_SLUG)) {
    console.warn(`⚠️ MAIN_COLLECTION_SLUG='${MAIN_COLLECTION_SLUG}' não existe nas collections. Vou continuar sem main.`);
  }

  const mappingObj = buildMapping(collections);

  // 1) ensure facet
  const facet = ensureFacet();

  // 2) we will need facetValues for: main + any second-level we use
  const usedCollectionSlugs = new Set();
  if (collectionsBySlug.has(MAIN_COLLECTION_SLUG)) usedCollectionSlugs.add(MAIN_COLLECTION_SLUG);

  for (const p of products) {
    const second = pickSecondLevelCollectionSlug(p.productSlug, collections, mappingObj);
    if (second && collectionsBySlug.has(second)) usedCollectionSlugs.add(second);
  }

  // create facet values map
  const facetValueIdByCollectionSlug = new Map();
  for (const slug of usedCollectionSlugs) {
    const col = collectionsBySlug.get(slug);
    if (!col) continue;
    const fvCode = slugify(col.slug);
    const fvName = col.name;
    const fv = ensureFacetValue(facet.id, fvCode, fvName);
    facetValueIdByCollectionSlug.set(slug, fv.id);
  }

  // 3) update collections filters
  for (const slug of usedCollectionSlugs) {
    const col = collectionsBySlug.get(slug);
    const fvId = facetValueIdByCollectionSlug.get(slug);
    if (!col || !fvId) continue;
    updateCollectionFilter(col.id, col.slug, fvId);
  }

  // 4) apply facet values to variants
  const summary = {
    DRY_RUN,
    productsProcessed: 0,
    variantsTouched: 0,
    productsMissingInDb: 0,
    productsWithNoVariants: 0,
    appliedMain: 0,
    appliedSecond: 0,
    errors: [],
  };

  for (const p of products) {
    summary.productsProcessed += 1;

    const mainSlug = collectionsBySlug.has(MAIN_COLLECTION_SLUG) ? MAIN_COLLECTION_SLUG : "";
    const secondSlug = pickSecondLevelCollectionSlug(p.productSlug, collections, mappingObj);

    const facetValueIds = [];
    if (mainSlug) {
      const id = facetValueIdByCollectionSlug.get(mainSlug);
      if (id) facetValueIds.push(id);
    }
    if (secondSlug && collectionsBySlug.has(secondSlug)) {
      const id = facetValueIdByCollectionSlug.get(secondSlug);
      if (id) facetValueIds.push(id);
    }

    if (!facetValueIds.length) continue;

    try {
      const { productId, variantIds } = loadProductVariantIdsBySlug(p.productSlug);

      if (!productId) {
        summary.productsMissingInDb += 1;
        continue;
      }
      if (!variantIds.length) {
        summary.productsWithNoVariants += 1;
        continue;
      }

      console.log(
        `Produto ${p.productSlug}: variants=${variantIds.length} applyFacetValues=[${facetValueIds.join(",")}] second=${secondSlug || "-"}`
      );

      updateVariantsFacetValues(variantIds, facetValueIds);
      summary.variantsTouched += variantIds.length;
      if (mainSlug) summary.appliedMain += 1;
      if (secondSlug) summary.appliedSecond += 1;
    } catch (e) {
      const logPath = path.join(LOG_DIR, `FATAL_assign_${p.productSlug}_${nowId()}.json`);
      writeJson(logPath, {
        product: p.productSlug,
        message: e?.message ?? String(e),
        details: e?.details ?? null,
        response: e?.response ?? null,
        stack: e?.stack ?? null,
      });
      console.error("FATAL:", e?.details ?? e);
      console.error("Log:", logPath);
      summary.errors.push({ product: p.productSlug, log: logPath });
      // parar no primeiro fatal
      break;
    }
  }

  const summaryPath = path.join(LOG_DIR, `assign-products-to-main-collections-summary.${nowId()}.json`);
  writeJson(summaryPath, summary);
  console.log("DONE:", summary);
  console.log("Summary:", summaryPath);
}

main().catch((e) => {
  console.error("FATAL (top-level):", e);
  process.exit(1);
});
