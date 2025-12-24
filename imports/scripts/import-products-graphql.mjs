#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { execFileSync } from "node:child_process";
import xlsx from "xlsx";

/**
 * ENV
 */
const ADMIN_API_URL = process.env.ADMIN_API_URL ?? "http://localhost:3000/admin-api";
const COOKIE_JAR = process.env.COOKIE_JAR ?? path.join(process.env.HOME ?? "/root", "carsandvibes/cookie-plain.jar");

const INPUT_XLSX =
  process.env.INPUT_XLSX ??
  path.join(process.env.HOME ?? "/root", "carsandvibes/imports/source/2026_RRP_OMP_23102025_variants_options_prepared.xlsx");

const SHEET_NAME = (process.env.SHEET_NAME ?? "").trim(); // se vazio, usa primeira sheet
const DRY_RUN = (process.env.DRY_RUN ?? "1") === "1";

const LIMIT_PRODUCTS = Number(process.env.LIMIT_PRODUCTS ?? "0"); // 0 = sem limite
const ONLY_PRODUCT_SLUG = (process.env.ONLY_PRODUCT_SLUG ?? "").trim(); // filtra por slug final
const ONLY_PRODUCT_CODE = (process.env.ONLY_PRODUCT_CODE ?? "").trim(); // filtra por ProductCode

const BATCH_VARIANTS = Number(process.env.BATCH_VARIANTS ?? "50");
const LOG_DIR = process.env.LOG_DIR ?? path.join(process.env.HOME ?? "/root", "carsandvibes/imports/logs");

// idiomas disponíveis: en, pt, fr, es
const LANGUAGE_CODE = (process.env.LANGUAGE_CODE ?? "pt").trim(); // "pt"
const ALSO_EN = (process.env.ALSO_EN ?? "0") === "1"; // cria tradução EN também (opcional)

// channel
const CHANNEL_ID = (process.env.CHANNEL_ID ?? "1").trim();

// auto collections
const AUTO_COLLECTIONS = (process.env.AUTO_COLLECTIONS ?? "1") === "1"; // tenta meter em collection
const FORCE_COLLECTION_ID = (process.env.FORCE_COLLECTION_ID ?? "").trim(); // força uma collection
const FORCE_COLLECTION_SLUG = (process.env.FORCE_COLLECTION_SLUG ?? "").trim(); // força slug de collection

// brand facet (opcional)
const BRAND_FACET_CODE = (process.env.BRAND_FACET_CODE ?? "brand").trim();
const BRAND_VALUE_NAME = (process.env.BRAND_VALUE_NAME ?? "OMP").trim();

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
  return json;
}

function safeGraphQL(query, variables = {}) {
  try {
    return { ok: true, data: curlGraphQL(query, variables) };
  } catch (e) {
    return { ok: false, err: e };
  }
}

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

function moneyToInt(v) {
  // no teu ficheiro parece já vir em minor units (ex: 129900)
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return Math.round(v);
  const s = String(v).trim();
  if (!s) return null;
  const cleaned = s.replace(/[^\d.]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  if (!Number.isFinite(n)) return null;
  return Math.round(n);
}

/**
 * Introspection helpers
 */
function getTypeFields(typeName) {
  const q = `
    query($name:String!){
      __type(name:$name){
        name
        kind
        fields { name type { kind name ofType{ kind name ofType{ kind name }}}}
        inputFields { name type { kind name ofType{ kind name ofType{ kind name }}}}
      }
    }
  `;
  const r = curlGraphQL(q, { name: typeName });
  return r.data.__type;
}

function mutationExists(name) {
  const q = `query{ __schema{ mutationType{ fields{ name } } } }`;
  const r = curlGraphQL(q);
  return (r.data.__schema?.mutationType?.fields ?? []).some((f) => f.name === name);
}

function whoAmI() {
  const q = `query{ activeAdministrator{ id firstName lastName emailAddress } }`;
  const r = curlGraphQL(q);
  const a = r.data.activeAdministrator;
  const name = [a?.firstName, a?.lastName].filter(Boolean).join(" ") || "(sem nome)";
  const email = a?.emailAddress || "(sem emailAddress)";
  console.log(`Auth OK: ${name} <${email}> (id=${a?.id ?? "?"})`);
}

/**
 * XLSX load
 */
function loadProductsFromXlsx() {
  const wb = xlsx.readFile(INPUT_XLSX);
  const sheetName = SHEET_NAME || wb.SheetNames[0];
  const ws = wb.Sheets[sheetName];
  if (!ws) throw new Error(`Sheet não encontrada: ${sheetName}`);

  const rows = xlsx.utils.sheet_to_json(ws, { defval: "" });

  const byCode = new Map();

  for (const r of rows) {
    const productCode = normVal(r["ProductCode"]);
    const variantSku = normVal(r["VariantSKU"] || r["SKU"] || r["VariantSku"]);
    const productName = normVal(r["ProductName"] || r["SHORT DESCRIPTION"] || r["Product Name"]);
    const longDesc = normVal(r["LONG DESCRIPTION"] || r["LONG_DESCRIPTION"] || r["Description"]);
    const price = r["RRP 2026"] ?? r["RRP"] ?? r["price"] ?? "";

    const optionGroup1 = normVal(r["OptionGroup1"]);
    const optionGroup2 = normVal(r["OptionGroup2"]);
    const optionValue1 = normVal(r["OptionValue1"]);
    const optionValue2 = normVal(r["OptionValue2"]);

    const color = normVal(r["Color"]);
    const size = normVal(r["Size"]);

    // categorias (para collections)
    const cat4 = normVal(r["Categoryn4"]);
    const cat3 = normVal(r["Categoryn3"]);
    const cat2 = normVal(r["Categoryn2"]);
    const cat1 = normVal(r["Categoryn1"]);
    const cat0 = normVal(r["CATEGORY"]);

    if (!productCode) continue;
    if (!variantSku) continue;

    if (!byCode.has(productCode)) {
      byCode.set(productCode, {
        productCode,
        productName: productName || productCode,
        longDesc,
        categories: { cat4, cat3, cat2, cat1, cat0 },
        lines: [],
      });
    }

    byCode.get(productCode).lines.push({
      productCode,
      productName,
      longDesc,
      sku: variantSku,
      price,
      optionGroup1,
      optionGroup2,
      optionValue1,
      optionValue2,
      color,
      size,
    });
  }

  let products = [];
  for (const [productCode, p] of byCode.entries()) {
    const baseSlug = slugify(p.productName);
    const finalSlug = `${baseSlug}-${slugify(productCode)}`;

    let hasColor = false;
    let hasSize = false;
    for (const l of p.lines) {
      const og1 = slugify(l.optionGroup1);
      const og2 = slugify(l.optionGroup2);
      if (og1.includes("color") || og2.includes("color") || l.color) hasColor = true;
      if (og1.includes("size") || og2.includes("size") || l.size) hasSize = true;
    }

    products.push({
      productCode,
      productName: p.productName,
      longDesc: p.longDesc,
      productSlug: finalSlug,
      hasColor,
      hasSize,
      categories: p.categories,
      lines: p.lines,
    });
  }

  if (ONLY_PRODUCT_CODE) products = products.filter((p) => p.productCode === ONLY_PRODUCT_CODE);
  if (ONLY_PRODUCT_SLUG) products = products.filter((p) => p.productSlug === ONLY_PRODUCT_SLUG);
  if (LIMIT_PRODUCTS > 0) products = products.slice(0, LIMIT_PRODUCTS);

  return products;
}

/**
 * Queries/Mutations (compatível com o teu schema observado)
 */
const Q_OPTION_GROUPS = `
query {
  productOptionGroups {
    id
    code
    name
    options {
      id
      code
      name
    }
  }
}`;

const M_CREATE_OPTION_GROUP = `
mutation($input: CreateProductOptionGroupInput!) {
  createProductOptionGroup(input: $input) {
    id
    code
    name
    options { id code name }
  }
}`;

const M_CREATE_OPTION = `
mutation($input: CreateProductOptionInput!) {
  createProductOption(input: $input) {
    id
    code
    name
  }
}`;

const Q_PRODUCTS_BY_SLUG = `
query($slug: String!) {
  products(options: { take: 1, filter: { slug: { eq: $slug } } }) {
    items { id slug name }
    totalItems
  }
}`;

const M_CREATE_PRODUCT = `
mutation($input: CreateProductInput!) {
  createProduct(input: $input) {
    id
    slug
    name
  }
}`;

const M_ADD_GROUP_TO_PRODUCT = `
mutation($productId: ID!, $optionGroupId: ID!) {
  addOptionGroupToProduct(productId: $productId, optionGroupId: $optionGroupId) {
    id
  }
}`;

const M_CREATE_VARIANTS = `
mutation($input: [CreateProductVariantInput!]!) {
  createProductVariants(input: $input) {
    id
    sku
  }
}`;

const Q_VARIANT_BY_SKU = `
query($sku: String!) {
  productVariants(options: { take: 1, filter: { sku: { eq: $sku } } }) {
    items { id sku }
    totalItems
  }
}`;

const M_RUN_PENDING_INDEX = `
mutation {
  runPendingSearchIndexUpdates { success }
}`;

const Q_CHANNELS = `
query{
  channels{ totalItems items{ id code token } }
}`;

const M_ASSIGN_PRODUCTS_TO_CHANNEL = `
mutation($input: AssignProductsToChannelInput!){
  assignProductsToChannel(input:$input){ id slug }
}`;

// collections
const Q_COLLECTIONS = `
query{
  collections(options:{take:200}){
    items{ id slug name }
    totalItems
  }
}`;

const Q_COLLECTION_VARIANTS_TRY1 = `
query($id:ID!){
  collection(id:$id){
    id
    slug
    productVariants{
      totalItems
      items{ id }
    }
  }
}`;

const Q_COLLECTION_VARIANTS_TRY2 = `
query($id:ID!){
  collection(id:$id){
    id
    slug
    productVariants{ id }
  }
}`;

const M_UPDATE_COLLECTION = `
mutation($input: UpdateCollectionInput!){
  updateCollection(input:$input){ id slug }
}`;

// facets/brand (best-effort)
const Q_FACETS_TRY = `
query{
  facets(options:{take:200}){
    items{
      id
      code
      name
      values{ id code name }
    }
    totalItems
  }
}`;

const M_CREATE_FACET = `
mutation($input: CreateFacetInput!){
  createFacet(input:$input){
    id
    code
    name
    values{ id code name }
  }
}`;

const M_CREATE_FACET_VALUE = `
mutation($input: CreateFacetValueInput!){
  createFacetValue(input:$input){
    id
    code
    name
  }
}`;

/**
 * Cache
 */
let optionGroupsCache = null;
let collectionsCache = null;
let facetsCache = null;

function loadOptionGroups() {
  if (optionGroupsCache) return optionGroupsCache;
  const r = curlGraphQL(Q_OPTION_GROUPS);
  optionGroupsCache = r.data.productOptionGroups || [];
  return optionGroupsCache;
}

function findOptionGroupByCode(code) {
  return loadOptionGroups().find((g) => g.code === code) || null;
}

function ensureOptionGroup(code, displayName) {
  const existing = findOptionGroupByCode(code);
  if (existing) return existing;

  if (DRY_RUN) {
    console.log(`[DRY] criar optionGroup ${code} (${displayName})`);
    const fake = { id: `DRY_group_${code}`, code, name: displayName, options: [] };
    optionGroupsCache = (optionGroupsCache || []).concat([fake]);
    return fake;
  }

  const input = {
    code,
    translations: [
      { languageCode: LANGUAGE_CODE, name: displayName },
      ...(ALSO_EN ? [{ languageCode: "en", name: displayName }] : []),
    ],
    options: [],
  };

  const r = curlGraphQL(M_CREATE_OPTION_GROUP, { input });
  optionGroupsCache = null;
  return r.data.createProductOptionGroup;
}

function ensureOption(groupCode, productOptionGroupId, optionName) {
  const name = normVal(optionName);
  const code = slugify(name);

  const group = findOptionGroupByCode(groupCode);
  const existing = group?.options?.find((o) => o.code === code);
  if (existing) return existing;

  if (DRY_RUN) {
    console.log(`[DRY] criar option ${groupCode}:${code} (${name})`);
    const fake = { id: `DRY_opt_${groupCode}_${code}`, code, name };
    if (group) {
      group.options = group.options || [];
      group.options.push(fake);
    }
    return fake;
  }

  const input = {
    productOptionGroupId,
    code,
    translations: [
      { languageCode: LANGUAGE_CODE, name },
      ...(ALSO_EN ? [{ languageCode: "en", name }] : []),
    ],
  };

  const r = curlGraphQL(M_CREATE_OPTION, { input });
  optionGroupsCache = null;
  return r.data.createProductOption;
}

function findProductBySlug(slug) {
  const r = curlGraphQL(Q_PRODUCTS_BY_SLUG, { slug });
  return (r.data.products?.items ?? [])[0] ?? null;
}

function createProductIfMissing(p) {
  const existing = findProductBySlug(p.productSlug);
  if (existing) return { product: existing, created: false };

  if (DRY_RUN) {
    console.log(`[DRY] criar product ${p.productName} (${p.productSlug})`);
    return { product: { id: `DRY_${p.productSlug}`, slug: p.productSlug, name: p.productName }, created: true };
  }

  const translations = [
    {
      languageCode: LANGUAGE_CODE,
      name: p.productName,
      slug: p.productSlug,
      description: p.longDesc || "",
    },
  ];

  if (ALSO_EN) {
    translations.push({
      languageCode: "en",
      name: p.productName,
      slug: p.productSlug,
      description: p.longDesc || "",
    });
  }

  const input = {
    enabled: true,
    translations,
  };

  const r = curlGraphQL(M_CREATE_PRODUCT, { input });
  return { product: r.data.createProduct, created: true };
}

function addOptionGroupToProduct(productId, optionGroupId, groupCode) {
  if (DRY_RUN) {
    console.log(`[DRY] addOptionGroupToProduct product=${productId} group=${groupCode}`);
    return;
  }
  const res = safeGraphQL(M_ADD_GROUP_TO_PRODUCT, { productId, optionGroupId });
  if (!res.ok) {
    const msg = res.err?.details?.[0]?.message ?? String(res.err);
    if (msg.includes("already assigned") || msg.toLowerCase().includes("already")) return;
    throw res.err;
  }
}

function variantExistsBySku(sku) {
  const r = curlGraphQL(Q_VARIANT_BY_SKU, { sku });
  return (r.data.productVariants?.totalItems ?? 0) > 0;
}

function extractColorSize(line) {
  const og1 = slugify(line.optionGroup1);
  const og2 = slugify(line.optionGroup2);

  let colorVal = "";
  let sizeVal = "";

  if (og1.includes("color")) colorVal = line.optionValue1;
  if (og2.includes("color")) colorVal = line.optionValue2;

  if (og1.includes("size")) sizeVal = line.optionValue1;
  if (og2.includes("size")) sizeVal = line.optionValue2;

  if (!colorVal) colorVal = line.color;
  if (!sizeVal) sizeVal = line.size;

  return { colorVal: normVal(colorVal), sizeVal: normVal(sizeVal) };
}

/**
 * Channels
 */
function ensureChannel() {
  const r = curlGraphQL(Q_CHANNELS);
  const items = r.data.channels?.items ?? [];
  const found = items.find((c) => String(c.id) === String(CHANNEL_ID));
  if (!found) {
    console.log(`WARN: CHANNEL_ID=${CHANNEL_ID} não encontrado. Vou continuar (pode ficar só no default).`);
  }
  return found || items[0] || null;
}

function assignProductToChannel(productId) {
  if (DRY_RUN) return;
  if (!mutationExists("assignProductsToChannel")) return;

  const ch = ensureChannel();
  if (!ch) return;

  const res = safeGraphQL(M_ASSIGN_PRODUCTS_TO_CHANNEL, { input: { channelId: ch.id, productIds: [productId] } });
  if (!res.ok) {
    const msg = res.err?.details?.[0]?.message ?? String(res.err);
    console.log(`WARN: assignProductsToChannel falhou: ${msg}`);
  }
}

/**
 * Collections (best-effort, sem apagar membership)
 */
function loadCollections() {
  if (collectionsCache) return collectionsCache;
  const r = curlGraphQL(Q_COLLECTIONS);
  collectionsCache = r.data.collections?.items ?? [];
  return collectionsCache;
}

function pickCollectionIdForProduct(p) {
  if (FORCE_COLLECTION_ID) return FORCE_COLLECTION_ID;

  const colls = loadCollections();
  const slugToId = new Map(colls.map((c) => [c.slug, c.id]));

  if (FORCE_COLLECTION_SLUG) {
    const id = slugToId.get(FORCE_COLLECTION_SLUG);
    if (id) return id;
    console.log(`WARN: FORCE_COLLECTION_SLUG=${FORCE_COLLECTION_SLUG} não existe. Vou tentar auto.`);
  }

  const cats = p.categories || {};
  const candidates = [cats.cat4, cats.cat3, cats.cat2, cats.cat1, cats.cat0].filter(Boolean);

  for (const c of candidates) {
    const s = slugify(c);
    if (slugToId.has(s)) return slugToId.get(s);
  }
  return null;
}

function getCollectionVariantIds(collectionId) {
  // tenta vários formatos porque o schema pode variar
  let r = safeGraphQL(Q_COLLECTION_VARIANTS_TRY1, { id: collectionId });
  if (r.ok) {
    const pv = r.data.data.collection?.productVariants;
    const items = pv?.items ?? [];
    return items.map((x) => x.id).filter(Boolean);
  }
  r = safeGraphQL(Q_COLLECTION_VARIANTS_TRY2, { id: collectionId });
  if (r.ok) {
    const pv = r.data.data.collection?.productVariants ?? [];
    return pv.map((x) => x.id).filter(Boolean);
  }
  return null;
}

function updateCollectionAddVariants(collectionId, newVariantIds) {
  if (DRY_RUN) return;
  if (!AUTO_COLLECTIONS) return;
  if (!mutationExists("updateCollection")) return;

  const inputType = getTypeFields("UpdateCollectionInput");
  const fields = new Set((inputType?.inputFields ?? []).map((f) => f.name));

  // só fazemos isto se existir productVariantIds no input
  if (!fields.has("productVariantIds")) {
    console.log("WARN: UpdateCollectionInput não tem productVariantIds. Vou ignorar collections para não estragar nada.");
    return;
  }

  const existing = getCollectionVariantIds(collectionId);
  if (existing === null) {
    console.log("WARN: Não consegui ler os productVariantIds atuais da collection. Vou ignorar para não apagar nada.");
    return;
  }

  const merged = Array.from(new Set([...existing, ...newVariantIds]));
  const res = safeGraphQL(M_UPDATE_COLLECTION, { input: { id: collectionId, productVariantIds: merged } });
  if (!res.ok) {
    const msg = res.err?.details?.[0]?.message ?? String(res.err);
    console.log(`WARN: updateCollection falhou: ${msg}`);
  } else {
    const slug = res.data.data.updateCollection?.slug ?? "?";
    console.log(`updateCollection: added ${newVariantIds.length} variantIds -> collection=${collectionId} (${slug})`);
  }
}

/**
 * Brand facet (best-effort)
 */
function loadFacets() {
  if (facetsCache) return facetsCache;
  const r = safeGraphQL(Q_FACETS_TRY);
  if (!r.ok) return null;
  facetsCache = r.data.data.facets?.items ?? [];
  return facetsCache;
}

function ensureBrandFacetValueId() {
  const facets = loadFacets();
  if (!facets) return null;

  let facet = facets.find((f) => f.code === BRAND_FACET_CODE);
  if (!facet) {
    if (DRY_RUN) {
      console.log(`[DRY] criar facet ${BRAND_FACET_CODE}`);
      return "DRY_facetValue_brand_omp";
    }
    if (!mutationExists("createFacet")) return null;

    // CreateFacetInput costuma aceitar: code, isPrivate, translations
    const inputType = getTypeFields("CreateFacetInput");
    const fields = new Set((inputType?.inputFields ?? []).map((f) => f.name));

    const input = { code: BRAND_FACET_CODE };
    if (fields.has("isPrivate")) input.isPrivate = false;
    if (fields.has("translations")) {
      input.translations = [
        { languageCode: LANGUAGE_CODE, name: BRAND_FACET_CODE.toUpperCase() },
        ...(ALSO_EN ? [{ languageCode: "en", name: BRAND_FACET_CODE.toUpperCase() }] : []),
      ];
    }

    const r = safeGraphQL(M_CREATE_FACET, { input });
    if (!r.ok) return null;

    facetsCache = null;
    facet = r.data.data.createFacet;
  }

  const valueCode = slugify(BRAND_VALUE_NAME); // "omp"
  const existingValue = (facet.values ?? []).find((v) => v.code === valueCode);
  if (existingValue) return existingValue.id;

  if (DRY_RUN) {
    console.log(`[DRY] criar facetValue ${BRAND_FACET_CODE}:${BRAND_VALUE_NAME}`);
    return `DRY_facetValue_${BRAND_FACET_CODE}_${valueCode}`;
  }

  if (!mutationExists("createFacetValue")) return null;

  // CreateFacetValueInput costuma aceitar: facetId, code, translations
  const inputType = getTypeFields("CreateFacetValueInput");
  const fields = new Set((inputType?.inputFields ?? []).map((f) => f.name));

  const input = { code: valueCode };
  if (fields.has("facetId")) input.facetId = facet.id;
  if (fields.has("translations")) {
    input.translations = [
      { languageCode: LANGUAGE_CODE, name: BRAND_VALUE_NAME },
      ...(ALSO_EN ? [{ languageCode: "en", name: BRAND_VALUE_NAME }] : []),
    ];
  }

  const r = safeGraphQL(M_CREATE_FACET_VALUE, { input });
  if (!r.ok) return null;

  facetsCache = null;
  return r.data.data.createFacetValue?.id ?? null;
}

function createVariantInputSupportsFacetValueIds() {
  try {
    const t = getTypeFields("CreateProductVariantInput");
    const names = new Set((t?.inputFields ?? []).map((f) => f.name));
    return names.has("facetValueIds");
  } catch {
    return false;
  }
}

/**
 * Index updates
 */
function runPendingSearchIndexUpdates() {
  if (DRY_RUN) return;
  const res = safeGraphQL(M_RUN_PENDING_INDEX);
  if (!res.ok) {
    const msg = res.err?.details?.[0]?.message ?? String(res.err);
    console.log(`WARN: runPendingSearchIndexUpdates falhou: ${msg}`);
    return;
  }
  console.log(`runPendingSearchIndexUpdates: success=${res.data.data.runPendingSearchIndexUpdates?.success === true}`);
}

async function run() {
  whoAmI();

  // prepara caches
  loadOptionGroups();
  if (AUTO_COLLECTIONS) loadCollections();

  const products = loadProductsFromXlsx();
  console.log(
    `Produtos encontrados: ${products.length}` +
      (ONLY_PRODUCT_SLUG ? ` (ONLY_PRODUCT_SLUG=${ONLY_PRODUCT_SLUG})` : "") +
      (ONLY_PRODUCT_CODE ? ` (ONLY_PRODUCT_CODE=${ONLY_PRODUCT_CODE})` : "")
  );

  const summary = {
    DRY_RUN,
    productsProcessed: 0,
    createdProducts: 0,
    createdVariants: 0,
    skippedExistingSkus: 0,
    skippedDuplicateCombos: 0,
    skippedMissingOptionValues: 0,
    errors: [],
  };

  // brand facet (best-effort)
  const brandFacetValueId = ensureBrandFacetValueId();
  const variantSupportsFacetValueIds = createVariantInputSupportsFacetValueIds();
  if (brandFacetValueId && variantSupportsFacetValueIds) {
    console.log(`Brand OK: facetValueId=${brandFacetValueId} (vai aplicar nas variantes)`);
  } else if (brandFacetValueId && !variantSupportsFacetValueIds) {
    console.log(`WARN: CreateProductVariantInput não suporta facetValueIds -> não consigo aplicar brand automaticamente.`);
  }

  for (const p of products) {
    summary.productsProcessed += 1;

    try {
      // 1) criar/obter produto
      const { product: prod, created } = createProductIfMissing(p);
      if (created) summary.createdProducts += 1;

      const productId = prod.id;

      // 2) channel
      assignProductToChannel(productId);

      // 3) optionGroups por produto (EVITA conflitos)
      const pc = slugify(p.productCode); // ex: ia0-1876-a01
      const colorGroupCode = `color-${pc}`;
      const sizeGroupCode = `size-${pc}`;

      const colorGroup = p.hasColor ? ensureOptionGroup(colorGroupCode, "Color") : null;
      const sizeGroup = p.hasSize ? ensureOptionGroup(sizeGroupCode, "Size") : null;

      if (p.hasColor && colorGroup) addOptionGroupToProduct(productId, colorGroup.id, colorGroupCode);
      if (p.hasSize && sizeGroup) addOptionGroupToProduct(productId, sizeGroup.id, sizeGroupCode);

      // 4) needed options
      const neededColor = new Set();
      const neededSize = new Set();
      for (const line of p.lines) {
        const { colorVal, sizeVal } = extractColorSize(line);
        if (p.hasColor && colorVal) neededColor.add(colorVal);
        if (p.hasSize && sizeVal) neededSize.add(sizeVal);
      }

      const colorIdByCode = new Map();
      const sizeIdByCode = new Map();

      if (p.hasColor && colorGroup) {
        for (const c of neededColor) {
          const opt = ensureOption(colorGroupCode, colorGroup.id, c);
          colorIdByCode.set(opt.code, opt.id);
        }
      }
      if (p.hasSize && sizeGroup) {
        for (const s of neededSize) {
          const opt = ensureOption(sizeGroupCode, sizeGroup.id, s);
          sizeIdByCode.set(opt.code, opt.id);
        }
      }

      // 5) construir inputs variantes
      const inputs = [];
      const seenCombos = new Set();

      let skipSku = 0,
        skipMissing = 0,
        skipDupCombo = 0;

      for (const line of p.lines) {
        const sku = normVal(line.sku);
        if (!sku) continue;

        if (!DRY_RUN && variantExistsBySku(sku)) {
          summary.skippedExistingSkus += 1;
          skipSku += 1;
          continue;
        }

        const { colorVal, sizeVal } = extractColorSize(line);

        const optionIds = [];
        let comboKey = "";

        if (p.hasColor) {
          const cCode = slugify(colorVal);
          const id = colorIdByCode.get(cCode);
          if (!id) {
            summary.skippedMissingOptionValues += 1;
            skipMissing += 1;
            continue;
          }
          optionIds.push(id);
          comboKey += `c:${cCode}|`;
        }

        if (p.hasSize) {
          const sCode = slugify(sizeVal);
          const id = sizeIdByCode.get(sCode);
          if (!id) {
            summary.skippedMissingOptionValues += 1;
            skipMissing += 1;
            continue;
          }
          optionIds.push(id);
          comboKey += `s:${sCode}|`;
        }

        if (comboKey) {
          if (seenCombos.has(comboKey)) {
            summary.skippedDuplicateCombos += 1;
            skipDupCombo += 1;
            continue;
          }
          seenCombos.add(comboKey);
        }

        const price = moneyToInt(line.price);
        const variantName = [normVal(colorVal), normVal(sizeVal)].filter(Boolean).join(" / ") || p.productName;

        const translations = [{ languageCode: LANGUAGE_CODE, name: variantName }];
        if (ALSO_EN) translations.push({ languageCode: "en", name: variantName });

        const one = {
          productId,
          enabled: true,
          sku,
          price: price ?? 0,
          optionIds,
          translations,
        };

        if (brandFacetValueId && variantSupportsFacetValueIds) {
          one.facetValueIds = [brandFacetValueId];
        }

        inputs.push(one);
      }

      console.log(
        `Produto ${p.productSlug}: linhas=${p.lines.length} inputs=${inputs.length} skipSku=${skipSku} skipMissing=${skipMissing} skipDupCombo=${skipDupCombo}`
      );

      if (!inputs.length) {
        console.log(`SKIP createProductVariants (0 inputs) product=${p.productSlug}`);
        continue;
      }

      // 6) criar variantes em batches
      const createdVariantIds = [];

      for (let i = 0; i < inputs.length; i += BATCH_VARIANTS) {
        const batch = inputs.slice(i, i + BATCH_VARIANTS);
        if (!batch.length) continue;

        if (DRY_RUN) {
          console.log(
            `[DRY] createProductVariants product=${p.productSlug} batch (${batch.length}) [${batch[0].sku}..${batch[batch.length - 1].sku}]`
          );
          summary.createdVariants += batch.length;
          continue;
        }

        const res = safeGraphQL(M_CREATE_VARIANTS, { input: batch });
        if (!res.ok) {
          const logPath = path.join(LOG_DIR, `FATAL_${p.productSlug}_${nowId()}.json`);
          writeJson(logPath, {
            product: p.productSlug,
            message: "GraphQL errors",
            details: res.err?.details ?? String(res.err),
            response: res.err?.response ?? null,
            batchSample: { firstSku: batch[0]?.sku, lastSku: batch[batch.length - 1]?.sku, size: batch.length, example: batch[0] },
          });

          summary.errors.push({ product: p.productSlug, log: logPath });
          throw res.err;
        }

        const createdList = res.data.data.createProductVariants ?? [];
        console.log(`OK createProductVariants product=${p.productSlug} (${createdList.length})`);
        summary.createdVariants += createdList.length;

        for (const v of createdList) {
          if (v?.id) createdVariantIds.push(v.id);
        }
      }

      // 7) collections (best-effort)
      if (!DRY_RUN && AUTO_COLLECTIONS && createdVariantIds.length) {
        const collectionId = pickCollectionIdForProduct(p);
        if (collectionId) {
          updateCollectionAddVariants(collectionId, createdVariantIds);
        } else {
          // não encontrou match de categoria -> não mexe
        }
      }
    } catch (e) {
      const logPath = path.join(LOG_DIR, `FATAL_${p.productSlug}_${nowId()}.json`);
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
      break;
    }
  }

  runPendingSearchIndexUpdates();

  const summaryPath = path.join(LOG_DIR, `import-products-summary.${nowId()}.json`);
  writeJson(summaryPath, summary);
  console.log("DONE:", summary);
  console.log("Summary:", summaryPath);
}

run().catch((e) => {
  console.error("FATAL (top-level):", e);
  process.exit(1);
});
