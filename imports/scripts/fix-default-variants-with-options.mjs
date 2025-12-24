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
const COOKIE_JAR =
  process.env.COOKIE_JAR ?? path.join(process.env.HOME ?? "/root", "carsandvibes/cookie-plain.jar");

const INPUT_XLSX =
  process.env.INPUT_XLSX ??
  path.join(
    process.env.HOME ?? "/root",
    "carsandvibes/imports/source/2026_RRP_OMP_23102025_variants_options_prepared.xlsx"
  );

const SHEET_NAME = (process.env.SHEET_NAME ?? "").trim(); // vazio => primeira sheet
const DRY_RUN = (process.env.DRY_RUN ?? "1") === "1";
const LOG_DIR = process.env.LOG_DIR ?? path.join(process.env.HOME ?? "/root", "carsandvibes/imports/logs");

const LANGUAGE_CODE = (process.env.LANGUAGE_CODE ?? "pt").trim(); // pt
const ALSO_EN = (process.env.ALSO_EN ?? "0") === "1";

const BATCH = Number(process.env.BATCH ?? "50");

// Se quiseres aplicar brand (ex: 88) nestas default variants
const BRAND_FACET_VALUE_ID = (process.env.BRAND_FACET_VALUE_ID ?? "").trim(); // ex: "88" ou vazio

// Opcional: filtrar
const ONLY_PRODUCT_SLUG = (process.env.ONLY_PRODUCT_SLUG ?? "").trim();
const LIMIT_PRODUCTS = Number(process.env.LIMIT_PRODUCTS ?? "0");

// Labels default
const DEFAULT_OPTION_NAME = (process.env.DEFAULT_OPTION_NAME ?? "DEFAULT").trim();

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
  } catch {
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

function whoAmI() {
  const q = `query{ activeAdministrator{ id firstName lastName emailAddress } }`;
  const r = curlGraphQL(q);
  const a = r.activeAdministrator;
  const name = [a?.firstName, a?.lastName].filter(Boolean).join(" ") || "(sem nome)";
  const email = a?.emailAddress || "(sem emailAddress)";
  console.log(`Auth OK: ${name} <${email}> (id=${a?.id ?? "?"})`);
}

/**
 * XLSX: group by ProductCode
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
    if (!productCode) continue;

    const productName = normVal(r["ProductName"] || r["SHORT DESCRIPTION"] || r["Product Name"]) || productCode;
    const price = r["RRP 2026"] ?? r["RRP"] ?? r["price"] ?? "";

    if (!byCode.has(productCode)) {
      byCode.set(productCode, { productCode, productName, prices: [] });
    }
    if (price !== "" && price !== null && price !== undefined) byCode.get(productCode).prices.push(price);
  }

  let products = [];
  for (const [productCode, p] of byCode.entries()) {
    const baseSlug = slugify(p.productName);
    const finalSlug = `${baseSlug}-${slugify(productCode)}`;
    products.push({
      productCode,
      productName: p.productName,
      productSlug: finalSlug,
      price: p.prices.length ? p.prices[0] : null,
    });
  }

  if (ONLY_PRODUCT_SLUG) products = products.filter((p) => p.productSlug === ONLY_PRODUCT_SLUG);
  if (LIMIT_PRODUCTS > 0) products = products.slice(0, LIMIT_PRODUCTS);
  return products;
}

/**
 * Introspection helpers
 */
function detectProductOptionGroupsField() {
  const typeRes = curlGraphQL(
    `query{
      __type(name:"Product"){
        fields{ name }
      }
    }`
  );
  const fields = (typeRes.__type?.fields ?? []).map((f) => f.name);
  const candidates = ["optionGroups", "productOptionGroups", "productOptionGroup", "optionGroup"];
  const present = candidates.find((c) => fields.includes(c));
  return present ?? null;
}

function fetchProduct(slug, optionGroupsFieldOrNull) {
  const ogSel = optionGroupsFieldOrNull ? `${optionGroupsFieldOrNull}{ id code options{ id code name } }` : "";
  const q = `
    query($slug:String!){
      products(options:{take:1,filter:{slug:{eq:$slug}}}){
        items{
          id
          slug
          variants{ id }
          ${ogSel}
        }
      }
    }
  `;
  const r = curlGraphQL(q, { slug });
  return r.products?.items?.[0] ?? null;
}

/**
 * Create ProductOption (DEFAULT) for a given group, if missing
 */
const M_CREATE_OPTION = `
mutation($input: CreateProductOptionInput!) {
  createProductOption(input: $input) { id code name }
}`;

function ensureDefaultOptionInGroup(group, defaultName) {
  // group has: id, code, options[{id,code,name}]
  const defaultCode = slugify(defaultName); // "default"
  const existing = (group.options ?? []).find((o) => o.code === defaultCode);
  if (existing) return existing;

  if (DRY_RUN) {
    console.log(`[DRY] criar option DEFAULT no group=${group.code} (${group.id})`);
    return { id: `DRY_OPT_${group.code}_${defaultCode}`, code: defaultCode, name: defaultName };
  }

  const input = {
    productOptionGroupId: group.id,
    code: defaultCode,
    translations: [
      { languageCode: LANGUAGE_CODE, name: defaultName },
      ...(ALSO_EN ? [{ languageCode: "en", name: defaultName }] : []),
    ],
  };

  const r = curlGraphQL(M_CREATE_OPTION, { input });
  return r.createProductOption;
}

/**
 * Create variants
 */
const M_CREATE_VARIANTS = `
mutation($input:[CreateProductVariantInput!]!){
  createProductVariants(input:$input){ id sku }
}`;

const M_RUN_PENDING_INDEX = `
mutation{
  runPendingSearchIndexUpdates { success }
}`;

async function main() {
  whoAmI();

  const optionGroupsField = detectProductOptionGroupsField();
  console.log(`Detected Product optionGroups field: ${optionGroupsField ?? "(none found)"}`);

  const products = loadProductsFromXlsx();
  console.log(
    `Produtos únicos (pai) no XLSX: ${products.length}${ONLY_PRODUCT_SLUG ? ` (ONLY_PRODUCT_SLUG=${ONLY_PRODUCT_SLUG})` : ""}`
  );

  const summary = {
    DRY_RUN,
    productsInXlsx: products.length,
    productsFoundInDb: 0,
    productsMissingInDb: 0,
    alreadyHadVariants: 0,
    defaultVariantsPlanned: 0,
    defaultVariantsCreated: 0,
    errors: [],
  };

  const batchInputs = [];
  const batchMeta = [];

  function flushBatch() {
    if (!batchInputs.length) return;

    if (DRY_RUN) {
      console.log(
        `[DRY] createProductVariants batch size=${batchInputs.length} [${batchInputs[0].sku}..${batchInputs[batchInputs.length - 1].sku}]`
      );
      summary.defaultVariantsCreated += batchInputs.length;
      batchInputs.length = 0;
      batchMeta.length = 0;
      return;
    }

    const res = safeGraphQL(M_CREATE_VARIANTS, { input: batchInputs });
    if (!res.ok) {
      const logPath = path.join(LOG_DIR, `FATAL_default-variants_${nowId()}.json`);
      writeJson(logPath, {
        message: "GraphQL errors on createProductVariants batch",
        details: res.err?.details ?? String(res.err),
        response: res.err?.response ?? null,
        batchSize: batchInputs.length,
        sample: batchMeta.slice(0, 20),
      });
      console.error("FATAL batch createProductVariants:", res.err?.details ?? res.err);
      console.error("Log:", logPath);
      summary.errors.push({ log: logPath });
      process.exit(1);
    } else {
      const created = res.data.createProductVariants?.length ?? 0;
      console.log(`OK createProductVariants batch created=${created}`);
      summary.defaultVariantsCreated += created;
      batchInputs.length = 0;
      batchMeta.length = 0;
    }
  }

  for (const p of products) {
    const prod = fetchProduct(p.productSlug, optionGroupsField);
    if (!prod) {
      summary.productsMissingInDb += 1;
      continue;
    }
    summary.productsFoundInDb += 1;

    const variants = prod.variants ?? [];
    if (variants.length > 0) {
      summary.alreadyHadVariants += 1;
      continue;
    }

    // Build required optionIds: 1 default option per assigned group (if any)
    const optionIds = [];
    if (optionGroupsField) {
      const groups = prod[optionGroupsField] ?? [];
      for (const g of groups) {
        const opt = ensureDefaultOptionInGroup(g, DEFAULT_OPTION_NAME);
        optionIds.push(opt.id);
      }
    }

    const priceInt = moneyToInt(p.price) ?? 0;
    const sku = `${slugify(p.productCode).toUpperCase()}-DEFAULT`;

    const translations = [{ languageCode: LANGUAGE_CODE, name: p.productName || "Default" }];
    if (ALSO_EN) translations.push({ languageCode: "en", name: p.productName || "Default" });

    const input = {
      productId: prod.id,
      enabled: true,
      sku,
      price: priceInt,
      optionIds, // <-- aqui está o fix
      translations,
    };

    if (BRAND_FACET_VALUE_ID) input.facetValueIds = [BRAND_FACET_VALUE_ID];

    summary.defaultVariantsPlanned += 1;
    batchInputs.push(input);
    batchMeta.push({ slug: prod.slug, productId: prod.id, sku, optionIdsCount: optionIds.length });

    if (batchInputs.length >= BATCH) flushBatch();
  }

  flushBatch();

  const idx = safeGraphQL(M_RUN_PENDING_INDEX);
  if (idx.ok) {
    console.log(`runPendingSearchIndexUpdates: success=${idx.data.runPendingSearchIndexUpdates?.success === true}`);
  } else {
    console.log("WARN: runPendingSearchIndexUpdates falhou:", idx.err?.details ?? idx.err);
  }

  const summaryPath = path.join(LOG_DIR, `fix-default-variants-with-options-summary.${nowId()}.json`);
  writeJson(summaryPath, summary);
  console.log("DONE:", summary);
  console.log("Summary:", summaryPath);
}

main().catch((e) => {
  console.error("FATAL (top-level):", e?.details ?? e);
  process.exit(1);
});
