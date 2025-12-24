import fs from "node:fs";
import path from "node:path";
import xlsx from "xlsx";

const INPUT_XLSX = process.env.INPUT_XLSX ?? path.join(process.env.HOME ?? "/root","carsandvibes","imports","source","2026_RRP_OMP_23102025_variants_options_prepared.xlsx");
const OUT_CSV = process.env.OUT_CSV ?? path.join(process.env.HOME ?? "/root","carsandvibes","imports","working","vendure_products_import.csv");
const SHEET_NAME = process.env.SHEET_NAME ?? "";
const CURRENCY = process.env.CURRENCY_CODE ?? "EUR";
const LANGUAGE = process.env.LANGUAGE_CODE ?? "en";

function norm(v){ return String(v ?? "").trim(); }
function lower(v){ return norm(v).toLowerCase(); }

function findHeader(headers, candidates) {
  const map = new Map(headers.map(h => [lower(h), h]));
  for (const c of candidates) if (map.has(lower(c))) return map.get(lower(c));
  for (const h of headers) {
    const hl = lower(h);
    for (const c of candidates) if (hl.includes(lower(c))) return h;
  }
  return null;
}

function toCents(v) {
  const s = String(v ?? "").replace(",", ".").trim();
  const n = Number(s);
  if (!Number.isFinite(n)) return "";
  return String(Math.round(n * 100));
}

function slugify(s) {
  return lower(s)
    .normalize("NFKD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 60) || "item";
}

function csvEscape(v) {
  const s = String(v ?? "");
  if (/[;"\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function writeCSV(filePath, rows, headers) {
  const lines = [];
  lines.push(headers.join(";"));
  for (const r of rows) {
    lines.push(headers.map(h => csvEscape(r[h] ?? "")).join(";"));
  }
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, lines.join("\n"), "utf8");
}

const wb = xlsx.readFile(INPUT_XLSX);
const sheet = SHEET_NAME ? wb.Sheets[SHEET_NAME] : wb.Sheets[wb.SheetNames[0]];
if (!sheet) throw new Error(`Sheet não encontrada. Sheets: ${wb.SheetNames.join(", ")}`);

const rows = xlsx.utils.sheet_to_json(sheet, { defval: "" });
if (!rows.length) throw new Error("Sheet vazia.");

const headers = Object.keys(rows[0]);

// tenta detetar colunas (ajusta aqui se necessário)
const H_VARIANT_SKU = findHeader(headers, ["variant_sku","variant sku","sku"]);
const H_PRODUCT_SKU = findHeader(headers, ["product_sku","product sku","parent_sku","reference","model"]);
const H_NAME = findHeader(headers, ["product_name","name","nome"]);
const H_PRICE = findHeader(headers, ["rrp","price","preco","pvp"]);
const H_COLOR = findHeader(headers, ["color","cor"]);
const H_SIZE  = findHeader(headers, ["size","tamanho"]);

if (!H_VARIANT_SKU) throw new Error(`Não encontrei coluna variant sku. Headers: ${headers.join(" | ")}`);

console.log("Detetado:");
console.log(" - Variant SKU:", H_VARIANT_SKU);
console.log(" - Product SKU:", H_PRODUCT_SKU ?? "(fallback: prefixo do variant)");
console.log(" - Name:", H_NAME ?? "(fallback: product sku)");
console.log(" - Price:", H_PRICE ?? "(vazio => 0)");
console.log(" - Color:", H_COLOR ?? "(sem)");
console.log(" - Size:", H_SIZE ?? "(sem)");

function productKey(r) {
  const vsku = norm(r[H_VARIANT_SKU]);
  const psku = H_PRODUCT_SKU ? norm(r[H_PRODUCT_SKU]) : "";
  if (psku) return psku;
  const parts = vsku.split("-");
  return parts.length > 1 ? parts.slice(0, -1).join("-") : vsku;
}

// CSV para importProducts (formato compatível comum):
// productName; productSlug; productDescription; sku; price; currencyCode; optionGroups; optionValues
// (Assets/facets depois)
const out = [];
for (const r of rows) {
  const sku = norm(r[H_VARIANT_SKU]);
  if (!sku) continue;

  const psku = productKey(r);
  const name = H_NAME ? norm(r[H_NAME]) : psku;

  const color = H_COLOR ? norm(r[H_COLOR]) : "";
  const size  = H_SIZE ? norm(r[H_SIZE]) : "";

  const optionGroups = [color ? "Color" : null, size ? "Size" : null].filter(Boolean).join("|");
  const optionValues = [
    color ? `Color:${color}` : null,
    size ? `Size:${size}` : null,
  ].filter(Boolean).join("|");

  out.push({
    productName: name,
    productSlug: slugify(name || psku),
    productDescription: "",
    sku,
    price: (H_PRICE ? toCents(r[H_PRICE]) : "") || "0",
    currencyCode: CURRENCY,
    optionGroups,
    optionValues,
  });
}

const outHeaders = ["productName","productSlug","productDescription","sku","price","currencyCode","optionGroups","optionValues"];
writeCSV(OUT_CSV, out, outHeaders);
console.log(`OK -> ${OUT_CSV} (${out.length} linhas)`);
