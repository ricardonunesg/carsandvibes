#!/usr/bin/env bash
set -euo pipefail

ADMIN_API_URL="${ADMIN_API_URL:-http://localhost:3000/admin-api}"
COOKIE_JAR="${COOKIE_JAR:-$HOME/carsandvibes/cookie-plain.jar}"
CSV_PATH="${CSV_PATH:-$HOME/carsandvibes/imports/working/vendure_products_import.csv}"
OUT_JSON="${OUT_JSON:-$HOME/carsandvibes/imports/logs/importProducts_response.json}"

[[ -f "$CSV_PATH" ]] || { echo "CSV não encontrado: $CSV_PATH"; exit 1; }
[[ -f "$COOKIE_JAR" ]] || { echo "Cookie jar não encontrado: $COOKIE_JAR"; exit 1; }

mkdir -p "$(dirname "$OUT_JSON")"

echo "A importar: $CSV_PATH"
echo "A guardar resposta em: $OUT_JSON"

# -m 0 => sem timeout; podes pôr -m 600 se quiseres 10min max
curl -sS "$ADMIN_API_URL" \
  -b "$COOKIE_JAR" \
  -F 'operations={"query":"mutation($file: Upload!){ importProducts(csvFile:$file){ processed imported errors } }","variables":{"file":null}}' \
  -F 'map={"0":["variables.file"]}' \
  -F "0=@${CSV_PATH};type=text/csv" \
  -o "$OUT_JSON"

echo "== PRIMEIRAS LINHAS =="
head -n 40 "$OUT_JSON" || true
echo "== FIM =="
echo "Depois faz: jq . $OUT_JSON"
