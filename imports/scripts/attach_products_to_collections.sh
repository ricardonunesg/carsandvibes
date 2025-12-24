#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Attach products to MANUAL collections using product-id-filter
# Input CSV: variant_sku;collection_lvl1;collection_lvl2
#
# Requires:
#   - Admin API session cookie jar (curl -b)
#   - collections already exist in Vendure
#   - products/variants already imported in Vendure (else nothing to do)
#
# ENV:
#   ADMIN_API_URL  default http://localhost:3000/admin-api
#   COOKIE_JAR     default $HOME/carsandvibes/cookie-plain.jar
#   INPUT_CSV      default $HOME/carsandvibes/imports/source/vendure_collection_mapping.csv
#   DRY_RUN        default 1 (0 to apply)
#   PREFER_LVL2    default 1 (use collection_lvl2 when present)
#   APPLY_LVL1_TOO default 0 (also add to lvl1)
#   ALIASES_FILE   default $HOME/carsandvibes/imports/source/collection_aliases.csv
# ============================================================

ADMIN_API_URL="${ADMIN_API_URL:-http://localhost:3000/admin-api}"
COOKIE_JAR="${COOKIE_JAR:-$HOME/carsandvibes/cookie-plain.jar}"
INPUT_CSV="${INPUT_CSV:-$HOME/carsandvibes/imports/source/vendure_collection_mapping.csv}"
DRY_RUN="${DRY_RUN:-1}"
PREFER_LVL2="${PREFER_LVL2:-1}"
APPLY_LVL1_TOO="${APPLY_LVL1_TOO:-0}"
ALIASES_FILE="${ALIASES_FILE:-$HOME/carsandvibes/imports/source/collection_aliases.csv}"

command -v jq >/dev/null 2>&1 || { echo "Erro: precisa de jq instalado"; exit 1; }
[[ -f "$INPUT_CSV" ]] || { echo "Erro: CSV não encontrado em $INPUT_CSV"; exit 1; }
[[ -f "$COOKIE_JAR" ]] || { echo "Erro: cookie jar não encontrado em $COOKIE_JAR"; exit 1; }

slugify() {
  echo "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[[:space:]]+/-/g; s/[^a-z0-9-]//g; s/--+/-/g; s/^-|-$//g'
}

# Request GraphQL with a payload identical to your working curl calls
gql() {
  local query="$1"
  local variables_json="${2:-{}}"

  # validate variables JSON
  if ! echo "$variables_json" | jq -e . >/dev/null 2>&1; then
    variables_json="{}"
  fi

  local q_escaped
  q_escaped="$(jq -Rs . <<<"$query")"
  local payload
  payload="$(printf '{"query":%s,"variables":%s}' "$q_escaped" "$variables_json")"

  curl -sS "$ADMIN_API_URL" \
    -H 'Content-Type: application/json' \
    -b "$COOKIE_JAR" \
    --data-binary "$payload"
}

# Queries (keep them minimal & known-good)
Q_COLLECTIONS_LIST='query{ collections(options:{take:100}){ items{ id name slug } totalItems } }'
Q_VARIANT_BY_SKU='query($sku:String!){ productVariants(options:{take:1, filter:{ sku:{ eq:$sku } }}){ items{ id sku product{ id } } totalItems } }'
M_UPDATE_COLLECTION='mutation($input: UpdateCollectionInput!){ updateCollection(input:$input){ id name slug } }'

# ------------------------------------------------------------
# Load collections (IMPORTANT: take=100 because take=10000 returns empty in your setup)
# ------------------------------------------------------------
echo "== Carregar todas as collections (cache) =="

ALL_COLLS="$(gql "$Q_COLLECTIONS_LIST")"

# If unauthorized or schema mismatch, show response
if echo "$ALL_COLLS" | jq -e '.errors' >/dev/null 2>&1; then
  echo "Erro ao carregar collections (GraphQL errors):"
  echo "$ALL_COLLS" | jq '.errors'
  exit 1
fi

if ! echo "$ALL_COLLS" | jq -e '.data.collections.items | length >= 0' >/dev/null 2>&1; then
  echo "Erro: resposta inesperada ao carregar collections:"
  echo "$ALL_COLLS" | head -n 80
  exit 1
fi

TOTAL_COLLS="$(echo "$ALL_COLLS" | jq -r '.data.collections.totalItems // (.data.collections.items|length)')"
ITEMS_LEN="$(echo "$ALL_COLLS" | jq -r '.data.collections.items | length')"
echo "Collections encontradas: $ITEMS_LEN (totalItems=$TOTAL_COLLS)"

# ------------------------------------------------------------
# Aliases (optional)
# ------------------------------------------------------------
declare -A COLLECTION_ALIASES
if [[ -f "$ALIASES_FILE" ]]; then
  echo "Aliases: a ler $ALIASES_FILE"
  while IFS=';' read -r src dst; do
    src="$(echo "${src:-}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    dst="$(echo "${dst:-}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$src" || "$src" == "source_name" ]] && continue
    [[ -z "$dst" ]] && continue
    COLLECTION_ALIASES["$src"]="$dst"
  done < "$ALIASES_FILE"
fi

resolve_alias() {
  local name="$1"
  [[ -n "${COLLECTION_ALIASES[$name]:-}" ]] && echo "${COLLECTION_ALIASES[$name]}" || echo "$name"
}

get_collection_id() {
  local name="$1"
  local slug
  slug="$(slugify "$name")"

  # try slug
  local id
  id="$(echo "$ALL_COLLS" | jq -r --arg s "$slug" '.data.collections.items[] | select(.slug==$s) | .id' | head -n1)"
  [[ -n "${id:-}" && "$id" != "null" ]] && { echo "$id"; return; }

  # try exact name
  id="$(echo "$ALL_COLLS" | jq -r --arg n "$name" '.data.collections.items[] | select(.name==$n) | .id' | head -n1)"
  [[ -n "${id:-}" && "$id" != "null" ]] && { echo "$id"; return; }

  echo ""
}

get_collection_label() {
  local cid="$1"
  echo "$ALL_COLLS" | jq -r --arg id "$cid" '.data.collections.items[] | select(.id==$id) | (.name + " (" + .slug + ")")' | head -n1
}

get_product_id_from_variant_sku() {
  local sku="$1"
  local vars
  vars="$(jq -c -n --arg sku "$sku" '{sku:$sku}')"
  local res
  res="$(gql "$Q_VARIANT_BY_SKU" "$vars")"
  echo "$res" | jq -r '.data.productVariants.items[0].product.id // empty'
}

# ------------------------------------------------------------
# Build plan: collectionId -> [productId, ...]
# ------------------------------------------------------------
declare -A coll_to_products_json

add_product_to_plan() {
  local coll_id="$1"
  local prod_id="$2"
  local current="${coll_to_products_json[$coll_id]:-[]}"
  local updated
  updated="$(echo "$current" | jq -c --arg pid "$prod_id" '(. + [$pid]) | unique')"
  coll_to_products_json[$coll_id]="$updated"
}

echo "== Ler CSV e construir plano =="

# NOTE: while-loop is in a subshell because of pipe; to keep associative arrays,
# we use process substitution instead.
while IFS=';' read -r variant_sku c1 c2; do
  # skip header
  [[ "$variant_sku" == "variant_sku" ]] && continue

  variant_sku="$(echo "${variant_sku:-}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  c1="$(echo "${c1:-}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  c2="$(echo "${c2:-}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$variant_sku" ]] && continue

  # get product id from variant sku (will be empty until products imported)
  prod_id="$(get_product_id_from_variant_sku "$variant_sku")"
  [[ -z "$prod_id" ]] && continue

  target_lvl1=""
  target_lvl2=""
  [[ -n "$c1" ]] && target_lvl1="$(resolve_alias "$c1")"
  [[ -n "$c2" ]] && target_lvl2="$(resolve_alias "$c2")"

  if [[ "$PREFER_LVL2" == "1" && -n "$target_lvl2" ]]; then
    main_target="$target_lvl2"
  else
    main_target="$target_lvl1"
  fi

  if [[ -n "$main_target" ]]; then
    coll_id="$(get_collection_id "$main_target")"
    if [[ -n "$coll_id" ]]; then
      add_product_to_plan "$coll_id" "$prod_id"
    fi
  fi

  if [[ "$APPLY_LVL1_TOO" == "1" && -n "$target_lvl1" ]]; then
    coll_id1="$(get_collection_id "$target_lvl1")"
    [[ -n "$coll_id1" ]] && add_product_to_plan "$coll_id1" "$prod_id"
  fi

done < "$INPUT_CSV"

echo
echo "== Resumo (dry-run=$DRY_RUN) =="

keys=("${!coll_to_products_json[@]}")
if [[ "${#keys[@]}" -eq 0 ]]; then
  echo "Nada para fazer (normal se ainda não importaste produtos)."
  exit 0
fi

for cid in "${keys[@]}"; do
  n="$(echo "${coll_to_products_json[$cid]}" | jq 'length')"
  label="$(get_collection_label "$cid")"
  echo "- $label => $n products"
done | sort

if [[ "$DRY_RUN" == "1" ]]; then
  echo
  echo "DRY-RUN ativo: não foi aplicada nenhuma alteração."
  exit 0
fi

echo
echo "== Aplicar updates (product-id-filter) =="

for cid in "${keys[@]}"; do
  product_ids_json="${coll_to_products_json[$cid]}"
  count="$(echo "$product_ids_json" | jq 'length')"
  [[ "$count" -eq 0 ]] && continue

  args="$(echo "$product_ids_json" | jq -c '[ .[] | { name:"productIds", value:(.) } ]')"
  input="$(jq -c -n --arg id "$cid" --argjson args "$args" '{id:$id, filters:[{code:"product-id-filter", arguments:$args}] }')"
  vars="$(jq -c -n --argjson input "$input" '{input:$input}')"

  res="$(gql "$M_UPDATE_COLLECTION" "$vars")"

  if echo "$res" | jq -e '.errors' >/dev/null 2>&1; then
    echo "ERRO ao atualizar collection $cid:"
    echo "$res" | jq '.errors'
    exit 1
  fi

  name="$(echo "$res" | jq -r '.data.updateCollection.name')"
  slug="$(echo "$res" | jq -r '.data.updateCollection.slug')"
  echo "OK: $name ($slug) <= $count products"
done

echo "Concluído."
