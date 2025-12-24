#!/usr/bin/env bash
set -euo pipefail
set +H  # desativa history expansion (evita problemas com "ID!")

ADMIN_API_URL="${ADMIN_API_URL:-http://127.0.0.1:3000/admin-api}"
COOKIE="${COOKIE:-/root/carsandvibes/cookie-local.jar}"
JOB_ID="${JOB_ID:-}"

if [[ -z "$JOB_ID" ]]; then
  echo "Erro: define JOB_ID (ex.: JOB_ID=147)"; exit 2
fi

RESP=""
while :; do
  RESP="$(
    jq -n \
      --arg id "$JOB_ID" \
      --arg q 'query($id:ID!){ job(jobId:$id){ id state progress queueName isSettled error result } }' \
      '{query:$q, variables:{id:$id}}' \
    | curl -sS "$ADMIN_API_URL" -b "$COOKIE" -H 'Content-Type: application/json' --data @-
  )"

  state=$(jq -r '.data.job.state // empty' <<<"$RESP")
  queue=$(jq -r '.data.job.queueName // "n/a"' <<<"$RESP")
  prog=$(jq -r '.data.job.progress // 0' <<<"$RESP")
  settled=$(jq -r '.data.job.isSettled // false' <<<"$RESP")

  if [[ -n "$state" ]]; then
    echo "$state [$queue] ${prog}%"
  else
    err=$(jq -r '.errors // empty' <<<"$RESP")
    if [[ -n "$err" && "$err" != "null" ]]; then
      echo "Erro GraphQL: $err"
    else
      echo "Job não encontrado (sem permissões? cookie inválido? id errado?)"
    fi
  fi

  [[ "$settled" == "true" ]] && break
  sleep 2
done

# Mostra o resultado no fim (se existir)
res=$(jq -c '.data.job.result // empty' <<<"$RESP")
if [[ -n "$res" ]]; then
  echo "Resultado:"
  echo "$res" | jq .
fi
