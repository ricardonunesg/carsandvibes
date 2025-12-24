#!/usr/bin/env bash
set -Eeuo pipefail

# Config
ENDPOINT="${ADMIN_API_URL:-http://127.0.0.1:3000/admin-api}"
USER="${VENDURE_USER:-superadmin}"
PASS="${VENDURE_PASS:-superadmin}"
COOKIE_JAR="${COOKIE_JAR:-/root/carsandvibes/cookie-local.jar}"

# Args
JOB_ID=""
INTERVAL="1"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --jobId|--id) JOB_ID="$2"; shift 2 ;;
    --interval|--intervalMs) INTERVAL="$2"; shift 2 ;;
    --endpoint) ENDPOINT="$2"; shift 2 ;;
    --user) USER="$2"; shift 2 ;;
    --pass) PASS="$2"; shift 2 ;;
    --cookie|--cookieJar) COOKIE_JAR="$2"; shift 2 ;;
    -h|--help)
      echo "Uso: $0 --jobId <ID> [--interval 1] [--endpoint URL] [--user u] [--pass p] [--cookie CAMINHO]"
      exit 0
      ;;
    *) echo "Arg desconhecido: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "${JOB_ID}" ]]; then
  echo "Falta --jobId (ou use env JOB_ID)"; exit 1
fi

command -v jq >/dev/null || { echo "Precisas do jq" >&2; exit 1; }

echo "A autenticar em ${ENDPOINT}..."
jq -n \
  --arg q 'mutation ($u:String!, $p:String!, $remember:Boolean!) { login(username:$u, password:$p, rememberMe:$remember) { __typename ... on CurrentUser { id identifier } ... on InvalidCredentialsError { errorCode message } ... on NativeAuthStrategyError { errorCode message } } }' \
  --arg u "$USER" --arg p "$PASS" --argjson r true \
  '{query:$q, variables:{u:$u,p:$p,remember:$r}}' \
| curl -sS "$ENDPOINT" \
    -c "$COOKIE_JAR" \
    -H 'Content-Type: application/json' \
    --data @- \
| tee /tmp/login.json >/dev/null

LOGIN_TYPE="$(jq -r '.data.login.__typename // empty' /tmp/login.json)"
if [[ "$LOGIN_TYPE" != "CurrentUser" ]]; then
  echo "Falha no login:" >&2
  jq -r '.errors // .data.login' /tmp/login.json >&2
  exit 3
fi
IDENT="$(jq -r '.data.login.identifier' /tmp/login.json)"
echo "Login OK como ${IDENT}"

QUERY='query ($id: ID!) { job(jobId:$id){ id state progress queueName isSettled error result } }'

echo "A vigiar job ${JOB_ID} (intervalo ${INTERVAL}s)..."
while true; do
  jq -n --arg q "$QUERY" --arg id "$JOB_ID" \
    '{query:$q, variables:{id:$id}}' \
  | curl -sS "$ENDPOINT" \
      -b "$COOKIE_JAR" \
      -H 'Content-Type: application/json' \
      --data @- \
  | tee /tmp/job.json >/dev/null

  # Erros da API
  if jq -e '.errors? // empty' /tmp/job.json >/dev/null; then
    echo "Erro na query do job:" >&2
    jq '.errors' /tmp/job.json >&2
    exit 4
  fi

  # Job não encontrado
  if [[ "$(jq -r '.data.job // empty' /tmp/job.json)" == "null" ]] || [[ -z "$(jq -r '.data.job.id // empty' /tmp/job.json)" ]]; then
    echo "Job não encontrado ou sem permissões." >&2
    exit 5
  fi

  STATE="$(jq -r '.data.job.state' /tmp/job.json)"
  PROG="$(jq -r '.data.job.progress // "-"' /tmp/job.json)"
  QUEUE="$(jq -r '.data.job.queueName // empty' /tmp/job.json)"
  ERR="$(jq -r '.data.job.error // empty' /tmp/job.json)"
  LINE="${STATE}"
  [[ -n "$QUEUE" ]] && LINE+=" [${QUEUE}]"
  LINE+=" ${PROG}%"
  [[ -n "$ERR" ]] && LINE+=" — ERROR: ${ERR}"
  echo "$LINE"

  SETTLED="$(jq -r '.data.job.isSettled' /tmp/job.json)"
  if [[ "$STATE" == "COMPLETED" || "$STATE" == "FAILED" || "$SETTLED" == "true" ]]; then
    if jq -e '.data.job.result? // empty' /tmp/job.json >/dev/null; then
      echo "Resultado:"
      jq '.data.job.result' /tmp/job.json
    fi
    break
  fi
  sleep "$INTERVAL"
done
