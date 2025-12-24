# Makefile — helpers Vendure Admin API
ADMIN_API_URL ?= http://127.0.0.1:3000/admin-api
VENDURE_USER  ?= superadmin
VENDURE_PASS  ?= superadmin
COOKIE_JAR    ?= /root/carsandvibes/cookie-local.jar

export ADMIN_API_URL VENDURE_USER VENDURE_PASS COOKIE_JAR

.PHONY: help vars login me reindex watch-noauth reindex-watch

help:
	@echo "Targets:"
	@echo "  make login                   # faz login e grava cookie"
	@echo "  make me                      # mostra utilizador autenticado"
	@echo "  make reindex                 # dispara reindex e mostra resposta"
	@echo "  make watch-noauth JOB_ID=ID  # segue job usando apenas o cookie"
	@echo "  make reindex-watch           # dispara reindex e segue de imediato"
	@echo "  make vars                    # mostra variáveis atuais"

vars:
	@echo ADMIN_API_URL=$(ADMIN_API_URL)
	@echo VENDURE_USER=$(VENDURE_USER)
	@echo COOKIE_JAR=$(COOKIE_JAR)

login:
	@curl -sS "$(ADMIN_API_URL)" -c "$(COOKIE_JAR)" -H 'Content-Type: application/json' \
	  --data '{"query":"mutation($u:String!,$p:String!,$r:Boolean!){ login(username:$u,password:$p,rememberMe:$r){ __typename ... on CurrentUser{ id identifier } ... on InvalidCredentialsError{ errorCode message } ... on NativeAuthStrategyError{ errorCode message } } }","variables":{"u":"$(VENDURE_USER)","p":"$(VENDURE_PASS)","r":true}}' \
	| jq

me:
	@curl -sS "$(ADMIN_API_URL)" -b "$(COOKIE_JAR)" -H 'Content-Type: application/json' \
	  --data '{"query":"{ me { id identifier } }"}' \
	| jq

reindex:
	@curl -sS "$(ADMIN_API_URL)" -b "$(COOKIE_JAR)" -H 'Content-Type: application/json' \
	  --data '{"query":"mutation{ reindex{ id } }"}' \
	| jq

watch-noauth:
	@test -n "$(JOB_ID)" || (echo "Erro: passa JOB_ID=..." && exit 2)
	@ADMIN_API_URL="$(ADMIN_API_URL)" COOKIE="$(COOKIE_JAR)" JOB_ID="$(JOB_ID)" /root/carsandvibes/watch-one.sh

reindex-watch:
	@id=$$(curl -sS "$(ADMIN_API_URL)" -b "$(COOKIE_JAR)" -H 'Content-Type: application/json' \
	  --data '{"query":"mutation{ reindex{ id } }"}' | jq -r '.data.reindex.id'); \
	if [ -z "$$id" ] || [ "$$id" = "null" ]; then echo "Falha a obter id do reindex (cookie/permissões?)."; exit 2; fi; \
	echo "Job $$id"; \
	ADMIN_API_URL="$(ADMIN_API_URL)" COOKIE="$(COOKIE_JAR)" JOB_ID="$$id" /root/carsandvibes/watch-one.sh
