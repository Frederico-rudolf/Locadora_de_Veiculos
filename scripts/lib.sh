#!/usr/bin/env bash
# Funções comuns dos cenários de falha. Rode os scripts a partir da raiz do repositório.
# Requer: docker compose, curl, jq.
set -euo pipefail
cd "$(dirname "$0")/.."

API=${API:-http://localhost:3000}
FROTA=${FROTA:-http://localhost:8081}
GW=${GW:-http://localhost:8090}
NOTIF=${NOTIF:-http://localhost:3001}

titulo() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
ok()     { printf '\033[1;32m✔ %s\033[0m\n' "$*"; }
falha()  { printf '\033[1;31m✘ %s\033[0m\n' "$*"; }

# data futura única para não misturar execuções (dias a partir de hoje)
data_futura() { date -d "+${1:-$((RANDOM % 300 + 60))} days" +%F 2>/dev/null || date -v+"${1:-90}"d +%F; }

reservar() {  # reservar FILIAL CATEGORIA INICIO FIM -> imprime reservaId
  curl -sf -X POST "$API/reservas" -H 'Content-Type: application/json' \
    -d "{\"clienteId\":\"cli-$RANDOM\",\"clienteEmail\":\"cliente@example.com\",\"filial\":\"$1\",\"categoria\":\"$2\",\"dataInicio\":\"$3\",\"dataFim\":\"$4\"}" \
    | jq -r .reservaId
}

status_reserva() { curl -sf "$API/reservas/$1" | jq -r .status; }

esperar_final() {  # esperar_final ID [segundos]
  local id=$1 lim=${2:-60} s
  for _ in $(seq 1 "$lim"); do
    s=$(status_reserva "$id")
    case "$s" in CONFIRMADA|CANCELADA|REJEITADA|EXPIRADA) echo "$s"; return 0;; esac
    sleep 1
  done
  echo "$s"
}

gateway() { curl -sf -X POST "$GW/config" -H 'Content-Type: application/json' -d "$1" | jq -c .; }
gateway_padrao() { gateway '{"latencia_ms":800,"jitter_ms":700,"taxa_falha":0.2,"taxa_recusa":0.05}'; }
gateway_estavel() { gateway '{"latencia_ms":200,"jitter_ms":100,"taxa_falha":0,"taxa_recusa":0}'; }

sql() {  # sql SERVICO "SELECT ..."
  docker compose exec -T "$1-db" psql -U app -d app -P pager=off -c "$2"
}

awsl() {  # aws CLI apontando para o MiniStack, sem precisar instalar nada na máquina
  docker compose run --rm -T --no-deps --entrypoint aws infra-init \
    --endpoint-url http://ministack:4566 --region us-east-1 "$@"
}

topic_arn() { awsl sns list-topics --query "Topics[?ends_with(TopicArn, ':locacao-eventos')].TopicArn | [0]" --output text | tr -d '\r'; }

publicar() {  # publicar TIPO 'JSON'
  awsl sns publish --topic-arn "$(topic_arn)" --message "$2" \
    --message-attributes "{\"type\":{\"DataType\":\"String\",\"StringValue\":\"$1\"}}" >/dev/null
}

contar_fila() {  # contar_fila NOME
  local url; url=$(awsl sqs get-queue-url --queue-name "$1" --query QueueUrl --output text | tr -d '\r')
  awsl sqs get-queue-attributes --queue-url "$url" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible --output json | jq -c .Attributes
}
