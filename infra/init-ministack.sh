#!/usr/bin/env bash
# Cria TODA a infraestrutura AWS do LocaFácil no MiniStack (ou numa conta AWS real).
# Idempotente: pode rodar quantas vezes quiser.
# Variáveis: AWS_ENDPOINT_URL, AWS_REGION, TOPIC_NAME, BUCKET_CONTRATOS, MAX_RECEIVE_COUNT
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://ministack:4566}"
REGION="${AWS_REGION:-us-east-1}"
TOPIC_NAME="${TOPIC_NAME:-locacao-eventos}"
BUCKET="${BUCKET_CONTRATOS:-locacao-contratos}"
MAX_RECEIVE="${MAX_RECEIVE_COUNT:-3}"
TMP="$(mktemp -d)"

aws() { command aws --endpoint-url "$ENDPOINT" --region "$REGION" --output text "$@"; }

echo ">> aguardando MiniStack em $ENDPOINT"
for i in $(seq 1 60); do
  if aws sqs list-queues >/dev/null 2>&1; then break; fi
  sleep 1
done

echo ">> tópico SNS $TOPIC_NAME"
TOPIC_ARN="$(aws sns create-topic --name "$TOPIC_NAME" --query TopicArn)"
echo "   $TOPIC_ARN"

# fila;visibility-timeout;tipos que ela assina (filtro no atributo "type")
QUEUES=(
  "reservas-queue;30;VeiculoBloqueado,VeiculoIndisponivel,PagamentoAprovado,PagamentoRecusado"
  "frota-queue;30;ReservaSolicitada,ReservaConfirmada,ReservaCancelada"
  "pagamentos-queue;60;PagamentoSolicitado,EstornoSolicitado"
  "notificacoes-queue;30;ReservaConfirmada,ReservaCancelada,PagamentoEstornado"
)

for entry in "${QUEUES[@]}"; do
  IFS=';' read -r NAME VIS TYPES <<<"$entry"
  DLQ="${NAME%-queue}-dlq"

  echo ">> fila $NAME (DLQ $DLQ, maxReceiveCount=$MAX_RECEIVE)"
  DLQ_URL="$(aws sqs create-queue --queue-name "$DLQ" --attributes MessageRetentionPeriod=1209600 --query QueueUrl)"
  DLQ_ARN="$(aws sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query Attributes.QueueArn)"

  cat >"$TMP/attrs.json" <<JSON
{
  "VisibilityTimeout": "$VIS",
  "ReceiveMessageWaitTimeSeconds": "10",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"$DLQ_ARN\",\"maxReceiveCount\":\"$MAX_RECEIVE\"}"
}
JSON
  Q_URL="$(aws sqs create-queue --queue-name "$NAME" --query QueueUrl)"
  aws sqs set-queue-attributes --queue-url "$Q_URL" --attributes "file://$TMP/attrs.json"
  Q_ARN="$(aws sqs get-queue-attributes --queue-url "$Q_URL" --attribute-names QueueArn --query Attributes.QueueArn)"

  # política que deixa o SNS escrever na fila (ignorada pelo MiniStack, exigida na AWS real)
  cat >"$TMP/policy.json" <<JSON
{"Policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"sns.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"$Q_ARN\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"$TOPIC_ARN\"}}}]}"}
JSON
  aws sqs set-queue-attributes --queue-url "$Q_URL" --attributes "file://$TMP/policy.json" || true

  # assinatura (uma só por fila)
  EXISTING="$(aws sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN" \
      --query "Subscriptions[?Endpoint=='$Q_ARN'].SubscriptionArn | [0]")"
  if [[ -z "$EXISTING" || "$EXISTING" == "None" ]]; then
    SUB_ARN="$(aws sns subscribe --topic-arn "$TOPIC_ARN" --protocol sqs \
        --notification-endpoint "$Q_ARN" --return-subscription-arn --query SubscriptionArn)"
  else
    SUB_ARN="$EXISTING"
  fi
  FILTER="$(printf '%s' "$TYPES" | awk -F, '{printf "{\"type\":["; for(i=1;i<=NF;i++){printf "%s\"%s\"", (i>1?",":""), $i} printf "]}"}')"
  aws sns set-subscription-attributes --subscription-arn "$SUB_ARN" --attribute-name RawMessageDelivery --attribute-value true
  # os consumidores também ignoram tipos que não conhecem, então o filtro é otimização, não requisito
  aws sns set-subscription-attributes --subscription-arn "$SUB_ARN" --attribute-name FilterPolicy --attribute-value "$FILTER" \
    || echo "   (aviso) FilterPolicy não suportada; consumidores descartam tipos desconhecidos"
  echo "   assinatura ok: $TYPES"
done

echo ">> bucket S3 $BUCKET"
aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1 || aws s3api create-bucket --bucket "$BUCKET" >/dev/null

echo ">> parâmetros SSM"
put() { aws ssm put-parameter --name "$1" --value "$2" --type "${3:-String}" --overwrite >/dev/null; echo "   $1"; }
put /locacao/sns/topico-eventos-arn "$TOPIC_ARN"
put /locacao/reservas/timeout-pagamento-segundos "${RESERVA_TIMEOUT_SEGUNDOS:-30}"
put /locacao/reservas/precos-diaria '{"HATCH":120,"SEDAN":180,"SUV":250}'
put /locacao/pagamentos/max-tentativas "${PAGAMENTO_MAX_TENTATIVAS:-3}"
put /locacao/gateway/api-key "${GATEWAY_API_KEY:-gw-chave-dev}" SecureString

rm -rf "$TMP"
echo ">> infraestrutura pronta"
