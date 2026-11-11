#!/usr/bin/env bash
# Fluxo principal: POST -> 202 -> consulta de status até CONFIRMADA -> contrato no S3
source "$(dirname "$0")/lib.sh"
gateway_estavel >/dev/null
D1=$(data_futura 30); D2=$(data_futura 32)
titulo "POST /reservas (resposta imediata 202)"
curl -si -X POST "$API/reservas" -H 'Content-Type: application/json' -H "Idempotency-Key: demo-$RANDOM" \
  -d "{\"clienteId\":\"cli-ana\",\"clienteEmail\":\"ana@example.com\",\"filial\":\"BNU\",\"categoria\":\"SUV\",\"dataInicio\":\"$D1\",\"dataFim\":\"$D2\"}" | tee /tmp/resp.txt
ID=$(tail -1 /tmp/resp.txt | jq -r .reservaId)
titulo "GET /reservas/$ID até o fim da saga"
echo "status final: $(esperar_final "$ID" 30)"
curl -s "$API/reservas/$ID" | jq .
titulo "Interessados em ReservaConfirmada"
sleep 2
sql frota "SELECT reserva_id, status FROM bloqueios WHERE reserva_id = '$ID'"
curl -s "$NOTIF/notificacoes?reservaId=$ID" | jq '.[] | {tipo, assunto, contrato_s3, versao_evento}'
awsl s3 ls s3://locacao-contratos/contratos/ | grep "$ID" || true
gateway_padrao >/dev/null
