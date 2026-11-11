#!/usr/bin/env bash
# F2 – Serviço morre no meio: docker kill na réplica de Pagamentos que está cobrando
source "$(dirname "$0")/lib.sh"
titulo "F2: matar a réplica de pagamentos durante a cobrança"
# timeout da reserva maior que o visibility timeout (60 s) para a reentrega ainda achar a reserva aberta
awsl ssm put-parameter --name /locacao/reservas/timeout-pagamento-segundos --value 150 --overwrite >/dev/null
echo "timeout da reserva = 150 s (SSM); aguardando o Reservas reler (10 s)"; sleep 11
gateway '{"latencia_ms":8000,"jitter_ms":0,"taxa_falha":0,"taxa_recusa":0}'
D=$(data_futura)
ID=$(reservar BNU HATCH "$D" "$D"); echo "reserva $ID"
echo "procurando a réplica que pegou a mensagem..."
for _ in $(seq 1 30); do
  REPLICA=$(docker compose logs --no-log-prefix pagamentos 2>/dev/null | grep "cobrando no gateway" | grep "$ID" | jq -r .replica | tail -1 || true)
  [[ -n "$REPLICA" ]] && break; sleep 1
done
echo "réplica $REPLICA está cobrando -> docker kill"
docker kill "$REPLICA" >/dev/null && ok "réplica $REPLICA morta no meio do processamento"
echo "a mensagem não foi apagada; volta para a fila após o visibility timeout (60 s) e outra réplica processa"
echo "status final: $(esperar_final "$ID" 120)"
titulo "Efeito aplicado uma única vez"
sql pagamentos "SELECT reserva_id, status, transacao_id, processado_por FROM pagamentos WHERE reserva_id = '$ID'"
echo "cobranças no gateway para esta reserva (Idempotency-Key = reservaId):"
curl -s "$GW/cobrancas" | jq --arg id "$ID" '[.cobrancas[] | select(.idempotencyKey == $id)]'
docker compose logs --no-log-prefix gateway-pagamento | grep "$ID" || true
awsl ssm put-parameter --name /locacao/reservas/timeout-pagamento-segundos --value 30 --overwrite >/dev/null
gateway_padrao >/dev/null; docker compose up -d pagamentos >/dev/null 2>&1; ok "ambiente restaurado"
