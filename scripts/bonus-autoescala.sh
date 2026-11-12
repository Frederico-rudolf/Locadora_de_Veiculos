#!/usr/bin/env bash
# Bônus – Autoescala: o autoscaler sobe réplicas de pagamentos quando a pagamentos-queue enche
# e as remove (uma por vez, após o esfriamento) quando ela esvazia.
source "$(dirname "$0")/lib.sh"
titulo "Autoescala de pagamentos pelo tamanho da fila"
docker compose --profile autoscale up -d autoscaler 2>/dev/null
replicas() { docker compose ps -q pagamentos | wc -l | tr -d ' '; }
echo "réplicas agora: $(replicas)"
# gateway lento -> cada réplica processa pouco -> a fila cresce
awsl ssm put-parameter --name /locacao/reservas/timeout-pagamento-segundos --value 300 --overwrite >/dev/null
sleep 11
gateway '{"latencia_ms":3000,"jitter_ms":0,"taxa_falha":0,"taxa_recusa":0}'
TMP=$(mktemp)
for i in $(seq 1 40); do reservar BNU HATCH "$(data_futura $((500 + i)))" "$(data_futura $((500 + i)))" >> "$TMP"; done
echo "40 reservas criadas"
titulo "Fila x réplicas (a cada 5 s)"
for _ in $(seq 1 18); do
  printf '%s  réplicas=%s  autoscaler=%s\n' "$(date +%T)" "$(replicas)" \
    "$(curl -s localhost:8091/health | jq -c '{profundidade, desejadas, ultimaAcao}')"
  sleep 5
done
while read -r id; do esperar_final "$id" 120; done < "$TMP" | sort | uniq -c
titulo "Sem efeito duplicado"
sql pagamentos "SELECT processado_por AS replica, count(*) FROM pagamentos GROUP BY 1 ORDER BY 1"
sql pagamentos "SELECT reserva_id, count(*) FROM pagamentos GROUP BY 1 HAVING count(*) > 1"
gateway_padrao >/dev/null
awsl ssm put-parameter --name /locacao/reservas/timeout-pagamento-segundos --value 30 --overwrite >/dev/null
titulo "Fila vazia: descida gradual (uma réplica a cada esfriamento de 30 s)"
for _ in $(seq 1 10); do printf '%s  réplicas=%s\n' "$(date +%T)" "$(replicas)"; sleep 10; done
echo "para desligar: docker compose --profile autoscale stop autoscaler"
rm -f "$TMP"
