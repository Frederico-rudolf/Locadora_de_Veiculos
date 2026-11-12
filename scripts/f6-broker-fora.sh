#!/usr/bin/env bash
# F6 – Broker fora do ar por 30 s: serviços reconectam sozinhos e a outbox drena
source "$(dirname "$0")/lib.sh"
SEG=${SEG:-30}
titulo "F6: MiniStack congelado por $SEG s"
gateway_estavel >/dev/null
# 'pause' congela o processo sem perder o estado em memória do MiniStack (filas, tópicos, mensagens).
# Para a AWS real/RabbitMQ o equivalente seria derrubar a rede ou o broker.
docker compose pause ministack && ok "broker fora do ar"
D=$(data_futura); IDS=()
for i in 1 2 3; do IDS+=("$(reservar JOI SEDAN "$D" "$D")"); done
echo "3 reservas aceitas DURANTE a queda (a API não depende do broker): ${IDS[*]}"
sleep 3; curl -s "$API/health" | jq '{outboxPendente, broker: .broker.ultimoErro[0:80]}'
sleep "$SEG"
docker compose unpause ministack && ok "broker de volta, sem reiniciar nenhum serviço"
for id in "${IDS[@]}"; do echo "$id -> $(esperar_final "$id" 90)"; done
curl -s "$API/health" | jq '{outboxPendente}'
docker compose logs --no-log-prefix --since 2m reservas frota pagamentos notificacoes 2>/dev/null | grep -E "reconectando|broker" | tail -8
docker compose ps --format '{{.Name}} {{.Status}}' | grep -E "reservas|frota|pagamentos|notificacoes"
