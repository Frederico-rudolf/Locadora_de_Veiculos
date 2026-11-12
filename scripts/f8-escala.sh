#!/usr/bin/env bash
# F8 – Escala horizontal: 3 réplicas de Pagamentos dividem a carga sem efeito duplicado
source "$(dirname "$0")/lib.sh"
titulo "F8: subindo 3 réplicas de pagamentos"
docker compose up -d --scale pagamentos=3 --no-recreate pagamentos
sleep 8; docker compose ps pagamentos --format '{{.Name}} {{.Status}}'
gateway '{"latencia_ms":600,"jitter_ms":400,"taxa_falha":0.2,"taxa_recusa":0}'
INICIO=$(date -u +%FT%TZ)
TMP=$(mktemp)
# datas distintas para que nenhuma reserva seja rejeitada por falta de veículo: todas chegam ao pagamento
for i in $(seq 1 30); do D=$(data_futura $((400 + i))); reservar BNU HATCH "$D" "$D" >> "$TMP"; done
echo "30 reservas criadas; aguardando..."; sleep 25
while read -r id; do esperar_final "$id" 60; done < "$TMP" | sort | uniq -c
titulo "Carga por réplica"
sql pagamentos "SELECT processado_por AS replica, count(*) FROM pagamentos p WHERE criado_em >= '$INICIO' GROUP BY 1"
titulo "Duplicidade (as duas consultas devem vir vazias)"
sql pagamentos "SELECT reserva_id, count(*) FROM pagamentos GROUP BY 1 HAVING count(*) > 1"
sql pagamentos "SELECT message_id, count(*) FROM mensagens_processadas GROUP BY 1 HAVING count(*) > 1"
rm -f "$TMP"; gateway_padrao >/dev/null
