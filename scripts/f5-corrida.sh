#!/usr/bin/env bash
# F5 – Corrida pelo recurso: 100 pedidos simultâneos para 10 SUVs (BNU) no mesmo dia
source "$(dirname "$0")/lib.sh"
N=${N:-100}
titulo "F5: $N requisições simultâneas para 10 unidades"
gateway_estavel
D=$(data_futura)
echo "dia $D, filial BNU, categoria SUV (capacidade 10)"
TMP=$(mktemp)
seq 1 "$N" | xargs -P "$N" -I{} sh -c \
  "curl -s -X POST $API/reservas -H 'Content-Type: application/json' \
   -d '{\"clienteId\":\"cli-{}\",\"filial\":\"BNU\",\"categoria\":\"SUV\",\"dataInicio\":\"$D\",\"dataFim\":\"$D\"}' | jq -r .reservaId" > "$TMP"
echo "$(wc -l < "$TMP") pedidos aceitos com 202"
echo "aguardando a saga terminar..."; sleep 20
while read -r id; do esperar_final "$id" 60; done < "$TMP" | sort | uniq -c
titulo "Ocupação na Frota (nunca passa da capacidade)"
sql frota "SELECT data, capacidade, reservados FROM ocupacao WHERE filial='BNU' AND categoria='SUV' AND data='$D'"
sql frota "SELECT status, count(*) FROM bloqueios WHERE filial='BNU' AND categoria='SUV' AND data_inicio='$D' GROUP BY status"
rm -f "$TMP"; gateway_padrao >/dev/null
