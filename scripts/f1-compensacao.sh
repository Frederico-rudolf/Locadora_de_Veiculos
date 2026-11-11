#!/usr/bin/env bash
# F1 – Compensação: gateway com 100% de falha -> veículo liberado, reserva cancelada, cliente notificado
source "$(dirname "$0")/lib.sh"
titulo "F1: taxa de falha do gateway em 100%"
gateway '{"latencia_ms":300,"jitter_ms":0,"taxa_falha":1.0,"taxa_recusa":0}'
D=$(data_futura)
ID=$(reservar BNU SEDAN "$D" "$D"); echo "reserva $ID para $D"
echo "status final: $(esperar_final "$ID" 60)"
curl -s "$API/reservas/$ID" | jq '{status, motivo, historico: [.historico[] | "\(.de // "-") -> \(.para) (\(.causa))"]}'
sleep 2  # a ReservaCancelada leva um instante para chegar à Frota
titulo "Frota: bloqueio e ocupação do dia"
sql frota "SELECT reserva_id, status, motivo FROM bloqueios WHERE reserva_id = '$ID'"
sql frota "SELECT data, capacidade, reservados FROM ocupacao WHERE filial='BNU' AND categoria='SEDAN' AND data='$D'"
titulo "Pagamentos e notificação"
sql pagamentos "SELECT reserva_id, status, motivo, tentativas_gateway, processado_por FROM pagamentos WHERE reserva_id = '$ID'"
sleep 2; curl -s "$NOTIF/notificacoes?reservaId=$ID" | jq '.[] | {tipo, assunto, corpo}'
gateway_padrao >/dev/null; ok "gateway voltou ao padrão"
