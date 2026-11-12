#!/usr/bin/env bash
# F7 – Resposta tardia: o pagamento é aprovado DEPOIS que a reserva expirou -> estorno
source "$(dirname "$0")/lib.sh"
titulo "F7: latência do gateway (9 s) maior que o timeout da reserva (5 s)"
awsl ssm put-parameter --name /locacao/reservas/timeout-pagamento-segundos --value 5 --overwrite >/dev/null
echo "timeout da reserva = 5 s (SSM); aguardando o Reservas reler (10 s)"; sleep 11
gateway '{"latencia_ms":9000,"jitter_ms":0,"taxa_falha":0,"taxa_recusa":0}'
D=$(data_futura)
ID=$(reservar BNU SEDAN "$D" "$D"); echo "reserva $ID"
sleep 25
curl -s "$API/reservas/$ID" | jq '{status, motivo, estornoSolicitado, historico: [.historico[] | "\(.de // "-") -> \(.para) (\(.causa))"]}'
sql pagamentos "SELECT status, motivo, transacao_id FROM pagamentos WHERE reserva_id = '$ID'"
sql frota "SELECT status, motivo FROM bloqueios WHERE reserva_id = '$ID'"
curl -s "$NOTIF/notificacoes?reservaId=$ID" | jq '.[] | {tipo, assunto}'
awsl ssm put-parameter --name /locacao/reservas/timeout-pagamento-segundos --value 30 --overwrite >/dev/null
gateway_padrao >/dev/null; ok "ambiente restaurado"
