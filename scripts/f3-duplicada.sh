#!/usr/bin/env bash
# F3 – Mensagem duplicada: republicar a MESMA ReservaSolicitada (mesmo messageId) 5 vezes
source "$(dirname "$0")/lib.sh"
titulo "F3: mesma mensagem publicada 5 vezes"
gateway_estavel >/dev/null
D=$(data_futura)
ID=$(reservar BNU SUV "$D" "$D"); echo "reserva $ID -> $(esperar_final "$ID" 30)"
MSG=$(docker compose exec -T reservas-db psql -U app -d app -At -c \
  "SELECT corpo FROM outbox WHERE tipo='ReservaSolicitada' AND corpo->>'correlationId' = '$ID'")
MID=$(echo "$MSG" | jq -r .messageId); echo "messageId original: $MID"
sql frota "SELECT data, reservados FROM ocupacao WHERE filial='BNU' AND categoria='SUV' AND data='$D'"
for i in 1 2 3 4 5; do publicar ReservaSolicitada "$MSG"; echo "publicada cópia $i"; done
sleep 8
titulo "Resultado: um único efeito"
docker compose logs --no-log-prefix frota | grep "$MID" | tail -6
sql frota "SELECT count(*) AS vezes_registrada FROM mensagens_processadas WHERE message_id = '$MID'"
sql frota "SELECT data, reservados FROM ocupacao WHERE filial='BNU' AND categoria='SUV' AND data='$D'"
gateway_padrao >/dev/null
