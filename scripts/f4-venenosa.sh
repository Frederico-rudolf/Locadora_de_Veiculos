#!/usr/bin/env bash
# F4 – Mensagem venenosa: N tentativas, depois DLQ; as demais seguem normalmente
source "$(dirname "$0")/lib.sh"
titulo "F4: payload inválido"
gateway_estavel >/dev/null
echo "DLQ antes: $(contar_fila frota-dlq)"
publicar ReservaSolicitada "$(jq -c ".messageId = \"$(cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen)\"" contracts/examples/venenosa.json)"
publicar ReservaSolicitada 'isto nao e json {{{'
ok "2 mensagens venenosas publicadas"
D=$(data_futura)
ID=$(reservar BNU HATCH "$D" "$D"); echo "reserva normal publicada em seguida: $ID"
echo "status da reserva normal: $(esperar_final "$ID" 40)"
echo "aguardando as tentativas (maxReceiveCount=3)..."; sleep 15
docker compose logs --no-log-prefix frota | grep VENENOSA | tail -6
echo "DLQ depois: $(contar_fila frota-dlq)"
