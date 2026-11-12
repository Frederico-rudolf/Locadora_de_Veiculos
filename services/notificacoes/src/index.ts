// Serviço Notificações: um dos interessados em ReservaConfirmada / ReservaCancelada.
// Envia e-mail (simulado em log), grava a notificação no próprio banco e o contrato no S3.
// Demonstra versionamento de eventos: aceita ReservaConfirmada v1 e v2 (upcaster v1 -> v2).
import express from "express";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { PoolClient } from "pg";
import {
  COMMON_SQL, Envelope, Handler, PoisonMessage, brokerStatus, config, log, migrate, pool,
  requireFields, resolveBroker, startConsumer,
} from "./infra";

const SQL = `
${COMMON_SQL}
CREATE TABLE IF NOT EXISTS notificacoes (
  id           bigserial PRIMARY KEY,
  reserva_id   uuid NOT NULL,
  tipo         text NOT NULL,
  destinatario text,
  assunto      text NOT NULL,
  corpo        text NOT NULL,
  contrato_s3  text,
  message_id   uuid NOT NULL,
  versao_evento int NOT NULL,
  criada_em    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (reserva_id, tipo)
);
`;

const bucket = process.env.BUCKET_CONTRATOS ?? "locacao-contratos";
const s3 = new S3Client({ region: config.region, endpoint: config.endpoint, forcePathStyle: true });

// ------------------------------------------------------------------ versionamento
interface ConfirmadaV2 {
  reservaId: string; clienteId: string; clienteEmail?: string; filial: string; categoria: string;
  dataInicio: string; dataFim: string; valor: { total: number; moeda: string }; transacaoId?: string;
}

/** Upcaster: converte qualquer versão conhecida para o modelo atual (v2). */
function reservaConfirmada(e: Envelope): ConfirmadaV2 {
  const p = e.payload;
  requireFields(p, { reservaId: "uuid", clienteId: "string", filial: "string", categoria: "string", dataInicio: "date", dataFim: "date" });
  if (e.version === 1) {
    requireFields(p, { valorTotal: "number" });
    return { ...p, valor: { total: p.valorTotal, moeda: "BRL" } };
  }
  if (e.version === 2) {
    if (typeof p.valor?.total !== "number" || typeof p.valor?.moeda !== "string") throw new PoisonMessage("valor inválido na v2");
    return p;
  }
  throw new PoisonMessage(`ReservaConfirmada versão ${e.version} desconhecida`);
}

// ------------------------------------------------------------------ efeitos
async function registrar(c: PoolClient, e: Envelope, tipo: string, destinatario: string | undefined, assunto: string, corpo: string, contrato?: string) {
  await c.query(
    `INSERT INTO notificacoes (reserva_id, tipo, destinatario, assunto, corpo, contrato_s3, message_id, versao_evento)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (reserva_id, tipo) DO NOTHING`,
    [e.payload.reservaId, tipo, destinatario ?? null, assunto, corpo, contrato ?? null, e.messageId, e.version]
  );
  log("info", `E-MAIL (simulado) para ${destinatario ?? "cliente"}: ${assunto}`, { correlationId: e.correlationId });
}

function contratoHtml(r: ConfirmadaV2) {
  return `<!doctype html><html lang="pt-BR"><meta charset="utf-8"><title>Contrato ${r.reservaId}</title>
<body style="font-family:sans-serif;max-width:640px;margin:40px auto">
<h1>LocaFácil – Contrato de locação</h1>
<p><b>Reserva:</b> ${r.reservaId}<br><b>Cliente:</b> ${r.clienteId}</p>
<p><b>Filial:</b> ${r.filial} · <b>Categoria:</b> ${r.categoria}</p>
<p><b>Período:</b> ${r.dataInicio} a ${r.dataFim}</p>
<p><b>Valor:</b> ${r.valor.moeda} ${r.valor.total.toFixed(2)}${r.transacaoId ? ` · transação ${r.transacaoId}` : ""}</p>
<p>Emitido em ${new Date().toISOString()}</p></body></html>`;
}

const handlers: Record<string, Handler> = {
  async ReservaConfirmada(c, e) {
    const r = reservaConfirmada(e);
    const chave = `contratos/${r.reservaId}.html`;
    // chave determinística: regravar o mesmo contrato numa reentrega é inofensivo.
    // Se o S3 falhar, a exceção desfaz a transação e a mensagem volta para a fila.
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: chave, Body: contratoHtml(r), ContentType: "text/html; charset=utf-8" }));
    await registrar(c, e, "CONFIRMACAO", r.clienteEmail, `Reserva ${r.reservaId} confirmada (${r.categoria}, ${r.dataInicio} a ${r.dataFim})`,
      `Valor ${r.valor.moeda} ${r.valor.total}`, `s3://${bucket}/${chave}`);
  },

  async ReservaCancelada(c, e) {
    requireFields(e.payload, { reservaId: "uuid", motivo: "string" });
    const motivos: Record<string, string> = {
      SEM_DISPONIBILIDADE: "não há veículo disponível para o período",
      PAGAMENTO_RECUSADO: "o pagamento não foi aprovado",
      TIMEOUT_PAGAMENTO: "o pagamento não foi confirmado a tempo",
    };
    await registrar(c, e, "CANCELAMENTO", e.payload.clienteEmail, `Reserva ${e.payload.reservaId} cancelada`,
      motivos[e.payload.motivo] ?? e.payload.motivo);
  },

  async PagamentoEstornado(c, e) {
    requireFields(e.payload, { reservaId: "uuid", valor: "number" });
    await registrar(c, e, "ESTORNO", undefined, `Estorno de R$ ${e.payload.valor} da reserva ${e.payload.reservaId}`,
      "O valor cobrado após o cancelamento foi devolvido.");
  },
};

// ------------------------------------------------------------------ API
const app = express();
app.get("/notificacoes", async (req: any, res: any) => {
  const id = req.query.reservaId;
  const { rows } = id
    ? await pool.query("SELECT * FROM notificacoes WHERE reserva_id = $1 ORDER BY id", [id])
    : await pool.query("SELECT * FROM notificacoes ORDER BY id DESC LIMIT 50");
  res.json(rows);
});
app.get("/health", async (_req: any, res: any) => {
  try {
    await pool.query("SELECT 1");
    res.json({ status: "UP", servico: "notificacoes", broker: brokerStatus });
  } catch (e) {
    res.status(503).json({ status: "DOWN", erro: String(e) });
  }
});

async function main() {
  await migrate(SQL);
  const port = Number(process.env.PORT ?? 3000);
  app.listen(port, () => log("info", `API ouvindo na porta ${port}`));
  await resolveBroker();
  startConsumer(handlers);
}

main().catch((e) => {
  log("error", "falha fatal", { erro: String(e) });
  process.exit(1);
});
