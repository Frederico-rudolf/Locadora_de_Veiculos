// Serviço Reservas: API assíncrona (R3) + orquestrador da saga de locação.
//
// Máquina de estados da reserva:
//   PENDENTE --VeiculoBloqueado--> AGUARDANDO_PAGAMENTO --PagamentoAprovado--> CONFIRMADA
//   PENDENTE --VeiculoIndisponivel--> REJEITADA
//   AGUARDANDO_PAGAMENTO --PagamentoRecusado--> CANCELADA        (compensação: Frota libera)
//   AGUARDANDO_PAGAMENTO --timeout--> EXPIRADA                    (compensação: Frota libera)
//   EXPIRADA/CANCELADA + PagamentoAprovado tardio --> EstornoSolicitado (F7)
import express from "express";
import { randomUUID } from "crypto";
import { PoolClient } from "pg";
import {
  COMMON_SQL, Envelope, Handler, PoisonMessage, brokerStatus, config, enqueue, getParam, log, migrate,
  pool, requireFields, resolveBroker, sleep, startConsumer, startOutboxRelay, withTx,
} from "./infra";

const SQL = `
${COMMON_SQL}
CREATE TABLE IF NOT EXISTS reservas (
  id                 uuid PRIMARY KEY,
  idempotency_key    text UNIQUE,
  cliente_id         text NOT NULL,
  cliente_email      text,
  filial             text NOT NULL,
  categoria          text NOT NULL,
  data_inicio        date NOT NULL,
  data_fim           date NOT NULL,
  valor_total        numeric(12,2) NOT NULL,
  status             text NOT NULL,
  motivo             text,
  transacao_id       text,
  expira_em          timestamptz,
  estorno_solicitado boolean NOT NULL DEFAULT false,
  criado_em          timestamptz NOT NULL DEFAULT now(),
  atualizado_em      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS reservas_expiracao ON reservas (expira_em) WHERE status = 'AGUARDANDO_PAGAMENTO';
CREATE TABLE IF NOT EXISTS reserva_historico (
  id         bigserial PRIMARY KEY,
  reserva_id uuid NOT NULL REFERENCES reservas(id),
  de         text,
  para       text NOT NULL,
  causa      text NOT NULL,
  em         timestamptz NOT NULL DEFAULT now()
);
`;

const CATEGORIAS = ["HATCH", "SEDAN", "SUV"];
let precos: Record<string, number> = { HATCH: 120, SEDAN: 180, SUV: 250 };
let timeoutPagamentoSeg = 30;
const versaoConfirmada = Number(process.env.RESERVA_CONFIRMADA_VERSAO ?? "2");

// ------------------------------------------------------------------ helpers
type Reserva = {
  id: string; cliente_id: string; cliente_email: string | null; filial: string; categoria: string;
  data_inicio: string; data_fim: string; valor_total: string; status: string; estorno_solicitado: boolean;
  transacao_id: string | null;
};

async function lockReserva(c: PoolClient, id: string): Promise<Reserva> {
  const { rows } = await c.query(
    `SELECT id, cliente_id, cliente_email, filial, categoria,
            to_char(data_inicio,'YYYY-MM-DD') AS data_inicio, to_char(data_fim,'YYYY-MM-DD') AS data_fim,
            valor_total, status, estorno_solicitado, transacao_id
       FROM reservas WHERE id = $1 FOR UPDATE`,
    [id]
  );
  // reserva inexistente não deveria acontecer (o evento nasce na mesma transação da reserva): trata como venenosa
  if (rows.length === 0) throw new PoisonMessage(`reserva ${id} não existe`);
  return rows[0];
}

async function transicao(c: PoolClient, r: Reserva, para: string, causa: string, extra: Record<string, unknown> = {}) {
  const sets = ["status = $2", "atualizado_em = now()"];
  const vals: unknown[] = [r.id, para];
  for (const [k, v] of Object.entries(extra)) {
    vals.push(v);
    sets.push(`${k} = $${vals.length}`);
  }
  await c.query(`UPDATE reservas SET ${sets.join(", ")} WHERE id = $1`, vals);
  await c.query("INSERT INTO reserva_historico (reserva_id, de, para, causa) VALUES ($1,$2,$3,$4)", [r.id, r.status, para, causa]);
  log("info", "transição de estado", { correlationId: r.id, de: r.status, para, causa });
}

function dadosReserva(r: Reserva) {
  return {
    reservaId: r.id, clienteId: r.cliente_id, clienteEmail: r.cliente_email ?? undefined,
    filial: r.filial, categoria: r.categoria, dataInicio: r.data_inicio, dataFim: r.data_fim,
  };
}

const cancelar = (c: PoolClient, r: Reserva, motivo: string) =>
  enqueue(c, "ReservaCancelada", 1, r.id, { ...dadosReserva(r), motivo });

// ------------------------------------------------------------------ handlers da saga
const handlers: Record<string, Handler> = {
  async VeiculoBloqueado(c, e: Envelope) {
    requireFields(e.payload, { reservaId: "uuid" });
    const r = await lockReserva(c, e.payload.reservaId);
    if (r.status !== "PENDENTE") return log("warn", "VeiculoBloqueado fora de ordem, ignorado", { correlationId: r.id, status: r.status });
    await transicao(c, r, "AGUARDANDO_PAGAMENTO", "VeiculoBloqueado", {
      expira_em: new Date(Date.now() + timeoutPagamentoSeg * 1000),
    });
    await enqueue(c, "PagamentoSolicitado", 1, r.id, { reservaId: r.id, clienteId: r.cliente_id, valor: Number(r.valor_total) });
  },

  async VeiculoIndisponivel(c, e) {
    requireFields(e.payload, { reservaId: "uuid", motivo: "string" });
    const r = await lockReserva(c, e.payload.reservaId);
    if (r.status !== "PENDENTE") return;
    await transicao(c, r, "REJEITADA", "VeiculoIndisponivel", { motivo: "SEM_DISPONIBILIDADE" });
    await cancelar(c, r, "SEM_DISPONIBILIDADE");
  },

  async PagamentoAprovado(c, e) {
    requireFields(e.payload, { reservaId: "uuid", transacaoId: "string", valor: "number" });
    const r = await lockReserva(c, e.payload.reservaId);
    if (r.status === "AGUARDANDO_PAGAMENTO") {
      await transicao(c, r, "CONFIRMADA", "PagamentoAprovado", { transacao_id: e.payload.transacaoId, expira_em: null });
      const base = dadosReserva(r);
      if (versaoConfirmada >= 2) {
        await enqueue(c, "ReservaConfirmada", 2, r.id, {
          ...base, valor: { total: Number(r.valor_total), moeda: "BRL" }, transacaoId: e.payload.transacaoId,
        });
      } else {
        await enqueue(c, "ReservaConfirmada", 1, r.id, { ...base, valorTotal: Number(r.valor_total) });
      }
    } else if (["EXPIRADA", "CANCELADA", "REJEITADA"].includes(r.status) && !r.estorno_solicitado) {
      // F7: o pagamento chegou depois que a reserva já foi desfeita -> compensar o pagamento também
      await c.query("UPDATE reservas SET estorno_solicitado = true, atualizado_em = now() WHERE id = $1", [r.id]);
      await c.query("INSERT INTO reserva_historico (reserva_id, de, para, causa) VALUES ($1,$2,$2,$3)", [
        r.id, r.status, "PagamentoAprovado tardio -> EstornoSolicitado",
      ]);
      await enqueue(c, "EstornoSolicitado", 1, r.id, { reservaId: r.id, motivo: `PAGAMENTO_TARDIO_RESERVA_${r.status}` });
      log("warn", "pagamento aprovado após expiração: estorno solicitado", { correlationId: r.id });
    }
  },

  async PagamentoRecusado(c, e) {
    requireFields(e.payload, { reservaId: "uuid", motivo: "string" });
    const r = await lockReserva(c, e.payload.reservaId);
    if (r.status !== "AGUARDANDO_PAGAMENTO") return;
    await transicao(c, r, "CANCELADA", `PagamentoRecusado:${e.payload.motivo}`, { motivo: "PAGAMENTO_RECUSADO", expira_em: null });
    await cancelar(c, r, "PAGAMENTO_RECUSADO");
  },
};

// ------------------------------------------------------------------ expiração (timeout da saga)
function startExpiracao() {
  (async () => {
    for (;;) {
      try {
        await withTx(async (c) => {
          const { rows } = await c.query(
            `SELECT id FROM reservas
              WHERE status = 'AGUARDANDO_PAGAMENTO' AND expira_em < now()
              ORDER BY expira_em LIMIT 100 FOR UPDATE SKIP LOCKED`
          );
          for (const { id } of rows) {
            const r = await lockReserva(c, id);
            await transicao(c, r, "EXPIRADA", "timeout de pagamento", { motivo: "TIMEOUT_PAGAMENTO", expira_em: null });
            await cancelar(c, r, "TIMEOUT_PAGAMENTO");
          }
        });
      } catch (e) {
        log("error", "varredura de expiração falhou", { erro: String(e) });
      }
      await sleep(1000);
    }
  })();
}

// ------------------------------------------------------------------ API HTTP
const app = express();
app.use(express.json());

const isDate = (s: unknown) => typeof s === "string" && /^\d{4}-\d{2}-\d{2}$/.test(s) && !isNaN(Date.parse(s));

// R3: POST responde 202 Accepted + id; o resultado vem depois em GET /reservas/:id
app.post("/reservas", async (req, res) => {
  const b = req.body ?? {};
  const erros: string[] = [];
  if (typeof b.clienteId !== "string" || !b.clienteId) erros.push("clienteId obrigatório");
  if (typeof b.filial !== "string" || !b.filial) erros.push("filial obrigatória");
  if (!CATEGORIAS.includes(b.categoria)) erros.push(`categoria deve ser ${CATEGORIAS.join("|")}`);
  if (!isDate(b.dataInicio) || !isDate(b.dataFim)) erros.push("dataInicio/dataFim no formato AAAA-MM-DD");
  const dias = erros.length ? 0 : Math.round((Date.parse(b.dataFim) - Date.parse(b.dataInicio)) / 86400000) + 1;
  if (!erros.length && (dias < 1 || dias > 30)) erros.push("período deve ter de 1 a 30 dias");
  if (erros.length) return res.status(400).json({ erros });

  const idemKey = req.header("Idempotency-Key") ?? null;
  const valorTotal = dias * (precos[b.categoria] ?? 200);
  try {
    const out = await withTx(async (c) => {
      if (idemKey) {
        const ex = await c.query("SELECT id, status FROM reservas WHERE idempotency_key = $1", [idemKey]);
        if (ex.rows.length) return { id: ex.rows[0].id, status: ex.rows[0].status, repetida: true };
      }
      const id = randomUUID();
      await c.query(
        `INSERT INTO reservas (id, idempotency_key, cliente_id, cliente_email, filial, categoria, data_inicio, data_fim, valor_total, status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'PENDENTE')`,
        [id, idemKey, b.clienteId, b.clienteEmail ?? null, b.filial, b.categoria, b.dataInicio, b.dataFim, valorTotal]
      );
      await c.query("INSERT INTO reserva_historico (reserva_id, de, para, causa) VALUES ($1, NULL, 'PENDENTE', 'POST /reservas')", [id]);
      // a reserva e o evento são gravados juntos (outbox): ou os dois existem, ou nenhum
      await enqueue(c, "ReservaSolicitada", 1, id, {
        reservaId: id, clienteId: b.clienteId, clienteEmail: b.clienteEmail, filial: b.filial, categoria: b.categoria,
        dataInicio: b.dataInicio, dataFim: b.dataFim, valorTotal,
      });
      return { id, status: "PENDENTE", repetida: false };
    });
    res.status(202).location(`/reservas/${out.id}`).json({
      reservaId: out.id, status: out.status, valorTotal, statusUrl: `/reservas/${out.id}`, repetida: out.repetida,
    });
  } catch (e: any) {
    if (e?.code === "23505" && idemKey) return res.status(409).json({ erro: "requisição concorrente com a mesma Idempotency-Key" });
    log("error", "falha ao criar reserva", { erro: String(e) });
    res.status(503).json({ erro: "indisponível, tente de novo" });
  }
});

app.get("/reservas/resumo", async (_req, res) => {
  const { rows } = await pool.query("SELECT status, count(*)::int AS total FROM reservas GROUP BY status ORDER BY status");
  res.json(Object.fromEntries(rows.map((r) => [r.status, r.total])));
});

app.get("/reservas/:id", async (req, res) => {
  if (!/^[0-9a-f-]{36}$/i.test(req.params.id)) return res.status(404).end();
  const { rows } = await pool.query(
    `SELECT id AS "reservaId", status, motivo, cliente_id AS "clienteId", filial, categoria,
            to_char(data_inicio,'YYYY-MM-DD') AS "dataInicio", to_char(data_fim,'YYYY-MM-DD') AS "dataFim",
            valor_total::float AS "valorTotal", transacao_id AS "transacaoId", expira_em AS "expiraEm",
            estorno_solicitado AS "estornoSolicitado", criado_em AS "criadoEm", atualizado_em AS "atualizadoEm"
       FROM reservas WHERE id = $1`,
    [req.params.id]
  );
  if (!rows.length) return res.status(404).json({ erro: "reserva não encontrada" });
  const hist = await pool.query("SELECT de, para, causa, em FROM reserva_historico WHERE reserva_id = $1 ORDER BY id", [req.params.id]);
  res.json({ ...rows[0], historico: hist.rows });
});

app.get("/health", async (_req, res) => {
  try {
    await pool.query("SELECT 1");
    const pend = await pool.query("SELECT count(*)::int AS n FROM outbox WHERE publicado_em IS NULL");
    res.json({ status: "UP", servico: "reservas", outboxPendente: pend.rows[0].n, broker: brokerStatus });
  } catch (e) {
    res.status(503).json({ status: "DOWN", erro: String(e) });
  }
});

// ------------------------------------------------------------------ inicialização
async function carregarConfig(primeira: boolean) {
  const antes = JSON.stringify({ precos, timeoutPagamentoSeg });
  try {
    precos = JSON.parse(await getParam("/locacao/reservas/precos-diaria", JSON.stringify(precos)));
  } catch {
    /* mantém o anterior */
  }
  const t = Number(await getParam("/locacao/reservas/timeout-pagamento-segundos", String(timeoutPagamentoSeg)));
  if (Number.isFinite(t) && t > 0) timeoutPagamentoSeg = t;
  if (primeira || antes !== JSON.stringify({ precos, timeoutPagamentoSeg })) {
    log("info", "configuração carregada do SSM", { precos, timeoutPagamentoSeg, versaoConfirmada, fila: config.queueName });
  }
}

async function main() {
  await migrate(SQL);
  const port = Number(process.env.PORT ?? 3000);
  app.listen(port, () => log("info", `API ouvindo na porta ${port}`));
  await resolveBroker();
  await carregarConfig(true);
  // configuração central no SSM, relida a cada 10 s (o F7 muda o timeout sem reiniciar o serviço)
  setInterval(() => carregarConfig(false).catch(() => undefined), 10000);
  startOutboxRelay();
  startConsumer(handlers);
  startExpiracao();
}

main().catch((e) => {
  log("error", "falha fatal", { erro: String(e) });
  process.exit(1);
});
