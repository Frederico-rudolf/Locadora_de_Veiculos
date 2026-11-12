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
import { Pool, PoolClient } from "pg";
import {
  COMMON_SQL, Envelope, Handler, PoisonMessage, brokerStatus, config, enqueue, getParam, log, migrate,
  pool, requireFields, resolveBroker, sleep, startConsumer, startOutboxRelay, withTx,
  contextoDe, traceparentAtual, tracer,
} from "./infra";
import { SpanStatusCode, context, trace } from "@opentelemetry/api";

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
ALTER TABLE reservas ADD COLUMN IF NOT EXISTS traceparent text;  -- trace do POST, retomado na expiração
CREATE INDEX IF NOT EXISTS reservas_expiracao ON reservas (expira_em) WHERE status = 'AGUARDANDO_PAGAMENTO';
CREATE TABLE IF NOT EXISTS reserva_historico (
  id         bigserial PRIMARY KEY,
  reserva_id uuid NOT NULL REFERENCES reservas(id),
  de         text,
  para       text NOT NULL,
  causa      text NOT NULL,
  em         timestamptz NOT NULL DEFAULT now()
);
-- CQRS: reservas alteradas que ainda precisam ser copiadas para o modelo de leitura.
-- Gravada na MESMA transação da mudança, então nenhuma alteração escapa da projeção.
CREATE TABLE IF NOT EXISTS projecao_pendente (
  reserva_id uuid PRIMARY KEY,
  marcada_em timestamptz NOT NULL DEFAULT now()
);
`;

// Modelo de leitura (CQRS): banco SEPARADO, desnormalizado, otimizado para as consultas da API.
const SQL_LEITURA = `
CREATE TABLE IF NOT EXISTS reservas_view (
  reserva_id         uuid PRIMARY KEY,
  status             text NOT NULL,
  motivo             text,
  cliente_id         text NOT NULL,
  cliente_email      text,
  filial             text NOT NULL,
  categoria          text NOT NULL,
  data_inicio        text NOT NULL,
  data_fim           text NOT NULL,
  valor_total        numeric(12,2) NOT NULL,
  transacao_id       text,
  expira_em          timestamptz,
  estorno_solicitado boolean NOT NULL,
  criado_em          timestamptz NOT NULL,
  atualizado_em      timestamptz NOT NULL,
  historico          jsonb NOT NULL,
  projetado_em       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS reservas_view_cliente ON reservas_view (cliente_id, criado_em DESC);
CREATE INDEX IF NOT EXISTS reservas_view_status ON reservas_view (status);
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

/** CQRS: marca a reserva para o projetor copiar o novo estado para o banco de leitura. */
async function marcarProjecao(c: PoolClient, id: string) {
  await c.query("INSERT INTO projecao_pendente (reserva_id) VALUES ($1) ON CONFLICT DO NOTHING", [id]);
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
  await marcarProjecao(c, r.id);
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
      await marcarProjecao(c, r.id);
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
            `SELECT id, traceparent FROM reservas
              WHERE status = 'AGUARDANDO_PAGAMENTO' AND expira_em < now()
              ORDER BY expira_em LIMIT 100 FOR UPDATE SKIP LOCKED`
          );
          for (const { id, traceparent } of rows) {
            // a expiração vira um span do MESMO trace da reserva, então a compensação aparece na mesma linha do tempo
            const parent = contextoDe(traceparent);
            const span = tracer.startSpan("reserva expirada (timeout da saga)", { attributes: { "locafacil.correlation_id": id } }, parent);
            try {
              await context.with(trace.setSpan(parent, span), async () => {
                const r = await lockReserva(c, id);
                await transicao(c, r, "EXPIRADA", "timeout de pagamento", { motivo: "TIMEOUT_PAGAMENTO", expira_em: null });
                await cancelar(c, r, "TIMEOUT_PAGAMENTO");
              });
            } catch (e) {
              span.setStatus({ code: SpanStatusCode.ERROR, message: String(e) });
              throw e;
            } finally {
              span.end();
            }
          }
        });
      } catch (e) {
        log("error", "varredura de expiração falhou", { erro: String(e) });
      }
      await sleep(1000);
    }
  })();
}

// ------------------------------------------------------------------ CQRS: projetor do modelo de leitura
// Lado de escrita: tabelas reservas/reserva_historico (banco reservas-db), normalizadas, com locks e máquina de estados.
// Lado de leitura: reservas_view (banco reservas-leitura-db), uma linha pronta por reserva com o histórico embutido.
// O projetor copia o ESTADO ATUAL (não aplica deltas), então reprocessar é sempre seguro (idempotente),
// e a ordem em que as reservas são projetadas não importa.
const leitura = new Pool({ connectionString: process.env.READ_DATABASE_URL ?? config.databaseUrl, max: 10 });
const projecao = { ultimaProjecao: 0 };

async function migrarLeitura() {
  for (let i = 1; ; i++) {
    try {
      const c = await leitura.connect();
      try {
        await c.query("BEGIN");
        await c.query("SELECT pg_advisory_xact_lock(43)");
        await c.query(SQL_LEITURA);
        await c.query("COMMIT");
      } finally {
        c.release();
      }
      return;
    } catch (e) {
      log("warn", "banco de leitura indisponível, tentando de novo", { tentativa: i, erro: String(e) });
      await sleep(Math.min(1000 * i, 5000));
    }
  }
}

async function projetarLote(): Promise<number> {
  return withTx(async (c) => {
    const { rows: ids } = await c.query(
      "SELECT reserva_id FROM projecao_pendente ORDER BY marcada_em LIMIT 100 FOR UPDATE SKIP LOCKED"
    );
    if (!ids.length) return 0;
    const lista = ids.map((r) => r.reserva_id);
    const { rows } = await c.query(
      `SELECT r.id, r.status, r.motivo, r.cliente_id, r.cliente_email, r.filial, r.categoria,
              to_char(r.data_inicio,'YYYY-MM-DD') AS data_inicio, to_char(r.data_fim,'YYYY-MM-DD') AS data_fim,
              r.valor_total, r.transacao_id, r.expira_em, r.estorno_solicitado, r.criado_em, r.atualizado_em,
              COALESCE((SELECT jsonb_agg(jsonb_build_object('de', h.de, 'para', h.para, 'causa', h.causa, 'em', h.em) ORDER BY h.id)
                          FROM reserva_historico h WHERE h.reserva_id = r.id), '[]'::jsonb) AS historico
         FROM reservas r WHERE r.id = ANY($1::uuid[])`,
      [lista]
    );
    for (const r of rows) {
      // upsert do estado completo; o WHERE impede que uma cópia mais velha sobrescreva uma mais nova
      await leitura.query(
        `INSERT INTO reservas_view (reserva_id, status, motivo, cliente_id, cliente_email, filial, categoria, data_inicio,
                                    data_fim, valor_total, transacao_id, expira_em, estorno_solicitado, criado_em, atualizado_em, historico)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
         ON CONFLICT (reserva_id) DO UPDATE SET
           status = EXCLUDED.status, motivo = EXCLUDED.motivo, transacao_id = EXCLUDED.transacao_id,
           expira_em = EXCLUDED.expira_em, estorno_solicitado = EXCLUDED.estorno_solicitado,
           atualizado_em = EXCLUDED.atualizado_em, historico = EXCLUDED.historico, projetado_em = now()
         WHERE reservas_view.atualizado_em <= EXCLUDED.atualizado_em`,
        [r.id, r.status, r.motivo, r.cliente_id, r.cliente_email, r.filial, r.categoria, r.data_inicio, r.data_fim,
         r.valor_total, r.transacao_id, r.expira_em, r.estorno_solicitado, r.criado_em, r.atualizado_em, JSON.stringify(r.historico)]
      );
    }
    // só remove as marcas depois de gravar no banco de leitura; se cair no meio, o lote é projetado de novo
    await c.query("DELETE FROM projecao_pendente WHERE reserva_id = ANY($1::uuid[])", [lista]);
    projecao.ultimaProjecao = Date.now();
    return lista.length;
  });
}

function startProjetor() {
  (async () => {
    for (;;) {
      try {
        const n = await projetarLote();
        if (n === 0) await sleep(100);
      } catch (e) {
        log("warn", "projeção para o modelo de leitura falhou, tentando de novo", { erro: String(e) });
        await sleep(1000);
      }
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
        `INSERT INTO reservas (id, idempotency_key, cliente_id, cliente_email, filial, categoria, data_inicio, data_fim, valor_total, status, traceparent)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'PENDENTE',$10)`,
        [id, idemKey, b.clienteId, b.clienteEmail ?? null, b.filial, b.categoria, b.dataInicio, b.dataFim, valorTotal, traceparentAtual()]
      );
      await c.query("INSERT INTO reserva_historico (reserva_id, de, para, causa) VALUES ($1, NULL, 'PENDENTE', 'POST /reservas')", [id]);
      await marcarProjecao(c, id);
      // a reserva e o evento são gravados juntos (outbox): ou os dois existem, ou nenhum
      await enqueue(c, "ReservaSolicitada", 1, id, {
        reservaId: id, clienteId: b.clienteId, clienteEmail: b.clienteEmail, filial: b.filial, categoria: b.categoria,
        dataInicio: b.dataInicio, dataFim: b.dataFim, valorTotal,
      });
      trace.getActiveSpan()?.setAttribute("locafacil.correlation_id", id);
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

// ---- consultas (CQRS): leem SOMENTE o banco de leitura, nunca as tabelas de escrita
const COLUNAS_VIEW = `reserva_id AS "reservaId", status, motivo, cliente_id AS "clienteId", filial, categoria,
  data_inicio AS "dataInicio", data_fim AS "dataFim", valor_total::float AS "valorTotal", transacao_id AS "transacaoId",
  expira_em AS "expiraEm", estorno_solicitado AS "estornoSolicitado", criado_em AS "criadoEm",
  atualizado_em AS "atualizadoEm", projetado_em AS "projetadoEm"`;

app.get("/reservas/resumo", async (_req, res) => {
  try {
    const { rows } = await leitura.query("SELECT status, count(*)::int AS total FROM reservas_view GROUP BY status ORDER BY status");
    res.json(Object.fromEntries(rows.map((r) => [r.status, r.total])));
  } catch (e) {
    res.status(503).json({ erro: "modelo de leitura indisponível" });
  }
});

app.get("/reservas", async (req, res) => {
  const filtros: string[] = [];
  const vals: unknown[] = [];
  for (const [param, col] of [["clienteId", "cliente_id"], ["status", "status"], ["filial", "filial"], ["categoria", "categoria"]]) {
    const v = req.query[param];
    if (typeof v === "string" && v) {
      vals.push(v);
      filtros.push(`${col} = $${vals.length}`);
    }
  }
  const limite = Math.min(Number(req.query.limite ?? 50) || 50, 500);
  try {
    const { rows } = await leitura.query(
      `SELECT ${COLUNAS_VIEW} FROM reservas_view ${filtros.length ? "WHERE " + filtros.join(" AND ") : ""}
        ORDER BY criado_em DESC LIMIT ${limite}`,
      vals
    );
    res.json(rows);
  } catch (e) {
    res.status(503).json({ erro: "modelo de leitura indisponível" });
  }
});

app.get("/reservas/:id", async (req, res) => {
  if (!/^[0-9a-f-]{36}$/i.test(req.params.id)) return res.status(404).end();
  try {
    const { rows } = await leitura.query(`SELECT ${COLUNAS_VIEW}, historico FROM reservas_view WHERE reserva_id = $1`, [req.params.id]);
    // logo após o 202 a projeção pode levar alguns milissegundos (consistência eventual)
    if (!rows.length) return res.status(404).json({ erro: "reserva não encontrada (ou ainda não projetada; tente de novo)" });
    res.json(rows[0]);
  } catch (e) {
    res.status(503).json({ erro: "modelo de leitura indisponível" });
  }
});

app.get("/health", async (_req, res) => {
  try {
    await pool.query("SELECT 1");
    const pend = await pool.query("SELECT count(*)::int AS n FROM outbox WHERE publicado_em IS NULL");
    const proj = await pool.query("SELECT count(*)::int AS n FROM projecao_pendente");
    res.json({
      status: "UP", servico: "reservas", outboxPendente: pend.rows[0].n, broker: brokerStatus,
      modeloLeitura: { pendentes: proj.rows[0].n, ultimaProjecao: projecao.ultimaProjecao },
    });
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
  await migrarLeitura();
  // projeção inicial: garante que reservas antigas (de antes do modelo de leitura existir) também apareçam
  await pool.query("INSERT INTO projecao_pendente (reserva_id) SELECT id FROM reservas ON CONFLICT DO NOTHING");
  startProjetor();
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
