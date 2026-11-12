// Infraestrutura compartilhada do serviço: banco, clientes AWS, envelope,
// outbox (publicação confiável) e consumidor idempotente (R1, R6).
import { randomUUID } from "crypto";
import { Pool, PoolClient } from "pg";
import { SNSClient, PublishCommand, ListTopicsCommand } from "@aws-sdk/client-sns";
import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  ChangeMessageVisibilityCommand,
  GetQueueUrlCommand,
  Message,
} from "@aws-sdk/client-sqs";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { NodeHttpHandler } from "@smithy/node-http-handler";

// ------------------------------------------------------------------ config
export const SERVICE = process.env.SERVICE_NAME ?? "servico";

function env(name: string, fallback?: string): string {
  const v = process.env[name] ?? fallback;
  if (v === undefined) throw new Error(`variável de ambiente ${name} ausente`);
  return v;
}

export const config = {
  region: env("AWS_REGION", "us-east-1"),
  endpoint: process.env.AWS_ENDPOINT_URL || undefined, // vazio = AWS real
  topicName: env("TOPIC_NAME", "locacao-eventos"),
  queueName: env("QUEUE_NAME"),
  databaseUrl: env("DATABASE_URL"),
};

export function log(level: "info" | "warn" | "error", msg: string, extra: Record<string, unknown> = {}) {
  // log estruturado: facilita seguir um correlationId pelos serviços com `docker compose logs | grep`
  console.log(JSON.stringify({ ts: new Date().toISOString(), level, service: SERVICE, msg, ...extra }));
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ------------------------------------------------------------------ banco
export const pool = new Pool({ connectionString: config.databaseUrl, max: 15 });

export async function withTx<T>(fn: (c: PoolClient) => Promise<T>): Promise<T> {
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    const r = await fn(c);
    await c.query("COMMIT");
    return r;
  } catch (e) {
    await c.query("ROLLBACK").catch(() => undefined);
    throw e;
  } finally {
    c.release();
  }
}

/** Migração protegida por advisory lock: várias réplicas podem subir juntas. */
export async function migrate(sql: string) {
  for (let i = 1; ; i++) {
    try {
      await withTx(async (c) => {
        await c.query("SELECT pg_advisory_xact_lock(42)");
        await c.query(sql);
      });
      return;
    } catch (e) {
      log("warn", "banco indisponível, tentando de novo", { tentativa: i, erro: String(e) });
      await sleep(Math.min(1000 * i, 5000));
    }
  }
}

export const COMMON_SQL = `
CREATE TABLE IF NOT EXISTS mensagens_processadas (
  message_id   uuid PRIMARY KEY,
  tipo         text NOT NULL,
  processada_em timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
  id           bigserial PRIMARY KEY,
  message_id   uuid NOT NULL UNIQUE,
  tipo         text NOT NULL,
  corpo        jsonb NOT NULL,
  criado_em    timestamptz NOT NULL DEFAULT now(),
  publicado_em timestamptz,
  tentativas   int NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS outbox_pendentes ON outbox (id) WHERE publicado_em IS NULL;
`;

// ------------------------------------------------------------------ AWS
const http = new NodeHttpHandler({ connectionTimeout: 3000, requestTimeout: 25000 });
const awsCfg = { region: config.region, endpoint: config.endpoint, requestHandler: http, maxAttempts: 2 };
export const sns = new SNSClient(awsCfg);
export const sqs = new SQSClient(awsCfg);
export const ssm = new SSMClient(awsCfg);

let topicArn: string | undefined;
let queueUrl: string | undefined;

/** Resolve ARN do tópico e URL da fila pelos NOMES vindos do ambiente. Repete até o broker responder. */
export async function resolveBroker(): Promise<void> {
  for (let i = 1; ; i++) {
    try {
      if (!topicArn) {
        let next: string | undefined;
        do {
          const r = await sns.send(new ListTopicsCommand({ NextToken: next }));
          topicArn = r.Topics?.map((t) => t.TopicArn!).find((a) => a.endsWith(`:${config.topicName}`));
          next = r.NextToken;
        } while (!topicArn && next);
        if (!topicArn) throw new Error(`tópico ${config.topicName} não encontrado`);
      }
      if (!queueUrl) {
        queueUrl = (await sqs.send(new GetQueueUrlCommand({ QueueName: config.queueName }))).QueueUrl;
      }
      log("info", "broker resolvido", { topicArn, queueUrl });
      return;
    } catch (e) {
      log("warn", "broker indisponível ao iniciar, nova tentativa", { tentativa: i, erro: String(e) });
      await sleep(Math.min(1000 * i, 5000));
    }
  }
}

export async function getParam(name: string, fallback: string): Promise<string> {
  try {
    const r = await ssm.send(new GetParameterCommand({ Name: name, WithDecryption: true }));
    return r.Parameter?.Value ?? fallback;
  } catch (e) {
    log("warn", "parâmetro SSM indisponível, usando padrão", { name, fallback, erro: String(e) });
    return fallback;
  }
}

// ------------------------------------------------------------------ envelope
export interface Envelope<P = any> {
  messageId: string;
  correlationId: string;
  type: string;
  version: number;
  occurredAt: string;
  payload: P;
}

/** Grava o evento na outbox DENTRO da transação do efeito. O relay publica depois. */
export async function enqueue(c: PoolClient, type: string, version: number, correlationId: string, payload: object) {
  const env: Envelope = {
    messageId: randomUUID(),
    correlationId,
    type,
    version,
    occurredAt: new Date().toISOString(),
    payload,
  };
  await c.query("INSERT INTO outbox (message_id, tipo, corpo) VALUES ($1, $2, $3)", [env.messageId, type, env]);
  return env;
}

// ------------------------------------------------------------------ outbox relay
export const brokerStatus = { ultimoSucesso: 0, ultimoErro: "" };

async function relayOnce(): Promise<number> {
  return withTx(async (c) => {
    // SKIP LOCKED: várias réplicas drenam a outbox sem publicar a mesma linha ao mesmo tempo
    const { rows } = await c.query(
      `SELECT id, tipo, corpo FROM outbox WHERE publicado_em IS NULL ORDER BY id LIMIT 25 FOR UPDATE SKIP LOCKED`
    );
    let enviados = 0;
    for (const row of rows) {
      try {
        await sns.send(
          new PublishCommand({
            TopicArn: topicArn,
            Message: JSON.stringify(row.corpo),
            MessageAttributes: { type: { DataType: "String", StringValue: row.tipo } },
          })
        );
        await c.query("UPDATE outbox SET publicado_em = now() WHERE id = $1", [row.id]);
        brokerStatus.ultimoSucesso = Date.now();
        enviados++;
      } catch (e) {
        // broker fora: a linha fica pendente e é publicada quando ele voltar (F6)
        await c.query("UPDATE outbox SET tentativas = tentativas + 1 WHERE id = $1", [row.id]);
        brokerStatus.ultimoErro = String(e);
        log("warn", "falha ao publicar, outbox vai tentar de novo", { tipo: row.tipo, erro: String(e) });
        break;
      }
    }
    return enviados;
  });
}

export function startOutboxRelay() {
  (async () => {
    for (;;) {
      try {
        const n = await relayOnce();
        if (n === 0) await sleep(300);
      } catch (e) {
        log("error", "relay da outbox falhou", { erro: String(e) });
        await sleep(2000);
      }
    }
  })();
}

// ------------------------------------------------------------------ consumidor
/** Erro de mensagem inválida: não é apagada, o SQS reentrega até maxReceiveCount e manda para a DLQ (F4). */
export class PoisonMessage extends Error {}

export type Handler = (c: PoolClient, env: Envelope) => Promise<void>;

export function parseBody(body: string | undefined): Envelope {
  let obj: any;
  try {
    obj = JSON.parse(body ?? "");
    // sem RawMessageDelivery o SNS embrulha a mensagem; aceitamos os dois formatos
    if (obj && obj.Type === "Notification" && typeof obj.Message === "string") obj = JSON.parse(obj.Message);
  } catch {
    throw new PoisonMessage("corpo não é JSON");
  }
  const req = ["messageId", "correlationId", "type", "version", "occurredAt", "payload"];
  for (const k of req) if (obj?.[k] === undefined) throw new PoisonMessage(`envelope sem ${k}`);
  if (!isUuid(obj.messageId)) throw new PoisonMessage("messageId inválido");
  if (typeof obj.payload !== "object" || obj.payload === null) throw new PoisonMessage("payload inválido");
  return obj as Envelope;
}

export const isUuid = (v: unknown) =>
  typeof v === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v);

/** Validação mínima do payload conforme contracts/events/*.schema.json */
export function requireFields(p: any, spec: Record<string, "uuid" | "string" | "number" | "date">) {
  for (const [k, t] of Object.entries(spec)) {
    const v = p?.[k];
    const ok =
      t === "uuid" ? isUuid(v)
      : t === "string" ? typeof v === "string" && v.length > 0
      : t === "number" ? typeof v === "number" && Number.isFinite(v)
      : typeof v === "string" && /^\d{4}-\d{2}-\d{2}$/.test(v);
    if (!ok) throw new PoisonMessage(`campo ${k} inválido (esperado ${t})`);
  }
}

async function handleMessage(m: Message, handlers: Record<string, Handler>) {
  const receiveCount = Number(m.Attributes?.ApproximateReceiveCount ?? "1");
  let env: Envelope | undefined;
  try {
    env = parseBody(m.Body);
    const h = handlers[env.type];
    if (!h) {
      log("info", "tipo não interessa a este serviço, descartando", { type: env.type });
    } else {
      const e = env;
      await withTx(async (c) => {
        // R1: registra o messageId NA MESMA TRANSAÇÃO do efeito.
        // Se outra réplica estiver processando o mesmo messageId, este INSERT espera o commit dela
        // e então não insere nada -> duplicata.
        const r = await c.query(
          "INSERT INTO mensagens_processadas (message_id, tipo) VALUES ($1, $2) ON CONFLICT DO NOTHING",
          [e.messageId, e.type]
        );
        if (r.rowCount === 0) {
          log("info", "mensagem duplicada ignorada", { messageId: e.messageId, type: e.type, correlationId: e.correlationId });
          return;
        }
        await h(c, e);
        log("info", "mensagem processada", { messageId: e.messageId, type: e.type, version: e.version, correlationId: e.correlationId, receiveCount });
      });
    }
    await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle }));
  } catch (e) {
    // não apaga: volta para a fila após o visibility timeout; após maxReceiveCount vai para a DLQ
    log(e instanceof PoisonMessage ? "warn" : "error", "falha ao processar mensagem", {
      messageId: env?.messageId ?? m.MessageId,
      type: env?.type,
      receiveCount,
      venenosa: e instanceof PoisonMessage,
      erro: String(e),
    });
    if (e instanceof PoisonMessage) {
      // volta para a fila em 2 s em vez de esperar o visibility timeout inteiro
      await sqs
        .send(new ChangeMessageVisibilityCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 2 }))
        .catch(() => undefined);
    }
  }
}

export function startConsumer(handlers: Record<string, Handler>) {
  (async () => {
    let backoff = 1000;
    for (;;) {
      try {
        const r = await sqs.send(
          new ReceiveMessageCommand({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 10,
            MessageSystemAttributeNames: ["ApproximateReceiveCount"],
          })
        );
        backoff = 1000;
        brokerStatus.ultimoSucesso = Date.now();
        for (const m of r.Messages ?? []) await handleMessage(m, handlers);
      } catch (e) {
        // R6: broker caiu. Não derruba o processo: espera e tenta de novo (backoff exponencial até 15 s)
        brokerStatus.ultimoErro = String(e);
        log("warn", "broker inacessível, reconectando", { esperaMs: backoff, erro: String(e) });
        await sleep(backoff);
        backoff = Math.min(backoff * 2, 15000);
      }
    }
  })();
}
