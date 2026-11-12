"""Serviço Pagamentos (Python).

- Consome PagamentoSolicitado e EstornoSolicitado da fila pagamentos-queue.
- Roda com 2+ réplicas na MESMA fila (R4/F8); cada pagamento registra qual réplica o processou.
- Chama o gateway externo instável com retentativas e Idempotency-Key = reservaId,
  de modo que uma reentrega (F2) nunca gera segunda cobrança.
- Idempotência (R1): messageId gravado em mensagens_processadas na mesma transação do efeito.
- Outbox: eventos de saída gravados na mesma transação e publicados por uma thread relay.
- Reconexão (R6): laços de consumo e relay nunca morrem com erro de broker/banco; esperam e tentam de novo.
"""
import json
import os
import re
import socket
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
import psycopg
from botocore.config import Config
from opentelemetry import context as otel_context
from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind, Status, StatusCode

SERVICE = os.getenv("SERVICE_NAME", "pagamentos")
REPLICA = socket.gethostname()
REGION = os.getenv("AWS_REGION", "us-east-1")
ENDPOINT = os.getenv("AWS_ENDPOINT_URL") or None
TOPIC_NAME = os.environ.get("TOPIC_NAME", "locacao-eventos")
QUEUE_NAME = os.environ["QUEUE_NAME"]
DATABASE_URL = os.environ["DATABASE_URL"]
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://gateway-pagamento:8090")
GATEWAY_TIMEOUT = float(os.getenv("GATEWAY_TIMEOUT_SEGUNDOS", "10"))

state = {"topic_arn": None, "queue_url": None, "max_tentativas": 3, "api_key": os.getenv("GATEWAY_API_KEY", "gw-chave-dev"),
         "broker_ok": 0.0, "broker_erro": ""}

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)

SQL = """
CREATE TABLE IF NOT EXISTS mensagens_processadas (
  message_id    uuid PRIMARY KEY,
  tipo          text NOT NULL,
  processada_por text NOT NULL,
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
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS traceparent text;
CREATE INDEX IF NOT EXISTS outbox_pendentes ON outbox (id) WHERE publicado_em IS NULL;
CREATE TABLE IF NOT EXISTS pagamentos (
  id             uuid PRIMARY KEY,
  reserva_id     uuid NOT NULL UNIQUE,
  valor          numeric(12,2) NOT NULL,
  status         text NOT NULL,          -- APROVADO | RECUSADO | ESTORNADO
  motivo         text,
  transacao_id   text,
  tentativas_gateway int NOT NULL,
  processado_por text NOT NULL,
  criado_em      timestamptz NOT NULL DEFAULT now(),
  atualizado_em  timestamptz NOT NULL DEFAULT now()
);
"""


def log(level, msg, **kw):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "level": level, "service": SERVICE,
                      "replica": REPLICA, "msg": msg, **kw}, default=str), flush=True)


class PoisonMessage(Exception):
    """Mensagem inválida: não é apagada; o SQS reentrega até maxReceiveCount e manda para a DLQ (F4)."""


# --------------------------------------------------------------------------- tracing (OpenTelemetry)
# O contexto do trace viaja no atributo "traceparent" da mensagem SNS/SQS; o envelope não muda.
# Sem OTEL_EXPORTER_OTLP_ENDPOINT o tracer é no-op e o serviço funciona igual.
def _init_tracing():
    if not os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or os.getenv("OTEL_SDK_DISABLED") == "true":
        return
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    provider = TracerProvider(resource=Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", SERVICE),
                                                        "service.instance.id": REPLICA}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)


_init_tracing()
tracer = trace.get_tracer(SERVICE)


def traceparent_atual():
    carrier = {}
    propagate.inject(carrier)
    return carrier.get("traceparent")


def contexto_de(traceparent):
    return propagate.extract({"traceparent": traceparent}) if traceparent else otel_context.Context()


# --------------------------------------------------------------------------- AWS
_cfg = Config(connect_timeout=3, read_timeout=25, retries={"max_attempts": 2, "mode": "standard"})
sns = boto3.client("sns", region_name=REGION, endpoint_url=ENDPOINT, config=_cfg)
sqs = boto3.client("sqs", region_name=REGION, endpoint_url=ENDPOINT, config=_cfg)
ssm = boto3.client("ssm", region_name=REGION, endpoint_url=ENDPOINT, config=_cfg)


def resolve_broker():
    """Resolve ARN do tópico e URL da fila pelos nomes do ambiente; repete até o broker responder."""
    i = 0
    while True:
        i += 1
        try:
            if not state["topic_arn"]:
                for page in sns.get_paginator("list_topics").paginate():
                    for t in page.get("Topics", []):
                        if t["TopicArn"].endswith(":" + TOPIC_NAME):
                            state["topic_arn"] = t["TopicArn"]
                if not state["topic_arn"]:
                    raise RuntimeError(f"tópico {TOPIC_NAME} não encontrado")
            if not state["queue_url"]:
                state["queue_url"] = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
            log("info", "broker resolvido", topicArn=state["topic_arn"], queueUrl=state["queue_url"])
            return
        except Exception as e:  # noqa: BLE001
            log("warn", "broker indisponível ao iniciar, nova tentativa", tentativa=i, erro=str(e))
            time.sleep(min(i, 5))


def load_params():
    for nome, chave, conv in (("/locacao/pagamentos/max-tentativas", "max_tentativas", int),
                              ("/locacao/gateway/api-key", "api_key", str)):
        try:
            state[chave] = conv(ssm.get_parameter(Name=nome, WithDecryption=True)["Parameter"]["Value"])
        except Exception as e:  # noqa: BLE001
            log("warn", "parâmetro SSM indisponível, mantendo valor atual", nome=nome, erro=str(e))
    log("info", "configuração carregada do SSM", maxTentativas=state["max_tentativas"])


# --------------------------------------------------------------------------- banco
def connect():
    i = 0
    while True:
        i += 1
        try:
            return psycopg.connect(DATABASE_URL, autocommit=True)
        except Exception as e:  # noqa: BLE001
            log("warn", "banco indisponível, nova tentativa", tentativa=i, erro=str(e))
            time.sleep(min(i, 5))


def migrate():
    with connect() as conn, conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(42)")
        conn.execute(SQL)


def enqueue(cur, tipo, version, correlation_id, payload):
    env = {"messageId": str(uuid.uuid4()), "correlationId": correlation_id, "type": tipo, "version": version,
           "occurredAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "payload": payload}
    cur.execute("INSERT INTO outbox (message_id, tipo, corpo, traceparent) VALUES (%s, %s, %s, %s)",
                (env["messageId"], tipo, json.dumps(env), traceparent_atual()))


# --------------------------------------------------------------------------- gateway
def gateway(path, body, idem_key=None):
    """Uma chamada ao gateway (um span CLIENT por tentativa). Retorna (http_status, corpo) ou (None, erro)."""
    with tracer.start_as_current_span(f"gateway POST {path}", kind=SpanKind.CLIENT,
                                      attributes={"http.request.method": "POST", "url.full": GATEWAY_URL + path}) as span:
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {state['api_key']}",
                   **({"Idempotency-Key": idem_key} if idem_key else {})}
        propagate.inject(headers)  # o gateway recebe o traceparent (um parceiro real poderia continuar o trace)
        req = urllib.request.Request(GATEWAY_URL + path, data=json.dumps(body).encode(), method="POST", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=GATEWAY_TIMEOUT) as r:
                span.set_attribute("http.response.status_code", r.status)
                return r.status, json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            span.set_attribute("http.response.status_code", e.code)
            if e.code >= 500:
                span.set_status(Status(StatusCode.ERROR, f"HTTP {e.code}"))
            try:
                return e.code, json.loads(e.read() or b"{}")
            except json.JSONDecodeError:
                return e.code, {}
        except Exception as e:  # noqa: BLE001  (timeout, conexão recusada...)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            return None, {"erro": str(e)}


def com_retentativas(path, body, idem_key, correlation_id):
    """Retentativas com backoff exponencial para falhas técnicas (503/timeout). 200/402 são definitivos."""
    n = state["max_tentativas"]
    for tentativa in range(1, n + 1):
        status, resp = gateway(path, body, idem_key)
        if status in (200, 402):
            return status, resp, tentativa
        log("warn", "gateway falhou", correlationId=correlation_id, tentativa=tentativa, de=n, status=status, resp=resp)
        if tentativa < n:
            time.sleep(0.5 * 2 ** (tentativa - 1))
    return None, {}, n


# --------------------------------------------------------------------------- handlers
def exige(p, campos):
    """Validação mínima do payload conforme contracts/events/*.schema.json."""
    for k, t in campos.items():
        v = p.get(k) if isinstance(p, dict) else None
        if t == "uuid":
            ok = isinstance(v, str) and bool(UUID_RE.match(v))
        elif t == "number":
            ok = isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0
        else:
            ok = isinstance(v, str) and v != ""
        if not ok:
            raise PoisonMessage(f"campo {k} inválido (esperado {t})")


def pagamento_solicitado(cur, env):
    p = env["payload"]
    exige(p, {"reservaId": "uuid", "clienteId": "string", "valor": "number"})
    reserva = p["reservaId"]
    cur.execute("SELECT id, status FROM pagamentos WHERE reserva_id = %s", (reserva,))
    if cur.fetchone():
        log("info", "reserva já tem pagamento, nada a fazer", correlationId=reserva)
        return
    log("info", "cobrando no gateway", correlationId=reserva, messageId=env["messageId"])
    # Idempotency-Key = reservaId: mesmo que este processo morra depois de cobrar (F2), a reentrega
    # devolve o mesmo resultado do gateway em vez de cobrar duas vezes.
    status, resp, tentativas = com_retentativas("/cobrancas", {"reservaId": reserva, "valor": p["valor"]}, reserva, reserva)
    pid = str(uuid.uuid4())
    if status == 200:
        cur.execute("""INSERT INTO pagamentos (id, reserva_id, valor, status, transacao_id, tentativas_gateway, processado_por)
                       VALUES (%s,%s,%s,'APROVADO',%s,%s,%s)""", (pid, reserva, p["valor"], resp["transacaoId"], tentativas, REPLICA))
        enqueue(cur, "PagamentoAprovado", 1, reserva,
                {"reservaId": reserva, "pagamentoId": pid, "transacaoId": resp["transacaoId"], "valor": p["valor"]})
        log("info", "pagamento aprovado", correlationId=reserva, transacaoId=resp["transacaoId"], tentativas=tentativas)
    else:
        motivo = "RECUSADO_PELO_EMISSOR" if status == 402 else "GATEWAY_INDISPONIVEL"
        cur.execute("""INSERT INTO pagamentos (id, reserva_id, valor, status, motivo, tentativas_gateway, processado_por)
                       VALUES (%s,%s,%s,'RECUSADO',%s,%s,%s)""", (pid, reserva, p["valor"], motivo, tentativas, REPLICA))
        enqueue(cur, "PagamentoRecusado", 1, reserva, {"reservaId": reserva, "pagamentoId": pid, "motivo": motivo})
        log("warn", "pagamento recusado", correlationId=reserva, motivo=motivo, tentativas=tentativas)


def estorno_solicitado(cur, env):
    p = env["payload"]
    exige(p, {"reservaId": "uuid", "motivo": "string"})
    reserva = p["reservaId"]
    cur.execute("SELECT id, status, transacao_id, valor FROM pagamentos WHERE reserva_id = %s FOR UPDATE", (reserva,))
    row = cur.fetchone()
    if not row or row[1] != "APROVADO":
        log("info", "nada a estornar", correlationId=reserva, status=row[1] if row else None)
        return
    pid, _, tx, valor = row
    status, resp, tentativas = com_retentativas("/estornos", {"transacaoId": tx}, None, reserva)
    if status != 200:
        # exceção desfaz a transação; o SQS reentrega depois (estorno é idempotente no gateway)
        raise RuntimeError(f"estorno não concluído após {tentativas} tentativas")
    cur.execute("UPDATE pagamentos SET status='ESTORNADO', motivo=%s, atualizado_em=now() WHERE id=%s", (p["motivo"], pid))
    enqueue(cur, "PagamentoEstornado", 1, reserva, {"reservaId": reserva, "pagamentoId": str(pid), "valor": float(valor)})
    log("warn", "pagamento estornado (compensação)", correlationId=reserva, transacaoId=tx)


HANDLERS = {"PagamentoSolicitado": pagamento_solicitado, "EstornoSolicitado": estorno_solicitado}


# --------------------------------------------------------------------------- consumidor
def parse(body):
    try:
        obj = json.loads(body)
        if isinstance(obj, dict) and obj.get("Type") == "Notification" and isinstance(obj.get("Message"), str):
            obj = json.loads(obj["Message"])  # sem RawMessageDelivery
    except (json.JSONDecodeError, TypeError):
        raise PoisonMessage("corpo não é JSON") from None
    if not isinstance(obj, dict):
        raise PoisonMessage("envelope não é objeto")
    for k in ("messageId", "correlationId", "type", "version", "occurredAt", "payload"):
        if k not in obj:
            raise PoisonMessage(f"envelope sem {k}")
    if not UUID_RE.match(str(obj["messageId"])) or not isinstance(obj["payload"], dict):
        raise PoisonMessage("messageId ou payload inválido")
    return obj


def handle(conn, m):
    # span CONSUMER que continua o trace de quem publicou a mensagem
    tp = m.get("MessageAttributes", {}).get("traceparent", {}).get("StringValue")
    with tracer.start_as_current_span("mensagem process", context=contexto_de(tp), kind=SpanKind.CONSUMER,
                                      attributes={"messaging.system": "aws_sqs", "messaging.destination.name": QUEUE_NAME,
                                                  "locafacil.replica": REPLICA}) as span:
        _handle(conn, m, span)


def _handle(conn, m, span):
    receive_count = int(m.get("Attributes", {}).get("ApproximateReceiveCount", "1"))
    env = None
    try:
        env = parse(m.get("Body"))
        span.update_name(f"{env['type']} process")
        span.set_attributes({"messaging.message.id": env["messageId"], "locafacil.correlation_id": env["correlationId"],
                             "messaging.message.delivery_count": receive_count})
        h = HANDLERS.get(env["type"])
        if h is None:
            log("info", "tipo não interessa a este serviço, descartando", type=env["type"])
        else:
            with conn.transaction(), conn.cursor() as cur:
                # R1: se outra réplica estiver com o mesmo messageId em andamento, este INSERT espera o commit
                # dela e então não insere nada -> duplicata ignorada.
                cur.execute("""INSERT INTO mensagens_processadas (message_id, tipo, processada_por) VALUES (%s,%s,%s)
                               ON CONFLICT DO NOTHING""", (env["messageId"], env["type"], REPLICA))
                if cur.rowcount == 0:
                    span.set_attribute("locafacil.duplicada", True)
                    log("info", "mensagem duplicada ignorada", messageId=env["messageId"], correlationId=env["correlationId"])
                else:
                    h(cur, env)
                    log("info", "mensagem processada", messageId=env["messageId"], type=env["type"],
                        correlationId=env["correlationId"], receiveCount=receive_count)
        sqs.delete_message(QueueUrl=state["queue_url"], ReceiptHandle=m["ReceiptHandle"])
    except psycopg.OperationalError:
        raise  # conexão com o banco caiu: o laço reconecta
    except Exception as e:  # noqa: BLE001
        span.set_status(Status(StatusCode.ERROR, str(e)))
        span.set_attribute("locafacil.venenosa", isinstance(e, PoisonMessage))
        log("warn" if isinstance(e, PoisonMessage) else "error", "falha ao processar mensagem",
            messageId=(env or {}).get("messageId", m.get("MessageId")), receiveCount=receive_count,
            venenosa=isinstance(e, PoisonMessage), erro=str(e))
        if isinstance(e, PoisonMessage):
            try:  # volta para a fila em 2 s em vez de esperar o visibility timeout inteiro
                sqs.change_message_visibility(QueueUrl=state["queue_url"], ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=2)
            except Exception:  # noqa: BLE001
                pass


def consumer_loop():
    conn = connect()
    backoff = 1
    while True:
        try:
            r = sqs.receive_message(QueueUrl=state["queue_url"], MaxNumberOfMessages=5, WaitTimeSeconds=10,
                                    MessageSystemAttributeNames=["ApproximateReceiveCount"],
                                    MessageAttributeNames=["All"])
            state["broker_ok"] = time.time()
            backoff = 1
            for m in r.get("Messages", []):
                handle(conn, m)
        except psycopg.OperationalError as e:
            log("warn", "conexão com banco perdida, reconectando", erro=str(e))
            conn = connect()
        except Exception as e:  # noqa: BLE001
            # R6: broker fora do ar -> espera com backoff exponencial e volta a consumir sozinho
            state["broker_erro"] = str(e)
            log("warn", "broker inacessível, reconectando", esperaSeg=backoff, erro=str(e))
            time.sleep(backoff)
            backoff = min(backoff * 2, 15)


def relay_loop():
    conn = connect()
    while True:
        try:
            enviados = 0
            with conn.transaction(), conn.cursor() as cur:
                cur.execute("""SELECT id, tipo, corpo, traceparent FROM outbox WHERE publicado_em IS NULL
                               ORDER BY id LIMIT 25 FOR UPDATE SKIP LOCKED""")
                for oid, tipo, corpo, tp in cur.fetchall():
                    attrs = {"type": {"DataType": "String", "StringValue": tipo}}
                    span = None
                    if tp:  # span PRODUCER filho da operação que gerou o evento
                        span = tracer.start_span(f"{tipo} publish", context=contexto_de(tp), kind=SpanKind.PRODUCER,
                                                 attributes={"messaging.system": "aws_sns", "messaging.message.id": corpo["messageId"],
                                                             "locafacil.correlation_id": corpo["correlationId"]})
                        carrier = {}
                        propagate.inject(carrier, context=trace.set_span_in_context(span))
                        if carrier.get("traceparent"):
                            attrs["traceparent"] = {"DataType": "String", "StringValue": carrier["traceparent"]}
                    try:
                        sns.publish(TopicArn=state["topic_arn"], Message=json.dumps(corpo), MessageAttributes=attrs)
                        if span:
                            span.end()
                    except Exception as e:  # noqa: BLE001
                        if span:
                            span.set_status(Status(StatusCode.ERROR, str(e)))
                            span.end()
                        cur.execute("UPDATE outbox SET tentativas = tentativas + 1 WHERE id = %s", (oid,))
                        log("warn", "falha ao publicar, outbox tenta de novo", tipo=tipo, erro=str(e))
                        break
                    cur.execute("UPDATE outbox SET publicado_em = now() WHERE id = %s", (oid,))
                    enviados += 1
            if enviados == 0:
                time.sleep(0.3)
        except psycopg.OperationalError:
            conn = connect()
        except Exception as e:  # noqa: BLE001
            log("error", "relay da outbox falhou", erro=str(e))
            time.sleep(2)


# --------------------------------------------------------------------------- health
class Health(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        code, body = 404, {}
        if self.path == "/health":
            try:
                with psycopg.connect(DATABASE_URL, connect_timeout=2) as c:
                    pend = c.execute("SELECT count(*) FROM outbox WHERE publicado_em IS NULL").fetchone()[0]
                code, body = 200, {"status": "UP", "servico": SERVICE, "replica": REPLICA, "outboxPendente": pend,
                                   "brokerUltimoSucesso": state["broker_ok"], "brokerUltimoErro": state["broker_erro"]}
            except Exception as e:  # noqa: BLE001
                code, body = 503, {"status": "DOWN", "erro": str(e)}
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(data)


def main():
    threading.Thread(target=ThreadingHTTPServer(("0.0.0.0", 8080), Health).serve_forever, daemon=True).start()
    migrate()
    resolve_broker()
    load_params()
    threading.Thread(target=relay_loop, daemon=True, name="relay").start()
    log("info", "consumidor iniciado", fila=QUEUE_NAME, gateway=GATEWAY_URL)
    consumer_loop()


if __name__ == "__main__":
    main()
