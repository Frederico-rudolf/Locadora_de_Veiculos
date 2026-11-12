"""Autoscaler (bônus): escala as réplicas de um consumidor conforme o tamanho da fila SQS.

A cada INTERVALO_SEGUNDOS lê a profundidade da fila (mensagens visíveis + em processamento) e calcula
    réplicas desejadas = ceil(profundidade / MENSAGENS_POR_REPLICA), limitado a [MIN_REPLICAS, MAX_REPLICAS].
- Subida: imediata (fila crescendo = clientes esperando).
- Descida: uma réplica por vez e só depois de ESFRIAMENTO_SEGUNDOS com a fila baixa (evita "serrote").
A escala é feita com `docker compose up --scale` pelo socket do Docker, o mesmo que se faria à mão no F8.
Na AWS o equivalente é Application Auto Scaling do ECS com alarme do CloudWatch em ApproximateNumberOfMessagesVisible.
Reduzir réplicas é seguro: o consumidor trata SIGTERM e devolve à fila o que não processou (e tudo é idempotente).
"""
import json
import math
import os
import socket
import subprocess
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
from botocore.config import Config

REGION = os.getenv("AWS_REGION", "us-east-1")
ENDPOINT = os.getenv("AWS_ENDPOINT_URL") or None
FILA = os.getenv("QUEUE_NAME", "pagamentos-queue")
SERVICO = os.getenv("ALVO_SERVICO", "pagamentos")
MIN = int(os.getenv("MIN_REPLICAS", "2"))
MAX = int(os.getenv("MAX_REPLICAS", "6"))
POR_REPLICA = int(os.getenv("MENSAGENS_POR_REPLICA", "5"))
INTERVALO = float(os.getenv("INTERVALO_SEGUNDOS", "5"))
ESFRIAMENTO = float(os.getenv("ESFRIAMENTO_SEGUNDOS", "30"))
COMPOSE_FILE = os.getenv("COMPOSE_FILE_PATH", "/projeto/docker-compose.yml")

sqs = boto3.client("sqs", region_name=REGION, endpoint_url=ENDPOINT,
                   config=Config(connect_timeout=3, read_timeout=10, retries={"max_attempts": 2}))
estado = {"fila": FILA, "servico": SERVICO, "profundidade": None, "replicas": None, "desejadas": None,
          "ultimaAcao": None, "erro": ""}


def log(level, msg, **kw):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "level": level, "service": "autoscaler",
                      "msg": msg, **kw}, default=str), flush=True)


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=True).stdout.strip()


def projeto():
    """Nome do projeto compose, lido do rótulo do próprio contêiner (não depende do nome da pasta)."""
    return docker("inspect", socket.gethostname(), "--format", '{{index .Config.Labels "com.docker.compose.project"}}')


def replicas_atuais(proj):
    ids = docker("ps", "-q", "--filter", f"label=com.docker.compose.project={proj}",
                 "--filter", f"label=com.docker.compose.service={SERVICO}", "--filter", "status=running")
    return len([i for i in ids.splitlines() if i])


def escalar(proj, n):
    subprocess.run(["docker", "compose", "-p", proj, "-f", COMPOSE_FILE, "up", "-d", "--no-deps", "--no-recreate",
                    "--no-build", "--scale", f"{SERVICO}={n}", SERVICO], capture_output=True, text=True, check=True)


def profundidade(url):
    a = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=[
        "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
    return int(a["ApproximateNumberOfMessages"]) + int(a["ApproximateNumberOfMessagesNotVisible"])


def laco():
    url, proj, baixa_desde = None, None, None
    while True:
        try:
            proj = proj or projeto()
            url = url or sqs.get_queue_url(QueueName=FILA)["QueueUrl"]
            prof = profundidade(url)
            atuais = replicas_atuais(proj)
            desejadas = max(MIN, min(MAX, math.ceil(prof / POR_REPLICA)))
            estado.update(profundidade=prof, replicas=atuais, desejadas=desejadas, erro="")
            if desejadas > atuais:
                log("info", "ESCALANDO PARA CIMA", fila=FILA, profundidade=prof, de=atuais, para=desejadas)
                escalar(proj, desejadas)
                estado["ultimaAcao"] = f"{atuais}->{desejadas} (profundidade {prof})"
                baixa_desde = None
            elif desejadas < atuais:
                baixa_desde = baixa_desde or time.time()
                if time.time() - baixa_desde >= ESFRIAMENTO:
                    log("info", "escalando para baixo", fila=FILA, profundidade=prof, de=atuais, para=atuais - 1)
                    escalar(proj, atuais - 1)
                    estado["ultimaAcao"] = f"{atuais}->{atuais - 1} (profundidade {prof})"
                    baixa_desde = time.time()  # próximo passo só depois de outro esfriamento
            else:
                baixa_desde = None
            log("info", "medição", profundidade=prof, replicas=atuais, desejadas=desejadas)
        except subprocess.CalledProcessError as e:
            estado["erro"] = e.stderr
            log("error", "falha ao chamar o docker", erro=e.stderr)
        except Exception as e:  # noqa: BLE001  broker fora: tenta de novo no próximo ciclo (R6)
            estado["erro"] = str(e)
            log("warn", "falha ao medir a fila, tentando de novo", erro=str(e))
        time.sleep(INTERVALO)


class Health(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        ok = self.path == "/health"
        body = json.dumps({"status": "UP", **estado} if ok else {}).encode()
        self.send_response(200 if ok else 404)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    threading.Thread(target=ThreadingHTTPServer(("0.0.0.0", 8080), Health).serve_forever, daemon=True).start()
    log("info", "autoscaler iniciado", fila=FILA, servico=SERVICO, min=MIN, max=MAX, porReplica=POR_REPLICA)
    laco()
