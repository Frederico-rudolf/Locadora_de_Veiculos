"""Gateway de pagamento SIMULADO: a dependência externa lenta e instável (R5).

Tudo configurável por variável de ambiente (e alterável em tempo de execução via POST /config
para facilitar a demonstração ao vivo):
  GATEWAY_LATENCIA_MS   latência base de cada chamada
  GATEWAY_JITTER_MS     latência extra aleatória (0..jitter)
  GATEWAY_TAXA_FALHA    probabilidade (0..1) de responder 503 (falha técnica)
  GATEWAY_TAXA_RECUSA   probabilidade (0..1) de recusar a cobrança (402, regra de negócio)
  GATEWAY_API_KEY       chave exigida no header Authorization

Idempotência: a cobrança é identificada pelo header Idempotency-Key. Repetir a mesma chave
devolve o MESMO resultado e não cobra de novo (é assim que gateways reais se comportam).
Só usa biblioteca padrão.
"""
import json
import os
import random
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

lock = threading.Lock()
cfg = {
    "latencia_ms": int(os.getenv("GATEWAY_LATENCIA_MS", "800")),
    "jitter_ms": int(os.getenv("GATEWAY_JITTER_MS", "700")),
    "taxa_falha": float(os.getenv("GATEWAY_TAXA_FALHA", "0.2")),
    "taxa_recusa": float(os.getenv("GATEWAY_TAXA_RECUSA", "0.05")),
}
API_KEY = os.getenv("GATEWAY_API_KEY", "gw-chave-dev")
cobrancas: dict[str, dict] = {}   # idempotency-key -> resultado
estornos: dict[str, dict] = {}    # transacaoId -> resultado
stats = {"chamadas": 0, "falhas_simuladas": 0}


def log(msg, **kw):
    print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "service": "gateway-pagamento", "msg": msg, **kw}), flush=True)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # silencia o log padrão
        pass

    def _json(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        try:
            return json.loads(self.rfile.read(n) or b"{}")
        except json.JSONDecodeError:
            return None

    def _instavel(self):
        """Aplica latência e decide se esta chamada falha. Retorna True se deve falhar."""
        with lock:
            c = dict(cfg)
            stats["chamadas"] += 1
        time.sleep((c["latencia_ms"] + random.randint(0, max(c["jitter_ms"], 0))) / 1000)
        if random.random() < c["taxa_falha"]:
            with lock:
                stats["falhas_simuladas"] += 1
            return True
        return False

    def do_GET(self):
        if self.path == "/health":
            return self._json(200, {"status": "UP"})
        if self.path == "/config":
            return self._json(200, cfg)
        if self.path == "/cobrancas":
            with lock:
                return self._json(200, {"total": len(cobrancas), "estornos": len(estornos), "stats": stats,
                                        "cobrancas": list(cobrancas.values())[-50:]})
        self._json(404, {"erro": "não encontrado"})

    def do_POST(self):
        if self.path == "/config":
            b = self._body() or {}
            with lock:
                for k in cfg:
                    if k in b:
                        cfg[k] = type(cfg[k])(b[k])
            log("configuração alterada", **cfg)
            return self._json(200, cfg)

        if self.headers.get("Authorization") != f"Bearer {API_KEY}":
            return self._json(401, {"erro": "chave de API inválida"})

        if self.path == "/cobrancas":
            key = self.headers.get("Idempotency-Key")
            b = self._body()
            if not key or not b or "valor" not in b:
                return self._json(400, {"erro": "Idempotency-Key e valor são obrigatórios"})
            if self._instavel():
                log("falha simulada (503)", idempotencyKey=key)
                return self._json(503, {"erro": "gateway indisponível"})
            with lock:
                if key in cobrancas:
                    r = cobrancas[key]
                    log("cobrança repetida, devolvendo o mesmo resultado", idempotencyKey=key, status=r["status"])
                    return self._json(200 if r["status"] == "APROVADA" else 402, r)
                aprovado = random.random() >= cfg["taxa_recusa"]
                r = {"transacaoId": f"tx-{uuid.uuid4().hex[:12]}", "idempotencyKey": key,
                     "valor": b["valor"], "status": "APROVADA" if aprovado else "RECUSADA"}
                cobrancas[key] = r
            log("cobrança processada", idempotencyKey=key, status=r["status"], transacaoId=r["transacaoId"])
            return self._json(200 if aprovado else 402, r)

        if self.path == "/estornos":
            b = self._body() or {}
            tx = b.get("transacaoId")
            if not tx:
                return self._json(400, {"erro": "transacaoId obrigatório"})
            if self._instavel():
                return self._json(503, {"erro": "gateway indisponível"})
            with lock:
                if tx not in estornos:
                    estornos[tx] = {"estornoId": f"es-{uuid.uuid4().hex[:12]}", "transacaoId": tx, "status": "ESTORNADA"}
                    log("estorno processado", transacaoId=tx)
                return self._json(200, estornos[tx])

        self._json(404, {"erro": "não encontrado"})


if __name__ == "__main__":
    log("gateway simulado iniciado", **cfg)
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
