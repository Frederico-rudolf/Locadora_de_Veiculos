-- Executado a cada inicialização (idempotente).
CREATE TABLE IF NOT EXISTS frota_categoria (
  filial     text NOT NULL,
  categoria  text NOT NULL,
  quantidade int  NOT NULL CHECK (quantidade >= 0),
  PRIMARY KEY (filial, categoria)
);

-- Uma linha por filial/categoria/dia. A CHECK garante no próprio banco que nunca há
-- mais reservas do que veículos (R2), mesmo que o código tenha um bug.
CREATE TABLE IF NOT EXISTS ocupacao (
  filial     text NOT NULL,
  categoria  text NOT NULL,
  data       date NOT NULL,
  capacidade int  NOT NULL,
  reservados int  NOT NULL DEFAULT 0,
  PRIMARY KEY (filial, categoria, data),
  CONSTRAINT sem_venda_dupla CHECK (reservados >= 0 AND reservados <= capacidade)
);

-- Um bloqueio por reserva. status: BLOQUEADO | CONFIRMADO | LIBERADO | RECUSADO
CREATE TABLE IF NOT EXISTS bloqueios (
  reserva_id    uuid PRIMARY KEY,
  filial        text NOT NULL,
  categoria     text NOT NULL,
  data_inicio   date NOT NULL,
  data_fim      date NOT NULL,
  status        text NOT NULL,
  motivo        text,
  criado_em     timestamptz NOT NULL DEFAULT now(),
  atualizado_em timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mensagens_processadas (
  message_id    uuid PRIMARY KEY,
  tipo          text NOT NULL,
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

-- Frota inicial (o cenário F5 usa BNU/SUV = 10 unidades)
INSERT INTO frota_categoria (filial, categoria, quantidade) VALUES
  ('BNU', 'SUV', 10), ('BNU', 'SEDAN', 15), ('BNU', 'HATCH', 20),
  ('JOI', 'SUV', 5),  ('JOI', 'SEDAN', 8),  ('JOI', 'HATCH', 12)
ON CONFLICT DO NOTHING;
