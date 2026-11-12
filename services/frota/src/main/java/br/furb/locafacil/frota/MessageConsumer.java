package br.furb.locafacil.frota;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * N threads fazendo long polling na mesma fila (consumidores concorrentes dentro do processo).
 * Idempotência (R1): INSERT do messageId em mensagens_processadas na MESMA transação do efeito.
 * Reconexão (R6): erro de broker nunca mata a thread; ela espera (backoff) e tenta de novo.
 */
@Component
public class MessageConsumer {
    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);
    private static final List<String> CAMPOS = List.of("messageId", "correlationId", "type", "version", "occurredAt", "payload");

    private final SqsClient sqs;
    private final Broker broker;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final FrotaService frota;
    private final ObjectMapper json;
    private final int threads;
    private final Tracing tracing;

    public MessageConsumer(SqsClient sqs, Broker broker, JdbcTemplate jdbc, TransactionTemplate tx, FrotaService frota,
                           ObjectMapper json, Tracing tracing, @Value("${locafacil.consumer-threads}") int threads) {
        this.tracing = tracing;
        this.sqs = sqs;
        this.broker = broker;
        this.jdbc = jdbc;
        this.tx = tx;
        this.frota = frota;
        this.json = json;
        this.threads = threads;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        pool.submit(() -> {
            broker.resolve();
            for (int i = 0; i < threads; i++) pool.submit(this::loop);
        });
    }

    private void loop() {
        long backoff = 1000;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                List<Message> msgs = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(broker.queueUrl())
                        .maxNumberOfMessages(5)
                        .waitTimeSeconds(10)
                        .attributeNamesWithStrings("ApproximateReceiveCount")
                        .messageAttributeNames("All")
                        .build()).messages();
                backoff = 1000;
                broker.ultimoSucesso = System.currentTimeMillis();
                for (Message m : msgs) handle(m);
            } catch (Exception e) {
                broker.ultimoErro = e.toString();
                log.warn("broker inacessível, reconectando em {} ms: {}", backoff, e.toString());
                try { Thread.sleep(backoff); } catch (InterruptedException ie) { return; }
                backoff = Math.min(backoff * 2, 15000);
            }
        }
    }

    JsonNode parse(String body) {
        JsonNode n;
        try {
            n = json.readTree(body);
            // sem RawMessageDelivery o SNS embrulha a mensagem
            if (n != null && "Notification".equals(n.path("Type").asText()) && n.path("Message").isTextual()) {
                n = json.readTree(n.get("Message").asText());
            }
        } catch (Exception e) {
            throw new PoisonMessageException("corpo não é JSON");
        }
        if (n == null || !n.isObject()) throw new PoisonMessageException("envelope não é objeto");
        for (String c : CAMPOS) if (!n.has(c)) throw new PoisonMessageException("envelope sem " + c);
        try {
            UUID.fromString(n.get("messageId").asText());
        } catch (IllegalArgumentException e) {
            throw new PoisonMessageException("messageId não é UUID");
        }
        if (!n.get("payload").isObject()) throw new PoisonMessageException("payload não é objeto");
        return n;
    }

    private void handle(Message m) {
        // span CONSUMER que continua o trace de quem publicou (atributo traceparent da mensagem)
        MessageAttributeValue tp = m.messageAttributes().get("traceparent");
        Span span = tracing.tracer().spanBuilder("mensagem process").setSpanKind(SpanKind.CONSUMER)
                .setParent(tracing.contextoDe(tp == null ? null : tp.stringValue()))
                .setAttribute("messaging.system", "aws_sqs")
                .setAttribute("messaging.destination.name", "frota-queue")
                .startSpan();
        try (Scope ignored = span.makeCurrent()) {
            handle(m, span);
        } finally {
            span.end();
        }
    }

    private void handle(Message m, Span span) {
        String receiveCount = m.attributesAsStrings().getOrDefault("ApproximateReceiveCount", "1");
        String messageId = m.messageId();
        try {
            JsonNode env = parse(m.body());
            messageId = env.get("messageId").asText();
            String type = env.get("type").asText();
            String correlationId = env.get("correlationId").asText();
            span.updateName(type + " process");
            span.setAttribute("messaging.message.id", messageId);
            span.setAttribute("locafacil.correlation_id", correlationId);
            span.setAttribute("messaging.message.delivery_count", Long.parseLong(receiveCount));
            if (!List.of("ReservaSolicitada", "ReservaConfirmada", "ReservaCancelada").contains(type)) {
                log.info("tipo {} não interessa à frota, descartando", type);
            } else {
                final String mid = messageId;
                tx.executeWithoutResult(status -> {
                    int novo = jdbc.update(
                            "INSERT INTO mensagens_processadas (message_id, tipo) VALUES (?::uuid, ?) ON CONFLICT DO NOTHING", mid, type);
                    if (novo == 0) {
                        span.setAttribute("locafacil.duplicada", true);
                        log.info("mensagem duplicada ignorada messageId={} correlationId={}", mid, correlationId);
                        return;
                    }
                    switch (type) {
                        case "ReservaSolicitada" -> frota.reservaSolicitada(env, status);
                        case "ReservaConfirmada" -> frota.reservaConfirmada(env);
                        case "ReservaCancelada" -> frota.reservaCancelada(env);
                        default -> { }
                    }
                    log.info("mensagem processada type={} messageId={} correlationId={} receiveCount={}", type, mid, correlationId, receiveCount);
                });
            }
            sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(broker.queueUrl()).receiptHandle(m.receiptHandle()).build());
        } catch (PoisonMessageException e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.setAttribute("locafacil.venenosa", true);
            log.warn("mensagem VENENOSA messageId={} receiveCount={}: {} (irá para a DLQ após o limite)", messageId, receiveCount, e.getMessage());
            try {
                // volta para a fila em 2 s em vez de esperar o visibility timeout inteiro
                sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                        .queueUrl(broker.queueUrl()).receiptHandle(m.receiptHandle()).visibilityTimeout(2).build());
            } catch (Exception ignored) { }
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.toString());
            log.error("falha ao processar messageId={} receiveCount={}: {}", messageId, receiveCount, e.toString());
        }
    }
}
