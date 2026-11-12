package br.furb.locafacil.frota;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.Topic;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

/** Resolve tópico/fila pelos nomes do ambiente e grava eventos na outbox. */
@Component
public class Broker {
    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final SnsClient sns;
    private final SqsClient sqs;
    private final ObjectMapper json;
    private final String topicName;
    private final String queueName;
    private final Tracing tracing;
    private volatile String topicArn;
    private volatile String queueUrl;
    volatile long ultimoSucesso;
    volatile String ultimoErro = "";

    public Broker(SnsClient sns, SqsClient sqs, ObjectMapper json, Tracing tracing,
                  @Value("${locafacil.topic-name}") String topicName,
                  @Value("${locafacil.queue-name}") String queueName) {
        this.tracing = tracing;
        this.sns = sns;
        this.sqs = sqs;
        this.json = json;
        this.topicName = topicName;
        this.queueName = queueName;
    }

    /** Bloqueia até o broker responder (MiniStack pode ainda estar subindo). */
    public synchronized void resolve() {
        for (int i = 1; topicArn == null || queueUrl == null; i++) {
            try {
                if (topicArn == null) {
                    for (Topic t : sns.listTopicsPaginator().topics()) {
                        if (t.topicArn().endsWith(":" + topicName)) topicArn = t.topicArn();
                    }
                    if (topicArn == null) throw new IllegalStateException("tópico " + topicName + " não encontrado");
                }
                if (queueUrl == null) {
                    queueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
                }
            } catch (Exception e) {
                log.warn("broker indisponível ao iniciar (tentativa {}): {}", i, e.toString());
                try { Thread.sleep(Math.min(1000L * i, 5000L)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
            }
        }
        log.info("broker resolvido topicArn={} queueUrl={}", topicArn, queueUrl);
    }

    public String topicArn() { return topicArn; }
    public String queueUrl() { return queueUrl; }

    /** Grava o evento na outbox; deve ser chamado DENTRO da transação do efeito. */
    public void enqueue(JdbcTemplate jdbc, String type, int version, String correlationId, Map<String, Object> payload) {
        ObjectNode env = json.createObjectNode();
        String messageId = UUID.randomUUID().toString();
        env.put("messageId", messageId);
        env.put("correlationId", correlationId);
        env.put("type", type);
        env.put("version", version);
        env.put("occurredAt", java.time.Instant.now().toString());
        env.set("payload", json.valueToTree(payload));
        // o traceparent da operação atual é gravado junto: o relay continua o mesmo trace ao publicar
        jdbc.update("INSERT INTO outbox (message_id, tipo, corpo, traceparent) VALUES (?::uuid, ?, ?::jsonb, ?)",
                messageId, type, env.toString(), tracing.traceparentAtual());
    }
}
