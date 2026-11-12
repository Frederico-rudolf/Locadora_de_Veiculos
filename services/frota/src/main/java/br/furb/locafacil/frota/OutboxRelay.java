package br.furb.locafacil.frota;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;

/** Publica no SNS o que foi gravado na outbox. Se o broker cair, as linhas ficam pendentes e drenam depois (F6). */
@Component
public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final SnsClient sns;
    private final Broker broker;

    public OutboxRelay(JdbcTemplate jdbc, TransactionTemplate tx, SnsClient sns, Broker broker) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.sns = sns;
        this.broker = broker;
    }

    @Scheduled(fixedDelay = 300)
    public void drenar() {
        if (broker.topicArn() == null) return;
        try {
            tx.executeWithoutResult(status -> {
                List<Map<String, Object>> rows = jdbc.queryForList(
                        "SELECT id, tipo, corpo::text AS corpo FROM outbox WHERE publicado_em IS NULL ORDER BY id LIMIT 25 FOR UPDATE SKIP LOCKED");
                for (Map<String, Object> row : rows) {
                    long id = ((Number) row.get("id")).longValue();
                    String tipo = (String) row.get("tipo");
                    try {
                        sns.publish(PublishRequest.builder()
                                .topicArn(broker.topicArn())
                                .message((String) row.get("corpo"))
                                .messageAttributes(Map.of("type", MessageAttributeValue.builder().dataType("String").stringValue(tipo).build()))
                                .build());
                        jdbc.update("UPDATE outbox SET publicado_em = now() WHERE id = ?", id);
                        broker.ultimoSucesso = System.currentTimeMillis();
                    } catch (Exception e) {
                        jdbc.update("UPDATE outbox SET tentativas = tentativas + 1 WHERE id = ?", id);
                        broker.ultimoErro = e.toString();
                        log.warn("falha ao publicar {}, outbox tenta de novo: {}", tipo, e.toString());
                        break;
                    }
                }
            });
        } catch (Exception e) {
            log.error("relay da outbox falhou: {}", e.toString());
        }
    }
}
