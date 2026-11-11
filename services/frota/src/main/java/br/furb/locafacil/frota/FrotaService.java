package br.furb.locafacil.frota;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Regras da Frota. Cada método roda dentro da transação aberta pelo consumidor,
 * a mesma em que o messageId é registrado (R1).
 */
@Service
public class FrotaService {
    private static final Logger log = LoggerFactory.getLogger(FrotaService.class);

    private final JdbcTemplate jdbc;
    private final Broker broker;

    public FrotaService(JdbcTemplate jdbc, Broker broker) {
        this.jdbc = jdbc;
        this.broker = broker;
    }

    // ------------------------------------------------------------------ ReservaSolicitada
    /**
     * R2 – controle de concorrência sem venda dupla.
     * Para cada dia do período (sempre em ordem crescente, evitando deadlock):
     *   UPDATE ocupacao SET reservados = reservados + 1 WHERE ... AND reservados < capacidade
     * O UPDATE trava a linha; o Postgres reavalia o WHERE depois de obter o lock, então
     * dois pedidos simultâneos para a última unidade não passam os dois. Se algum dia falhar,
     * volta ao savepoint (desfaz os dias já incrementados) e responde VeiculoIndisponivel.
     */
    public void reservaSolicitada(JsonNode env, TransactionStatus status) {
        JsonNode p = env.get("payload");
        String reservaId = uuid(p, "reservaId");
        String filial = texto(p, "filial");
        String categoria = texto(p, "categoria");
        LocalDate inicio = data(p, "dataInicio");
        LocalDate fim = data(p, "dataFim");
        if (fim.isBefore(inicio)) throw new PoisonMessageException("dataFim antes de dataInicio");

        Integer existe = jdbc.queryForObject("SELECT count(*)::int FROM bloqueios WHERE reserva_id = ?::uuid", Integer.class, reservaId);
        if (existe != null && existe > 0) {
            // já tratada (ou cancelada antes de chegar aqui: lápide LIBERADO)
            log.info("reserva {} já tem bloqueio, ignorando", reservaId);
            return;
        }

        Object savepoint = status.createSavepoint();
        LocalDate semVaga = null;
        for (LocalDate d = inicio; !d.isAfter(fim); d = d.plusDays(1)) {
            jdbc.update("""
                    INSERT INTO ocupacao (filial, categoria, data, capacidade, reservados)
                    SELECT filial, categoria, ?, quantidade, 0 FROM frota_categoria WHERE filial = ? AND categoria = ?
                    ON CONFLICT DO NOTHING""", d, filial, categoria);
            int n = jdbc.update("""
                    UPDATE ocupacao SET reservados = reservados + 1
                     WHERE filial = ? AND categoria = ? AND data = ? AND reservados < capacidade""", filial, categoria, d);
            if (n == 0) { semVaga = d; break; }
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("reservaId", reservaId);
        if (semVaga == null) {
            status.releaseSavepoint(savepoint);
            jdbc.update("""
                    INSERT INTO bloqueios (reserva_id, filial, categoria, data_inicio, data_fim, status)
                    VALUES (?::uuid, ?, ?, ?, ?, 'BLOQUEADO')""", reservaId, filial, categoria, inicio, fim);
            out.put("filial", filial);
            out.put("categoria", categoria);
            out.put("dataInicio", inicio.toString());
            out.put("dataFim", fim.toString());
            broker.enqueue(jdbc, "VeiculoBloqueado", 1, reservaId, out);
            log.info("BLOQUEADO reserva={} {}/{} {}..{}", reservaId, filial, categoria, inicio, fim);
        } else {
            status.rollbackToSavepoint(savepoint);
            String motivo = "SEM_DISPONIBILIDADE em " + semVaga;
            jdbc.update("""
                    INSERT INTO bloqueios (reserva_id, filial, categoria, data_inicio, data_fim, status, motivo)
                    VALUES (?::uuid, ?, ?, ?, ?, 'RECUSADO', ?)""", reservaId, filial, categoria, inicio, fim, motivo);
            out.put("motivo", motivo);
            broker.enqueue(jdbc, "VeiculoIndisponivel", 1, reservaId, out);
            log.info("INDISPONIVEL reserva={} {}/{} dia {}", reservaId, filial, categoria, semVaga);
        }
    }

    // ------------------------------------------------------------------ ReservaConfirmada (v1 e v2)
    /** Leitor tolerante: só precisa de reservaId, que existe nas duas versões do evento. */
    public void reservaConfirmada(JsonNode env) {
        String reservaId = uuid(env.get("payload"), "reservaId");
        int n = jdbc.update("""
                UPDATE bloqueios SET status = 'CONFIRMADO', atualizado_em = now()
                 WHERE reserva_id = ?::uuid AND status = 'BLOQUEADO'""", reservaId);
        log.info("CONFIRMADO reserva={} (evento v{}, linhas={})", reservaId, env.get("version").asInt(), n);
    }

    // ------------------------------------------------------------------ ReservaCancelada (compensação)
    public void reservaCancelada(JsonNode env) {
        JsonNode p = env.get("payload");
        String reservaId = uuid(p, "reservaId");
        String motivo = texto(p, "motivo");
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT filial, categoria, data_inicio, data_fim, status FROM bloqueios
                 WHERE reserva_id = ?::uuid FOR UPDATE""", reservaId);

        if (rows.isEmpty()) {
            // cancelamento chegou antes do pedido (sem ordenação garantida): grava uma lápide
            // para que uma ReservaSolicitada atrasada não bloqueie veículo
            jdbc.update("""
                    INSERT INTO bloqueios (reserva_id, filial, categoria, data_inicio, data_fim, status, motivo)
                    VALUES (?::uuid, ?, ?, ?, ?, 'LIBERADO', ?)""",
                    reservaId, texto(p, "filial"), texto(p, "categoria"), data(p, "dataInicio"), data(p, "dataFim"),
                    "cancelada antes do bloqueio: " + motivo);
            log.info("LAPIDE reserva={} cancelada antes do bloqueio", reservaId);
            return;
        }
        Map<String, Object> b = rows.get(0);
        if (!"BLOQUEADO".equals(b.get("status"))) {
            log.info("reserva {} em {}, nada a liberar", reservaId, b.get("status"));
            return;
        }
        LocalDate inicio = ((java.sql.Date) b.get("data_inicio")).toLocalDate();
        LocalDate fim = ((java.sql.Date) b.get("data_fim")).toLocalDate();
        for (LocalDate d = inicio; !d.isAfter(fim); d = d.plusDays(1)) {
            jdbc.update("UPDATE ocupacao SET reservados = reservados - 1 WHERE filial = ? AND categoria = ? AND data = ?",
                    b.get("filial"), b.get("categoria"), d);
        }
        jdbc.update("UPDATE bloqueios SET status = 'LIBERADO', motivo = ?, atualizado_em = now() WHERE reserva_id = ?::uuid",
                motivo, reservaId);
        log.warn("COMPENSACAO reserva={} veículo liberado ({})", reservaId, motivo);
    }

    // ------------------------------------------------------------------ validação (contracts/events/*.schema.json)
    private static String texto(JsonNode p, String campo) {
        JsonNode v = p == null ? null : p.get(campo);
        if (v == null || !v.isTextual() || v.asText().isBlank()) throw new PoisonMessageException("campo " + campo + " inválido");
        return v.asText();
    }

    private static String uuid(JsonNode p, String campo) {
        String s = texto(p, campo);
        try {
            return UUID.fromString(s).toString();
        } catch (IllegalArgumentException e) {
            throw new PoisonMessageException("campo " + campo + " não é UUID");
        }
    }

    private static LocalDate data(JsonNode p, String campo) {
        try {
            return LocalDate.parse(texto(p, campo));
        } catch (DateTimeParseException e) {
            throw new PoisonMessageException("campo " + campo + " não é data");
        }
    }
}
