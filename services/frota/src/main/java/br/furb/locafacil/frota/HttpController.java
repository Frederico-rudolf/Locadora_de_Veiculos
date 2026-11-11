package br.furb.locafacil.frota;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HttpController {
    private final JdbcTemplate jdbc;
    private final Broker broker;

    public HttpController(JdbcTemplate jdbc, Broker broker) {
        this.jdbc = jdbc;
        this.broker = broker;
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        try {
            Integer pend = jdbc.queryForObject("SELECT count(*)::int FROM outbox WHERE publicado_em IS NULL", Integer.class);
            return ResponseEntity.ok(Map.of("status", "UP", "servico", "frota", "outboxPendente", pend,
                    "brokerUltimoSucesso", broker.ultimoSucesso, "brokerUltimoErro", broker.ultimoErro));
        } catch (Exception e) {
            return ResponseEntity.status(503).body(Map.of("status", "DOWN", "erro", e.toString()));
        }
    }

    /** Ocupação por dia: usado na demonstração do F5 (reservados nunca passa da capacidade). */
    @GetMapping("/ocupacao")
    public List<Map<String, Object>> ocupacao(@RequestParam String filial, @RequestParam String categoria) {
        return jdbc.queryForList("""
                SELECT to_char(data,'YYYY-MM-DD') AS data, capacidade, reservados
                  FROM ocupacao WHERE filial = ? AND categoria = ? ORDER BY data""", filial, categoria);
    }

    @GetMapping("/bloqueios/resumo")
    public List<Map<String, Object>> resumo() {
        return jdbc.queryForList("SELECT status, count(*)::int AS total FROM bloqueios GROUP BY status ORDER BY status");
    }
}
