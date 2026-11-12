package br.furb.locafacil.frota;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;

/**
 * Bônus: tracing distribuído com OpenTelemetry.
 * O SDK é configurado só por variáveis OTEL_* (endpoint, protocolo, nome do serviço).
 * Sem OTEL_EXPORTER_OTLP_ENDPOINT, usa a implementação no-op e nada muda no serviço.
 * O contexto viaja no atributo "traceparent" das mensagens SNS/SQS (o envelope não muda).
 */
@Component
public class Tracing {
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override public Iterable<String> keys(Map<String, String> c) { return c.keySet(); }
        @Override public String get(Map<String, String> c, String k) { return c == null ? null : c.get(k); }
    };

    private final OpenTelemetry otel;
    private final Tracer tracer;

    public Tracing() {
        String endpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
        this.otel = (endpoint == null || endpoint.isBlank())
                ? OpenTelemetry.noop()
                : AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();
        this.tracer = otel.getTracer("frota");
    }

    public Tracer tracer() { return tracer; }

    private TextMapPropagator propagator() { return otel.getPropagators().getTextMapPropagator(); }

    /** traceparent do contexto atual (para gravar na outbox junto do evento). */
    public String traceparentAtual() { return traceparentDe(Context.current()); }

    public String traceparentDe(Context ctx) {
        Map<String, String> carrier = new HashMap<>();
        propagator().inject(ctx, carrier, Map::put);
        return carrier.get("traceparent");
    }

    public String traceparentDe(Span span) { return traceparentDe(Context.root().with(span)); }

    public Context contextoDe(String traceparent) {
        if (traceparent == null || traceparent.isBlank()) return Context.root();
        return propagator().extract(Context.root(), Map.of("traceparent", traceparent), GETTER);
    }
}
