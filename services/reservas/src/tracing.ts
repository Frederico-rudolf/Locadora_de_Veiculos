// Bônus: tracing distribuído com OpenTelemetry.
// Carregado ANTES do resto do serviço (node --require ./dist/tracing.js) para instrumentar express/http/pg.
// Configuração só por ambiente: OTEL_EXPORTER_OTLP_ENDPOINT (ex.: http://jaeger:4318) e OTEL_SERVICE_NAME.
// Sem endpoint, nada é exportado e o serviço funciona igual.
import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { HttpInstrumentation } from "@opentelemetry/instrumentation-http";
import { ExpressInstrumentation } from "@opentelemetry/instrumentation-express";
import { PgInstrumentation } from "@opentelemetry/instrumentation-pg";

if (process.env.OTEL_EXPORTER_OTLP_ENDPOINT && process.env.OTEL_SDK_DISABLED !== "true") {
  const sdk = new NodeSDK({
    serviceName: process.env.OTEL_SERVICE_NAME ?? process.env.SERVICE_NAME,
    traceExporter: new OTLPTraceExporter(),
    instrumentations: [
      new HttpInstrumentation({
        ignoreIncomingRequestHook: (req) => req.url === "/health",
        // chamadas do SDK AWS (long polling a cada 10 s) gerariam um trace "solto" por chamada;
        // a mensageria é rastreada pelos spans PRODUCER/CONSUMER criados em infra.ts
        ignoreOutgoingRequestHook: () => true,
      }),
      new ExpressInstrumentation(),
      // só registra SQL que acontece dentro de uma operação rastreada (evita ruído dos laços de polling)
      new PgInstrumentation({ requireParentSpan: true }),
    ],
  });
  sdk.start();
  process.on("SIGTERM", () => sdk.shutdown().finally(() => process.exit(0)));
}
