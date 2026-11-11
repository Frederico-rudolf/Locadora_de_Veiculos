package br.furb.locafacil.frota;

import java.net.URI;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

/** Endpoint, região e credenciais vêm do ambiente: o mesmo código roda no MiniStack e na AWS. */
@Configuration
public class AwsConfig {

    @Value("${locafacil.aws.region}") String region;
    @Value("${locafacil.aws.endpoint}") String endpoint;

    private ClientOverrideConfiguration timeouts() {
        // timeouts curtos: com o broker fora do ar, a chamada falha logo e o laço tenta de novo (R6)
        return ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofSeconds(25))
                .apiCallAttemptTimeout(Duration.ofSeconds(22))
                .build();
    }

    @Bean
    SqsClient sqsClient() {
        var b = SqsClient.builder().region(Region.of(region)).overrideConfiguration(timeouts());
        if (endpoint != null && !endpoint.isBlank()) b.endpointOverride(URI.create(endpoint));
        return b.build();
    }

    @Bean
    SnsClient snsClient() {
        var b = SnsClient.builder().region(Region.of(region)).overrideConfiguration(timeouts());
        if (endpoint != null && !endpoint.isBlank()) b.endpointOverride(URI.create(endpoint));
        return b.build();
    }
}
