package br.furb.locafacil.frota;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FrotaApplication {
    public static void main(String[] args) {
        SpringApplication.run(FrotaApplication.class, args);
    }
}
