package com.windowforsun.kafka.retry.nonblocking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan("com.windowforsun.kafka.retry.nonblocking.demo")
@EnableJpaRepositories("com.windowforsun.kafka.retry.nonblocking.demo")
//@ComponentScan(basePackages = "com.windowforsun.kafka.retry.nonblocking")
public class DemoApplication {
    public static void main(String... args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
