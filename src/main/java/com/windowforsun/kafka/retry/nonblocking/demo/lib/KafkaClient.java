package com.windowforsun.kafka.retry.nonblocking.demo.lib;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaClient {
    private final KafkaTemplate kafkaTemplate;

    public SendResult sendMessage(final String topic, final String data) {
        return this.sendMessage(topic, data, null);
    }

    public SendResult sendMessage(final String topic, final String data, final Map<String, Object> headers) {
        try {
            final MessageBuilder builder = MessageBuilder
                    .withPayload(data)
                    .setHeader(KafkaHeaders.TOPIC, topic);

            if(headers != null) {
                headers.forEach((key, value) -> builder.setHeader(key, value));
            }

            final Message<String> message = builder.build();
            return (SendResult) this.kafkaTemplate.send(message).get();
        } catch (Exception e) {
            String message = "Error sending message to topic " + topic;
            log.error(message);
            throw new RuntimeException(message, e);
        }
    }
}
