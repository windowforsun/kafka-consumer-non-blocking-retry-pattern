package com.windowforsun.kafka.retry.nonblocking.retryable.consumer;

import com.windowforsun.kafka.retry.nonblocking.retryable.exception.RetryableMessagingException;
import com.windowforsun.kafka.retry.nonblocking.retryable.lib.RetryableHeaders;
import com.windowforsun.kafka.retry.nonblocking.retryable.service.RetryableService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryableConsumer {
    private final RetryableService retryableService;

    @KafkaListener(topics = "#{'${retry.messaging.topic}'}", containerFactory = "kafkaListenerRetryContainerFactory")
    public void listen(@Payload final String payload,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) final Long receivedTimestamp,
                       @Header(value = RetryableHeaders.ORIGINAL_RECEIVED_TIMESTAMP, required = false) final Long originalReceivedTimestamp,
                       @Header(value = RetryableHeaders.ORIGINAL_RECEIVED_TOPIC) final String originalTopic) {
        log.info("Retry Item Consumer: Received message - receivedTimestamp ["+receivedTimestamp+"] - originalReceivedTimestamp ["+originalReceivedTimestamp+"] payload: " + payload);

        try {
            this.retryableService.handle(payload, receivedTimestamp, originalReceivedTimestamp, originalTopic);
        } catch (RetryableMessagingException e) {
            log.error("Retrying ... message : {}", payload);
            throw e;
        } catch (Exception e) {
            log.error("Retry event - error processing message: " + e.getMessage());
        }
    }
}
