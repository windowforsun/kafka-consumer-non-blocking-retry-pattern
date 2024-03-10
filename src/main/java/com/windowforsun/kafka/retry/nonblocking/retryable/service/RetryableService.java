package com.windowforsun.kafka.retry.nonblocking.retryable.service;

import com.windowforsun.kafka.retry.nonblocking.retryable.exception.RetryableMessagingException;
import com.windowforsun.kafka.retry.nonblocking.retryable.lib.RetryableHeaders;
import com.windowforsun.kafka.retry.nonblocking.retryable.lib.RetryableKafkaClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

@Slf4j
@Service
//@RequiredArgsConstructor
public class RetryableService {
    private final RetryableKafkaClient retryableKafkaClient;
//    @Value("${retry.messaging.topic}")
    private String retryTopic;
//    @Value("${retry.messaging.retryIntervalSeconds}")
    private Long retryIntervalSeconds;
//    @Value("${retry.messaging.maxRetryDurationSeconds}")
    private Long maxRetryDurationSeconds;

    public RetryableService(@Autowired RetryableKafkaClient retryableKafkaClient,
                            @Value("${retry.messaging.topic}") String retryTopic,
                            @Value("${retry.messaging.retryIntervalSeconds}") Long retryIntervalSeconds,
                            @Value("${retry.messaging.maxRetryDurationSeconds}") Long maxRetryDurationSeconds) {
        this.retryableKafkaClient = retryableKafkaClient;
        this.retryTopic = retryTopic;
        this.retryIntervalSeconds = retryIntervalSeconds;
        this.maxRetryDurationSeconds = maxRetryDurationSeconds;
    }

    public void retry(final String payload, final MessageHeaders headers) {
//        final Long verifiedOriginalReceivedTimestamp = headers.get(RetryableHeaders.ORIGINAL_RECEIVED_TIMESTAMP) != null ? ()
        final Long verifiedOriginalReceivedTimestamp = (Long) headers.getOrDefault(RetryableHeaders.ORIGINAL_RECEIVED_TIMESTAMP, (Long) headers.get(KafkaHeaders.RECEIVED_TIMESTAMP));
        this.retryableKafkaClient.sendMessage(retryTopic, payload,
                Map.of(RetryableHeaders.ORIGINAL_RECEIVED_TIMESTAMP, verifiedOriginalReceivedTimestamp,
                        RetryableHeaders.ORIGINAL_RECEIVED_TOPIC, headers.get(KafkaHeaders.RECEIVED_TOPIC)));
    }

    public void handle(final String payload, final Long receivedTimestamp, final Long originalReceivedTimestamp, final String originalTopic) {
        if(this.shouldDiscard(originalReceivedTimestamp)) {
            log.debug("Item {} has exceeded total retry duration - item discarded.", payload);
        } else if(this.shouldRetry(receivedTimestamp)) {
            log.debug("Item {} is ready to retry - sending to update-item topic.", payload);
            this.retryableKafkaClient.sendMessage(originalTopic, payload, Map.of(RetryableHeaders.ORIGINAL_RECEIVED_TIMESTAMP, originalReceivedTimestamp));
        } else {
            log.debug("Item {} is not yet ready to retry on the update-item topic - delaying.", payload);
            throw new RetryableMessagingException("Delaying attempt to retry item " + payload);
        }
    }

    private boolean shouldDiscard(final Long originalReceivedTimestamp) {
        long cutOffTime = originalReceivedTimestamp + (this.maxRetryDurationSeconds * 1000);
        return Instant.now().toEpochMilli() > cutOffTime;
    }

    private boolean shouldRetry(final Long receivedTimestamp) {
        long timeForNextRetry = receivedTimestamp + (this.retryIntervalSeconds * 1000);
        log.debug("retryIntervalSeconds: {} - receivedTimestamp: {} - timeForNextRetry: {} - now: {} - (now > timeForNextRetry): {}", retryIntervalSeconds, receivedTimestamp, timeForNextRetry, Instant.now().toEpochMilli(), Instant.now().toEpochMilli() > timeForNextRetry);
        return Instant.now().toEpochMilli() > timeForNextRetry;
    }
}
