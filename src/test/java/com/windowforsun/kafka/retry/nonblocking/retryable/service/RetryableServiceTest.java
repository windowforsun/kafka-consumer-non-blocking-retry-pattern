package com.windowforsun.kafka.retry.nonblocking.retryable.service;


import com.windowforsun.kafka.retry.nonblocking.retryable.exception.RetryableMessagingException;
import com.windowforsun.kafka.retry.nonblocking.retryable.lib.RetryableHeaders;
import com.windowforsun.kafka.retry.nonblocking.retryable.lib.RetryableKafkaClient;
import com.windowforsun.kafka.retry.nonblocking.retryable.service.RetryableService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.mockito.Mockito.*;

public class RetryableServiceTest {
    private RetryableService service;
    private RetryableKafkaClient kafkaClientMock;

    @BeforeEach
    public void setUp() {
        this.kafkaClientMock = mock(RetryableKafkaClient.class);
        final Long retryIntervalSeconds = 10L;
        final Long maxRetryDurationSEconds = 30L;
        this.service = new RetryableService(this.kafkaClientMock, "retry-topic", retryIntervalSeconds, maxRetryDurationSEconds);
    }

    @Test
    public void testHandle_shouldDiscard() {
        Long receivedTimestamp = Instant.now().toEpochMilli();
        Long originalReceivedTimestamp = Instant.now().minusSeconds(31).toEpochMilli();
        this.service.handle("my-payload", receivedTimestamp, originalReceivedTimestamp, "my-topic");

        verifyNoInteractions(this.kafkaClientMock);
    }

    @Test
    public void testHandle_shouldRetry() {
        Long receivedTimestamp = Instant.now().minusSeconds(11).toEpochMilli();
        Long originalReceivedTimestamp = Instant.now().minusSeconds(29).toEpochMilli();
        this.service.handle("my-payload", receivedTimestamp, originalReceivedTimestamp, "my-topic");

        verify(this.kafkaClientMock, times(1)).sendMessage("my-topic", "my-payload", Map.of(RetryableHeaders.ORIGINAL_RECEIVED_TIMESTAMP, originalReceivedTimestamp));
    }

    @Test
    public void testHandle_shouldDelayRetry() {
        Long receivedTimestamp = Instant.now().minusSeconds(9).toEpochMilli();
        Long originalReceivedTimestamp = Instant.now().minusSeconds(29).toEpochMilli();
        Assertions.assertThrows(RetryableMessagingException.class, () -> this.service.handle("my-payload", receivedTimestamp, originalReceivedTimestamp, "my-topic"));
        verifyNoMoreInteractions(this.kafkaClientMock);
    }

}
