package com.windowforsun.kafka.retry.nonblocking.retryable.consumer;

import com.windowforsun.kafka.retry.nonblocking.retryable.exception.RetryableMessagingException;
import com.windowforsun.kafka.retry.nonblocking.retryable.service.RetryableService;
import com.windowforsun.kafka.retry.nonblocking.retryable.util.TestEventData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class RetryConsumerTest {
    private RetryableService retryableServiceMock;
    private RetryableConsumer consumer;

    @BeforeEach
    public void setUp() {
        this.retryableServiceMock = mock(RetryableService.class);
        this.consumer = new RetryableConsumer(this.retryableServiceMock);
    }

    @Test
    public void testListen_Success() {
        String payload = TestEventData.buildEvent();

        this.consumer.listen(payload, 1L, 1L, "topic");

        verify(this.retryableServiceMock, times(1)).handle(payload, 1L, 1L, "topic");
    }

    @Test
    public void testListen_ServiceThrowsException() {
        String payload = TestEventData.buildEvent();

        doThrow(new RuntimeException("Service failure")).when(this.retryableServiceMock).handle(payload, 1L, 1L, "topic");

        this.consumer.listen(payload, 1L, 1L, "topic");

        verify(this.retryableServiceMock, times(1)).handle(payload, 1L, 1L, "topic");
    }

    @Test
    public void testListen_ServiceThrowsRetryableMessagingException() {
        String payload = TestEventData.buildEvent();

        doThrow(new RetryableMessagingException("Transient error")).when(this.retryableServiceMock).handle(payload, 1L, 1L, "topic");

        Exception exception = Assertions.assertThrows(RetryableMessagingException.class, () -> this.consumer.listen(payload, 1L, 1L, "topic"));
        assertThat(exception.getMessage(), is("Transient error"));
        verify(this.retryableServiceMock, times(1)).handle(payload, 1L, 1L, "topic");
    }
}
