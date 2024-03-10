package com.windowforsun.kafka.retry.nonblocking.retryable.exception;

public class RetryableMessagingException extends RuntimeException {
    public RetryableMessagingException(String message) {
        super(message);
    }
}
