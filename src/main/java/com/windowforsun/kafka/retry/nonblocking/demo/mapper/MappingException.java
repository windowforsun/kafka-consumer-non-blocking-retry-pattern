package com.windowforsun.kafka.retry.nonblocking.demo.mapper;

public class MappingException extends RuntimeException{
    public MappingException(Throwable cause) {
        super(cause);
    }
}
