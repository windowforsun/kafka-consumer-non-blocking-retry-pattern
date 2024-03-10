package com.windowforsun.kafka.retry.nonblocking.demo.util;

import com.windowforsun.kafka.retry.nonblocking.demo.event.CreateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.event.UpdateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.service.ItemStatus;

import java.util.UUID;

public class TestEventData {

    public static CreateItem buildCreateItemEvent(UUID id, String name) {
        return CreateItem.builder()
                .id(id)
                .name(name)
                .build();
    }

    public static UpdateItem buildUpdateItemEvent(UUID id, ItemStatus status) {
        return UpdateItem.builder()
                .id(id)
                .status(status)
                .build();
    }
}
