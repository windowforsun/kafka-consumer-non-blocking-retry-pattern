package com.windowforsun.kafka.retry.nonblocking.demo.util;

import com.windowforsun.kafka.retry.nonblocking.demo.domain.Item;
import com.windowforsun.kafka.retry.nonblocking.demo.service.ItemStatus;

import java.util.UUID;

public class TestEntityData {

    public static Item buildItem(UUID id, String name) {
        return Item.builder()
                .id(id)
                .name(name)
                .status(ItemStatus.ACTIVE)
                .build();
    }
}
