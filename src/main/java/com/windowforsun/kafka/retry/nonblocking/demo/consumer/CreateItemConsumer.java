package com.windowforsun.kafka.retry.nonblocking.demo.consumer;

import com.windowforsun.kafka.retry.nonblocking.demo.event.CreateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.mapper.JsonMapper;
import com.windowforsun.kafka.retry.nonblocking.demo.service.ItemService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CreateItemConsumer {
    private final ItemService itemService;

    @KafkaListener(topics = "create-item", containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Payload final String payload) {
        log.info("Create Item Consumer: Received message with payload: " + payload);

        try {
            CreateItem event = JsonMapper.readFromJson(payload, CreateItem.class);

            this.itemService.createItem(event);
        } catch (Exception e) {
            log.error("Create item - error processing message: " + e.getMessage());
        }
    }
}
