package com.windowforsun.kafka.retry.nonblocking.demo.consumer;

import com.windowforsun.kafka.retry.nonblocking.demo.event.UpdateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.mapper.JsonMapper;
import com.windowforsun.kafka.retry.nonblocking.demo.service.ItemService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UpdateItemConsumer {
    private final ItemService itemService;

    @KafkaListener(topics = "update-item", containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Payload final String payload, @Headers final MessageHeaders headers) {
        log.info("Update Item Consumer: Received message with payload: " + payload);
        try {
            UpdateItem event = JsonMapper.readFromJson(payload, UpdateItem.class);
            itemService.updateItem(event, headers);
        } catch (Exception e) {
            log.error("Update item - error processing message: " + e.getMessage());
        }
    }
}
