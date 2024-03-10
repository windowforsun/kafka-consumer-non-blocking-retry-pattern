package com.windowforsun.kafka.retry.nonblocking.demo.service;

import com.windowforsun.kafka.retry.nonblocking.demo.domain.Item;
import com.windowforsun.kafka.retry.nonblocking.demo.event.CreateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.event.UpdateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.mapper.JsonMapper;
import com.windowforsun.kafka.retry.nonblocking.demo.repository.ItemRepository;
import com.windowforsun.kafka.retry.nonblocking.retryable.service.RetryableService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ItemService {
    private final ItemRepository itemRepository;
    private final RetryableService retryableService;

    public void createItem(final CreateItem event) {
        Item item = Item.builder()
                .id(event.getId())
                .name(event.getName())
                .status(ItemStatus.NEW)
                .build();
        this.itemRepository.save(item);
        log.debug("Item persisted to database with Id: {}", event.getId());
    }

    public void updateItem(final UpdateItem event, final MessageHeaders headers) {
        final Optional<Item> item = this.itemRepository.findById(event.getId());

        if(item.isPresent()) {
            item.get().setStatus(event.getStatus());
            this.itemRepository.save(item.get());
            log.debug("Item updated in database with Id: {}", event.getId());
        } else {
            this.retryableService.retry(JsonMapper.writeToJson(event), headers);
            log.debug("Item sent to retry with Id: {}", event.getId());
        }
    }
}
