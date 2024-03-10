package com.windowforsun.kafka.retry.nonblocking.demo.service;


import com.windowforsun.kafka.retry.nonblocking.demo.domain.Item;
import com.windowforsun.kafka.retry.nonblocking.demo.event.CreateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.event.UpdateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.repository.ItemRepository;
import com.windowforsun.kafka.retry.nonblocking.demo.util.TestEntityData;
import com.windowforsun.kafka.retry.nonblocking.demo.util.TestEventData;
import com.windowforsun.kafka.retry.nonblocking.retryable.service.RetryableService;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.MessageHeaders;

import java.util.Optional;
import java.util.UUID;

import static org.mockito.Mockito.*;

public class ItemServiceTest {
    private ItemService itemService;
    private ItemRepository itemRepositoryMock;
    private RetryableService retryableServiceMock;

    @BeforeEach
    public void setUp() {
        this.itemRepositoryMock = mock(ItemRepository.class);
        this.retryableServiceMock = mock(RetryableService.class);
        this.itemService = new ItemService(this.itemRepositoryMock, this.retryableServiceMock);
    }

    @Test
    public void testCreateItem() {
        final String name = RandomStringUtils.randomAlphabetic(8);
        CreateItem testEvent = TestEventData.buildCreateItemEvent(UUID.randomUUID(), name);

        this.itemService.createItem(testEvent);

        verify(this.itemRepositoryMock, times(1)).save(argThat(s -> s.getName().equals(name)));
    }

    @Test
    public void testUpdateItem_ItemUpdated() {
        UUID itemId = UUID.randomUUID();
        Item item = TestEntityData.buildItem(itemId, "my-item");
        when(this.itemRepositoryMock.findById(itemId)).thenReturn(Optional.of(item));

        UpdateItem testEvent = TestEventData.buildUpdateItemEvent(itemId, ItemStatus.ACTIVE);

        this.itemService.updateItem(testEvent, new MessageHeaders(null));

        verify(this.itemRepositoryMock, times(1)).save(argThat(s -> s.getStatus().equals(ItemStatus.ACTIVE)));
        verifyNoInteractions(this.retryableServiceMock);
    }

    @Test
    public void testUpdateItem_ItemRetried() {
        UUID itemId = UUID.randomUUID();
        when(this.itemRepositoryMock.findById(itemId)).thenReturn(Optional.empty());

        UpdateItem testEvent = TestEventData.buildUpdateItemEvent(itemId, ItemStatus.ACTIVE);

        this.itemService.updateItem(testEvent, new MessageHeaders(null));

        verify(this.itemRepositoryMock, times(0)).save(any());
        verify(this.retryableServiceMock, times(1)).retry(any(), any());
    }
}
