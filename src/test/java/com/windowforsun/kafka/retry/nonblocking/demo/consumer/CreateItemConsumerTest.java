package com.windowforsun.kafka.retry.nonblocking.demo.consumer;

import com.windowforsun.kafka.retry.nonblocking.demo.event.CreateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.mapper.JsonMapper;
import com.windowforsun.kafka.retry.nonblocking.demo.service.ItemService;
import com.windowforsun.kafka.retry.nonblocking.demo.util.TestEventData;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;

public class CreateItemConsumerTest {
    private ItemService itemServiceMock;
    private CreateItemConsumer consumer;

    @BeforeEach
    public void setUp() {
        this.itemServiceMock = mock(ItemService.class);
        this.consumer = new CreateItemConsumer(this.itemServiceMock);
    }

    @Test
    public void testListen_Success() {
        CreateItem testEvent = TestEventData.buildCreateItemEvent(UUID.randomUUID(), RandomStringUtils.randomAlphabetic(8));
        String payload = JsonMapper.writeToJson(testEvent);

        this.consumer.listen(payload);

        verify(this.itemServiceMock, times(1)).createItem(testEvent);
    }

    @Test
    public void testListen_ServiceThrowsException() {
        CreateItem testEvent = TestEventData.buildCreateItemEvent(UUID.randomUUID(), RandomStringUtils.randomAlphabetic(8));
        String payload = JsonMapper.writeToJson(testEvent);

        doThrow(new RuntimeException("Service failure")).when(this.itemServiceMock).createItem(testEvent);

        this.consumer.listen(payload);

        verify(this.itemServiceMock, times(1)).createItem(testEvent);
    }
}
