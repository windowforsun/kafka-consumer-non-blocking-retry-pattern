package com.windowforsun.kafka.retry.nonblocking.demo.consumer;

import com.windowforsun.kafka.retry.nonblocking.demo.event.UpdateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.mapper.JsonMapper;
import com.windowforsun.kafka.retry.nonblocking.demo.service.ItemService;
import com.windowforsun.kafka.retry.nonblocking.demo.service.ItemStatus;
import com.windowforsun.kafka.retry.nonblocking.demo.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.MessageHeaders;

import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.*;

public class UpdateItemConsumerTest {
    private ItemService itemServiceMock;
    private UpdateItemConsumer consumer;
    private MessageHeaders headers = new MessageHeaders(Map.of("some-header", "some-value"));

    @BeforeEach
    public void setUp() {
        this.itemServiceMock = mock(ItemService.class);
        this.consumer = new UpdateItemConsumer(this.itemServiceMock);
    }

    @Test
    public void testListen_Success() {
        UpdateItem testEvent = TestEventData.buildUpdateItemEvent(UUID.randomUUID(), ItemStatus.ACTIVE);
        String payload = JsonMapper.writeToJson(testEvent);

        this.consumer.listen(payload, this.headers);

        verify(this.itemServiceMock, times(1)).updateItem(testEvent, headers);
    }

    @Test
    public void testListen_ServiceThrowsException() {
        UpdateItem testEvent = TestEventData.buildUpdateItemEvent(UUID.randomUUID(), ItemStatus.ACTIVE);
        String payload = JsonMapper.writeToJson(testEvent);

        doThrow(new RuntimeException("Service failure")).when(this.itemServiceMock).updateItem(testEvent, this.headers);

        this.consumer.listen(payload, headers);

        verify(this.itemServiceMock, times(1)).updateItem(testEvent, headers);
    }
}
