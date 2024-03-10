package com.windowforsun.kafka.retry.nonblocking.demo.intrgration;


import com.windowforsun.kafka.retry.nonblocking.demo.DemoConfig;
import com.windowforsun.kafka.retry.nonblocking.demo.event.CreateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.event.UpdateItem;
import com.windowforsun.kafka.retry.nonblocking.demo.lib.KafkaClient;
import com.windowforsun.kafka.retry.nonblocking.demo.mapper.JsonMapper;
import com.windowforsun.kafka.retry.nonblocking.demo.repository.ItemRepository;
import com.windowforsun.kafka.retry.nonblocking.demo.service.ItemStatus;
import com.windowforsun.kafka.retry.nonblocking.demo.util.TestEventData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

@Slf4j
//@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = {DemoConfig.class})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = {})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true, topics = {"create-item", "update-item"})
public class KafkaIntegrationTest {
    final static String CREATE_ITEM_TOPIC = "create-item";
    final static String UPDATE_ITEM_TOPIC = "update-item";

    @Autowired
    private KafkaClient kafkaClient;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private ItemRepository itemRepository;
    @Autowired
    private TestRestTemplate restTemplate;

    @BeforeEach
    public void setUp() {
        this.itemRepository.deleteAll();

        this.registry.getListenerContainers().stream().forEach(container -> ContainerTestUtils.waitForAssignment(container, this.embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    @Test
    public void testCreateAndUpdateItems() {
        int totalMessages = 10;
        Set<UUID> itemsIds = new HashSet<>();

        for (int i = 0; i < totalMessages; i++) {
            UUID itemId = UUID.randomUUID();
            CreateItem createEvent = TestEventData.buildCreateItemEvent(itemId, RandomStringUtils.randomAlphabetic(8));
            this.kafkaClient.sendMessage(CREATE_ITEM_TOPIC, JsonMapper.writeToJson(createEvent));
            itemsIds.add(itemId);
        }

        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> this.itemRepository.findAll().size() == totalMessages);
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> this.itemRepository.findAll().stream().allMatch((item) -> item.getStatus().equals(ItemStatus.NEW)));

        itemsIds.forEach(itemId -> {
            UpdateItem updateEvent = TestEventData.buildUpdateItemEvent(itemId, ItemStatus.ACTIVE);
            this.kafkaClient.sendMessage(UPDATE_ITEM_TOPIC, JsonMapper.writeToJson(updateEvent));
        });
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> this.itemRepository.findAll().stream().allMatch(item -> item.getStatus().equals(ItemStatus.ACTIVE)));
    }

    @Test
    public void testUpdatedBeforeCreate() throws Exception {
        int totalMessages = 10;
        Set<UUID> itemIds = new HashSet<>();

        for(int i = 0; i < totalMessages; i++) {
            UUID itemId = UUID.randomUUID();
            UpdateItem updateEvent = TestEventData.buildUpdateItemEvent(itemId, ItemStatus.ACTIVE);
            this.kafkaClient.sendMessage(UPDATE_ITEM_TOPIC, JsonMapper.writeToJson(updateEvent));
            itemIds.add(itemId);
        }

        TimeUnit.SECONDS.sleep(5);

        assertThat(this.itemRepository.findAll().size(), is(0));

        itemIds.forEach(itemId -> {
            CreateItem createEvent = TestEventData.buildCreateItemEvent(itemId, RandomStringUtils.randomAlphabetic(8));
            this.kafkaClient.sendMessage(CREATE_ITEM_TOPIC, JsonMapper.writeToJson(createEvent));
        });

        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> this.itemRepository.findAll().size() == totalMessages);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> this.itemRepository.findAll().stream().allMatch(item -> item.getStatus().equals(ItemStatus.ACTIVE)));
    }

    @Test
    public void testUpdateEventDiscarded() throws Exception {
        UUID itemId = UUID.randomUUID();

        UpdateItem updateEvent = TestEventData.buildUpdateItemEvent(itemId, ItemStatus.ACTIVE);
        this.kafkaClient.sendMessage(UPDATE_ITEM_TOPIC, JsonMapper.writeToJson(updateEvent));

        TimeUnit.SECONDS.sleep(11);

        CreateItem createEvent = TestEventData.buildCreateItemEvent(itemId, RandomStringUtils.randomAlphabetic(8));
        this.kafkaClient.sendMessage(CREATE_ITEM_TOPIC, JsonMapper.writeToJson(createEvent));

        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> this.itemRepository.findAll().size() == 1);

        TimeUnit.SECONDS.sleep(3);

        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> {
                    ResponseEntity<String> response = this.restTemplate.getForEntity("/v1/demo/items/" + itemId + "/status", String.class);
                    return response.getStatusCode() == HttpStatus.OK && response.getBody().equals("NEW");
                });
    }
}
