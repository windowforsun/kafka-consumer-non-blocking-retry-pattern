package com.windowforsun.kafka.retry.nonblocking.demo.repository;

import com.windowforsun.kafka.retry.nonblocking.demo.domain.Item;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface ItemRepository extends JpaRepository<Item, UUID> {
}
