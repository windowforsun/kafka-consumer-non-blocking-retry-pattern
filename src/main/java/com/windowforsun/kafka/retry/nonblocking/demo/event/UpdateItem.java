package com.windowforsun.kafka.retry.nonblocking.demo.event;

import com.windowforsun.kafka.retry.nonblocking.demo.service.ItemStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpdateItem {
    @NotNull
    private UUID id;
    @Valid
    private ItemStatus status;
}
