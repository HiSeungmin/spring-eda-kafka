package com.sminoh.paymentservice.outbox;

import com.sminoh.paymentservice.outbox.OutboxEvent;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelayScheduler {
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedDelay = 60_000)
    @Transactional
    public void relayUnpublishedEvent() {
        LocalDateTime threshold = LocalDateTime.now().minusMinutes(1);
        List<OutboxEvent> targets = outboxEventRepository.findRetryableBefore(threshold);

        if (targets.isEmpty()) {
            return;
        }

        log.info("action=OUTBOX_RELAY_START count={}", targets.size());

        for (OutboxEvent event : targets) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload());
                event.markAsSuccess();
                log.info("event=RELAY_PUBLISH topic={} eventId={} retryCount={}",
                        event.getTopic(), event.getEventId(), event.getRetryCount());

            } catch (Exception e) {
                event.markAsFail(e.getMessage());
                log.error("event=RELAY_FAIL eventId={} retryCount={} reason={}",
                        event.getEventId(), event.getRetryCount(), e.getMessage());
            }
        }
    }
}
