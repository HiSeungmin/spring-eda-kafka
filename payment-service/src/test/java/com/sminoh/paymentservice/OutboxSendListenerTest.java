package com.sminoh.paymentservice;

import com.sminoh.paymentservice.event.internal.PaymentCompletedAppEvent;
import com.sminoh.paymentservice.event.internal.PaymentFailedAppEvent;
import com.sminoh.paymentservice.event.published.PaymentCompletedEvent;
import com.sminoh.paymentservice.event.published.PaymentFailedEvent;
import com.sminoh.paymentservice.outbox.OutboxEvent;
import com.sminoh.paymentservice.outbox.OutboxEventRepository;
import com.sminoh.paymentservice.outbox.OutboxSendListener;
import com.sminoh.paymentservice.outbox.OutboxStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("OutboxSendListener 단위 테스트")
public class OutboxSendListenerTest {
    @Mock
    private KafkaTemplate<String, String> stringKafkaTemplate;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    private OutboxSendListener listener;

    @BeforeEach
    void setUp() {
        listener = new OutboxSendListener(stringKafkaTemplate, outboxEventRepository, new ObjectMapper());
    }


    @Nested
    @DisplayName("PaymentCompletedAppEvent 처리")
    class SendCompleted {

        @Test
        @DisplayName("payment.completed 토픽으로 발행하고 outbox를 SEND_SUCCESS로 변경한다")
        void sendCompleted_success() {
            // given
            PaymentCompletedEvent payload = new PaymentCompletedEvent(
                    "payment-1", "order-1", "user-1",
                    new BigDecimal("15000"), LocalDateTime.now()
            );
            PaymentCompletedAppEvent appEvent = new PaymentCompletedAppEvent(payload);

            OutboxEvent outbox = OutboxEvent.create(
                    payload.getEventId(), "Payment", "order-1",
                    "PaymentCompleted", "payment.completed", "{}"
            );
            when(outboxEventRepository.findByEventId(payload.getEventId()))
                    .thenReturn(Optional.of(outbox));

            // when
            listener.sendCompleted(appEvent);

            // then
            verify(stringKafkaTemplate).send(eq("payment.completed"), eq("order-1"), anyString());
            assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.SEND_SUCCESS);
        }

        @Test
        @DisplayName("발행 실패 시 outbox 상태를 변경하지 않는다 (배치가 재발행)")
        void sendCompleted_fail() {
            // given
            PaymentCompletedEvent payload = new PaymentCompletedEvent(
                    "payment-1", "order-1", "user-1",
                    new BigDecimal("15000"), LocalDateTime.now()
            );
            PaymentCompletedAppEvent appEvent = new PaymentCompletedAppEvent(payload);

            when(stringKafkaTemplate.send(anyString(), anyString(), anyString()))
                    .thenThrow(new RuntimeException("kafka down"));

            // when
            listener.sendCompleted(appEvent);

            // then
            verify(outboxEventRepository, never()).findByEventId(anyString());
        }
    }

    @Nested
    @DisplayName("PaymentFailedAppEvent 처리")
    class SendFailed {

        @Test
        @DisplayName("payment.failed 토픽으로 발행하고 outbox를 SEND_SUCCESS로 변경한다")
        void sendFailed_success() {
            // given
            PaymentFailedEvent payload = new PaymentFailedEvent(
                    "order-1", "user-1", "PG사 결제 실패"
            );
            PaymentFailedAppEvent appEvent = new PaymentFailedAppEvent(payload);

            OutboxEvent outbox = OutboxEvent.create(
                    payload.getEventId(), "Payment", "order-1",
                    "PaymentFailed", "payment.failed", "{}"
            );
            when(outboxEventRepository.findByEventId(payload.getEventId()))
                    .thenReturn(Optional.of(outbox));

            // when
            listener.sendFailed(appEvent);

            // then
            verify(stringKafkaTemplate).send(eq("payment.failed"), eq("order-1"), anyString());
            assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.SEND_SUCCESS);
        }
    }
}
