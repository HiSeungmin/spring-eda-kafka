package com.sminoh.paymentservice;

import com.sminoh.paymentservice.event.internal.PaymentCompletedAppEvent;
import com.sminoh.paymentservice.event.internal.PaymentFailedAppEvent;
import com.sminoh.paymentservice.event.published.PaymentCompletedEvent;
import com.sminoh.paymentservice.event.published.PaymentFailedEvent;
import com.sminoh.paymentservice.outbox.OutboxEvent;
import com.sminoh.paymentservice.outbox.OutboxEventRepository;
import com.sminoh.paymentservice.outbox.OutboxRecordListener;
import com.sminoh.paymentservice.outbox.OutboxStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("OutboxRecordListener 단위 테스트")
public class OutboxRecordListenerTest {
    @Mock
    private OutboxEventRepository outboxEventRepository;

    private OutboxRecordListener listener;

    @BeforeEach
    void setUp() {
        // ObjectMapper는 실제 객체로 사용 (직렬화 동작 검증)
        listener = new OutboxRecordListener(outboxEventRepository, new ObjectMapper());
    }

    @Test
    @DisplayName("Outbox에 PaymentCompleted 타입과 payment.completed 토픽으로 기록된다")
    void recordsCompletedEvent() {
        // given
        PaymentCompletedEvent payload = new PaymentCompletedEvent(
                "payment-1", "order-1", "user-1",
                new BigDecimal("15000"), LocalDateTime.now()
        );
        PaymentCompletedAppEvent appEvent = new PaymentCompletedAppEvent(payload);

        // when
        listener.recordCompleted(appEvent);

        // then
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());

        OutboxEvent saved = captor.getValue();
        assertThat(saved.getEventId()).isEqualTo(payload.getEventId());
        assertThat(saved.getAggregateType()).isEqualTo("Payment");
        assertThat(saved.getAggregateId()).isEqualTo("order-1");
        assertThat(saved.getType()).isEqualTo("PaymentCompleted");
        assertThat(saved.getTopic()).isEqualTo("payment.completed");
        assertThat(saved.getStatus()).isEqualTo(OutboxStatus.INIT);
        assertThat(saved.getPayload()).contains("\"orderId\":\"order-1\"");
        assertThat(saved.getPayload()).contains("\"amount\":15000");
    }

    @Test
    @DisplayName("Outbox에 PaymentFailed 타입과 payment.failed 토픽으로 기록된다")
    void recordsFailedEvent() {
        // given
        PaymentFailedEvent payload = new PaymentFailedEvent(
                "order-1", "user-1", "PG사 결제 실패"
        );
        PaymentFailedAppEvent appEvent = new PaymentFailedAppEvent(payload);

        // when
        listener.recordFailed(appEvent);

        // then
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());

        OutboxEvent saved = captor.getValue();
        assertThat(saved.getEventId()).isEqualTo(payload.getEventId());
        assertThat(saved.getAggregateType()).isEqualTo("Payment");
        assertThat(saved.getAggregateId()).isEqualTo("order-1");
        assertThat(saved.getType()).isEqualTo("PaymentFailed");
        assertThat(saved.getTopic()).isEqualTo("payment.failed");
        assertThat(saved.getStatus()).isEqualTo(OutboxStatus.INIT);
        assertThat(saved.getPayload()).contains("\"reason\":\"PG사 결제 실패\"");
    }
}

