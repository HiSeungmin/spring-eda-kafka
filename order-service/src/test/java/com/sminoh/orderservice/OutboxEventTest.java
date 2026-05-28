package com.sminoh.orderservice;


import com.sminoh.orderservice.outbox.OutboxEvent;
import com.sminoh.orderservice.outbox.OutboxStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class OutboxEventTest {
    private OutboxEvent createEvent() {
        return OutboxEvent.create(
                "event-1", "Order", "order-1",
                "OrderCreated", "order.created", "{}"
        );
    }


    @Test
    @DisplayName("이벤트 생성 시 상태는 INIT, retryCount는 0")
    void create(){
        OutboxEvent event = createEvent();

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.INIT);
        assertThat(event.getRetryCount()).isEqualTo(0);
        assertThat(event.getSentAt()).isNull();
    }

    @Test
    @DisplayName("이벤트 발행 성공 시 SEND_SUCCESS 상태로 변경되고 sentAt 기록")
    void markAsSuccess(){
        OutboxEvent event = createEvent();

        event.markAsSuccess();

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.SEND_SUCCESS);
        assertThat(event.getSentAt()).isNotNull();
    }

    @Nested
    @DisplayName("이벤트 발행 실패 시")
    class MarkAsFail {

        @Test
        @DisplayName("retryCount가 증가하고 SEND_FAIL 상태가 된다")
        void increaseRetryCount(){
            OutboxEvent event = createEvent();

            event.markAsFail("Kafka timeout");

            assertThat(event.getRetryCount()).isEqualTo(1);
            assertThat(event.getStatus()).isEqualTo(OutboxStatus.SEND_FAIL);
            assertThat(event.getLastError()).isEqualTo("Kafka timeout");
        }

        @Test
        @DisplayName("최대 재시도 횟수(5회)에 도달하면 DEAD 상태가 된다")
        void reachMaxRetry() {
            OutboxEvent event = createEvent();

            for(int i = 0; i<5; i++){
                event.markAsFail("repeated failure");
            }

            assertThat(event.getRetryCount()).isEqualTo(5);
            assertThat(event.getStatus()).isEqualTo(OutboxStatus.DEAD);
        }

        @Test
        @DisplayName("최대 재시도 직전(4회)까지는 SEND_FAIL 유지")
        void beforeMaxRetry() {
            OutboxEvent event = createEvent();

            for (int i = 0; i < 4; i++) {
                event.markAsFail("failure");
            }

            assertThat(event.getRetryCount()).isEqualTo(4);
            assertThat(event.getStatus()).isEqualTo(OutboxStatus.SEND_FAIL);
        }
    }
}
