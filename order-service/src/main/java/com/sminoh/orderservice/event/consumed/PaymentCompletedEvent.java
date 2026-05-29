package com.sminoh.orderservice.event.consumed;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PaymentCompletedEvent {
    private String paymentId;
    private String orderId;
    private String userId;
    private BigDecimal amount;
    private LocalDateTime paidAt;
}
