package com.sminoh.paymentservice.infra;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class PgClient {
    public boolean pay(String orderId, BigDecimal amount) {
        // 실제 PG API 호출
        return true;
    }
}
